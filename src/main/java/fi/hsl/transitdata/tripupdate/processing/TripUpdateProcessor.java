package fi.hsl.transitdata.tripupdate.processing;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.transitdata.tripupdate.gtfsrt.GtfsRtFactory;
import fi.hsl.transitdata.tripupdate.gtfsrt.GtfsRtValidator;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.shade.org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.google.transit.realtime.GtfsRealtime.TripUpdate.*;
import static com.google.transit.realtime.GtfsRealtime.*;

public class TripUpdateProcessor {
    private static final Logger log = LoggerFactory.getLogger(TripUpdateProcessor.class);

    private static final Duration CACHE_DURATION = Duration.of(4, ChronoUnit.HOURS);

    //for each trip (identified by tripId-String) store one estimate/event (StopTimeUpdate) for each stop (identified by stopSequence-Integer)
    private final LoadingCache<String, Map<Integer, StopTimeUpdate>> stopTimeUpdateCache;
    //for each trip (identified by tripId-String) store the full TripUpdate containing all StopTimeUpdates
    private final Cache<String, TripUpdate> tripUpdateCache;
    //for each trip (identified by tripId-String), keep track of whether the trip is included in static schedule (so that correct schedule relationship can be restored in case of cancellation-of-cancellation)
    private final Cache<String, TripDescriptor.ScheduleRelationship> scheduleRelationshipCache;

    //There can be multiple cancellations for each trip. We need to keep track of them to find out whether there is an active cancellation
    private final Cache<String, Map<Long, Map<InternalMessages.TripCancellation.Status, InternalMessages.TripCancellation>>> cancellationsCache;

    public TripUpdateProcessor(Producer<byte[]> producer) {
        this.tripUpdateCache = Caffeine.newBuilder()
                .expireAfterAccess(CACHE_DURATION)
                .build();

        this.stopTimeUpdateCache = Caffeine.newBuilder()
                .expireAfterAccess(CACHE_DURATION)
                .build(key -> {
                    //TreeMap keeps its entries sorted according to the natural ordering of its keys.
                    return new TreeMap<>();
                });

        this.scheduleRelationshipCache = Caffeine.newBuilder()
                .expireAfterWrite(CACHE_DURATION)
                .build();

        this.cancellationsCache = Caffeine.newBuilder()
                .expireAfterAccess(CACHE_DURATION)
                .build(key -> new HashMap<>());
    }

    public Optional<TripUpdate> processStopEstimate(InternalMessages.StopEstimate stopEstimate) {
        try {
            final StopTimeUpdate latest = updateStopTimeUpdateCache(stopEstimate);
            final String tripKey = cacheKey(stopEstimate);
            List<StopTimeUpdate> stopTimeUpdates = getStopTimeUpdates(tripKey);

            // We need to clean up the "raw data" StopTimeUpdates for any inconsistencies
            List<StopTimeUpdate> validated = GtfsRtValidator.cleanStopTimeUpdates(stopTimeUpdates, latest);

            TripUpdate tripUpdate = updateTripUpdateCacheWithStopTimes(stopEstimate, validated);
            if (tripUpdate.getTrip().getScheduleRelationship() == TripDescriptor.ScheduleRelationship.SCHEDULED
                    || tripUpdate.getTrip().getScheduleRelationship() == TripDescriptor.ScheduleRelationship.ADDED) {
                //Save schedule relationship to cache to restore it in case of cancellation-of-cancellation
                scheduleRelationshipCache.put(tripKey, tripUpdate.getTrip().getScheduleRelationship());

                //We want to act only if the status is still scheduled, let's not send estimates on cancelled trips.
                return Optional.of(tripUpdate);
            }
            else {
                log.debug("Discarding cancelled stop estimate");
                return Optional.empty();
            }

        } catch (Exception e) {
            log.error("Exception while translating StopEstimate into TripUpdate", e);
            return Optional.empty();
        }

    }

    public TripUpdate processTripCancellation(final String messageKey, long messageTimestamp, InternalMessages.TripCancellation tripCancellation) {
        return updateTripUpdateCacheWithCancellation(messageKey, messageTimestamp, tripCancellation);
    }

    private String cacheKey(final InternalMessages.StopEstimate stopEstimate) {
        return stopEstimate.getTripInfo().getTripId();
    }

    StopTimeUpdate updateStopTimeUpdateCache(final InternalMessages.StopEstimate stopEstimate) {
        // TODO refactor, now this method has dual responsibility: create new StopTimeUpdate and update cache.
        // reason for duplicate role is that we're using the cached entry to create the new one.
        // TODO think if we can separate these into two methods.

        final String tripKey = cacheKey(stopEstimate);
        Map<Integer, StopTimeUpdate> stopTimeUpdatesForThisTripUpdate = getStopTimeUpdatesWithStopSequences(tripKey);

        //StopSeq is the key since it's unique within one journey (running number).
        //There can be duplicate StopIds within journey, in case the same stop is used twice in one route (rare but possible)
        final int innerMapCacheKey = stopEstimate.getStopSequence();
        StopTimeUpdate previous = stopTimeUpdatesForThisTripUpdate.get(innerMapCacheKey);

        StopTimeUpdate latest = GtfsRtFactory.newStopTimeUpdateFromPrevious(stopEstimate, previous);
        stopTimeUpdatesForThisTripUpdate.put(innerMapCacheKey, latest);
        return latest;
    }

    Map<Integer, StopTimeUpdate> getStopTimeUpdatesWithStopSequences(String key) {
        return stopTimeUpdateCache.get(key);
    }

    LinkedList<StopTimeUpdate> getStopTimeUpdates(String key) {
        // Gtfs-rt standard requires the updates be sorted by stop seq but we already have this because we use TreeMap.
        Collection<StopTimeUpdate> updates = getStopTimeUpdatesWithStopSequences(key).values();
        return new LinkedList<>(updates);
    }

    private TripUpdate updateTripUpdateCacheWithStopTimes(final InternalMessages.StopEstimate latest, Collection<StopTimeUpdate> stopTimeUpdates) {
        final String tuCacheKey = cacheKey(latest);

        TripUpdate previousTripUpdate = tripUpdateCache.getIfPresent(tuCacheKey);
        if (previousTripUpdate == null) {
            previousTripUpdate = GtfsRtFactory.newTripUpdate(latest);
        }
        final long timestamp = GtfsRtFactory.lastModified(latest);

        TripUpdate tripUpdate = previousTripUpdate.toBuilder()
                .clearStopTimeUpdate()
                .addAllStopTimeUpdate(stopTimeUpdates)
                .setTimestamp(timestamp)
                .build();
        
        if (StringUtils.isNotBlank(latest.getTargetedStopId())
                && !latest.getTargetedStopId().equals(latest.getStopId())) {
            tripUpdate = processTargetedStopIds(tripUpdate, latest);
        }
        
        tripUpdateCache.put(tuCacheKey, tripUpdate);

        return tripUpdate;
    }
    
    private TripUpdate processTargetedStopIds(TripUpdate tripUpdate, InternalMessages.StopEstimate stopEstimate) {
        log.info("TargetedStopId has changed. TimetabledStopId={}, TargetedStopId={}, RouteId={}, DirectionId={}, Type={}, OperationDay={}, StartTime={}",
                stopEstimate.getStopId(), stopEstimate.getTargetedStopId(), stopEstimate.getTripInfo().getRouteId(),
                stopEstimate.getTripInfo().getDirectionId(), stopEstimate.getType(), stopEstimate.getTripInfo().getOperatingDay(),
                stopEstimate.getTripInfo().getStartTime());
        
        StopTimeEvent stopTimeEvent = GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder()
                .setDelay(0)
                .setTime(stopEstimate.getEstimatedTimeUtcMs())
                .setUncertainty(0)
                .build();
        
        StopTimeProperties stopTimeProperties = GtfsRealtime.TripUpdate.StopTimeProperties.newBuilder()
                .setAssignedStopId(stopEstimate.getTargetedStopId())
                .build();
        
        StopTimeUpdate.Builder stopTimeUpdate = GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder()
                .setStopSequence(stopEstimate.getStopSequence())
                .setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SCHEDULED)
                .setStopTimeProperties(stopTimeProperties);
        
        if (stopEstimate.getType() == InternalMessages.StopEstimate.Type.ARRIVAL) {
            stopTimeUpdate = stopTimeUpdate.setArrival(stopTimeEvent);
        } else if (stopEstimate.getType() == InternalMessages.StopEstimate.Type.DEPARTURE) {
            stopTimeUpdate = stopTimeUpdate.setDeparture(stopTimeEvent);
        } else {
            log.warn("Unknown stop estimate type: {}", stopEstimate.getType());
        }
        
        int stopTimeUpdateToBeRemovedIndex = getStopTimeUpdateToBeRemovedIndex(tripUpdate, stopEstimate.getStopId());
        log.info("TargetStopId has changed. Remove stop time update with stopId {}. Add stop time update with assignedStopId {}",
                stopEstimate.getStopId(), stopTimeUpdate.getStopTimeProperties().getAssignedStopId());
        
        return tripUpdate.toBuilder()
                .removeStopTimeUpdate(stopTimeUpdateToBeRemovedIndex)
                .addStopTimeUpdate(stopTimeUpdate)
                .build();
    }
    
    private int getStopTimeUpdateToBeRemovedIndex(TripUpdate tripUpdate, String stopId) {
        int indexToBeRemoved = -1;
        
        for (int i=0; i<tripUpdate.getStopTimeUpdateList().size(); i++) {
            if (stopId.equals(tripUpdate.getStopTimeUpdateList().get(i).getStopId())) {
                indexToBeRemoved = i;
                break;
            }
        }
        
        if (indexToBeRemoved < 0) {
            log.warn("No stop time update index to be removed found for stopId {}", stopId);
        }
        
        return indexToBeRemoved;
    }
    
    private TripUpdate updateTripUpdateCacheWithCancellation(final String cacheKey,
                                                             final long messageTimestampMs,
                                                             InternalMessages.TripCancellation cancellation) {
        cancellationsCache.get(cacheKey, k -> new HashMap<>()).compute(cancellation.getDeviationCaseId(), (deviationCaseId, tripCancellations) -> {
            if (tripCancellations == null) {
                tripCancellations = new HashMap<>();
            }

            tripCancellations.put(cancellation.getStatus(), cancellation);

            return tripCancellations;
        });
        final Map<Long, Map<InternalMessages.TripCancellation.Status, InternalMessages.TripCancellation>> cancellations = cancellationsCache.getIfPresent(cacheKey);
        
        boolean isCancelled = cancellation.getStatus() == InternalMessages.TripCancellation.Status.CANCELED ? true : false;
        
        if (cancellation.getDeviationCaseId() > 0) {
            isCancelled = cancellations.values().stream().anyMatch(cancellationsForDeviationCase -> {
                final Set<InternalMessages.TripCancellation.Status> statuses = cancellationsForDeviationCase.keySet();
                return statuses.stream().filter(status -> status == InternalMessages.TripCancellation.Status.CANCELED).count() > statuses.stream().filter(status -> status != InternalMessages.TripCancellation.Status.CANCELED).count();
            });
        }

        TripUpdate previousTripUpdate = tripUpdateCache.getIfPresent(cacheKey);
        if (previousTripUpdate == null) {
            previousTripUpdate = GtfsRtFactory.newTripUpdate(cancellation, messageTimestampMs);
        }

        //Assume that trip is scheduled if its schedule relationship is not found from the cache
        final GtfsRealtime.TripDescriptor.ScheduleRelationship status = isCancelled ? GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED : Optional.ofNullable(scheduleRelationshipCache.getIfPresent(cacheKey)).orElse(TripDescriptor.ScheduleRelationship.SCHEDULED);

        TripDescriptor tripDescriptor = previousTripUpdate.getTrip().toBuilder()
                .setScheduleRelationship(status)
                .build();

        TripUpdate.Builder builder = previousTripUpdate.toBuilder()
                .setTrip(tripDescriptor)
                .setTimestamp(TimeUnit.SECONDS.convert(messageTimestampMs, TimeUnit.MILLISECONDS))
                .clearStopTimeUpdate();


        if (status == TripDescriptor.ScheduleRelationship.SCHEDULED || status == TripDescriptor.ScheduleRelationship.ADDED) {
            // We need to re-attach all the StopTimeUpdates to the payload

            List<StopTimeUpdate> stopTimeUpdates = getStopTimeUpdates(cacheKey);
            // We need to clean up the "raw data" StopTimeUpdates for any inconsistencies
            List<StopTimeUpdate> validated = GtfsRtValidator.cleanStopTimeUpdates(stopTimeUpdates, null);
            if (validated.isEmpty()) {
                // This is probably cancellation of cancellation (CANCELED -> SCHEDULED/ADDED) as no stop time updates were available
                // Gtfs-rt standard requires SCHEDULED (OR ADDED) trip update to contain at least one stop time update, thus let's add one
                GtfsRealtime.TripUpdate.StopTimeUpdate.Builder stopTimeUpdateBuilder = GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder();
                stopTimeUpdateBuilder.setStopSequence(1);
                stopTimeUpdateBuilder.setScheduleRelationship(StopTimeUpdate.ScheduleRelationship.NO_DATA);
                builder.addStopTimeUpdate(stopTimeUpdateBuilder.build());
            } else {
                builder.addAllStopTimeUpdate(validated);
            }
        }

        TripUpdate newTripUpdate = builder.build();
        tripUpdateCache.put(cacheKey, newTripUpdate);
        return newTripUpdate;
    }
}
