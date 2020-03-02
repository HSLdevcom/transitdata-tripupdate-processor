package fi.hsl.transitdata.tripupdate.processing;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.transitdata.tripupdate.gtfsrt.GtfsRtFactory;
import fi.hsl.transitdata.tripupdate.gtfsrt.GtfsRtValidator;
import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import static com.google.transit.realtime.GtfsRealtime.TripUpdate.*;
import static com.google.transit.realtime.GtfsRealtime.*;

public class TripUpdateProcessor {
    private static final Logger log = LoggerFactory.getLogger(TripUpdateProcessor.class);

    private static final Duration CACHE_DURATION = Duration.of(4, ChronoUnit.HOURS);

    private Producer<byte[]> producer;

    //for each trip (identified by tripId-String) store one estimate/event (StopTimeUpdate) for each stop (identified by stopSequence-Integer)
    private final LoadingCache<String, Map<Integer, GtfsRealtime.TripUpdate.StopTimeUpdate>> stopTimeUpdateCache;
    //for each trip (identified by tripId-String) store the full TripUpdate containing all StopTimeUpdates
    private final Cache<String, GtfsRealtime.TripUpdate> tripUpdateCache;
    //for each trip (identified by tripId-String), keep track of whether the trip is included in static schedule (so that correct schedule relationship can be restored in case of cancellation-of-cancellation)
    private final Cache<String, TripDescriptor.ScheduleRelationship> scheduleRelationshipCache;

    public TripUpdateProcessor(Producer<byte[]> producer) {
        this.producer = producer;

        this.tripUpdateCache = CacheBuilder.newBuilder()
                .expireAfterAccess(CACHE_DURATION)
                .build();

        this.stopTimeUpdateCache = CacheBuilder.newBuilder()
                .expireAfterAccess(CACHE_DURATION)
                .build(new CacheLoader<String, Map<Integer, GtfsRealtime.TripUpdate.StopTimeUpdate>>() {
                    @Override
                    public Map<Integer, GtfsRealtime.TripUpdate.StopTimeUpdate> load(String key) {
                        //TreeMap keeps its entries sorted according to the natural ordering of its keys.
                        return new TreeMap<>();
                    }
                });

        this.scheduleRelationshipCache = CacheBuilder.newBuilder()
                .expireAfterWrite(CACHE_DURATION)
                .build();
    }

    public Optional<TripUpdate> processStopEstimate(InternalMessages.StopEstimate stopEstimate) {
        try {
            final StopTimeUpdate latest = updateStopTimeUpdateCache(stopEstimate);
            final String tripKey = cacheKey(stopEstimate);
            List<StopTimeUpdate> stopTimeUpdates = getStopTimeUpdates(tripKey);

            // We need to clean up the "raw data" StopTimeUpdates for any inconsistencies
            List<StopTimeUpdate> validated = GtfsRtValidator.cleanStopTimeUpdates(stopTimeUpdates, latest);

            TripUpdate tripUpdate = updateTripUpdateCacheWithStopTimes(stopEstimate, validated);
            scheduleRelationshipCache.put(tripKey, tripUpdate.getTrip().getScheduleRelationship());
            if (tripUpdate.getTrip().getScheduleRelationship() == TripDescriptor.ScheduleRelationship.SCHEDULED
                    || tripUpdate.getTrip().getScheduleRelationship() == TripDescriptor.ScheduleRelationship.ADDED) {
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
        try {
            return stopTimeUpdateCache.get(key);
        }
        catch (ExecutionException e) {
            log.error("Unexpected Error with StopTimeUpdate (Guava) Cache! ", e);
        }
        return new HashMap<>();
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

        tripUpdateCache.put(tuCacheKey, tripUpdate);

        return tripUpdate;
    }

    private TripUpdate updateTripUpdateCacheWithCancellation(final String cacheKey,
                                                             final long messageTimestampMs,
                                                             InternalMessages.TripCancellation cancellation) {
        TripUpdate previousTripUpdate = tripUpdateCache.getIfPresent(cacheKey);
        if (previousTripUpdate == null) {
            previousTripUpdate = GtfsRtFactory.newTripUpdate(cancellation, messageTimestampMs);
        }

        final GtfsRealtime.TripDescriptor.ScheduleRelationship status =
                cancellation.getStatus() == InternalMessages.TripCancellation.Status.CANCELED ?
                    GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED :
                    scheduleRelationshipCache.getIfPresent(cacheKey) != null ?
                        scheduleRelationshipCache.getIfPresent(cacheKey) :
                        TripDescriptor.ScheduleRelationship.SCHEDULED; //Assume that trip is scheduled if it is not found from the cache

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
