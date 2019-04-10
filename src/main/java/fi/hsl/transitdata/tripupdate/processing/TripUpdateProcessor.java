package fi.hsl.transitdata.tripupdate.processing;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.transitdata.tripupdate.gtfsrt.GtfsRtFactory;
import fi.hsl.transitdata.tripupdate.gtfsrt.GtfsRtValidator;
import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import static com.google.transit.realtime.GtfsRealtime.TripUpdate.*;
import static com.google.transit.realtime.GtfsRealtime.*;

public class TripUpdateProcessor {

    private static final Logger log = LoggerFactory.getLogger(TripUpdateProcessor.class);

    private Producer<byte[]> producer;

    //for each trip (identified by tripId-String) store one estimate/event (StopTimeUpdate) for each stop (identified by stopSequence-Integer)
    private final LoadingCache<String, Map<Integer, GtfsRealtime.TripUpdate.StopTimeUpdate>> stopTimeUpdateCache;
    //for each trip (identified by tripId-String) store the full TripUpdate containing all StopTimeUpdates
    private final Cache<String, GtfsRealtime.TripUpdate> tripUpdateCache;

    public TripUpdateProcessor(Producer<byte[]> producer) {
        this.producer = producer;

        this.tripUpdateCache = CacheBuilder.newBuilder()
                .expireAfterAccess(4, TimeUnit.HOURS)
                .build();

        this.stopTimeUpdateCache = CacheBuilder.newBuilder()
                .expireAfterAccess(4, TimeUnit.HOURS)
                .build(new CacheLoader<String, Map<Integer, GtfsRealtime.TripUpdate.StopTimeUpdate>>() {
                    @Override
                    public Map<Integer, GtfsRealtime.TripUpdate.StopTimeUpdate> load(String key) {
                        //TreeMap keeps its entries sorted according to the natural ordering of its keys.
                        return new TreeMap<>();
                    }
                });
    }

    public Optional<TripUpdate> processStopEstimate(InternalMessages.StopEstimate stopEstimate) {
        TripUpdate tripUpdate = null;
        try {
            final StopTimeUpdate latest = updateStopTimeUpdateCache(stopEstimate);
            final String tripKey = cacheKey(stopEstimate);
            List<StopTimeUpdate> stopTimeUpdates = getStopTimeUpdates(tripKey);

            // We need to clean up the "raw data" StopTimeUpdates for any inconsistencies
            List<StopTimeUpdate> validated = GtfsRtValidator.cleanStopTimeUpdates(stopTimeUpdates, latest);

            tripUpdate = updateTripUpdateCacheWithStopTimes(stopEstimate, validated);
            //According to GTFS spec, timestamp identifies the moment when the content of this feed has been created in POSIX time
        } catch (Exception e) {
            log.error("Exception while translating StopEstimate into TripUpdate", e);
        }

        return Optional.ofNullable(tripUpdate);

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
                    GtfsRealtime.TripDescriptor.ScheduleRelationship.SCHEDULED;

        TripDescriptor tripDescriptor = previousTripUpdate.getTrip().toBuilder()
                .setScheduleRelationship(status)
                .build();

        TripUpdate.Builder builder = previousTripUpdate.toBuilder()
                .setTrip(tripDescriptor)
                .setTimestamp(TimeUnit.SECONDS.convert(messageTimestampMs, TimeUnit.MILLISECONDS))
                .clearStopTimeUpdate();


        if (status == TripDescriptor.ScheduleRelationship.SCHEDULED) {
            // We need to re-attach all the StopTimeUpdates to the payload

            List<StopTimeUpdate> stopTimeUpdates = getStopTimeUpdates(cacheKey);
            // We need to clean up the "raw data" StopTimeUpdates for any inconsistencies
            List<StopTimeUpdate> validated = GtfsRtValidator.cleanStopTimeUpdates(stopTimeUpdates, null);
            builder.addAllStopTimeUpdate(validated);
        }

        TripUpdate newTripUpdate = builder.build();
        tripUpdateCache.put(cacheKey, newTripUpdate);
        return newTripUpdate;
    }
}
