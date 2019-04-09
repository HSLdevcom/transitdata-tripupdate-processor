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
import fi.hsl.transitdata.tripupdate.models.StopEvent;
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

    //for each JourneyId stores a list of StopTimeUpdates per StopSequence-ID
    private final LoadingCache<Long, Map<Integer, GtfsRealtime.TripUpdate.StopTimeUpdate>> stopTimeUpdateCache;
    private final Cache<Long, GtfsRealtime.TripUpdate> tripUpdateCache;

    public TripUpdateProcessor(Producer<byte[]> producer) {
        this.producer = producer;

        this.tripUpdateCache = CacheBuilder.newBuilder()
                .expireAfterAccess(4, TimeUnit.HOURS)
                .build();

        this.stopTimeUpdateCache = CacheBuilder.newBuilder()
                .expireAfterAccess(4, TimeUnit.HOURS)
                .build(new CacheLoader<Long, Map<Integer, GtfsRealtime.TripUpdate.StopTimeUpdate>>() {
                    @Override
                    public Map<Integer, GtfsRealtime.TripUpdate.StopTimeUpdate> load(Long key) {
                        //TreeMap keeps its entries sorted according to the natural ordering of its keys.
                        return new TreeMap<>();
                    }
                });
    }

    public TripUpdate processStopEvent(StopEvent stopEvent) {
        TripUpdate tripUpdate = null;
        try {
            StopTimeUpdate latest = updateStopTimeUpdateCache(stopEvent);
            List<StopTimeUpdate> stopTimeUpdates = getStopTimeUpdates(stopEvent.getDatedVehicleJourneyId());

            // We need to clean up the "raw data" StopTimeUpdates for any inconsistencies
            List<StopTimeUpdate> validated = GtfsRtValidator.cleanStopTimeUpdates(stopTimeUpdates, latest);

            tripUpdate = updateTripUpdateCacheWithStopTimes(stopEvent, validated);
            //According to GTFS spec, timestamp identifies the moment when the content of this feed has been created in POSIX time
        } catch (Exception e) {
            log.error("Exception while processing stopTimeUpdate into tripUpdate", e);
        }

        return tripUpdate;

    }


    public TripUpdate processTripCancellation(final String messageKey, long messageTimestamp, InternalMessages.TripCancellation tripCancellation) {
        //Message key is now DVJ-ID in cancellation messages, however it's DVJ-ID + JPP-ID in Stop Events.
        //TODO fix, make consistent.
        Long dvjId = Long.parseLong(messageKey);

        return updateTripUpdateCacheWithCancellation(dvjId, messageTimestamp, tripCancellation);
    }

    StopTimeUpdate updateStopTimeUpdateCache(final StopEvent stopEvent) throws Exception {

        final long dvjId = stopEvent.getDatedVehicleJourneyId();
        Map<Integer, StopTimeUpdate> stopTimeUpdatesForThisTripUpdate = getStopTimeUpdatesWithStopSequences(dvjId);

        //StopSeq is the key since it's unique within one journey (running number).
        //There can be duplicate StopIds within journey, in case the same stop is used twice in one route (rare but possible)
        final int cacheKey = stopEvent.getStopSeq();
        StopTimeUpdate previous = stopTimeUpdatesForThisTripUpdate.get(cacheKey);

        StopTimeUpdate latest = GtfsRtFactory.newStopTimeUpdateFromPrevious(stopEvent, previous);
        stopTimeUpdatesForThisTripUpdate.put(cacheKey, latest);
        return latest;
    }

    Map<Integer, StopTimeUpdate> getStopTimeUpdatesWithStopSequences(long dvjId) {
        try {
            return stopTimeUpdateCache.get(dvjId);
        }
        catch (ExecutionException e) {
            log.error("Unexpected Error with StopTimeUpdate (Guava) Cache! ", e);
        }
        return new HashMap<>();
    }

    LinkedList<StopTimeUpdate> getStopTimeUpdates(long dvjId) {
        // Gtfs-rt standard requires the updates be sorted by stop seq but we already have this because we use TreeMap.
        Collection<StopTimeUpdate> updates = getStopTimeUpdatesWithStopSequences(dvjId).values();
        return new LinkedList<>(updates);
    }


    private TripUpdate updateTripUpdateCacheWithStopTimes(final StopEvent latest, Collection<StopTimeUpdate> stopTimeUpdates) {

        final long dvjId = latest.getDatedVehicleJourneyId();

        TripUpdate previousTripUpdate = tripUpdateCache.getIfPresent(dvjId);
        if (previousTripUpdate == null) {
            previousTripUpdate = GtfsRtFactory.newTripUpdate(latest);
        }

        TripUpdate tripUpdate = previousTripUpdate.toBuilder()
                .clearStopTimeUpdate()
                .addAllStopTimeUpdate(stopTimeUpdates)
                .setTimestamp(latest.getLastModifiedTimestamp(TimeUnit.SECONDS))
                .build();

        tripUpdateCache.put(dvjId, tripUpdate);

        return tripUpdate;
    }

    private TripUpdate updateTripUpdateCacheWithCancellation(final long dvjId,
                                                             final long messageTimestampMs,
                                                             final InternalMessages.TripCancellation cancellation) {
        TripUpdate previousTripUpdate = tripUpdateCache.getIfPresent(dvjId);
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

            List<StopTimeUpdate> stopTimeUpdates = getStopTimeUpdates(dvjId);
            // We need to clean up the "raw data" StopTimeUpdates for any inconsistencies
            List<StopTimeUpdate> validated = GtfsRtValidator.cleanStopTimeUpdates(stopTimeUpdates, null);
            builder.addAllStopTimeUpdate(validated);
        }

        TripUpdate newTripUpdate = builder.build();
        tripUpdateCache.put(dvjId, newTripUpdate);
        return newTripUpdate;
    }
}
