package fi.hsl.transitdata.tripupdate;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.proto.InternalMessages;
import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import static com.google.transit.realtime.GtfsRealtime.TripUpdate.*;

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

    public void processStopEvent(final String messageKey, StopEvent stopEvent) {
        try {
            StopTimeUpdate latest = updateStopTimeUpdateCache(stopEvent);
            List<StopTimeUpdate> stopTimeUpdates = getStopTimeUpdates(stopEvent.getDatedVehicleJourneyId());

            // We need to clean up the "raw data" StopTimeUpdates for any inconsistencies
            List<StopTimeUpdate> validated = GtfsRtValidator.cleanStopTimeUpdates(stopTimeUpdates, latest);

            GtfsRealtime.TripUpdate tripUpdate = updateTripUpdates(stopEvent, validated);

            long timestamp = TransitdataProperties.currentTimestamp();
            String id = Long.toString(stopEvent.getDatedVehicleJourneyId());
            GtfsRealtime.FeedMessage feedMessage = GtfsRtFactory.newFeedMessage(id, tripUpdate, timestamp);
            producer.newMessage()
                    .key(messageKey)
                    .eventTime(timestamp)
                    .value(feedMessage.toByteArray())
                    .sendAsync()
                    .thenRun(() -> log.debug("Sending TripUpdate for dvjId " + id + " with " + tripUpdate.getStopTimeUpdateCount() + " StopTimeUpdates"));

        } catch (Exception e) {
            log.error("Exception while processing stopTimeUpdate into tripUpdate", e);
        }

    }

    public void processTripCancellation(final String messageKey, InternalMessages.TripCancellation tripCancellation) {
        Long dvjId = Long.parseLong(messageKey);

        if (tripCancellation.getStatus() == InternalMessages.TripCancellation.Status.CANCELED) {
            GtfsRealtime.TripUpdate oldTripUpdate = tripUpdateCache.getIfPresent(dvjId);
            if (oldTripUpdate != null) {
                if (oldTripUpdate.getTrip().getScheduleRelationship() == GtfsRealtime.TripDescriptor.ScheduleRelationship.SCHEDULED) {
                    GtfsRealtime.TripUpdate.Builder newTripUpdateBuilder = oldTripUpdate.toBuilder();

                    GtfsRealtime.TripDescriptor tripDescriptor = oldTripUpdate
                            .getTrip().toBuilder()
                            .setScheduleRelationship(GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED)
                            .build();

                    newTripUpdateBuilder.setTrip(tripDescriptor);
                    newTripUpdateBuilder.clearStopTimeUpdate();
                    tripUpdateCache.put(dvjId, newTripUpdateBuilder.build());
                }
            }
            else {
                log.warn("Failed to find TripUpdate for dvj-id {} from cache", dvjId);
            }

        } else if (tripCancellation.getStatus() == InternalMessages.TripCancellation.Status.RUNNING) {
            //Current hypothesis is that this never occurs. For now simply log this event and then implement the
            //functionality when necessary.
            //The correct functionality is to mark the ScheduleRelationship for the TripUpdate as SCHEDULED
            //and fetch the possible previous StopTimeUpdates from the cache and insert them to the TripUpdate.
            log.error("Received a TripCancellation event with status of RUNNING. This status is currently unsupported");
        } else {
            log.error("Unknown Trip Cancellation Status: " + tripCancellation.getStatus());
        }
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

    Map<Integer, StopTimeUpdate> getStopTimeUpdatesWithStopSequences(long dvjId) throws Exception {
        return stopTimeUpdateCache.get(dvjId);
    }

    LinkedList<StopTimeUpdate> getStopTimeUpdates(long dvjId) throws Exception {
        // Gtfs-rt standard requires the updates be sorted by stop seq but we already have this because we use TreeMap.
        Collection<StopTimeUpdate> updates = getStopTimeUpdatesWithStopSequences(dvjId).values();
        return new LinkedList<>(updates);
    }


    private GtfsRealtime.TripUpdate updateTripUpdates(final StopEvent latest, Collection<StopTimeUpdate> stopTimeUpdates) {

        final long dvjId = latest.getDatedVehicleJourneyId();

        GtfsRealtime.TripUpdate previousTripUpdate = tripUpdateCache.getIfPresent(dvjId);
        if (previousTripUpdate == null) {
            previousTripUpdate = GtfsRtFactory.newTripUpdate(latest);
        }

        GtfsRealtime.TripUpdate tripUpdate = previousTripUpdate.toBuilder()
                .clearStopTimeUpdate()
                .addAllStopTimeUpdate(stopTimeUpdates)
                .setTimestamp(latest.getLastModifiedTimestamp())
                .build();

        tripUpdateCache.put(dvjId, tripUpdate);

        return tripUpdate;
    }
}
