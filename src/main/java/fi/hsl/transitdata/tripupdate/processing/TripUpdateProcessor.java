package fi.hsl.transitdata.tripupdate.processing;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.transitdata.tripupdate.gtfsrt.GtfsRtFactory;
import fi.hsl.transitdata.tripupdate.gtfsrt.GtfsRtValidator;
import fi.hsl.transitdata.tripupdate.models.StopEvent;
import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
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

    public void processStopEvent(final String messageKey, StopEvent stopEvent) {
        try {
            StopTimeUpdate latest = updateStopTimeUpdateCache(stopEvent);
            List<StopTimeUpdate> stopTimeUpdates = getStopTimeUpdates(stopEvent.getDatedVehicleJourneyId());

            // We need to clean up the "raw data" StopTimeUpdates for any inconsistencies
            List<StopTimeUpdate> validated = GtfsRtValidator.cleanStopTimeUpdates(stopTimeUpdates, latest);

            final TripUpdate tripUpdate = updateTripUpdateCacheWithStopTimes(stopEvent, validated);
            //According to GTFS spec, timestamp identifies the moment when the content of this feed has been created in POSIX time
            final long timestamp = tripUpdate.getTimestamp();
            final String id = Long.toString(stopEvent.getDatedVehicleJourneyId());

            sendTripUpdate(messageKey, tripUpdate, id, timestamp);
        } catch (Exception e) {
            log.error("Exception while processing stopTimeUpdate into tripUpdate", e);
        }

    }

    private void sendTripUpdate(final String messageKey, final TripUpdate tripUpdate, final String dvjId, final long timestamp) {
        FeedMessage feedMessage = GtfsRtFactory.newFeedMessage(dvjId, tripUpdate, timestamp);
        producer.newMessage()
                .key(messageKey)
                .eventTime(timestamp)
                .value(feedMessage.toByteArray())
                .sendAsync()
                .thenRun(() -> log.debug("Sending TripUpdate for dvjId {} with {} StopTimeUpdates and status {}",
                        dvjId, tripUpdate.getStopTimeUpdateCount(), tripUpdate.getTrip().getScheduleRelationship()));

    }

    public void processTripCancellation(final String messageKey, long messageTimestamp, InternalMessages.TripCancellation tripCancellation) {
        //Message key is now DVJ-ID in cancellation messages, however it's DVJ-ID + JPP-ID in Stop Events.
        //TODO fix, make consistent.
        Long dvjId = Long.parseLong(messageKey);

        if (tripCancellation.getStatus() == InternalMessages.TripCancellation.Status.CANCELED) {
            TripUpdate update = updateTripUpdateCacheWithCancellation(dvjId, messageTimestamp, tripCancellation);
            sendTripUpdate(messageKey, update, dvjId.toString(), messageTimestamp);
        }
        else if (tripCancellation.getStatus() == InternalMessages.TripCancellation.Status.RUNNING) {
            //Current hypothesis is that this never occurs. For now simply log this event and then implement the
            //functionality when necessary.
            //The correct functionality is to mark the ScheduleRelationship for the TripUpdate as SCHEDULED
            //and fetch the possible previous StopTimeUpdates from the cache and insert them to the TripUpdate.
            log.error("Received a TripCancellation event with status of RUNNING. This status is currently unsupported");
        }
        else {
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


    private TripUpdate updateTripUpdateCacheWithStopTimes(final StopEvent latest, Collection<StopTimeUpdate> stopTimeUpdates) {

        final long dvjId = latest.getDatedVehicleJourneyId();

        TripUpdate previousTripUpdate = tripUpdateCache.getIfPresent(dvjId);
        if (previousTripUpdate == null) {
            previousTripUpdate = GtfsRtFactory.newTripUpdate(latest);
        }

        TripUpdate tripUpdate = previousTripUpdate.toBuilder()
                .clearStopTimeUpdate()
                .addAllStopTimeUpdate(stopTimeUpdates)
                .setTimestamp(latest.getLastModifiedTimestamp())
                .build();

        tripUpdateCache.put(dvjId, tripUpdate);

        return tripUpdate;
    }

    private TripUpdate updateTripUpdateCacheWithCancellation(long dvjId,
                                                             long messageTimestamp,
                                                             InternalMessages.TripCancellation cancellation) {
        TripUpdate previousTripUpdate = tripUpdateCache.getIfPresent(dvjId);
        if (previousTripUpdate == null) {
            previousTripUpdate = GtfsRtFactory.newTripUpdate(cancellation, messageTimestamp);
        }

        TripDescriptor tripDescriptor = previousTripUpdate.getTrip().toBuilder()
                .setScheduleRelationship(GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED)
                .build();

        TripUpdate newTripUpdate = previousTripUpdate.toBuilder()
                .setTrip(tripDescriptor)
                .clearStopTimeUpdate()
                .setTimestamp(messageTimestamp)
                .build();

        tripUpdateCache.put(dvjId, newTripUpdate);
        return newTripUpdate;
    }
}
