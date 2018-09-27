package fi.hsl.transitdata.tripupdate;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.transitdata.TransitdataProperties;
import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

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
            Map<Integer, GtfsRealtime.TripUpdate.StopTimeUpdate> stops = updateStopTimeUpdateCache(stopEvent);

            GtfsRealtime.TripUpdate tripUpdate = updateTripUpdates(stopEvent, stops);

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

    Map<Integer, GtfsRealtime.TripUpdate.StopTimeUpdate> updateStopTimeUpdateCache(final StopEvent stopEvent) throws Exception {

        final long dvjId = stopEvent.getDatedVehicleJourneyId();
        Map<Integer, GtfsRealtime.TripUpdate.StopTimeUpdate> stopTimeUpdatesForThisTripUpdate = getStopTimeUpdatesForTripUpdate(dvjId);

        //StopSeq is the key since it's unique within one journey (running number).
        //There can be duplicate StopIds within journey, in case the same stop is used twice in one route (rare but possible)
        final int cacheKey = stopEvent.getStopSeq();
        GtfsRealtime.TripUpdate.StopTimeUpdate previous = stopTimeUpdatesForThisTripUpdate.get(cacheKey);

        GtfsRealtime.TripUpdate.StopTimeUpdate latest = GtfsRtFactory.newStopTimeUpdateFromPrevious(stopEvent, previous);
        stopTimeUpdatesForThisTripUpdate.put(cacheKey, latest);

        return stopTimeUpdatesForThisTripUpdate;
    }

    Map<Integer, GtfsRealtime.TripUpdate.StopTimeUpdate> getStopTimeUpdatesForTripUpdate(long dvjId) throws Exception {
        return stopTimeUpdateCache.get(dvjId);
    }

    private GtfsRealtime.TripUpdate updateTripUpdates(final StopEvent latest, Map<Integer,
        GtfsRealtime.TripUpdate.StopTimeUpdate> stopTimeUpdatesPerStopSeq) {

        final long dvjId = latest.getDatedVehicleJourneyId();

        //Gtfs-rt standard requires the updates be sorted by stop seq. We already have this because we use TreeMap.
        Collection<GtfsRealtime.TripUpdate.StopTimeUpdate> stopTimeUpdates = stopTimeUpdatesPerStopSeq.values();
        GtfsRealtime.TripUpdate previousTripUpdate = tripUpdateCache.getIfPresent(dvjId);
        if (previousTripUpdate == null) {
            previousTripUpdate = GtfsRtFactory.newTripUpdate(latest);
        }

        GtfsRealtime.TripUpdate tripUpdate = previousTripUpdate.toBuilder()
                .clearStopTimeUpdate()
                .addAllStopTimeUpdate(stopTimeUpdates)
                .build();

        tripUpdateCache.put(dvjId, tripUpdate);

        return tripUpdate;
    }

}
