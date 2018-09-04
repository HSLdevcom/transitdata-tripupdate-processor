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
    private final LoadingCache<String, Map<Integer, GtfsRealtime.TripUpdate.StopTimeUpdate>> stopTimeUpdateLists;
    private final Cache<String, GtfsRealtime.TripUpdate> tripUpdates;

    public TripUpdateProcessor(Producer<byte[]> producer) {
        this.producer = producer;

        this.tripUpdates = CacheBuilder.newBuilder()
                .expireAfterAccess(4, TimeUnit.HOURS)
                .build();

        this.stopTimeUpdateLists = CacheBuilder.newBuilder()
                .expireAfterAccess(4, TimeUnit.HOURS)
                .build(new CacheLoader<String, Map<Integer, GtfsRealtime.TripUpdate.StopTimeUpdate>>() {
                    @Override
                    public Map<Integer, GtfsRealtime.TripUpdate.StopTimeUpdate> load(String s) {
                    //TreeMap keeps its entries sorted according to the natural ordering of its keys.
                    return new TreeMap<>();
                    }
                });
    }

    public void processStopEvent(final String key, StopEvent stopEvent) {
        try {
            Map<Integer, GtfsRealtime.TripUpdate.StopTimeUpdate> stops = updateStopTimeUpdateLists(key, stopEvent);

            GtfsRealtime.TripUpdate tripUpdate = updateTripUpdates(key, stopEvent, stops);

            long timestamp = TransitdataProperties.currentTimestamp();
            GtfsRealtime.FeedMessage feedMessage = GtfsFactory.newFeedMessage(tripUpdate, timestamp);
            producer.newMessage()
                    .key(key)
                    .eventTime(timestamp)
                    .value(feedMessage.toByteArray())
                    .sendAsync()
                    .thenRun(() -> log.info("stop id: " + stopEvent.stop_id + " n of TripUpdates in memory: " + tripUpdates.size()));

        } catch (Exception e) {
            log.error("Exception while processing stopTimeUpdate into tripUpdate", e);
        }

    }

    private Map<Integer, GtfsRealtime.TripUpdate.StopTimeUpdate> updateStopTimeUpdateLists
                            (final String datedVehicleJourneyId, final StopEvent stopEvent) throws Exception {

        Map<Integer, GtfsRealtime.TripUpdate.StopTimeUpdate> updatesForThisJourney = stopTimeUpdateLists.get(datedVehicleJourneyId);

        final int cacheKey = stopEvent.stop_seq;
        GtfsRealtime.TripUpdate.StopTimeUpdate previous = updatesForThisJourney.get(cacheKey);
        GtfsRealtime.TripUpdate.StopTimeUpdate latest = GtfsFactory.newStopTimeUpdateFromPrevious(stopEvent, previous);
        updatesForThisJourney.put(cacheKey, latest);

        stopTimeUpdateLists.put(datedVehicleJourneyId, updatesForThisJourney);
        return updatesForThisJourney;
    }

    private GtfsRealtime.TripUpdate updateTripUpdates(final String datedVehicleJourneyId,
                                                      final StopEvent latest, Map<Integer,
        GtfsRealtime.TripUpdate.StopTimeUpdate> updatesPerStop) {

        Collection<GtfsRealtime.TripUpdate.StopTimeUpdate> updates = updatesPerStop.values();
        GtfsRealtime.TripUpdate previousUpdate = tripUpdates.getIfPresent(datedVehicleJourneyId);
        if (previousUpdate == null) {
            previousUpdate = GtfsFactory.newTripUpdate(latest);
        }

        GtfsRealtime.TripUpdate tripUpdate = previousUpdate.toBuilder()
                .clearStopTimeUpdate()
                .addAllStopTimeUpdate(updates)
                .build();

        tripUpdates.put(datedVehicleJourneyId, tripUpdate);

        return tripUpdate;
    }

}
