package fi.hsl.transitdata.tripupdate;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.transitdata.TransitdataUtils;
import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

public class TripUpdateProcessor {

    private static final Logger log = LoggerFactory.getLogger(TripUpdateProcessor.class);

    private Jedis jedis;
    private Producer<byte[]> producer;

    //for each JourneyId stores a list of StopTimeUpdates per StopSequence-ID
    private final LoadingCache<String, Map<Integer, GtfsRealtime.TripUpdate.StopTimeUpdate>> stopTimeUpdateLists;
    private final LoadingCache<String, GtfsRealtime.TripUpdate> tripUpdates;

    public TripUpdateProcessor(Producer<byte[]> producer, Jedis jedis) {
        this.producer = producer;
        this.jedis = jedis;

        this.tripUpdates = CacheBuilder.newBuilder()
                .expireAfterAccess(4, TimeUnit.HOURS)
                .build(new CacheLoader<String, GtfsRealtime.TripUpdate>() {
                    @Override
                    public GtfsRealtime.TripUpdate load(String s) throws Exception {
                        return initializeNewTripUpdate(s);
                    }
                });

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

    public void processStopEvent(final String messageKey, StopEvent stopEvent) {
        try {
            updateStopTimeUpdateLists(stopEvent, messageKey);

            GtfsRealtime.TripUpdate tripUpdate = updateTripUpdates(messageKey);
            if (tripUpdate != null) {

                long timestamp = TransitdataUtils.currentMessageTimestamp();
                GtfsRealtime.FeedMessage feedMessage = GtfsFactory.newFeedMessage(tripUpdate, timestamp);
                producer.newMessage()
                        .key(messageKey)
                        .eventTime(timestamp)
                        .value(feedMessage.toByteArray())
                        .sendAsync()
                        //.thenCompose((msg) -> consumer.acknowledgeAsync(inMsg))
                        .thenRun(() -> log.info("stop id: " + stopEvent.stop_id + " n of TripUpdates in memory: " + tripUpdates.size()));
            }
            else {
                log.warn("Cannot create FeedMessage, trip update is null");
            }
        } catch (Exception e) {
            log.error("Exception while processing stopTimeUpdate into tripUpdate", e);
        }

    }

    private void updateStopTimeUpdateLists(StopEvent stopEvent, String datedVehicleJourneyId) {
        try {
            Map<Integer, GtfsRealtime.TripUpdate.StopTimeUpdate> updatesForThisJourney = stopTimeUpdateLists.get(datedVehicleJourneyId);

            final int cacheKey = stopEvent.stop_seq;
            GtfsRealtime.TripUpdate.StopTimeUpdate previous = updatesForThisJourney.get(cacheKey);
            GtfsRealtime.TripUpdate.StopTimeUpdate latest = GtfsFactory.newStopTimeUpdateFromPrevious(stopEvent, previous);
            updatesForThisJourney.put(cacheKey, latest);

            stopTimeUpdateLists.put(datedVehicleJourneyId, updatesForThisJourney);
        } catch (Exception e) {
            log.error("Failed to update StopTimeUpdateList", e);
        }
    }

    private GtfsRealtime.TripUpdate updateTripUpdates(String datedVehicleJourneyId) {
        GtfsRealtime.TripUpdate tripUpdate = null;
        try {
            Collection<GtfsRealtime.TripUpdate.StopTimeUpdate> updates = stopTimeUpdateLists.get(datedVehicleJourneyId).values();
            tripUpdate = tripUpdates.get(datedVehicleJourneyId).toBuilder()
                    .clearStopTimeUpdate()
                    .addAllStopTimeUpdate(updates)
                    .build();

            tripUpdates.put(datedVehicleJourneyId, tripUpdate);

        } catch (Exception e) {
            log.error("Failed to update trip updates", e);
        }
        return tripUpdate;
    }

    private GtfsRealtime.TripUpdate initializeNewTripUpdate(String datedVehicleJourneyId) throws IllegalArgumentException {

        Map<String, String> journeyInfo = jedis.hgetAll("dvj:" + datedVehicleJourneyId);
        //TODO check this, does the CacheLoader propagate the exception or just swallow it?
        if (journeyInfo.get("route-name") == null || journeyInfo.get("direction") == null || journeyInfo.get("start-time") == null || journeyInfo.get("operating-day") == null) {
            throw new IllegalArgumentException("No journey data found for DatedVehicleJourneyId " + datedVehicleJourneyId);
        }

        GtfsRealtime.TripDescriptor tripDescriptor = GtfsRealtime.TripDescriptor.newBuilder()
                .setRouteId(journeyInfo.get("route-name"))
                .setDirectionId(Integer.parseInt(journeyInfo.get("direction")))
                .setStartDate(journeyInfo.get("operating-day"))
                .setStartTime(journeyInfo.get("start-time"))
                .build();

        GtfsRealtime.TripUpdate.Builder tripUpdateBuilder = GtfsRealtime.TripUpdate.newBuilder()
                .setTrip(tripDescriptor);

        return tripUpdateBuilder.build();
    }

}
