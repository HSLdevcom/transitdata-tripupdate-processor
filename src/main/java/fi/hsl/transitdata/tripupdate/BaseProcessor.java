package fi.hsl.transitdata.tripupdate;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.proto.PubtransTableProtos;
import org.apache.pulsar.client.api.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.concurrent.TimeUnit;

public abstract class BaseProcessor implements IMessageProcessor {
    protected static final Logger log = LoggerFactory.getLogger(BaseProcessor.class);

    private Jedis jedis;
    StopEvent.EventType eventType;

    private final Cache<String, GtfsRealtime.TripUpdate.StopTimeUpdate> stopTimeCache;

    TripUpdateProcessor tripProcessor = null;

    public BaseProcessor(Jedis jedis, StopEvent.EventType eventType, TripUpdateProcessor tripProcessor) {
        this.jedis = jedis;
        this.eventType = eventType;
        this.tripProcessor = tripProcessor;

        this.stopTimeCache = CacheBuilder.newBuilder()
                .expireAfterAccess(4, TimeUnit.HOURS)
                .build();
    }

    protected abstract PubtransTableProtos.Common parseSharedDataFromMessage(Message msg) throws InvalidProtocolBufferException;

    private String getStopIdString(PubtransTableProtos.Common common) {
        String key = TransitdataProperties.REDIS_PREFIX_JPP + common.getIsTargetedAtJourneyPatternPointGid();
        return jedis.get(key);
    }

    @Override
    public void processMessage(Message msg) {
        try {
            PubtransTableProtos.Common common = parseSharedDataFromMessage(msg);
            // Create stop event
            StopEvent stop = createStopEvent(common, this.eventType);

            // Create stop time update event from that and store so we can later add the whole list to TripUpdate
            String key = msg.getKey();

            GtfsRealtime.TripUpdate.StopTimeUpdate previousUpdate = stopTimeCache.getIfPresent(key);
            GtfsRealtime.TripUpdate.StopTimeUpdate newUpdate = createStopTimeUpdate(stop, previousUpdate);
            stopTimeCache.put(key, newUpdate);

            // Create TripUpdate and send it out
            tripProcessor.processStopTimeUpdate(msg.getKey(), newUpdate);
        }
        catch (InvalidProtocolBufferException e) {
            log.error("Failed to parse ROIArrival from message payload", e);
        }

    }

    @Override
    public boolean validateMessage(Message msg) {
        try {
            PubtransTableProtos.Common common = parseSharedDataFromMessage(msg);
            return validate(common);
        }
        catch (InvalidProtocolBufferException e) {
            log.error("Failed to parse ROIArrival from message payload", e);
            return false;
        }
    }

    protected boolean validate(PubtransTableProtos.Common common) {
        if (common == null) {
            log.error("No Common in the payload, message discarded");
            return false;
        }

        String stopIdString = getStopIdString(common);
        if (stopIdString == null) {
            log.error("No stop id found for JourneyPatternPoint Gid " + common.getIsTargetedAtJourneyPatternPointGid());
            return false;
        }
        if (common.getTargetDateTime() == null || common.getTargetDateTime().isEmpty()) {
            log.error("No TargetDatetime, message discarded");
            return false;
        }
        return true;
    }

    private StopEvent createStopEvent(PubtransTableProtos.Common common, StopEvent.EventType type) {

        StopEvent event = new StopEvent();
        event.dated_vehicle_journey_id = common.getIsOnDatedVehicleJourneyId();
        event.event_type = type;
        event.stop_id = Integer.parseInt(getStopIdString(common));
        event.stop_seq = common.getJourneyPatternSequenceNumber();

        event.schedule_relationship = (common.getState() == 3L) ? StopEvent.ScheduleRelationship.Skipped : StopEvent.ScheduleRelationship.Scheduled;
        //TODO Use java OffsetDateTime?
        event.target_time = java.sql.Timestamp.valueOf(common.getTargetDateTime()).getTime(); //Don't set if skipped?

        return event;
    }

    private GtfsRealtime.TripUpdate.StopTimeUpdate createStopTimeUpdate(StopEvent stopEvent, GtfsRealtime.TripUpdate.StopTimeUpdate previousUpdate) {

        GtfsRealtime.TripUpdate.StopTimeUpdate.Builder stopTimeUpdateBuilder = null;
        if (previousUpdate != null) {
            stopTimeUpdateBuilder = previousUpdate.toBuilder();
        }
        else {
            stopTimeUpdateBuilder = GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder()
                    .setStopId(String.valueOf(stopEvent.stop_id))
                    .setStopSequence(stopEvent.stop_seq);
        }

        switch (stopEvent.schedule_relationship) {
            case Skipped:
                stopTimeUpdateBuilder.setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED);
                break;
            case Scheduled:
                stopTimeUpdateBuilder.setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SCHEDULED);
                break;
        }

        GtfsRealtime.TripUpdate.StopTimeEvent stopTimeEvent = GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder()
                .setTime(stopEvent.target_time)
                .build();
        switch (stopEvent.event_type) {
            case Arrival:
                stopTimeUpdateBuilder.setArrival(stopTimeEvent);
                break;
            case Departure:
                stopTimeUpdateBuilder.setDeparture(stopTimeEvent);
                break;
        }

        return stopTimeUpdateBuilder.build();
    }
}
