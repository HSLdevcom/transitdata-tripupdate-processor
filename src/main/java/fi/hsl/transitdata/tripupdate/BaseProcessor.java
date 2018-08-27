package fi.hsl.transitdata.tripupdate;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.proto.PubtransTableProtos;
import org.apache.pulsar.client.api.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

public abstract class BaseProcessor implements IMessageProcessor {
    protected static final Logger log = LoggerFactory.getLogger(BaseProcessor.class);

    private Jedis jedis;
    StopEvent.EventType eventType;

    //private static final String REDIS_PREFIX_STOP_EVENT = "stopev:";

    TripUpdateProcessor tripProcessor = null;

    public BaseProcessor(Jedis jedis, StopEvent.EventType eventType, TripUpdateProcessor tripProcessor) {
        this.jedis = jedis;
        this.eventType = eventType;
        this.tripProcessor = tripProcessor;
    }

    protected abstract PubtransTableProtos.ROIBase parseBaseFromMessage(Message msg) throws InvalidProtocolBufferException;

    private String getStopIdString(PubtransTableProtos.ROIBase base) {
        String key = TransitdataProperties.REDIS_PREFIX_JPP + base.getIsTargetedAtJourneyPatternPointGid();
        return jedis.get(key);
    }

    @Override
    public void processMessage(Message msg) {
        try {
            PubtransTableProtos.ROIBase base = parseBaseFromMessage(msg);
            //1 Create stop event

            StopEvent stop = createStopEvent(base, this.eventType);

            //2 create stop time update event from that
            GtfsRealtime.TripUpdate.StopTimeUpdate previousUpdate = null;//TODO READ old from cache
            GtfsRealtime.TripUpdate.StopTimeUpdate newUpdate = createStopTimeUpdate(stop, previousUpdate);
            //TODO Put to cache

            //3 Create TripUpdate and send it out
            tripProcessor.processStopTimeUpdate(msg.getKey(), newUpdate);
        }
        catch (InvalidProtocolBufferException e) {
            log.error("Failed to parse ROIArrival from message payload", e);
        }

    }
    /*
    private String cacheKey(Message msg) {
        return REDIS_PREFIX_STOP_EVENT + msg.getKey();
    }*/

    @Override
    public boolean validateMessage(Message msg) {
        try {
            PubtransTableProtos.ROIBase base = parseBaseFromMessage(msg);
            return validate(base);
        }
        catch (InvalidProtocolBufferException e) {
            log.error("Failed to parse ROIArrival from message payload", e);
            return false;
        }
    }

    protected boolean validate(PubtransTableProtos.ROIBase base) {
        if (base == null) {
            log.error("No ROIBase in the payload, message discarded");
            return false;
        }

        String stopIdString = getStopIdString(base);
        if (stopIdString == null) {
            log.error("No stop id found for JourneyPatternPoint Gid " + base.getIsTargetedAtJourneyPatternPointGid());
            return false;
        }
        if (base.getTargetDateTime() == null || base.getTargetDateTime().isEmpty()) {
            log.error("No TargetDatetime, message discarded");
            return false;
        }
        return true;
    }

    private StopEvent createStopEvent(PubtransTableProtos.ROIBase base, StopEvent.EventType type) {

        StopEvent event = new StopEvent();
        event.dated_vehicle_journey_id = base.getIsOnDatedVehicleJourneyId();
        event.event_type = type;
        event.stop_id = Integer.parseInt(getStopIdString(base));
        event.stop_seq = base.getJourneyPatternSequenceNumber();

        event.schedule_relationship = (base.getState() == 3L) ? StopEvent.ScheduleRelationship.Skipped : StopEvent.ScheduleRelationship.Scheduled;
        //TODO Use java OffsetDateTime?
        event.target_time = java.sql.Timestamp.valueOf(base.getTargetDateTime()).getTime(); //Don't set if skipped?

        return event;
    }

    /*
    public void stopTimeUpdate(Message received) throws Exception {
        try {
            PulsarMessageProtos.StopEvent stopEvent = PulsarMessageProtos.StopEvent.parseFrom(received.getData());

            GtfsRealtime.TripUpdate.StopTimeUpdate stopTimeUpdate = updateStopTimeUpdate(stopTimeUpdates.get(received.getKey()), stopEvent);
            stopTimeUpdates.put(received.getKey(), stopTimeUpdate);

            producer.newMessage()
                    .key(String.valueOf(stopEvent.getDatedVehicleJourneyId()))
                    .eventTime(18000l)
                    .value(stopTimeUpdate.toByteArray())
                    .sendAsync()
                    .thenCompose((id) -> consumer.acknowledgeAsync(received))
                    .thenRun(() -> log.info("Messages sent: " + this.msgCount.getAndIncrement() + ". Cache size: " + this.stopTimeUpdates.size() + " dvj: " + String.valueOf(stopEvent.getDatedVehicleJourneyId())));

        } catch (InvalidProtocolBufferException e) {
            log.error(e.getLocalizedMessage());
        }
    }*/


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
