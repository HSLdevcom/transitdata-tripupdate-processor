package fi.hsl.transitdata.tripupdate;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.proto.PubtransTableProtos;
import org.apache.pulsar.client.api.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

public class MessageProcessor implements IMessageProcessor {
    private static final Logger log = LoggerFactory.getLogger(MessageProcessor.class);

    private Jedis jedis;
    StopEvent.EventType eventType;

    public MessageProcessor(Jedis jedis, StopEvent.EventType eventType) {
        this.jedis = jedis;
        this.eventType = eventType;
    }

    @Override
    public boolean validateMessage(Message msg) {
        try {
            PubtransTableProtos.ROIArrival roiMessage = PubtransTableProtos.ROIArrival.parseFrom(msg.getData());

            String stopIdString = getStopIdString(roiMessage);
            if (stopIdString == null) {
                log.error("No stop id found for JourneyPatternPoint Gid " + roiMessage.getIsTargetedAtJourneyPatternPointGid());
                return false;
            }
            if (roiMessage.getTargetDateTime() == null || roiMessage.getTargetDateTime().isEmpty()) {
                log.error("No TargetDatetime, message discarded");
                return false;
            }
        }
        catch (InvalidProtocolBufferException e) {
            log.error("Failed to parse ROIArrival from message payload", e);
            return false;
        }
        return true;
    }

    private String getStopIdString(PubtransTableProtos.ROIArrival roiMessage) {
        return jedis.get(TransitdataProperties.REDIS_PREFIX_JPP + roiMessage.getIsTargetedAtJourneyPatternPointGid());
    }

    @Override
    public void processMessage(Message msg) {
        try {

            //1 Create stop event
            StopEvent stop = createStopEvent(msg, this.eventType);

            //2 create stop time update event from that
        }
        catch (InvalidProtocolBufferException e) {
            log.error("Failed to parse ROIArrival from message payload", e);
            return false;
        }

    }


    public void stopTime() {

    }

    private StopEvent createStopEvent(Message inMsg, StopEvent.EventType type) throws InvalidProtocolBufferException {

        PubtransTableProtos.ROIArrival roiMessage = PubtransTableProtos.ROIArrival.parseFrom(inMsg.getData());

        StopEvent event = new StopEvent();
        event.dated_vehicle_journey_id = roiMessage.getIsOnDatedVehicleJourneyId();
        event.event_type = type;
        event.stop_id = Integer.parseInt(getStopIdString(roiMessage));
        event.stop_seq = roiMessage.getJourneyPatternSequenceNumber();

        event.schedule_relationship = (roiMessage.getState() == 3L) ? StopEvent.ScheduleRelationship.Skipped : StopEvent.ScheduleRelationship.Scheduled;
        //TODO Use java OffsetDateTime?
        event.target_time = java.sql.Timestamp.valueOf(roiMessage.getTargetDateTime()).getTime(); //Don't set if skipped?

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
}
