package fi.hsl.transitdata.tripupdate;

import com.google.protobuf.InvalidProtocolBufferException;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.proto.PubtransTableProtos;
import org.apache.pulsar.client.api.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public abstract class BaseProcessor implements IMessageProcessor {
    protected static final Logger log = LoggerFactory.getLogger(BaseProcessor.class);

    final StopEvent.EventType eventType;

    final TripUpdateProcessor tripProcessor;

    public BaseProcessor(StopEvent.EventType eventType, TripUpdateProcessor tripProcessor) {
        this.eventType = eventType;
        this.tripProcessor = tripProcessor;
    }

    /**
     * Because the proto-classes don't have a common base class we need to extract the 'shared'-data with concrete implementations
     */
    protected abstract PubtransTableProtos.Common parseSharedDataFromMessage(Message msg) throws InvalidProtocolBufferException;

    @Override
    public void processMessage(Message msg) {
        try {
            PubtransTableProtos.Common common = parseSharedDataFromMessage(msg);
            // Create stop event

            StopEvent stop = createStopEvent(common, msg.getProperties(), this.eventType);

            // Create TripUpdate and send it out
            tripProcessor.processStopEvent(msg.getKey(), stop);
        }
        catch (InvalidProtocolBufferException e) {
            log.error("Failed to parse ROIArrival from message payload", e);
        }
    }

    @Override
    public boolean validateMessage(Message msg) {
        try {
            if (validateRequiredProperties(msg)) {
                PubtransTableProtos.Common common = parseSharedDataFromMessage(msg);
                return validate(common);
            }
            else {
                return false;
            }
        }
        catch (InvalidProtocolBufferException e) {
            log.error("Failed to parse ROIArrival from message payload", e);
            return false;
        }
    }

    private boolean validateRequiredProperties(Message msg) {
        final List<String> requiredProperties = Arrays.asList(
                TransitdataProperties.KEY_ROUTE_NAME,
                TransitdataProperties.KEY_DIRECTION,
                TransitdataProperties.KEY_START_TIME,
                TransitdataProperties.KEY_OPERATING_DAY
        );
        for (String p: requiredProperties) {
            if (!msg.hasProperty(p)) {
                log.error("Message is missing required property " + p);
                return false;
            }
        }
        return true;
    }

    protected boolean validate(PubtransTableProtos.Common common) {
        if (common == null) {
            log.error("No Common in the payload, message discarded");
            return false;
        }
        if (!common.hasIsTargetedAtJourneyPatternPointGid()) {
            log.error("No JourneyPatternPointGid, message discarded");
            return false;
        }
        if (common.getTargetDateTime() == null || common.getTargetDateTime().isEmpty()) {
            log.error("No TargetDatetime, message discarded");
            return false;
        }
        return true;
    }

    private StopEvent createStopEvent(PubtransTableProtos.Common common, Map<String, String> properties, StopEvent.EventType type) {
        //TODO fix this, use builder pattern or factory method to create immutable StopEvent
        StopEvent event = new StopEvent();
        event.dated_vehicle_journey_id = common.getIsOnDatedVehicleJourneyId();
        event.event_type = type;
        event.stop_id = common.getIsTargetedAtJourneyPatternPointGid();
        event.stop_seq = common.getJourneyPatternSequenceNumber();

        event.schedule_relationship = (common.getState() == 3L) ? StopEvent.ScheduleRelationship.Skipped : StopEvent.ScheduleRelationship.Scheduled;
        //TODO Use java OffsetDateTime?
        event.target_time = java.sql.Timestamp.valueOf(common.getTargetDateTime()).getTime(); //Don't set if skipped?

        event.routeData.direction = Integer.parseInt(properties.get(TransitdataProperties.KEY_DIRECTION));
        event.routeData.route_name = properties.get(TransitdataProperties.KEY_ROUTE_NAME);
        event.routeData.operating_day = properties.get(TransitdataProperties.KEY_OPERATING_DAY);
        event.routeData.start_time = properties.get(TransitdataProperties.KEY_START_TIME);

        return event;
    }

}
