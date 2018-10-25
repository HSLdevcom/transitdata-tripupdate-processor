package fi.hsl.transitdata.tripupdate.processing;

import com.google.protobuf.InvalidProtocolBufferException;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.proto.PubtransTableProtos;
import fi.hsl.transitdata.tripupdate.application.IMessageProcessor;
import fi.hsl.transitdata.tripupdate.models.StopEvent;
import org.apache.pulsar.client.api.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

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

            StopEvent stop = StopEvent.newInstance(common, msg.getProperties(), this.eventType);

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
        if (common.getType() == 0) {
            log.error("Event is for a via point, message discarded");
            return false;
        }
        return true;
    }

}
