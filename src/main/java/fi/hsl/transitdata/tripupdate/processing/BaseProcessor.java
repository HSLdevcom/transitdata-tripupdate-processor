package fi.hsl.transitdata.tripupdate.processing;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.proto.PubtransTableProtos;
import fi.hsl.transitdata.tripupdate.application.IMessageProcessor;
import fi.hsl.transitdata.tripupdate.models.StopEvent;
import org.apache.pulsar.client.api.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

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
    public GtfsRealtime.TripUpdate processMessage(Message msg) {

        GtfsRealtime.TripUpdate tripUpdate = null;

        try {
            PubtransTableProtos.Common common = parseSharedDataFromMessage(msg);
            // Create stop event

            StopEvent stop = StopEvent.newInstance(common, msg.getProperties(), this.eventType);

            // Create TripUpdate and send it out
            tripUpdate = tripProcessor.processStopEvent(stop);
        }
        catch (InvalidProtocolBufferException e) {
            log.error("Failed to parse ROIArrival from message payload", e);
        }

        return tripUpdate;
    }

    @Override
    public boolean validateMessage(Message msg) {
        try {
            if (validateRequiredProperties(msg.getProperties())) {
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

    static boolean validateRequiredProperties(Map<String, String> properties) {
        final List<String> requiredProperties = Arrays.asList(
                TransitdataProperties.KEY_ROUTE_NAME,
                TransitdataProperties.KEY_DIRECTION,
                TransitdataProperties.KEY_START_TIME,
                TransitdataProperties.KEY_OPERATING_DAY,
                TransitdataProperties.KEY_DVJ_ID
        );

        if (properties == null) {
            log.error("Message has no properties");
            return false;
        }
        for (String p: requiredProperties) {
            if (!properties.containsKey(p)) {
                log.error("Message is missing required property " + p);
                return false;
            }
        }

        if (!ProcessorUtils.validateRouteName(properties.get(TransitdataProperties.KEY_ROUTE_NAME))) {
            return false;
        }

        //Filter out trains. Currently route IDs for trains are 3001 and 3002.
        Pattern trainPattern = Pattern.compile("^300(1|2)");
        if (trainPattern.matcher(properties.get(TransitdataProperties.KEY_ROUTE_NAME)).find()) {
            return false;
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
        if (!common.hasTargetUtcDateTimeMs()) {
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
