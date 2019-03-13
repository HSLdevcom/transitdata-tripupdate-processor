package fi.hsl.transitdata.tripupdate.processing;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.common.transitdata.proto.PubtransTableProtos;
import fi.hsl.transitdata.tripupdate.application.IMessageProcessor;
import fi.hsl.transitdata.tripupdate.models.PubtransData;
import fi.hsl.transitdata.tripupdate.models.StopEvent;
import org.apache.pulsar.client.api.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public abstract class BaseProcessor implements IMessageProcessor {
    protected static final Logger log = LoggerFactory.getLogger(BaseProcessor.class);

    final EventType eventType;

    final TripUpdateProcessor tripProcessor;

    public enum EventType {
        Arrival, Departure
    }

    public BaseProcessor(EventType eventType, TripUpdateProcessor tripProcessor) {
        this.eventType = eventType;
        this.tripProcessor = tripProcessor;
    }

    /**
     * Because the proto-classes don't have a common base class we need to extract the 'shared'-data with concrete implementations
     */
    protected abstract PubtransData parseSharedData(Message msg) throws InvalidProtocolBufferException;

    @Override
    public GtfsRealtime.TripUpdate processMessage(Message msg) {

        GtfsRealtime.TripUpdate tripUpdate = null;

        try {
            PubtransData data = parseSharedData(msg);

            // Create TripUpdate and send it out
            tripUpdate = tripProcessor.processStopEvent(data.toStopEstimate());
        }
        catch (InvalidProtocolBufferException e) {
            log.error("Failed to parse message payload", e);
        }

        return tripUpdate;
    }

    @Override
    public boolean validateMessage(Message msg) {
        try {
            if (validateRequiredProperties(msg.getProperties())) {
                PubtransData data = parseSharedData(msg);
                return validate(data);
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
        return properties != null && properties.containsKey(TransitdataProperties.KEY_DVJ_ID);
    }

    protected boolean validate(PubtransData data) {
        PubtransTableProtos.Common common = data.common;
        PubtransTableProtos.DOITripInfo tripInfo = data.tripInfo;
        return validateCommon(common) && validateTripInfo(tripInfo);
    }

    protected boolean validateCommon(PubtransTableProtos.Common common) {
        if (common == null) {
            log.error("No Common, discarding message");
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
            log.info("Event is for a via point, message discarded");
            return false;
        }
        return true;
    }

    protected boolean validateTripInfo(PubtransTableProtos.DOITripInfo tripInfo) {
        if (tripInfo == null) {
            log.error("No tripInfo, discarding message");
            return false;
        }

        if (!tripInfo.hasRouteId()) {
            log.error("TripInfo has no RouteId, discarding message");
            return false;
        }

        final String routeName = tripInfo.getRouteId();
        if (!ProcessorUtils.validateRouteName(routeName)) {
            log.warn("Invalid route name {}, discarding message", routeName);
            return false;
        }

        if (ProcessorUtils.isTrainRoute(routeName)) {
            log.info("Route {} is for trains, discarding message", routeName);
            return false;
        }

        return true;
    }
}
