package fi.hsl.transitdata.tripupdate.processing;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.common.transitdata.proto.PubtransTableProtos;
import fi.hsl.transitdata.tripupdate.application.IMessageProcessor;
import org.apache.pulsar.client.api.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class StopEstimateProcessor implements IMessageProcessor {
    protected static final Logger log = LoggerFactory.getLogger(StopEstimateProcessor.class);

    final TripUpdateProcessor tripProcessor;

    public StopEstimateProcessor(TripUpdateProcessor tripProcessor) {
        this.tripProcessor = tripProcessor;
    }

    @Override
    public Optional<TripUpdateWithId> processMessage(Message msg) {
        try {
            InternalMessages.StopEstimate data = InternalMessages.StopEstimate.parseFrom(msg.getData());
            final String tripId = data.getTripInfo().getTripId();

            Optional<GtfsRealtime.TripUpdate> maybeTripUpdate = tripProcessor.processStopEstimate(data);
            return maybeTripUpdate.flatMap(tripUpdate ->
                    TripUpdateWithId.newInstance(tripId, tripUpdate)
            );
        }
        catch (Exception e) {
            log.error("Failed to parse message payload", e);
            return Optional.empty();
        }
    }

    @Override
    public boolean validateMessage(Message msg) {
        try {
            InternalMessages.StopEstimate data = InternalMessages.StopEstimate.parseFrom(msg.getData());
            return validate(data);
        }
        catch (InvalidProtocolBufferException e) {
            log.error("Failed to parse StopEstimate from message payload", e);
            return false;
        }
    }

    protected boolean validate(InternalMessages.StopEstimate data) {
        /*PubtransTableProtos.Common common = data.common;
        PubtransTableProtos.DOITripInfo tripInfo = data.tripInfo;
        return validateCommon(common) && validateTripInfo(tripInfo);*/
        return true;
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
