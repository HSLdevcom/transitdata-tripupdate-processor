package fi.hsl.transitdata.tripupdate.processing;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.transitdata.tripupdate.application.AbstractMessageProcessor;
import org.apache.pulsar.client.api.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class StopEstimateProcessor extends AbstractMessageProcessor {
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
    public boolean validateMessage(byte[] payload) {
        try {
            InternalMessages.StopEstimate data = InternalMessages.StopEstimate.parseFrom(payload);
            String route = data.getTripInfo().getRouteId();
            int direction = data.getTripInfo().getDirectionId();

            return validateTripData(route, direction);
        }
        catch (InvalidProtocolBufferException e) {
            log.error("Failed to parse StopEstimate from message payload", e);
            return false;
        }
    }
}
