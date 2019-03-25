package fi.hsl.transitdata.tripupdate.processing;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.transitdata.tripupdate.application.IMessageProcessor;
import org.apache.pulsar.client.api.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class TripCancellationProcessor implements IMessageProcessor {

    private static final Logger log = LoggerFactory.getLogger(TripCancellationProcessor.class);

    private final TripUpdateProcessor tripUpdateProcessor;

    public TripCancellationProcessor(TripUpdateProcessor tripUpdateProcessor) {
        this.tripUpdateProcessor = tripUpdateProcessor;
    }

    @Override
    public boolean validateMessage(Message msg) {

        try {
            InternalMessages.TripCancellation tripCancellation = InternalMessages.TripCancellation.parseFrom(msg.getData());

            if (tripCancellation.hasDirectionId() && tripCancellation.hasRouteId() &&
                tripCancellation.hasStartDate() && tripCancellation.hasStartTime()) {

                boolean valid = true;

                int directionId = tripCancellation.getDirectionId();
                valid &= (directionId == 1 || directionId == 2);

                String route = tripCancellation.getRouteId();
                valid &= ProcessorUtils.validateRouteName(route);
                valid &= !ProcessorUtils.isTrainRoute(route);

                return valid;
            }
        } catch (InvalidProtocolBufferException e) {
            log.error("TripCancellation message could not be parsed: " + e.getMessage());
        }
        return false;
    }

    @Override
    public Optional<GtfsRealtime.TripUpdate> processMessage(Message msg) {
        try {
            InternalMessages.TripCancellation tripCancellation = InternalMessages.TripCancellation.parseFrom(msg.getData());
            return tripUpdateProcessor.processTripCancellation(msg.getEventTime(), tripCancellation);
        } catch (Exception e) {
            log.error("Could not parse TripCancellation: " + e.getMessage(), e);
            return Optional.empty();
        }
    }

}
