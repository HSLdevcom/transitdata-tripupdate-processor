package fi.hsl.transitdata.tripupdate.processing;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.transitdata.proto.InternalMessages;
import org.apache.pulsar.client.api.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class TripCancellationProcessor extends AbstractMessageProcessor {

    private static final Logger log = LoggerFactory.getLogger(TripCancellationProcessor.class);

    private final TripUpdateProcessor tripUpdateProcessor;

    public TripCancellationProcessor(TripUpdateProcessor tripUpdateProcessor) {
        this.tripUpdateProcessor = tripUpdateProcessor;
    }

    @Override
    public boolean validateMessage(byte[] payload) {

        try {
            InternalMessages.TripCancellation tripCancellation = InternalMessages.TripCancellation.parseFrom(payload);

            if (tripCancellation.hasDirectionId() && tripCancellation.hasRouteId() &&
                tripCancellation.hasStartDate() && tripCancellation.hasStartTime()) {

                String route = tripCancellation.getRouteId();
                int directionId = tripCancellation.getDirectionId();

                return validateTripData(route, directionId);
            }
        } catch (InvalidProtocolBufferException e) {
            log.error("TripCancellation message could not be parsed: " + e.getMessage());
        }
        return false;
    }

    @Override
    public Optional<TripUpdateWithId> processMessage(Message msg) {
        try {
            InternalMessages.TripCancellation tripCancellation = InternalMessages.TripCancellation.parseFrom(msg.getData());
            final String tripId = tripCancellation.getTripId();

            GtfsRealtime.TripUpdate tripUpdate = tripUpdateProcessor.processTripCancellation(msg.getKey(), msg.getEventTime(), tripCancellation);
            return TripUpdateWithId.newInstance(tripId, tripUpdate);
        } catch (Exception e) {
            log.error("Could not parse TripCancellation: " + e.getMessage(), e);
            return Optional.empty();
        }
    }

}
