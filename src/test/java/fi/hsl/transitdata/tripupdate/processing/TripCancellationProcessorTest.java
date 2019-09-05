package fi.hsl.transitdata.tripupdate.processing;

import fi.hsl.common.transitdata.MockDataUtils;
import fi.hsl.common.transitdata.PubtransFactory;
import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.common.transitdata.proto.PubtransTableProtos;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TripCancellationProcessorTest {
    private TripIdCache emptyTripIdCache = new TripIdCache(null) {
        @Override
        public Optional<String> getTripId(String routeId, String operatingDay, String startTime, int directionId) {
            return Optional.empty();
        }
    };

    @Test
    public void messageWithWrongPayloadIsDiscarded() throws Exception {
        PubtransTableProtos.ROIArrival arrival = MockDataUtils.mockROIArrival(MockDataUtils.generateValidJoreId(),
                MockDataUtils.generateValidRouteName(),
                System.currentTimeMillis());
        TripCancellationProcessor proc = new TripCancellationProcessor(null, emptyTripIdCache);

        assertFalse(proc.validateMessage(arrival.toByteArray()));
    }


    @Test
    public void messageWithValidPayloadIsAccepted() throws Exception {
        final boolean shouldPass = true;
        final String routeName = "1014";

        testValidationForTripCancellation(shouldPass, routeName, PubtransFactory.JORE_DIRECTION_ID_INBOUND, InternalMessages.StopEstimate.Type.ARRIVAL);
        testValidationForTripCancellation(shouldPass, routeName, PubtransFactory.JORE_DIRECTION_ID_INBOUND, InternalMessages.StopEstimate.Type.DEPARTURE);
        testValidationForTripCancellation(shouldPass, routeName, PubtransFactory.JORE_DIRECTION_ID_OUTBOUND, InternalMessages.StopEstimate.Type.ARRIVAL);
        testValidationForTripCancellation(shouldPass, routeName, PubtransFactory.JORE_DIRECTION_ID_OUTBOUND, InternalMessages.StopEstimate.Type.DEPARTURE);
    }


    private void testValidationForTripCancellation(boolean shouldPass, String routeName, int direction, InternalMessages.StopEstimate.Type eventType) throws Exception {
        long dvjId = MockDataUtils.generateValidJoreId();

        LocalDateTime someOperatingTime = Instant.now().plus(Duration.ofHours(5)).atOffset(ZoneOffset.UTC).toLocalDateTime();
        InternalMessages.TripCancellation cancellation = MockDataUtils.mockTripCancellation(dvjId, routeName, direction, someOperatingTime);

        TripCancellationProcessor proc = new TripCancellationProcessor(null, emptyTripIdCache);

        assertEquals(shouldPass, proc.validateMessage(cancellation.toByteArray()));
    }

    @Test
    public void messageForTrainRouteKIsDiscarded() throws Exception {

        final boolean shouldPass = false;
        final String routeName = "3001K";

        testValidationForTripCancellation(shouldPass, routeName, PubtransFactory.JORE_DIRECTION_ID_INBOUND, InternalMessages.StopEstimate.Type.ARRIVAL);
        testValidationForTripCancellation(shouldPass, routeName, PubtransFactory.JORE_DIRECTION_ID_INBOUND, InternalMessages.StopEstimate.Type.DEPARTURE);
        testValidationForTripCancellation(shouldPass, routeName, PubtransFactory.JORE_DIRECTION_ID_OUTBOUND, InternalMessages.StopEstimate.Type.ARRIVAL);
        testValidationForTripCancellation(shouldPass, routeName, PubtransFactory.JORE_DIRECTION_ID_OUTBOUND, InternalMessages.StopEstimate.Type.DEPARTURE);

    }


    @Test
    public void messageForTrainRouteUIsDiscarded() throws Exception {

        final boolean shouldPass = false;
        final String routeName = "3002U";

        testValidationForTripCancellation(shouldPass, routeName, PubtransFactory.JORE_DIRECTION_ID_INBOUND, InternalMessages.StopEstimate.Type.ARRIVAL);
        testValidationForTripCancellation(shouldPass, routeName, PubtransFactory.JORE_DIRECTION_ID_INBOUND, InternalMessages.StopEstimate.Type.DEPARTURE);
        testValidationForTripCancellation(shouldPass, routeName, PubtransFactory.JORE_DIRECTION_ID_OUTBOUND, InternalMessages.StopEstimate.Type.ARRIVAL);
        testValidationForTripCancellation(shouldPass, routeName, PubtransFactory.JORE_DIRECTION_ID_OUTBOUND, InternalMessages.StopEstimate.Type.DEPARTURE);
    }


    @Test
    public void messageWithJoreDirectionShouldPassAndOthersBeDiscarded() throws Exception {

        final String routeName = "1014";
        final int gtfsRtDir = 0;

        testValidationForTripCancellation(false, routeName, gtfsRtDir, InternalMessages.StopEstimate.Type.ARRIVAL);
        testValidationForTripCancellation(false, routeName, gtfsRtDir, InternalMessages.StopEstimate.Type.DEPARTURE);

        testValidationForTripCancellation(true, routeName, PubtransFactory.JORE_DIRECTION_ID_INBOUND, InternalMessages.StopEstimate.Type.ARRIVAL);
        testValidationForTripCancellation(true, routeName, PubtransFactory.JORE_DIRECTION_ID_INBOUND, InternalMessages.StopEstimate.Type.DEPARTURE);

        testValidationForTripCancellation(true, routeName, PubtransFactory.JORE_DIRECTION_ID_OUTBOUND, InternalMessages.StopEstimate.Type.ARRIVAL);
        testValidationForTripCancellation(true, routeName, PubtransFactory.JORE_DIRECTION_ID_OUTBOUND, InternalMessages.StopEstimate.Type.DEPARTURE);

        final int tooLargeDir = 3;
        testValidationForTripCancellation(false, routeName, tooLargeDir, InternalMessages.StopEstimate.Type.ARRIVAL);
        testValidationForTripCancellation(false, routeName, tooLargeDir, InternalMessages.StopEstimate.Type.DEPARTURE);

    }

}
