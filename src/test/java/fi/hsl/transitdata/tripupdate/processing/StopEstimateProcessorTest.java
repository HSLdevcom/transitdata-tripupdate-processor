package fi.hsl.transitdata.tripupdate.processing;

import fi.hsl.common.transitdata.MockDataUtils;
import fi.hsl.common.transitdata.PubtransFactory;
import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.common.transitdata.proto.PubtransTableProtos;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class StopEstimateProcessorTest {

    @Test
    public void messageWithWrongPayloadIsDiscarded() throws Exception {
        PubtransTableProtos.ROIArrival arrival = MockDataUtils.mockROIArrival(MockDataUtils.generateValidJoreId(),
                MockDataUtils.generateValidRouteName(),
                System.currentTimeMillis());
        StopEstimateProcessor proc = new StopEstimateProcessor(null);

        assertFalse(proc.validateMessage(arrival.toByteArray()));
    }


    @Test
    public void messageWithValidPayloadIsAccepted() throws Exception {
        final boolean shouldPass = true;
        final String routeName = "1014";

        testValidationForStopEstimate(shouldPass, routeName, PubtransFactory.JORE_DIRECTION_ID_INBOUND, InternalMessages.StopEstimate.Type.ARRIVAL);
        testValidationForStopEstimate(shouldPass, routeName, PubtransFactory.JORE_DIRECTION_ID_INBOUND, InternalMessages.StopEstimate.Type.DEPARTURE);
        testValidationForStopEstimate(shouldPass, routeName, PubtransFactory.JORE_DIRECTION_ID_OUTBOUND, InternalMessages.StopEstimate.Type.ARRIVAL);
        testValidationForStopEstimate(shouldPass, routeName, PubtransFactory.JORE_DIRECTION_ID_OUTBOUND, InternalMessages.StopEstimate.Type.DEPARTURE);
    }


    private void testValidationForStopEstimate(boolean shouldPass, String routeName, int direction, InternalMessages.StopEstimate.Type eventType) throws Exception {
        long dvjId = MockDataUtils.generateValidJoreId();
        PubtransTableProtos.Common common = MockDataUtils.mockCommon(dvjId).build();
        PubtransTableProtos.DOITripInfo mockTripInfo = MockDataUtils.mockDOITripInfo(dvjId, routeName, direction);
        InternalMessages.StopEstimate estimate = PubtransFactory.createStopEstimate(common, mockTripInfo, eventType);

        StopEstimateProcessor proc = new StopEstimateProcessor(null);

        assertEquals(shouldPass, proc.validateMessage(estimate.toByteArray()));
    }

    @Test
    public void messageForTrainRouteKIsDiscarded() throws Exception {

        final boolean shouldPass = false;
        final String routeName = "3001K";

        testValidationForStopEstimate(shouldPass, routeName, PubtransFactory.JORE_DIRECTION_ID_INBOUND, InternalMessages.StopEstimate.Type.ARRIVAL);
        testValidationForStopEstimate(shouldPass, routeName, PubtransFactory.JORE_DIRECTION_ID_INBOUND, InternalMessages.StopEstimate.Type.DEPARTURE);
        testValidationForStopEstimate(shouldPass, routeName, PubtransFactory.JORE_DIRECTION_ID_OUTBOUND, InternalMessages.StopEstimate.Type.ARRIVAL);
        testValidationForStopEstimate(shouldPass, routeName, PubtransFactory.JORE_DIRECTION_ID_OUTBOUND, InternalMessages.StopEstimate.Type.DEPARTURE);

    }


    @Test
    public void messageForTrainRouteUIsDiscarded() throws Exception {

        final boolean shouldPass = false;
        final String routeName = "3002U";

        testValidationForStopEstimate(shouldPass, routeName, PubtransFactory.JORE_DIRECTION_ID_INBOUND, InternalMessages.StopEstimate.Type.ARRIVAL);
        testValidationForStopEstimate(shouldPass, routeName, PubtransFactory.JORE_DIRECTION_ID_INBOUND, InternalMessages.StopEstimate.Type.DEPARTURE);
        testValidationForStopEstimate(shouldPass, routeName, PubtransFactory.JORE_DIRECTION_ID_OUTBOUND, InternalMessages.StopEstimate.Type.ARRIVAL);
        testValidationForStopEstimate(shouldPass, routeName, PubtransFactory.JORE_DIRECTION_ID_OUTBOUND, InternalMessages.StopEstimate.Type.DEPARTURE);
    }


    @Test
    public void messageWithJoreDirectionShouldPassAndOthersBeDiscarded() throws Exception {

        final String routeName = "1014";
        final int gtfsRtDir = 0;

        testValidationForStopEstimate(false, routeName, gtfsRtDir, InternalMessages.StopEstimate.Type.ARRIVAL);
        testValidationForStopEstimate(false, routeName, gtfsRtDir, InternalMessages.StopEstimate.Type.DEPARTURE);

        testValidationForStopEstimate(true, routeName, PubtransFactory.JORE_DIRECTION_ID_INBOUND, InternalMessages.StopEstimate.Type.ARRIVAL);
        testValidationForStopEstimate(true, routeName, PubtransFactory.JORE_DIRECTION_ID_INBOUND, InternalMessages.StopEstimate.Type.DEPARTURE);

        testValidationForStopEstimate(true, routeName, PubtransFactory.JORE_DIRECTION_ID_OUTBOUND, InternalMessages.StopEstimate.Type.ARRIVAL);
        testValidationForStopEstimate(true, routeName, PubtransFactory.JORE_DIRECTION_ID_OUTBOUND, InternalMessages.StopEstimate.Type.DEPARTURE);

        final int tooLargeDir = 3;
        testValidationForStopEstimate(false, routeName, tooLargeDir, InternalMessages.StopEstimate.Type.ARRIVAL);
        testValidationForStopEstimate(false, routeName, tooLargeDir, InternalMessages.StopEstimate.Type.DEPARTURE);

    }

}
