package fi.hsl.transitdata.tripupdate.application;

import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.transitdata.proto.InternalMessages;
import org.apache.pulsar.client.api.Message;
import org.junit.Test;

import java.time.LocalDateTime;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class ITTripUpdateProcessor extends ITBaseTripUpdateProcessor {

    final long dvjId = 1234567890L;
    final String route = "7575";
    final int direction = 2;
    final String date = "2020-12-24";
    final String time = "18:00:00";
    final LocalDateTime dateTime = LocalDateTime.parse(date + "T" + time);
    final int stopId = 0;

    @Test
    public void testValidCancellation() throws Exception {
        TestLogic logic = new TestLogic() {
            @Override
            public void testImpl(TestContext context) throws Exception {
                ITMockDataSource.CancellationSourceMessage msg = ITMockDataSource.newCancellationMessage(dvjId, route, direction, dateTime);
                ITMockDataSource.sendPulsarMessage(context.source, msg);
                logger.info("Message sent, reading it back");

                Message<byte[]> received = readOutputMessage(context);
                assertNotNull(received);

                validatePulsarProperties(received, dvjId, msg.timestamp);

                GtfsRealtime.FeedMessage feedMessage = GtfsRealtime.FeedMessage.parseFrom(received.getData());
                validateCancellationPayload(feedMessage, msg.timestamp, route, direction, dateTime);
                logger.info("Message read back, all good");

                validateAcks(1, context);
            }
        };
        testPulsar(logic, "-test-cancel");
    }

    @Test
    public void testCancellationWithGtfsRtDirection() throws Exception {
        //InternalMessages are in Jore format 1-2, gtfs-rt in 0-1
        ITMockDataSource.CancellationSourceMessage msg = ITMockDataSource.newCancellationMessage(dvjId, route, 0, dateTime);
        testInvalidInput(msg, "-test-gtfs-dir");
    }

    @Test
    public void testCancellationWithInvalidDirection() throws Exception {
        //InternalMessages are in Jore format 1-2, gtfs-rt in 0-1
        ITMockDataSource.CancellationSourceMessage msg = ITMockDataSource.newCancellationMessage(dvjId, route, 10, dateTime);
        testInvalidInput(msg, "-test-invalid-dir");
    }

    @Test
    public void testCancellationWithRunningStatus() throws Exception {
        final InternalMessages.TripCancellation.Status runningStatus = InternalMessages.TripCancellation.Status.RUNNING;
        ITMockDataSource.CancellationSourceMessage msg = ITMockDataSource.newCancellationMessage(dvjId, route, direction, dateTime, runningStatus);
        testInvalidInput(msg, "-test-running");
    }

    /**
     * Convenience method for running tests that should always be filtered by the TripUpdateProcessor,
     * meaning no Gtfs-RT should ever be received. However we should get an ack to producer.
     *
     * @throws Exception
     */
    private void testInvalidInput(final ITMockDataSource.SourceMessage somethingWrongWithPayload, String testId) throws Exception {
        TestLogic logic = new TestLogic() {
            @Override
            public void testImpl(TestContext context) throws Exception {
                ITMockDataSource.sendPulsarMessage(context.source, somethingWrongWithPayload);
                logger.info("Message sent, reading it back");

                Message<byte[]> received = readOutputMessage(context);
                //There should not be any output for Running-status since it's not supported yet
                assertNull(received);
                validateAcks(1, context); //sender should still get acks.
            }
        };
        testPulsar(logic, testId);
    }


    private void validateAcks(int numberOfMessagesSent, TestContext context) {
        assertEquals(numberOfMessagesSent, context.source.getStats().getNumAcksReceived());
    }

    private void validateCancellationPayload(final GtfsRealtime.FeedMessage feedMessage,
                                             final long eventTimeMs,
                                             final String routeId,
                                             final int direction,
                                             final LocalDateTime eventTimeLocal) {
        assertNotNull(feedMessage);
        assertEquals(1, feedMessage.getEntityCount());

        final long gtfsRtEventTime = eventTimeMs / 1000;//GTFS-RT outputs seconds.
        assertEquals(gtfsRtEventTime, feedMessage.getHeader().getTimestamp());

        final GtfsRealtime.TripUpdate tripUpdate = feedMessage.getEntity(0).getTripUpdate();
        assertEquals(0, tripUpdate.getStopTimeUpdateCount());
        assertEquals(gtfsRtEventTime, tripUpdate.getTimestamp());

        final GtfsRealtime.TripDescriptor trip = tripUpdate.getTrip();
        assertEquals(GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED, trip.getScheduleRelationship());
        assertEquals(routeId, trip.getRouteId());
        assertEquals(direction, trip.getDirectionId());

        String localDate = gtfsRtDatePattern.format(eventTimeLocal);
        String localTime = gtfsRtTimePattern.format(eventTimeLocal);

        assertEquals(localDate, trip.getStartDate());
        assertEquals(localTime, trip.getStartTime());
    }


    @Test
    public void testValidStopEvent() throws Exception {
        TestLogic logic = new TestLogic() {
            @Override
            public void testImpl(TestContext context) throws Exception {
                int startTimeOffsetInSeconds = 5 * 60;
                ITMockDataSource.ArrivalSourceMessage msg = ITMockDataSource.newArrivalMessage(startTimeOffsetInSeconds, dvjId, route, direction, stopId);
                ITMockDataSource.sendPulsarMessage(context.source, msg);
                logger.info("Message sent, reading it back");

                Message<byte[]> received = readOutputMessage(context);
                assertNotNull(received);

                validatePulsarProperties(received, dvjId, msg.timestamp);

                GtfsRealtime.FeedMessage feedMessage = GtfsRealtime.FeedMessage.parseFrom(received.getData());
                assertNotNull(feedMessage);
                //validateCancellationPayload(feedMessage, dvjId, msg.timestamp, route, direction, dateTime);
                logger.info("Message read back, all good");
                validateAcks(1, context);
            }
        };
        testPulsar(logic, "-test-valid-stop");
    }

    @Test
    public void testViaPointStopEvent() throws Exception {
        int startTimeOffsetInSeconds = 5 * 60;
        ITMockDataSource.ArrivalSourceMessage msg = ITMockDataSource.newArrivalMessage(startTimeOffsetInSeconds, dvjId, route, direction, stopId, true);
        testInvalidInput(msg,"-test-viapoint");
    }
}
