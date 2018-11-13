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

    final String dvjId = "1234567890";
    final String route = "757";
    final int direction = 2;
    final String date = "2020-12-24";
    final String time = "18:00:00";
    final LocalDateTime dateTime = LocalDateTime.parse(date + "T" + time);

    @Test
    public void testValidCancellation() throws Exception {
        TestLogic logic = new TestLogic() {
            @Override
            public void testImpl(TestContext context) throws Exception {
                final long now = System.currentTimeMillis();

                ITMockDataSource.CancellationSourceMessage msg = ITMockDataSource.newCancellationMessage(dvjId, route, direction, dateTime);
                ITMockDataSource.sendPulsarMessage(context.source, msg);
                logger.info("Message sent, reading it back");

                Message<byte[]> received = readOutputMessage(context);
                assertNotNull(received);

                validatePulsarProperties(received, dvjId, now);

                GtfsRealtime.FeedMessage feedMessage = GtfsRealtime.FeedMessage.parseFrom(received.getData());
                validateCancellationPayload(feedMessage, dvjId, now, route, direction, dateTime);
                logger.info("Message read back, all good");

                validateAcks(1, context);
            }
        };
        testPulsar(logic);
    }

    @Test
    public void testCancellationWithGtfsRtDirection() throws Exception {
        //InternalMessages are in Jore format 1-2, gtfs-rt in 0-1
        ITMockDataSource.CancellationSourceMessage msg = ITMockDataSource.newCancellationMessage(dvjId, route, 0, dateTime);
        testInvalidInput(msg);
    }

    @Test
    public void testCancellationWithInvalidDirection() throws Exception {
        //InternalMessages are in Jore format 1-2, gtfs-rt in 0-1
        ITMockDataSource.CancellationSourceMessage msg = ITMockDataSource.newCancellationMessage(dvjId, route, 10, dateTime);
        testInvalidInput(msg);
    }

    @Test
    public void testCancellationWithRunningStatus() throws Exception {
        final InternalMessages.TripCancellation.Status runningStatus = InternalMessages.TripCancellation.Status.RUNNING;
        ITMockDataSource.CancellationSourceMessage msg = ITMockDataSource.newCancellationMessage(dvjId, route, direction, dateTime, runningStatus);
        testInvalidInput(msg);
    }

    /**
     * Convenience method for running tests that should always be filtered by the TripUpdateProcessor,
     * meaning no Gtfs-RT should ever be received. However we should get an ack to producer.
     *
     * @throws Exception
     */
    private void testInvalidInput(final ITMockDataSource.SourceMessage somethingWrongWithPayload) throws Exception {
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
        testPulsar(logic);
    }


    private void validateAcks(int numberOfMessagesSent, TestContext context) {
        assertEquals(numberOfMessagesSent, context.source.getStats().getNumAcksReceived());
    }

    private void validateCancellationPayload(GtfsRealtime.FeedMessage feedMessage,
                                     String dvjId, long eventTime, String routeId, int direction, LocalDateTime eventTimeLocal) {
        assertNotNull(feedMessage);
        assertEquals(1, feedMessage.getEntityCount());
        GtfsRealtime.TripUpdate tripUpdate = feedMessage.getEntity(0).getTripUpdate();
        assertEquals(0, tripUpdate.getStopTimeUpdateCount());
        assertEquals(eventTime, tripUpdate.getTimestamp());

        GtfsRealtime.TripDescriptor trip = tripUpdate.getTrip();
        assertEquals(GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED, trip.getScheduleRelationship());
        assertEquals(routeId, trip.getRouteId());
        assertEquals(direction, trip.getDirectionId());

        String localDate = gtfsRtDatePattern.format(eventTimeLocal);
        String localTime = gtfsRtTimePattern.format(eventTimeLocal);

        assertEquals(localDate, trip.getStartDate());
        assertEquals(localTime, trip.getStartTime());
    }

}
