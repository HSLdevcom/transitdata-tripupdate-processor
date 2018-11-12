package fi.hsl.transitdata.tripupdate.application;

import com.google.transit.realtime.GtfsRealtime;
import org.apache.pulsar.client.api.Message;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ITTripUpdateProcessor extends ITBaseTripUpdateProcessor {

    @Test
    public void testCancellation() throws Exception {
        TestLogic logic = new TestLogic() {
            @Override
            public void testImpl(TestContext context) throws Exception {
                final String dvjId = "1234567890";
                final String route = "757";
                final int direction = 1;
                final String date = "2020-12-24";
                final String time = "18:00:00";
                final LocalDateTime dateTime = LocalDateTime.parse(date + "T" + time);
                final long now = System.currentTimeMillis();

                ITMockDataSource.SourceMessage msg = ITMockDataSource.newCancellationMessage(dvjId, route, direction, dateTime, now);
                ITMockDataSource.sendPulsarMessage(context.source, msg);
                logger.info("Message sent, reading it back");

                Message<byte[]> received = context.sink.receive(5, TimeUnit.SECONDS);
                assertNotNull(received);

                validatePulsarProperties(received, dvjId, now);

                GtfsRealtime.FeedMessage feedMessage = GtfsRealtime.FeedMessage.parseFrom(received.getData());
                validateCancellationPayload(feedMessage, dvjId, now, route, direction, date.replace("-", ""), time);
                logger.info("Message read back, all good");

                assertEquals(1, context.source.getStats().getNumAcksReceived());
            }
        };
        testPulsar(logic);
    }

    private void validateCancellationPayload(GtfsRealtime.FeedMessage feedMessage,
                                     String dvjId, long eventTime, String routeId, int direction,
                                     String date, String time) {
        assertNotNull(feedMessage);
        assertEquals(1, feedMessage.getEntityCount());
        GtfsRealtime.TripUpdate tripUpdate = feedMessage.getEntity(0).getTripUpdate();
        assertEquals(0, tripUpdate.getStopTimeUpdateCount());
        assertEquals(eventTime, tripUpdate.getTimestamp());

        GtfsRealtime.TripDescriptor trip = tripUpdate.getTrip();
        assertEquals(GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED, trip.getScheduleRelationship());
        assertEquals(routeId, trip.getRouteId());
        assertEquals(direction, trip.getDirectionId());
        assertEquals(date, trip.getStartDate());
        assertEquals(time, trip.getStartTime());
    }

}
