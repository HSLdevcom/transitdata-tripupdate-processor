package fi.hsl.transitdata.tripupdate.application;

import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.pulsar.*;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.transitdata.tripupdate.gtfsrt.GtfsRtFactory;
import org.apache.pulsar.client.api.Message;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static fi.hsl.common.pulsar.TestPipeline.readOutputMessage;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class ITTripUpdateProcessor extends ITBaseTestSuite {

    final static DateTimeFormatter gtfsRtDatePattern = DateTimeFormatter.ofPattern("yyyyMMdd");
    final static DateTimeFormatter gtfsRtTimePattern =  DateTimeFormatter.ofPattern("HH:mm:ss");

    final long dvjId = 1234567890L;
    final String route = "7575";
    final int direction = 2;
    final String date = "2020-12-24";
    final String time = "18:00:00";
    final LocalDateTime dateTime = LocalDateTime.parse(date + "T" + time);
    final int stopId = 0;

    @Test
    public void testValidCancellation() throws Exception {
        TestPipeline.TestLogic logic = new TestPipeline.TestLogic() {
            @Override
            public void testImpl(TestPipeline.TestContext context) throws Exception {
                ITMockDataSource.CancellationSourceMessage msg = ITMockDataSource.newCancellationMessage(dvjId, route, direction, dateTime);
                ITMockDataSource.sendPulsarMessage(context.source, msg);
                logger.info("Message sent, reading it back");

                Message<byte[]> received = TestPipeline.readOutputMessage(context);
                assertNotNull(received);

                validatePulsarProperties(received, Long.toString(dvjId), msg.timestamp, TransitdataProperties.ProtobufSchema.GTFS_TripUpdate);

                GtfsRealtime.FeedMessage feedMessage = GtfsRealtime.FeedMessage.parseFrom(received.getData());
                validateCancellationPayload(feedMessage, msg.timestamp, route, direction, dateTime);
                logger.info("Message read back, all good");

                TestPipeline.validateAcks(1, context);
            }
        };
        final String testId = "-test-cancel";
        PulsarApplication testApp = createPulsarApp("integration-test.conf", testId);
        IMessageHandler handlerToTest = new MessageRouter(testApp.getContext());
        testPulsarMessageHandler(handlerToTest, testApp, logic, testId);
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
        TestPipeline.TestLogic logic = new TestPipeline.TestLogic() {
            @Override
            public void testImpl(TestPipeline.TestContext context) throws Exception {
                ITMockDataSource.sendPulsarMessage(context.source, somethingWrongWithPayload);
                logger.info("Message sent, reading it back");

                Message<byte[]> received = TestPipeline.readOutputMessage(context);
                //There should not be any output for Running-status since it's not supported yet
                assertNull(received);
                TestPipeline.validateAcks(1, context); //sender should still get acks.
            }
        };
        PulsarApplication testApp = createPulsarApp("integration-test.conf", testId);
        IMessageHandler handlerToTest = new MessageRouter(testApp.getContext());
        testPulsarMessageHandler(handlerToTest, testApp, logic, testId);
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
        TestPipeline.TestLogic logic = new TestPipeline.TestLogic() {
            @Override
            public void testImpl(TestPipeline.TestContext context) throws Exception {
                int startTimeOffsetInSeconds = 5 * 60;
                ITMockDataSource.ArrivalSourceMessage msg = ITMockDataSource.newArrivalMessage(startTimeOffsetInSeconds, dvjId, route, direction, stopId);
                ITMockDataSource.sendPulsarMessage(context.source, msg);
                logger.info("Message sent, reading it back");

                Message<byte[]> received = TestPipeline.readOutputMessage(context);
                assertNotNull(received);

                validatePulsarProperties(received, Long.toString(dvjId), msg.timestamp, TransitdataProperties.ProtobufSchema.GTFS_TripUpdate);

                GtfsRealtime.FeedMessage feedMessage = GtfsRealtime.FeedMessage.parseFrom(received.getData());
                assertNotNull(feedMessage);
                //validateCancellationPayload(feedMessage, dvjId, msg.timestamp, route, direction, dateTime);
                logger.info("Message read back, all good");
                TestPipeline.validateAcks(1, context);
            }
        };

        final String testId = "-test-valid-stop";
        PulsarApplication testApp = createPulsarApp("integration-test.conf", testId);
        IMessageHandler handlerToTest = new MessageRouter(testApp.getContext());
        testPulsarMessageHandler(handlerToTest, testApp, logic, testId);
    }

    @Test
    public void testViaPointStopEvent() throws Exception {
        int startTimeOffsetInSeconds = 5 * 60;
        ITMockDataSource.ArrivalSourceMessage msg = ITMockDataSource.newArrivalMessage(startTimeOffsetInSeconds, dvjId, route, direction, stopId, true);
        testInvalidInput(msg,"-test-viapoint");
    }

    @Test
    public void testGarbageInput() throws Exception {
        final String testId = "-test-garbage-input";
        //TripUpdateProcessor should handle garbage data and process only the relevant messages
        ArrayList<PulsarMessageData> input = new ArrayList<>();
        ArrayList<PulsarMessageData> expectedOutput = new ArrayList<>();

        //Add garbage which should just be ignored
        PulsarMessageData nullData = new PulsarMessageData("".getBytes(), System.currentTimeMillis());
        input.add(nullData);
        PulsarMessageData dummyData = new PulsarMessageData("dummy-content".getBytes(), System.currentTimeMillis(), "invalid-key");
        input.add(dummyData);

        //Then a real message
        ITMockDataSource.ArrivalSourceMessage arrival = ITMockDataSource.newArrivalMessage(0, dvjId, route, direction, stopId);
        Map<String, String> properties = arrival.props;
        properties.put(TransitdataProperties.KEY_DVJ_ID, Long.toString(arrival.dvjId));
        properties.put(TransitdataProperties.KEY_PROTOBUF_SCHEMA, arrival.schema.toString());
        PulsarMessageData validMsg = new PulsarMessageData(arrival.payload, arrival.timestamp, Long.toString(arrival.dvjId), properties);
        input.add(validMsg);
        GtfsRealtime.FeedMessage asFeedMessage = arrival.toGtfsRt();
        //Expected output is GTFS-RT TripUpdate
        Map<String, String> outputProperties = new HashMap<>();
        outputProperties.put(TransitdataProperties.KEY_PROTOBUF_SCHEMA, TransitdataProperties.ProtobufSchema.GTFS_TripUpdate.toString());
        PulsarMessageData validOutput = new PulsarMessageData(asFeedMessage.toByteArray(), arrival.timestamp, Long.toString(arrival.dvjId), outputProperties);
        expectedOutput.add(validOutput);

        TestPipeline.MultiMessageTestLogic logic = new TestPipeline.MultiMessageTestLogic(input, expectedOutput) {
            @Override
            public void validateMessage(PulsarMessageData expected, PulsarMessageData received) {
                try {
                    assertNotNull(expected);
                    assertNotNull(received);
                    assertEquals(TransitdataProperties.ProtobufSchema.GTFS_TripUpdate.toString(), received.properties.get(TransitdataProperties.KEY_PROTOBUF_SCHEMA));
                    assertEquals(Long.toString(arrival.dvjId), received.key.get());
                    assertEquals(arrival.timestamp, (long)received.eventTime.get());

                    GtfsRealtime.FeedEntity entity = GtfsRealtime.FeedEntity.parseFrom(received.payload);
                    assertNotNull(entity);

                }
                catch (Exception e) {
                    logger.error("Failed to validate message", e);
                    assert(false);
                }
            }
        };

        PulsarApplication testApp = createPulsarApp("integration-test.conf", testId);
        IMessageHandler handlerToTest = new MessageRouter(testApp.getContext());
        testPulsarMessageHandler(handlerToTest, testApp, logic, testId);
    }
}
