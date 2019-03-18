package fi.hsl.transitdata.tripupdate.application;

import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.pulsar.*;
import fi.hsl.common.transitdata.MockDataUtils;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.proto.InternalMessages;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.*;

public class ITTripUpdateProcessor extends ITBaseTestSuite {

    final static DateTimeFormatter gtfsRtDatePattern = DateTimeFormatter.ofPattern("yyyyMMdd");
    final static DateTimeFormatter gtfsRtTimePattern =  DateTimeFormatter.ofPattern("HH:mm:ss");

    final long dvjId = 1234567890L;
    final String route = "7575";
    final int joreDirection = 2;
    final int gtfsRtDirection = 1;
    final String date = "2020-12-24";
    final String time = "18:00:00";
    final LocalDateTime dateTime = LocalDateTime.parse(date + "T" + time);
    final int stopId = 0;


    @Test
    public void testValidCancellationWithDirection1() throws Exception {
        final String testId = "-valid-cancel-joredir-1";
        testValidCancellation(route, route,1, testId);
    }

    @Test
    public void testValidCancellationWithDirection2() throws Exception {
        final String testId = "-valid-cancel-joredir-2";
        testValidCancellation(route, route,2, testId);
    }

    @Test
    public void testValidCancellationRouteNameFormatting() throws Exception {
        final String testId = "-valid-cancel-routename-formatting";
        String joreRouteName = "4250D8";
        String formattedRouteName = "4250D";
        testValidCancellation(joreRouteName, formattedRouteName, joreDirection, testId);
    }

    private void testValidCancellation(String routeName, String expectedRouteName, int joreDir, String testId) throws Exception {
        int gtfsDir = joreDir - 1;
        TestPipeline.TestLogic logic = new TestPipeline.TestLogic() {
            @Override
            public void testImpl(TestPipeline.TestContext context) throws Exception {
                final long ts = System.currentTimeMillis();
                InternalMessages.TripCancellation cancellation = MockDataUtils.mockTripCancellation(routeName, joreDir, dateTime);
                sendCancellationPulsarMessage(context.source, dvjId, ts, cancellation);
                logger.info("Message sent, reading it back");

                Message<byte[]> received = TestPipeline.readOutputMessage(context);
                assertNotNull(received);

                validatePulsarProperties(received, Long.toString(dvjId), ts, TransitdataProperties.ProtobufSchema.GTFS_TripUpdate);

                GtfsRealtime.FeedMessage feedMessage = GtfsRealtime.FeedMessage.parseFrom(received.getData());
                validateCancellationPayload(feedMessage, ts, expectedRouteName, gtfsDir, dateTime);
                logger.info("Message read back, all good");

                TestPipeline.validateAcks(1, context);
            }
        };
        PulsarApplication testApp = createPulsarApp("integration-test.conf", testId);
        IMessageHandler handlerToTest = new MessageRouter(testApp.getContext());
        testPulsarMessageHandler(handlerToTest, testApp, logic, testId);
    }

    @Test
    public void testCancellationWithGtfsRtDirection() throws Exception {
        //InternalMessages are in Jore format 1-2, gtfs-rt in 0-1
        final int invalidJoreDirection = 0;
        InternalMessages.TripCancellation cancellation = MockDataUtils.mockTripCancellation(route, invalidJoreDirection, dateTime);
        testInvalidInput(cancellation, "-test-gtfs-dir");
    }

    @Test
    public void testCancellationWithInvalidDirection() throws Exception {
        //InternalMessages are in Jore format 1-2, gtfs-rt in 0-1
        InternalMessages.TripCancellation cancellation = MockDataUtils.mockTripCancellation(route, 10, dateTime);
        testInvalidInput(cancellation, "-test-invalid-dir");
    }

    @Test
    public void testCancellationWithRunningStatus() throws Exception {
        final InternalMessages.TripCancellation.Status runningStatus = InternalMessages.TripCancellation.Status.RUNNING;
        InternalMessages.TripCancellation cancellation = MockDataUtils.mockTripCancellation(route, 10, dateTime, runningStatus);
        testInvalidInput(cancellation, "-test-running");
    }

    @Test
    public void testCancellationWithTrainRoute() throws Exception {
        String trainRoute = "3001";
        InternalMessages.TripCancellation cancellation = MockDataUtils.mockTripCancellation(trainRoute, joreDirection, dateTime);
        testInvalidInput(cancellation, "-test-train-route");
    }

    /**
     * Convenience method for running tests that should always be filtered by the TripUpdateProcessor,
     * meaning no Gtfs-RT should ever be received. However we should get an ack to producer.
     *
     * @throws Exception
     */
    private void testInvalidInput(final InternalMessages.TripCancellation cancellation, String testId) throws Exception {
        TestPipeline.TestLogic logic = new TestPipeline.TestLogic() {
            @Override
            public void testImpl(TestPipeline.TestContext context) throws Exception {
                final long ts = System.currentTimeMillis();
                sendCancellationPulsarMessage(context.source, dvjId, ts, cancellation);
                logger.info("Message sent, reading it back");

                Message<byte[]> received = TestPipeline.readOutputMessage(context);
                //There should not be any output, read should just timeout
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
                                             final int gtfsRtDirection,
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
        assertEquals(gtfsRtDirection, trip.getDirectionId());

        String localDate = gtfsRtDatePattern.format(eventTimeLocal);
        String localTime = gtfsRtTimePattern.format(eventTimeLocal);

        assertEquals(localDate, trip.getStartDate());
        assertEquals(localTime, trip.getStartTime());
    }

    @Test
    public void testValidArrivalStopEvent() throws Exception {
        int startTimeOffsetInSeconds = 5 * 60;
        ITMockDataSource.ArrivalSourceMessage msg = ITMockDataSource.newArrivalMessage(startTimeOffsetInSeconds, dvjId, route, joreDirection, stopId);
        testValidStopEvent(msg);
    }

    @Test
    public void testValidDepartureStopEvent() throws Exception {
        int startTimeOffsetInSeconds = 5 * 60;
        ITMockDataSource.DepartureSourceMessage msg = ITMockDataSource.newDepartureMessage(startTimeOffsetInSeconds, dvjId, route, joreDirection, stopId);
        testValidStopEvent(msg);
    }

    private void testValidStopEvent(ITMockDataSource.SourceMessage sourceMsg) throws Exception {
        TestPipeline.TestLogic logic = new TestPipeline.TestLogic() {
            @Override
            public void testImpl(TestPipeline.TestContext context) throws Exception {

                sendPulsarMessage(context.source, sourceMsg);
                logger.info("Message sent, reading it back");

                Message<byte[]> received = TestPipeline.readOutputMessage(context);
                assertNotNull(received);

                validatePulsarProperties(received, Long.toString(dvjId), sourceMsg.timestamp, TransitdataProperties.ProtobufSchema.GTFS_TripUpdate);

                GtfsRealtime.FeedMessage feedMessage = GtfsRealtime.FeedMessage.parseFrom(received.getData());
                assertNotNull(feedMessage);
                assertEquals(1, feedMessage.getEntityCount());
                GtfsRealtime.TripUpdate tu = feedMessage.getEntity(0).getTripUpdate();
                assertNotNull(tu);

                assertEquals(1, tu.getStopTimeUpdateCount());
                GtfsRealtime.TripUpdate.StopTimeUpdate update = tu.getStopTimeUpdate(0);
                assertNotNull(update);

                assertIfArrivalAndDepartureDiffer(update);

                assertEquals(Integer.toString(stopId), update.getStopId());

                assertTrue(tu.hasTrip());
                GtfsRealtime.TripDescriptor tripDescriptor = tu.getTrip();
                assertEquals(gtfsRtDirection, tripDescriptor.getDirectionId());
                assertEquals(route, tripDescriptor.getRouteId());

                logger.info("Message read back, all good");
                TestPipeline.validateAcks(1, context);
            }
        };

        final String testId = "-test-valid-stop";
        PulsarApplication testApp = createPulsarApp("integration-test.conf", testId);
        IMessageHandler handlerToTest = new MessageRouter(testApp.getContext());
        testPulsarMessageHandler(handlerToTest, testApp, logic, testId);
    }

    private void assertIfArrivalAndDepartureDiffer(GtfsRealtime.TripUpdate.StopTimeUpdate update) {
        // We currently always have both StopTimeUpdates (arrival and departure) for OpenTripPlanner,
        // If only one of them is sent we add identical one to the other field
        assertTrue(update.hasArrival());
        assertTrue(update.hasDeparture());
        assertEquals(update.getArrival(), update.getDeparture());
    }

    @Test
    public void testViaPointStopEvent() throws Exception {
        int startTimeOffsetInSeconds = 5 * 60;
        ITMockDataSource.ArrivalSourceMessage msg = ITMockDataSource.newArrivalMessage(startTimeOffsetInSeconds, dvjId, route, joreDirection, stopId, true);
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
        ITMockDataSource.ArrivalSourceMessage arrival = ITMockDataSource.newArrivalMessage(0, dvjId, route, joreDirection, stopId);
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

    static void sendCancellationPulsarMessage(Producer<byte[]> producer, long dvjId, long timestampEpochMs, InternalMessages.TripCancellation cancellation) throws PulsarClientException {
       sendPulsarMessage(producer, dvjId, cancellation.toByteArray(), timestampEpochMs, TransitdataProperties.ProtobufSchema.InternalMessagesTripCancellation);
    }

    static void sendPulsarMessage(Producer<byte[]> producer, long dvjId, byte[] payload, long timestampEpochMs, TransitdataProperties.ProtobufSchema schema) throws PulsarClientException {
        String dvjIdAsString = Long.toString(dvjId);
        TypedMessageBuilder<byte[]> builder = producer.newMessage().value(payload)
                .eventTime(timestampEpochMs)
                .key(dvjIdAsString)
                .property(TransitdataProperties.KEY_DVJ_ID, dvjIdAsString)
                .property(TransitdataProperties.KEY_PROTOBUF_SCHEMA, schema.toString());

        //msg.props.forEach((key, value) -> builder.property(key, value));
        builder.send();
    }
}
