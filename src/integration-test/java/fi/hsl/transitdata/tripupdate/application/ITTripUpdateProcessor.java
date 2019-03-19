package fi.hsl.transitdata.tripupdate.application;

import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.gtfsrt.FeedMessageFactory;
import fi.hsl.common.pulsar.*;
import fi.hsl.common.transitdata.MockDataUtils;
import fi.hsl.common.transitdata.PubtransFactory;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.common.transitdata.proto.PubtransTableProtos;
import fi.hsl.transitdata.tripupdate.gtfsrt.GtfsRtFactory;
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
    final int stopSequence = MockDataUtils.generateValidStopSequenceId();

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
                sendPubtransSourcePulsarMessage(context.source, new PubtransPulsarMessageData.CancellationPulsarMessageData(cancellation, ts, dvjId));
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
        PubtransPulsarMessageData data = new PubtransPulsarMessageData.CancellationPulsarMessageData(cancellation, System.currentTimeMillis(), dvjId);

        testInvalidInput(data, "-test-gtfs-dir");
    }

    @Test
    public void testCancellationWithInvalidDirection() throws Exception {
        //InternalMessages are in Jore format 1-2, gtfs-rt in 0-1
        InternalMessages.TripCancellation cancellation = MockDataUtils.mockTripCancellation(route, 10, dateTime);
        PubtransPulsarMessageData data = new PubtransPulsarMessageData.CancellationPulsarMessageData(cancellation, System.currentTimeMillis(), dvjId);
        testInvalidInput(data, "-test-invalid-dir");
    }

    @Test
    public void testCancellationWithRunningStatus() throws Exception {
        final InternalMessages.TripCancellation.Status runningStatus = InternalMessages.TripCancellation.Status.RUNNING;
        InternalMessages.TripCancellation cancellation = MockDataUtils.mockTripCancellation(route, 10, dateTime, runningStatus);
        PubtransPulsarMessageData data = new PubtransPulsarMessageData.CancellationPulsarMessageData(cancellation, System.currentTimeMillis(), dvjId);
        testInvalidInput(data, "-test-running");
    }

    @Test
    public void testCancellationWithTrainRoute() throws Exception {
        String trainRoute = "3001";
        InternalMessages.TripCancellation cancellation = MockDataUtils.mockTripCancellation(trainRoute, joreDirection, dateTime);
        PubtransPulsarMessageData data = new PubtransPulsarMessageData.CancellationPulsarMessageData(cancellation, System.currentTimeMillis(), dvjId);
        testInvalidInput(data, "-test-train-route");
    }

    /**
     * Convenience method for running tests that should always be filtered by the TripUpdateProcessor,
     * meaning no Gtfs-RT should ever be received. However we should get an ack to producer.
     *
     * @throws Exception
     */
    private void testInvalidInput(final PubtransPulsarMessageData data, String testId) throws Exception {
        TestPipeline.TestLogic logic = new TestPipeline.TestLogic() {
            @Override
            public void testImpl(TestPipeline.TestContext context) throws Exception {
                final long ts = System.currentTimeMillis();
                sendPubtransSourcePulsarMessage(context.source, data);
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
        long now = System.currentTimeMillis();
        long eventTime = now + 5 * 60000; // event to happen five minutes from now
        PubtransTableProtos.ROIArrival arrival = MockDataUtils.mockROIArrival(dvjId, route, joreDirection, stopId, eventTime);

        PubtransPulsarMessageData.ArrivalPulsarMessageData msg = new PubtransPulsarMessageData.ArrivalPulsarMessageData(arrival, now, dvjId);
        testValidStopEvent(msg, "-test-valid-arrival");
    }

    @Test
    public void testValidDepartureStopEvent() throws Exception {
        long now = System.currentTimeMillis();
        long eventTime = now + 3 * 60000; // event to happen three minutes from now
        PubtransTableProtos.ROIDeparture departure = MockDataUtils.mockROIDeparture(dvjId, route, joreDirection, stopId, eventTime);

        PubtransPulsarMessageData.DeparturePulsarMessageData msg = new PubtransPulsarMessageData.DeparturePulsarMessageData(departure, now, dvjId);
        testValidStopEvent(msg, "-test-valid-departure");
    }

    private void testValidStopEvent(PubtransPulsarMessageData sourceMsg, String testId) throws Exception {
        TestPipeline.TestLogic logic = new TestPipeline.TestLogic() {
            @Override
            public void testImpl(TestPipeline.TestContext context) throws Exception {

                sendPubtransSourcePulsarMessage(context.source, sourceMsg);
                logger.info("Message sent, reading it back");

                Message<byte[]> received = TestPipeline.readOutputMessage(context);
                assertNotNull(received);

                String expectedKey = Long.toString(sourceMsg.dvjId);
                validatePulsarProperties(received, expectedKey, sourceMsg.eventTime.get(), TransitdataProperties.ProtobufSchema.GTFS_TripUpdate);

                GtfsRealtime.FeedMessage feedMessage = GtfsRealtime.FeedMessage.parseFrom(received.getData());
                assertNotNull(feedMessage);
                assertEquals(1, feedMessage.getEntityCount());
                GtfsRealtime.TripUpdate tu = feedMessage.getEntity(0).getTripUpdate();
                assertNotNull(tu);

                assertEquals(1, tu.getStopTimeUpdateCount());
                GtfsRealtime.TripUpdate.StopTimeUpdate update = tu.getStopTimeUpdate(0);
                assertNotNull(update);

                assertIfArrivalAndDepartureDiffer(update);

                assertFalse(update.hasStopSequence()); // We don't include stopSequence since it's our via-points etc can confuse the feed.
                assertTrue(update.hasStopId()); // TODO add check for StopId

                assertTrue(tu.hasTrip());
                GtfsRealtime.TripDescriptor tripDescriptor = tu.getTrip();
                assertEquals(gtfsRtDirection, tripDescriptor.getDirectionId());
                assertEquals(route, tripDescriptor.getRouteId());

                logger.info("Message read back, all good");
                TestPipeline.validateAcks(1, context);
            }
        };

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
        testViaPointFiltering(true);
    }

    @Test
    public void testNonViaPointStopEvent() throws Exception {
        testViaPointFiltering(false);
    }

    private void testViaPointFiltering(boolean filter) throws Exception {
        long now = System.currentTimeMillis();
        long targetTime = now + 3 * 60000; // event to happen three minutes from now

        PubtransTableProtos.Common.Builder builder = MockDataUtils.generateValidCommon(dvjId, stopSequence, targetTime);
        MockDataUtils.setIsViaPoint(builder, filter);
        PubtransTableProtos.Common common = builder.build();
        PubtransTableProtos.DOITripInfo tripInfo = MockDataUtils.mockDOITripInfo(common.getIsOnDatedVehicleJourneyId(), route, stopId, targetTime);
        PubtransTableProtos.ROIArrival arrival = MockDataUtils.mockROIArrival(common, tripInfo);

        PubtransPulsarMessageData.ArrivalPulsarMessageData msg = new PubtransPulsarMessageData.ArrivalPulsarMessageData(
                arrival, now, common.getIsOnDatedVehicleJourneyId());
        if (filter) {
            testInvalidInput(msg, "-test-viapoint-filtered");
        }
        else {
            testValidStopEvent(msg, "-test-non-viapoint-should-pass");
        }
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
        final long now = System.currentTimeMillis();
        final long eventTime = now + 60000; //One minute from now
        PubtransTableProtos.ROIArrival arrival = MockDataUtils.mockROIArrival(dvjId, route, eventTime);

        PubtransPulsarMessageData.ArrivalPulsarMessageData validMsg = new PubtransPulsarMessageData.ArrivalPulsarMessageData(arrival, now, dvjId);
        input.add(validMsg);
        GtfsRealtime.FeedMessage asFeedMessage = toGtfsRt(validMsg);

        //Expected output is GTFS-RT TripUpdate
        Map<String, String> outputProperties = new HashMap<>();
        outputProperties.put(TransitdataProperties.KEY_PROTOBUF_SCHEMA, TransitdataProperties.ProtobufSchema.GTFS_TripUpdate.toString());
        final long expectedTimestamp = asFeedMessage.getHeader().getTimestamp();
        final String expectedKey = validMsg.key.get();
        PulsarMessageData validOutput = new PulsarMessageData(asFeedMessage.toByteArray(), expectedTimestamp, expectedKey, outputProperties);
        expectedOutput.add(validOutput);

        TestPipeline.MultiMessageTestLogic logic = new TestPipeline.MultiMessageTestLogic(input, expectedOutput) {
            @Override
            public void validateMessage(PulsarMessageData expected, PulsarMessageData received) {
                try {
                    assertNotNull(expected);
                    assertNotNull(received);
                    assertEquals(TransitdataProperties.ProtobufSchema.GTFS_TripUpdate.toString(), received.properties.get(TransitdataProperties.KEY_PROTOBUF_SCHEMA));
                    assertEquals(validMsg.key.get(), received.key.get());

                    long expectedPulsarTimestampInMs = validMsg.eventTime.get();
                    assertEquals(expectedPulsarTimestampInMs, (long)received.eventTime.get()); // This should be in ms

                    GtfsRealtime.FeedMessage feedMessage = GtfsRealtime.FeedMessage.parseFrom(received.payload);
                    assertNotNull(feedMessage);
                    assertEquals(expectedPulsarTimestampInMs / 1000, feedMessage.getHeader().getTimestamp());

                    GtfsRealtime.FeedEntity entity = feedMessage.getEntity(0);
                    assertTrue(entity.hasTripUpdate());
                    assertFalse(entity.hasAlert());
                    assertFalse(entity.hasVehicle());
                    assertEquals(Long.toString(validMsg.dvjId), entity.getId());
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

    private GtfsRealtime.FeedMessage toGtfsRt(PubtransPulsarMessageData.ArrivalPulsarMessageData arrivalMsg) throws Exception {
        PubtransTableProtos.ROIArrival arrival = arrivalMsg.actualPayload;

        InternalMessages.StopEstimate estimate = PubtransFactory.createStopEstimate(
                arrival.getCommon(),
                arrival.getTripInfo(),
                InternalMessages.StopEstimate.Type.ARRIVAL);
        GtfsRealtime.TripUpdate tu = GtfsRtFactory.newTripUpdate(estimate);

        Long timestampAsSecs = arrivalMsg.eventTime.map(utcMs -> utcMs / 1000).get();
        GtfsRealtime.FeedMessage feedMessage = FeedMessageFactory.createDifferentialFeedMessage(Long.toString(arrivalMsg.dvjId), tu, timestampAsSecs);
        return feedMessage;
    }

    static void sendPubtransSourcePulsarMessage(Producer<byte[]> producer, PubtransPulsarMessageData data) throws PulsarClientException {
        sendPulsarMessage(producer, data.dvjId, data.payload, data.eventTime.get(), data.schema);
    }

    static void sendPulsarMessage(Producer<byte[]> producer, long dvjId, byte[] payload, long timestampEpochMs, TransitdataProperties.ProtobufSchema schema) throws PulsarClientException {
        String dvjIdAsString = Long.toString(dvjId);
        TypedMessageBuilder<byte[]> builder = producer.newMessage().value(payload)
                .eventTime(timestampEpochMs)
                .key(dvjIdAsString)
                .property(TransitdataProperties.KEY_DVJ_ID, dvjIdAsString)
                .property(TransitdataProperties.KEY_PROTOBUF_SCHEMA, schema.toString());

        builder.send();
    }
}
