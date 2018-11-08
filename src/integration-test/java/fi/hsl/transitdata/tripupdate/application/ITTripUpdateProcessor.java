package fi.hsl.transitdata.tripupdate.application;

import com.google.transit.realtime.GtfsRealtime;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import fi.hsl.common.config.ConfigParser;
import fi.hsl.common.config.ConfigUtils;
import fi.hsl.common.pulsar.MockContainers;
import fi.hsl.common.pulsar.PulsarApplication;
import fi.hsl.common.pulsar.PulsarMockApplication;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.transitdata.tripupdate.MockDataFactory;
import fi.hsl.transitdata.tripupdate.gtfsrt.GtfsRtFactory;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PulsarContainer;

import java.nio.charset.Charset;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class ITTripUpdateProcessor {

    static final Logger logger = LoggerFactory.getLogger(ITTripUpdateProcessor.class);

    static final boolean PRINT_PULSAR_LOG = ConfigUtils.getEnv("PRINT_PULSAR_LOG").map(Boolean::parseBoolean).orElse(false);

    private static final String TENANT = "hsl";
    private static final String NAMESPACE = "transitdata";

    @ClassRule
    public static PulsarContainer pulsar = MockContainers.newPulsarContainer();

    @BeforeClass
    public static void setUp() throws Exception {
        MockContainers.configurePulsarContainer(pulsar, TENANT, NAMESPACE);

        if (PRINT_PULSAR_LOG) {
            MockContainers.tail(pulsar, logger);
        }
    }

    private Config defaultConfig(String configFileName) {
        Config config = ConfigParser.createConfig(configFileName);
        assertNotNull(config);
        return config;
    }

    private Config defaultConfigWithOverride(String configFileName, String key, Object value) {
        Map<String, Object> overrides = new HashMap<>();
        overrides.put(key, value);
        return defaultConfigWithOverrides(configFileName, overrides);
    }


    private Config defaultConfigWithOverrides(String configFileName, Map<String, Object> overrides) {
        Config configOverrides = ConfigFactory.parseMap(overrides);
        return ConfigParser.mergeConfigs(defaultConfig(configFileName), configOverrides);
    }

    private PulsarApplication createPulsarApp(String config) throws Exception {
        logger.info("Creating Pulsar Application for config " + config);

        Config base = defaultConfig(config);
        assertNotNull(base);
        PulsarApplication app = PulsarMockApplication.newInstance(base, null, pulsar);
        assertNotNull(app);
        return app;
    }

    class TestContext {
        Producer<byte[]> source;
        Consumer<byte[]> sink;
        PulsarApplication testApp;
    }
    abstract class TestLogic {
        public void test(TestContext context) {
            try {
                testImpl(context);
            }
            catch (Exception e) {
                logger.error("Test failed!", e);
                assertTrue(false);
            }
        }

        //Perform your test and assertions in here
        abstract void testImpl(TestContext context) throws Exception;
    }

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

    private void validatePulsarProperties(Message<byte[]> received, String dvjId, long eventTime) {
        assertEquals(TransitdataProperties.ProtobufSchema.GTFS_TripUpdate.toString(), received.getProperty(TransitdataProperties.KEY_PROTOBUF_SCHEMA));
        assertEquals(dvjId, received.getKey());
        //assertEquals(dvjId, received.getProperty(TransitdataProperties.KEY_DVJ_ID)); //TODO Should this exist?
        assertEquals(eventTime, received.getEventTime());

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


    public void testPulsar(TestLogic logic) throws Exception {

        logger.info("Initializing test resources");
        PulsarApplication sourceApp = createPulsarApp("integration-test-source.conf");
        Producer<byte[]> source = sourceApp.getContext().getProducer();
        assertNotNull(source);
        assertTrue(source.isConnected());

        PulsarApplication sinkApp = createPulsarApp("integration-test-sink.conf");
        Consumer<byte[]> sink = sinkApp.getContext().getConsumer();
        assertNotNull(sink);
        assertTrue(sink.isConnected());

        PulsarApplication testApp = createPulsarApp("integration-test.conf");

        TestContext context = new TestContext();
        context.sink = sink;
        context.source = source;
        context.testApp = testApp;

        Thread t = new Thread() {
            public void run() {
                try {
                    testApp.launchWithHandler(new MessageRouter(testApp.getContext()));
                }
                catch (Exception e) {
                    //This is expected after test is closed
                    logger.info("Pulsar application throws, as expected. " + e.getMessage());
                }
            }
        };
        t.start();

        logger.info("Test setup done, calling test method");

        logic.test(context);

        logger.info("Test done, all good");

        testApp.close(); // This exits the thread-loop above also
        t.join(10000); // Wait for thread to exit. Use timeout to prevent deadlock for whole the whole test class.
        assertFalse(t.isAlive());

        logger.info("Pulsar read thread finished");

        sourceApp.close();
        sinkApp.close();

        assertFalse(source.isConnected());
        assertFalse(sink.isConnected());
    }


}
