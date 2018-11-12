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
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PulsarContainer;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class ITBaseTripUpdateProcessor {

    static final Logger logger = LoggerFactory.getLogger(ITBaseTripUpdateProcessor.class);

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


    void validatePulsarProperties(Message<byte[]> received, String dvjId, long eventTime) {
        assertEquals(TransitdataProperties.ProtobufSchema.GTFS_TripUpdate.toString(), received.getProperty(TransitdataProperties.KEY_PROTOBUF_SCHEMA));
        assertEquals(dvjId, received.getKey());
        assertEquals(eventTime, received.getEventTime());

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
