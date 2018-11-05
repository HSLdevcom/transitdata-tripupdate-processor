package fi.hsl.transitdata.tripupdate.application;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import fi.hsl.common.config.ConfigParser;
import fi.hsl.common.config.ConfigUtils;
import fi.hsl.common.pulsar.MockContainers;
import fi.hsl.common.pulsar.PulsarApplication;
import fi.hsl.common.pulsar.PulsarMockApplication;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PulsarContainer;

import java.nio.charset.Charset;
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

    @Test
    public void readConfig() {
        defaultConfig();
    }

    private Config defaultConfig() {
        Config config = ConfigParser.createConfig("integration-test.conf");
        assertNotNull(config);
        return config;
    }

    private Config defaultConfigWithOverride(String key, Object value) {
        Map<String, Object> overrides = new HashMap<>();
        overrides.put(key, value);
        return defaultConfigWithOverrides(overrides);
    }


    private Config defaultConfigWithOverrides(Map<String, Object> overrides) {
        Config configOverrides = ConfigFactory.parseMap(overrides);
        return ConfigParser.mergeConfigs(defaultConfig(), configOverrides);
    }

    @Test
    public void testPulsar() throws Exception {
        Config base = defaultConfig();

        PulsarApplication app = PulsarMockApplication.newInstance(base, null, pulsar);
        assertNotNull(app);

        logger.info("Pulsar Application created, testing to send a message");

        final String payload = "Test-message";

        Producer<byte[]> producer = app.getContext().getProducer();
        producer.send(payload.getBytes());

        logger.info("Message sent, reading it back");

        Consumer<byte[]> consumer = app.getContext().getConsumer();
        readAndValidateMsg(consumer, new HashSet<>(Arrays.asList(payload)));

        assertTrue(consumer.isConnected());
        assertTrue(producer.isConnected());

        app.close();

        assertFalse(consumer.isConnected());
        assertFalse(producer.isConnected());

    }

    private static String formatTopicName(String topic) {
        return "persistent://" + TENANT + "/" + NAMESPACE + "/" + topic;
    }
    /*
    @Test
    public void testPulsarWithMultipleTopics() throws Exception {
        Map<String, Object> o1 = new HashMap<>();
        o1.put("pulsar.consumer.enabled", false);
        o1.put("redis.enabled", false);
        o1.put("pulsar.producer.topic", formatTopicName("test-1"));
        Config producer1Config = defaultConfigWithOverrides(o1);

        PulsarApplication app = PulsarMockApplication.newInstance(producer1Config, null, pulsar);
        assertNotNull(app);

        Producer<byte[]> producer = app.getContext().getProducer();

        //Create a second producer but bind into different topic
        Config producer2Config = defaultConfigWithOverride("pulsar.producer.topic", formatTopicName("test-2"));
        Producer<byte[]> secondProducer = app.createProducer(app.client, producer2Config);

        logger.info("Multi-topic Pulsar Application created, testing to send a message");

        //Next create the consumer
        Map<String, Object> overrides = new HashMap<>();
        overrides.put("pulsar.consumer.multipleTopics", true);
        overrides.put("pulsar.consumer.topicsPattern", formatTopicName("test-(1|2)"));
        Config consumerConfig = defaultConfigWithOverrides(overrides);
        Consumer<byte[]> consumer = app.createConsumer(app.client, consumerConfig);

        logger.debug("Consumer topic: " + consumer.getTopic());

        final String firstPayload = "to-topic1";
        producer.send(firstPayload.getBytes());

        final String secondPayload = "to-topic2";
        secondProducer.send(secondPayload.getBytes());

        Set<String> correctPayloads = new HashSet<>(Arrays.asList(firstPayload, secondPayload));
        readAndValidateMsg(consumer, correctPayloads);

        secondProducer.close();
        app.close();
    }*/

    private void readAndValidateMsg(Consumer<byte[]> consumer, Set<String> correctPayloads) throws Exception {
        logger.info("Reading messages from Pulsar");
        Set<String> received = new HashSet<>();
        //Pulsar consumer doesn't guarantee in which order messages come when reading multiple topics.
        //They should be in order when reading from the same topic.
        while (received.size() < correctPayloads.size()) {
            Message<byte[]> msg = consumer.receive(5, TimeUnit.SECONDS);
            assertNotNull(msg);
            String receivedPayload = new String(msg.getData(), Charset.defaultCharset());
            logger.info("Received: " + receivedPayload);
            received.add(receivedPayload);
        }
        assertEquals(correctPayloads, received);
    }

}
