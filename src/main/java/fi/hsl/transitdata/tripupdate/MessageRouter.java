package fi.hsl.transitdata.tripupdate;

import fi.hsl.common.pulsar.IMessageHandler;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.TransitdataProperties.*;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;


public class MessageRouter implements IMessageHandler {
    private static final Logger log = LoggerFactory.getLogger(MessageRouter.class);

    private Map<ProtobufSchema, IMessageProcessor> processors = new HashMap<>();

    private Consumer<byte[]> consumer;


    public MessageRouter(PulsarApplicationContext context) {
        consumer = context.getConsumer();
        registerHandlers(context);
    }

    private void registerHandlers(PulsarApplicationContext context) {
        processors.put(ProtobufSchema.PubtransRoiArrival, new MessageProcessor(context.getJedis(), StopEvent.EventType.Arrival));
        processors.put(ProtobufSchema.PubtransRoiDeparture, new MessageProcessor(context.getJedis(), StopEvent.EventType.Departure));
    }

    public void handleMessage(Message received) throws Exception {
        try {
            String schemaType = received.getProperty(TransitdataProperties.KEY_PROTOBUF_SCHEMA);
            log.debug("Received message with schema type " + schemaType);

            ProtobufSchema schema = ProtobufSchema.fromString(schemaType);
            IMessageProcessor processor = processors.get(schema);

            if (processor != null) {
                if (processor.validateMessage(received)) {
                    processor.processMessage(received);
                }
                else {
                    log.warn("Errors with message payload, ignoring.");
                }
            }
            else {
                log.warn("Received message with unknown schema, ignoring: " + schema);
            }
            consumer.acknowledgeAsync(received).thenRun(() -> {});

        }
        catch (Exception e) {
            log.error("Exception while handling message", e);
        }

    }

}
