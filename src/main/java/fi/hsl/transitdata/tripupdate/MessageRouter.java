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
        //Let's use the same instance of TripUpdateProcessor.
        TripUpdateProcessor tripUpdateProcessor = new TripUpdateProcessor(context.getProducer());

        processors.put(ProtobufSchema.PubtransRoiArrival, new ArrivalProcessor(tripUpdateProcessor));
        processors.put(ProtobufSchema.PubtransRoiDeparture, new DepartureProcessor(tripUpdateProcessor));
        processors.put(ProtobufSchema.InternalMessagesTripCancellation, new TripCancellationProcessor(tripUpdateProcessor));
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
            consumer.acknowledgeAsync(received)
                    .exceptionally(throwable -> {
                        log.error("Failed to ack Pulsar message", throwable);
                        return null;
                    })
                    .thenRun(() -> {});

        }
        catch (Exception e) {
            log.error("Exception while handling message", e);
        }

    }

}
