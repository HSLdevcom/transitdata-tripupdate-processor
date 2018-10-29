package fi.hsl.transitdata.tripupdate.application;

import fi.hsl.common.pulsar.IMessageHandler;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.TransitdataProperties.*;
import fi.hsl.transitdata.tripupdate.processing.ArrivalProcessor;
import fi.hsl.transitdata.tripupdate.processing.DepartureProcessor;
import fi.hsl.transitdata.tripupdate.processing.TripCancellationProcessor;
import fi.hsl.transitdata.tripupdate.processing.TripUpdateProcessor;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;


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

    private Optional<ProtobufSchema> parseProtobufSchema(Message received) {
        try {
            String schemaType = received.getProperty(TransitdataProperties.KEY_PROTOBUF_SCHEMA);
            log.debug("Received message with schema type " + schemaType);
            ProtobufSchema schema = ProtobufSchema.fromString(schemaType);
            return Optional.of(schema);
        }
        catch (Exception e) {
            log.error("Failed to parse protobuf schema", e);
            return Optional.empty();
        }
    }

    public void handleMessage(Message received) throws Exception {
        try {
            Optional<ProtobufSchema> maybeSchema = parseProtobufSchema(received);
            maybeSchema.ifPresent(schema -> {
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
            });

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