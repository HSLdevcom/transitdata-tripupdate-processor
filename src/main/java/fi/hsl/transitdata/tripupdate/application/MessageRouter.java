package fi.hsl.transitdata.tripupdate.application;

import com.google.transit.realtime.GtfsRealtime;
import com.typesafe.config.Config;
import fi.hsl.common.pulsar.IMessageHandler;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.TransitdataProperties.*;
import fi.hsl.transitdata.tripupdate.validators.ITripUpdateValidator;
import fi.hsl.transitdata.tripupdate.validators.PrematureDeparturesValidator;
import fi.hsl.transitdata.tripupdate.validators.TripUpdateMaxAgeValidator;
import fi.hsl.transitdata.tripupdate.gtfsrt.GtfsRtFactory;
import fi.hsl.transitdata.tripupdate.processing.ArrivalProcessor;
import fi.hsl.transitdata.tripupdate.processing.DepartureProcessor;
import fi.hsl.transitdata.tripupdate.processing.TripCancellationProcessor;
import fi.hsl.transitdata.tripupdate.processing.TripUpdateProcessor;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;


public class MessageRouter implements IMessageHandler {
    private static final Logger log = LoggerFactory.getLogger(MessageRouter.class);

    private Map<ProtobufSchema, IMessageProcessor> processors = new HashMap<>();
    private List<ITripUpdateValidator> tripUpdateValidators;

    private Consumer<byte[]> consumer;
    private Producer<byte[]> producer;
    private Config config;

    public MessageRouter(PulsarApplicationContext context, Config config) {
        consumer = context.getConsumer();
        producer = context.getProducer();
        this.config = config;
        tripUpdateValidators = registerTripUpdateValidators();
        registerHandlers(context);
    }

    private void registerHandlers(PulsarApplicationContext context) {
        //Let's use the same instance of TripUpdateProcessor.
        TripUpdateProcessor tripUpdateProcessor = new TripUpdateProcessor(context.getProducer());

        processors.put(ProtobufSchema.PubtransRoiArrival, new ArrivalProcessor(tripUpdateProcessor));
        processors.put(ProtobufSchema.PubtransRoiDeparture, new DepartureProcessor(tripUpdateProcessor));
        processors.put(ProtobufSchema.InternalMessagesTripCancellation, new TripCancellationProcessor(tripUpdateProcessor));
    }

    private List<ITripUpdateValidator> registerTripUpdateValidators() {

        List<ITripUpdateValidator> tripUpdateValidators = new ArrayList<>();

        tripUpdateValidators.add(new TripUpdateMaxAgeValidator(config.getDuration("validator.tripUpdateMaxAge", TimeUnit.SECONDS)));
        tripUpdateValidators.add(new PrematureDeparturesValidator(config.getDuration("validator.tripUpdateMinTimeBeforeDeparture", TimeUnit.SECONDS)));

        return tripUpdateValidators;

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
                        GtfsRealtime.TripUpdate tripUpdate = processor.processMessage(received);

                        boolean tripUpdateIsValid = true;

                        for (ITripUpdateValidator validator : tripUpdateValidators) {
                            tripUpdateIsValid = tripUpdateIsValid && validator.validate(tripUpdate);
                        }

                        if (tripUpdateIsValid) {
                            sendTripUpdate(tripUpdate, received.getProperty(TransitdataProperties.KEY_DVJ_ID));
                        }
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

    private void sendTripUpdate(final GtfsRealtime.TripUpdate tripUpdate, final String dvjId) {
        GtfsRealtime.FeedMessage feedMessage = GtfsRtFactory.newFeedMessage(dvjId, tripUpdate, tripUpdate.getTimestamp());
        producer.newMessage()
                .key(dvjId)
                .eventTime(tripUpdate.getTimestamp())
                .value(feedMessage.toByteArray())
                .sendAsync()
                .thenRun(() -> log.debug("Sending TripUpdate for dvjId {} with {} StopTimeUpdates and status {}",
                        dvjId, tripUpdate.getStopTimeUpdateCount(), tripUpdate.getTrip().getScheduleRelationship()));

    }
}
