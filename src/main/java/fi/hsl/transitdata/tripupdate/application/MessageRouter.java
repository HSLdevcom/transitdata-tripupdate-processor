package fi.hsl.transitdata.tripupdate.application;

import com.google.transit.realtime.GtfsRealtime;
import com.typesafe.config.Config;
import fi.hsl.common.gtfsrt.FeedMessageFactory;
import fi.hsl.common.pulsar.IMessageHandler;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.TransitdataProperties.*;
import fi.hsl.common.transitdata.TransitdataSchema;
import fi.hsl.transitdata.tripupdate.processing.StopEstimateProcessor;
import fi.hsl.transitdata.tripupdate.validators.ITripUpdateValidator;
import fi.hsl.transitdata.tripupdate.validators.PrematureDeparturesValidator;
import fi.hsl.transitdata.tripupdate.validators.TripUpdateMaxAgeValidator;
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

    private Map<ProtobufSchema, AbstractMessageProcessor> processors = new HashMap<>();
    private List<ITripUpdateValidator> tripUpdateValidators;

    private Consumer<byte[]> consumer;
    private Producer<byte[]> producer;
    private Config config;

    public MessageRouter(PulsarApplicationContext context) {
        consumer = context.getConsumer();
        producer = context.getProducer();
        this.config = context.getConfig();
        tripUpdateValidators = registerTripUpdateValidators();
        registerHandlers(context);
    }

    private void registerHandlers(PulsarApplicationContext context) {
        //Let's use the same instance of TripUpdateProcessor.
        TripUpdateProcessor tripUpdateProcessor = new TripUpdateProcessor(context.getProducer());

        processors.put(ProtobufSchema.InternalMessagesStopEstimate, new StopEstimateProcessor(tripUpdateProcessor));
        processors.put(ProtobufSchema.InternalMessagesTripCancellation, new TripCancellationProcessor(tripUpdateProcessor));
    }

    private List<ITripUpdateValidator> registerTripUpdateValidators() {

        List<ITripUpdateValidator> tripUpdateValidators = new ArrayList<>();

        tripUpdateValidators.add(new TripUpdateMaxAgeValidator(config.getDuration("validator.tripUpdateMaxAge", TimeUnit.SECONDS)));
        tripUpdateValidators.add(new PrematureDeparturesValidator(config.getDuration("validator.tripUpdateMinTimeBeforeDeparture", TimeUnit.SECONDS),
                config.getString("validator.timezone")));

        return tripUpdateValidators;

    }

    public void handleMessage(Message received) throws Exception {
        try {
            Optional<TransitdataSchema> maybeSchema = TransitdataSchema.parseFromPulsarMessage(received);
            maybeSchema.ifPresent(schema -> {
                AbstractMessageProcessor processor = processors.get(schema.schema);
                if (processor != null) {
                    if (processor.validateMessage(received)) {

                        Optional<AbstractMessageProcessor.TripUpdateWithId> maybeTripUpdate = processor.processMessage(received);
                        if (maybeTripUpdate.isPresent()) {
                            final AbstractMessageProcessor.TripUpdateWithId pair = maybeTripUpdate.get();
                            final GtfsRealtime.TripUpdate tripUpdate = pair.tripUpdate;
                            boolean tripUpdateIsValid = true;

                            for (ITripUpdateValidator validator : tripUpdateValidators) {
                                tripUpdateIsValid = tripUpdateIsValid && validator.validate(tripUpdate);
                            }

                            if (tripUpdateIsValid) {
                                long eventTimeMs = received.getEventTime();
                                sendTripUpdate(pair, eventTimeMs);
                            }
                        }
                        else {
                            log.warn("Failed to process TripUpdate from source schema {}", schema.schema.toString());
                        }
                    }
                    else {
                        log.info("Message didn't pass validation, ignoring.");
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

    private void sendTripUpdate(final AbstractMessageProcessor.TripUpdateWithId tuIdPair, final long pulsarEventTimestamp) {
        final String tripId = tuIdPair.tripId;
        final GtfsRealtime.TripUpdate tripUpdate = tuIdPair.tripUpdate;

        GtfsRealtime.FeedMessage feedMessage = FeedMessageFactory.createDifferentialFeedMessage(tripId, tripUpdate, tripUpdate.getTimestamp());
        producer.newMessage()
                .key(tripId)
                .eventTime(pulsarEventTimestamp)
                .property(TransitdataProperties.KEY_PROTOBUF_SCHEMA, TransitdataProperties.ProtobufSchema.GTFS_TripUpdate.toString())
                .value(feedMessage.toByteArray())
                .sendAsync()
                .thenRun(() -> log.debug("Sending TripUpdate for tripId {} with {} StopTimeUpdates and status {}",
                        tripId, tripUpdate.getStopTimeUpdateCount(), tripUpdate.getTrip().getScheduleRelationship()));

    }
}
