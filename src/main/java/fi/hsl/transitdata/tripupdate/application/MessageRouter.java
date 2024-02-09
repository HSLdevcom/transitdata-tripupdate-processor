package fi.hsl.transitdata.tripupdate.application;

import com.google.transit.realtime.GtfsRealtime;
import com.typesafe.config.Config;
import fi.hsl.common.gtfsrt.FeedMessageFactory;
import fi.hsl.common.pulsar.IMessageHandler;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.TransitdataProperties.*;
import fi.hsl.common.transitdata.TransitdataSchema;
import fi.hsl.transitdata.tripupdate.processing.AbstractMessageProcessor;
import fi.hsl.transitdata.tripupdate.processing.StopEstimateProcessor;
import fi.hsl.transitdata.tripupdate.utils.Debouncer;
import fi.hsl.transitdata.tripupdate.validators.ITripUpdateValidator;
import fi.hsl.transitdata.tripupdate.validators.MissingEstimatesValidator;
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

    private MessageStats messageStats = new MessageStats();

    private Debouncer debouncer;

    public MessageRouter(PulsarApplicationContext context) {
        consumer = context.getConsumer();
        producer = context.getProducer();
        this.config = context.getConfig();

        debouncer = new Debouncer(config.getDuration("publisher.debounceDelay"));

        tripUpdateValidators = registerTripUpdateValidators();
        registerHandlers(context);
    }

    private void registerHandlers(PulsarApplicationContext context) {
        //Let's use the same instance of TripUpdateProcessor.
        TripUpdateProcessor tripUpdateProcessor = new TripUpdateProcessor(context.getProducer());

        final boolean filterTrainData = config.getBoolean("validator.filterTrainData");

        processors.put(ProtobufSchema.InternalMessagesStopEstimate, new StopEstimateProcessor(tripUpdateProcessor, filterTrainData));
        processors.put(ProtobufSchema.InternalMessagesTripCancellation, new TripCancellationProcessor(tripUpdateProcessor, filterTrainData));
    }

    private List<ITripUpdateValidator> registerTripUpdateValidators() {

        List<ITripUpdateValidator> tripUpdateValidators = new ArrayList<>();

        tripUpdateValidators.add(new TripUpdateMaxAgeValidator(config.getDuration("validator.tripUpdateMaxAge", TimeUnit.SECONDS)));
        tripUpdateValidators.add(new PrematureDeparturesValidator(config.getDuration("validator.tripUpdateMinTimeBeforeDeparture", TimeUnit.SECONDS),
                config.getString("validator.timezone")));
        tripUpdateValidators.add(new MissingEstimatesValidator(config.getInt("validator.tripUpdateMaxMissingEstimates")));

        return tripUpdateValidators;

    }

    public void handleMessage(Message received) {
        messageStats.incrementMessagesReceived();

        try {
            Optional<TransitdataSchema> maybeSchema = TransitdataSchema.parseFromPulsarMessage(received);
            maybeSchema.ifPresent(schema -> {
                AbstractMessageProcessor processor = processors.get(schema.schema);
                if (processor != null) {
                    if (processor.validateMessage(received.getData())) {

                        Optional<AbstractMessageProcessor.TripUpdateWithId> maybeTripUpdate = processor.processMessage(received);
                        if (maybeTripUpdate.isPresent()) {
                            final AbstractMessageProcessor.TripUpdateWithId pair = maybeTripUpdate.get();
                            final GtfsRealtime.TripUpdate tripUpdate = pair.getTripUpdate();
                            
                            if (tripUpdate.getTrip().getScheduleRelationship() != GtfsRealtime.TripDescriptor.ScheduleRelationship.SCHEDULED &&
                                    (tripUpdate.getTrip().getRouteId().startsWith("106") || tripUpdate.getTrip().getRouteId().startsWith("107"))) {
                                log.info("NEW TRIP UPDATE: " + tripUpdate.getTrip().getScheduleRelationship() + " " + tripUpdate.getTrip().getRouteId() + " " + tripUpdate.getTrip().getDirectionId() + " " +  tripUpdate.getTrip().getStartDate() + " " + tripUpdate.getTrip().getStartTime());
                            }

                            final boolean tripUpdateIsValid = tripUpdateValidators.stream().allMatch(validator -> {
                                final boolean isValid = validator.validate(tripUpdate);
                                if (!isValid) {
                                    final GtfsRealtime.TripDescriptor trip = tripUpdate.getTrip();
                                    if (trip.getRouteId().startsWith("106") || trip.getRouteId().startsWith("107")) {
                                        log.info("Trip update for {} / {} / {} / {} failed validation when validating with {}", trip.getRouteId(), trip.getDirectionId(), trip.getStartDate(), trip.getStartTime(), validator.getClass().getName());
                                    }

                                    messageStats.incrementInvalidTripUpdates("validator-" + validator.getClass().getSimpleName());
                                }
                                return isValid;
                            });

                            if (tripUpdateIsValid) {
                                long eventTimeMs = received.getEventTime();
                                sendTripUpdate(pair, eventTimeMs);
                            }
                        } else {
                            log.info("Failed to process TripUpdate from source schema {}", schema.schema.toString());
                            messageStats.incrementInvalidTripUpdates("processing_failed-" + schema.schema);
                        }
                    } else {
                        log.info("Message didn't pass validation, ignoring.");
                        messageStats.incrementInvalidTripUpdates("message_validator");
                    }
                } else {
                    log.warn("Received message with unknown schema, ignoring: " + schema);
                    messageStats.incrementInvalidTripUpdates("unknown_schema-" + schema);
                }
            });

            consumer.acknowledgeAsync(received)
                    .exceptionally(throwable -> {
                        log.error("Failed to ack Pulsar message", throwable);
                        return null;
                    })
                    .thenRun(() -> {});
        } catch (Exception e) {
            log.error("Exception while handling message", e);
        }

        if (messageStats.getDurationSecs() >= 60) {
            messageStats.logAndReset(log);
        }
    }

    private void sendTripUpdate(final AbstractMessageProcessor.TripUpdateWithId tuIdPair, final long pulsarEventTimestamp) {
        messageStats.incrementMessagesSent();

        final String tripId = tuIdPair.getTripId();
        final GtfsRealtime.TripUpdate tripUpdate = tuIdPair.getTripUpdate();
        
        if (tripUpdate.getTrip().getRouteId().startsWith("106") || tripUpdate.getTrip().getRouteId().startsWith("107")) {
            log.info("SENDING: " + tripUpdate.getTrip().getScheduleRelationship() + " " + tripUpdate.getTrip().getRouteId() + " " + tripUpdate.getTrip().getDirectionId() + " " +  tripUpdate.getTrip().getStartDate() + " " + tripUpdate.getTrip().getStartTime());
        }

        debouncer.debounce(tripId, () -> {
            GtfsRealtime.FeedMessage feedMessage = FeedMessageFactory.createDifferentialFeedMessage(tripId, tripUpdate, tripUpdate.getTimestamp());
            producer.newMessage()
                    .key(tripId)
                    .eventTime(pulsarEventTimestamp)
                    .property(TransitdataProperties.KEY_PROTOBUF_SCHEMA, TransitdataProperties.ProtobufSchema.GTFS_TripUpdate.toString())
                    .value(feedMessage.toByteArray())
                    .sendAsync()
                    .thenRun(() -> log.debug("Sending TripUpdate for tripId {} with {} StopTimeUpdates and status {}",
                            tripId, tripUpdate.getStopTimeUpdateCount(), tripUpdate.getTrip().getScheduleRelationship()));
        });
    }
}
