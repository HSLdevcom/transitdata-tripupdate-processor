package fi.hsl.transitdata.tripupdate.processing;

import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.transitdata.PubtransFactory;
import fi.hsl.common.transitdata.RouteIdUtils;
import org.apache.pulsar.client.api.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public abstract class AbstractMessageProcessor {
    private static final Logger logger = LoggerFactory.getLogger(AbstractMessageProcessor.class);

    public static class TripUpdateWithId {
        String tripId;
        GtfsRealtime.TripUpdate tripUpdate;

        public static Optional<TripUpdateWithId> newInstance(String id, GtfsRealtime.TripUpdate tu) {
            TripUpdateWithId pair = new TripUpdateWithId();
            pair.tripId = id;
            pair.tripUpdate = tu;
            return Optional.of(pair);
        }

        public String getTripId() {
            return tripId;
        }

        public GtfsRealtime.TripUpdate getTripUpdate() {
            return tripUpdate;
        }
    }

    private final boolean filterTrainData;

    AbstractMessageProcessor(boolean filterTrainData) {
        this.filterTrainData = filterTrainData;
    }

    /**
     * Check the data within the payload
     *
     * @param payload
     * @return true if we can proceed, false if we want to ignore this message
     */
    public abstract boolean validateMessage(byte[] payload);

    /**
     * Invoked if message goes through the validation
     * @param msg
     */
    public abstract Optional<TripUpdateWithId> processMessage(Message msg);


    protected boolean validateTripData(String routeName, int direction) {
        //Normalize route ID before validation
        routeName = RouteIdUtils.normalizeRouteId(routeName);

        if (!ProcessorUtils.validateRouteName(routeName) && !ProcessorUtils.isMetroRoute(routeName)) {
            logger.warn("Invalid route name {}, discarding message", routeName);
            return false;
        }

        if (filterTrainData && ProcessorUtils.isTrainRoute(routeName)) {
            logger.debug("Route {} is for trains, discarding message", routeName);
            return false;
        }

        if (direction != PubtransFactory.JORE_DIRECTION_ID_INBOUND && direction != PubtransFactory.JORE_DIRECTION_ID_OUTBOUND) {
            logger.info("Direction {} is not a valid JORE-direction, discarding message", direction);
            return false;
        }
        return true;
    }
}
