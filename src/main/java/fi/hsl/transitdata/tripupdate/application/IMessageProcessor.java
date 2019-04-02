package fi.hsl.transitdata.tripupdate.application;

import com.google.transit.realtime.GtfsRealtime;
import org.apache.pulsar.client.api.Message;

import java.util.Optional;

public interface IMessageProcessor {

    class TripUpdateWithId {
        String tripId;
        GtfsRealtime.TripUpdate tripUpdate;

        public static Optional<TripUpdateWithId> newInstance(String id, GtfsRealtime.TripUpdate tu) {
            TripUpdateWithId pair = new TripUpdateWithId();
            pair.tripId = id;
            pair.tripUpdate = tu;
            return Optional.of(pair);
        }
    }

    /**
     * Check the data within the payload
     *
     * @param msg
     * @return true if we can proceed, false if we want to ignore this message
     */
    boolean validateMessage(Message msg);

    /**
     * Invoked if message goes through the validation
     * @param msg
     */
    Optional<TripUpdateWithId> processMessage(Message msg);
}
