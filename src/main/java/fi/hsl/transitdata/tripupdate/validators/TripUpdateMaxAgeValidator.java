package fi.hsl.transitdata.tripupdate.validators;

import com.google.transit.realtime.GtfsRealtime;

import java.util.OptionalLong;
import java.util.stream.Stream;

public class TripUpdateMaxAgeValidator implements ITripUpdateValidator {

    private long tripUpdateMaxAgeInSeconds;

    public TripUpdateMaxAgeValidator(long tripUpdateMaxAgeInSeconds) {
        this.tripUpdateMaxAgeInSeconds = tripUpdateMaxAgeInSeconds;
    }

    @Override
    public boolean validate(GtfsRealtime.TripUpdate tripUpdate) {
        return validateWithCurrentTime(tripUpdate, System.currentTimeMillis() / 1000);
    }

    boolean validateWithCurrentTime(GtfsRealtime.TripUpdate tripUpdate, long currentPosixTime) {

        //If a TripUpdate has no StopTimeUpdates, it is most likely represents a trip that has been cancelled
        //Current hypothesis is that these messages should always be relevant and thus routed through
        boolean isCancellation = tripUpdate.getTrip().hasScheduleRelationship() &&
                tripUpdate.getTrip().getScheduleRelationship() == GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED;

        if (isCancellation || tripUpdate.getStopTimeUpdateList().isEmpty()) {
            return true;
        }

        OptionalLong maxStopTimeEventTime = tripUpdate.getStopTimeUpdateList().stream()
                .flatMap(stu -> stu.getScheduleRelationship() == GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.NO_DATA ?
                        Stream.empty() :
                        Stream.of(stu.getArrival(), stu.getDeparture()))
                .mapToLong(GtfsRealtime.TripUpdate.StopTimeEvent::getTime)
                .max();

        //If maximum stop event time is not present, all stop updates are NO_DATA -> trip update is valid
        if (!maxStopTimeEventTime.isPresent()) {
            return true;
        }

        return currentPosixTime - maxStopTimeEventTime.getAsLong() <= tripUpdateMaxAgeInSeconds;
    }
}
