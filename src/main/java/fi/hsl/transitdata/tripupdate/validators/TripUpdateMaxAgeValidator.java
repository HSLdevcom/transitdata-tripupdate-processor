package fi.hsl.transitdata.tripupdate.validators;

import com.google.transit.realtime.GtfsRealtime;

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

        int lastStopTimeUpdateIndex = tripUpdate.getStopTimeUpdateCount() - 1;
        long tripLastStopTimeEventTime;

        if (tripUpdate.getStopTimeUpdate(0).hasArrival()) {
            tripLastStopTimeEventTime = tripUpdate.getStopTimeUpdate(lastStopTimeUpdateIndex).getArrival().getTime();
        } else {
            tripLastStopTimeEventTime = tripUpdate.getStopTimeUpdate(lastStopTimeUpdateIndex).getDeparture().getTime();
        }

        if (currentPosixTime - tripLastStopTimeEventTime > tripUpdateMaxAgeInSeconds) {
            return false;
        }

        return true;
    }
}
