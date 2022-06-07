package fi.hsl.transitdata.tripupdate.validators;

import com.google.transit.realtime.GtfsRealtime;

public class MissingEstimatesValidator implements ITripUpdateValidator {
    private final int maxMissingEstimates;

    public MissingEstimatesValidator(final int maxMissingEstimates) {
        this.maxMissingEstimates = maxMissingEstimates;
    }

    @Override
    public boolean validate(GtfsRealtime.TripUpdate tripUpdate) {
        int sameEstimate = 0;

        GtfsRealtime.TripUpdate.StopTimeUpdate previous = null;

        for (GtfsRealtime.TripUpdate.StopTimeUpdate stopTimeUpdate : tripUpdate.getStopTimeUpdateList()) {
            if (previous != null) {
                if (previous.hasDeparture()
                        && previous.getDeparture().hasTime()
                        && stopTimeUpdate.hasArrival()
                        && stopTimeUpdate.getArrival().hasTime()
                        && previous.getDeparture().getTime() == stopTimeUpdate.getArrival().getTime()) {
                    sameEstimate++;

                    if (sameEstimate >= maxMissingEstimates) {
                        return false;
                    }
                }

            }

            previous = stopTimeUpdate;
        }

        return true;
    }
}
