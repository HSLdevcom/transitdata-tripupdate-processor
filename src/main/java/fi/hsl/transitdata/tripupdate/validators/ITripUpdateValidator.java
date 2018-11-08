package fi.hsl.transitdata.tripupdate.validators;

import com.google.transit.realtime.GtfsRealtime;

public interface ITripUpdateValidator {

    boolean validate(GtfsRealtime.TripUpdate tripUpdate);
}
