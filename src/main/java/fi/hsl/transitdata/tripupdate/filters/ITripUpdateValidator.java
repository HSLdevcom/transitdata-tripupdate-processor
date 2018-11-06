package fi.hsl.transitdata.tripupdate.filters;

import com.google.transit.realtime.GtfsRealtime;

public interface ITripUpdateValidator {

    boolean validate(GtfsRealtime.TripUpdate tripUpdate);
}
