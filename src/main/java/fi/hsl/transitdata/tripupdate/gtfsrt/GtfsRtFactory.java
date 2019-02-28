package fi.hsl.transitdata.tripupdate.gtfsrt;

import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.transitdata.tripupdate.models.StopEvent;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GtfsRtFactory {

    public static final int DIRECTION_ID_OUTBOUND = 0;
    public static final int DIRECTION_ID_INBOUND = 1;

    //Discard the last character of the route name when it is a number, and trim the possible trailing whitespace
    private static final String ROUTE_NUMBER_REMOVAL_REGEX = "(\\d{4}[a-zA-Z]{0,2})";

    private GtfsRtFactory() {
    }

    public static GtfsRealtime.TripUpdate.StopTimeUpdate newStopTimeUpdate(StopEvent stopEvent) {
        return newStopTimeUpdateFromPrevious(stopEvent, null);
    }

    public static GtfsRealtime.TripUpdate.StopTimeUpdate newStopTimeUpdateFromPrevious(StopEvent stopEvent, GtfsRealtime.TripUpdate.StopTimeUpdate previousUpdate) {

        GtfsRealtime.TripUpdate.StopTimeUpdate.Builder stopTimeUpdateBuilder = null;
        if (previousUpdate != null) {
            stopTimeUpdateBuilder = previousUpdate.toBuilder();
        } else {
            stopTimeUpdateBuilder = GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder()
                    .setStopId(String.valueOf(stopEvent.getRouteData().getStopId()))
                    .setStopSequence(stopEvent.getStopSeq());
        }

        switch (stopEvent.getScheduleRelationship()) {
            case Skipped:
                stopTimeUpdateBuilder.setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED);
                break;
            case Scheduled:
                stopTimeUpdateBuilder.setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SCHEDULED);
                break;
        }

        GtfsRealtime.TripUpdate.StopTimeEvent stopTimeEvent = GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder()
                .setTime(stopEvent.getTargetTime())
                .build();
        switch (stopEvent.getEventType()) {
            case Arrival:
                stopTimeUpdateBuilder.setArrival(stopTimeEvent);
                break;
            case Departure:
                stopTimeUpdateBuilder.setDeparture(stopTimeEvent);
                break;
        }

        return stopTimeUpdateBuilder.build();
    }

    public static GtfsRealtime.TripUpdate newTripUpdate(StopEvent event) {

        GtfsRealtime.TripDescriptor tripDescriptor = GtfsRealtime.TripDescriptor.newBuilder()
                .setRouteId(reformatRouteName(event.getRouteData().getRouteName()))
                .setDirectionId(event.getRouteData().getDirection())
                .setStartDate(event.getRouteData().getOperatingDay())
                .setStartTime(event.getRouteData().getStartTime())
                .build();

        GtfsRealtime.TripUpdate.Builder tripUpdateBuilder = GtfsRealtime.TripUpdate.newBuilder()
                .setTrip(tripDescriptor)
                .setTimestamp(event.getLastModifiedTimestamp(TimeUnit.SECONDS));

        return tripUpdateBuilder.build();
    }

    public static GtfsRealtime.TripUpdate newTripUpdate(InternalMessages.TripCancellation cancellation, long timestampMs) {

        GtfsRealtime.TripDescriptor tripDescriptor = GtfsRealtime.TripDescriptor.newBuilder()
                .setRouteId(reformatRouteName(cancellation.getRouteId()))
                .setDirectionId(cancellation.getDirectionId())
                .setStartDate(cancellation.getStartDate())
                .setStartTime(cancellation.getStartTime())
                .setScheduleRelationship(GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED)
                .build();

        GtfsRealtime.TripUpdate.Builder tripUpdateBuilder = GtfsRealtime.TripUpdate.newBuilder()
                .setTrip(tripDescriptor)
                .setTimestamp(timestampMs / 1000);

        return tripUpdateBuilder.build();
    }

    static String reformatRouteName(String routeName) {

        Pattern routePattern = Pattern.compile(ROUTE_NUMBER_REMOVAL_REGEX);
        Matcher matcher = routePattern.matcher(routeName);
        matcher.find();
        return matcher.group(1);
    }
}
