package fi.hsl.transitdata.tripupdate.gtfsrt;

import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.transitdata.PubtransFactory;
import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.transitdata.tripupdate.processing.ProcessorUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GtfsRtFactory {

    public static final int DIRECTION_ID_OUTBOUND = 0;
    public static final int DIRECTION_ID_INBOUND = 1;

    //Discard the last character of the route name when it is a number, and trim the possible trailing whitespace
    private static final String ROUTE_NUMBER_REMOVAL_REGEX = "(\\d{4}[a-zA-Z]{0,2})";
    private static final Pattern ROUTE_NUMBER_PATTERN = Pattern.compile(ROUTE_NUMBER_REMOVAL_REGEX);

    private GtfsRtFactory() {
    }

    public static GtfsRealtime.TripUpdate.StopTimeUpdate newStopTimeUpdate(InternalMessages.StopEstimate stopEstimate) {
        return newStopTimeUpdateFromPrevious(stopEstimate, null);
    }

    public static GtfsRealtime.TripUpdate.StopTimeUpdate newStopTimeUpdateFromPrevious(
            final InternalMessages.StopEstimate stopEstimate,
            GtfsRealtime.TripUpdate.StopTimeUpdate previousUpdate) {

        GtfsRealtime.TripUpdate.StopTimeUpdate.Builder stopTimeUpdateBuilder = null;
        if (previousUpdate != null) {
            stopTimeUpdateBuilder = previousUpdate.toBuilder();
        } else {
            String stopId = stopEstimate.getStopId();
            int stopSequence = stopEstimate.getStopSequence();
            stopTimeUpdateBuilder = GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder()
                    .setStopId(stopId)
                    .setStopSequence(stopSequence);
        }

        switch (stopEstimate.getStatus()) {
            case SKIPPED:
                stopTimeUpdateBuilder.setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED);
                break;
            case SCHEDULED:
                stopTimeUpdateBuilder.setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SCHEDULED);
                break;
        }
        // GTFS-RT treats times in seconds
        long stopEventTimeInSeconds = stopEstimate.getEstimatedTimeUtcMs() / 1000;

        GtfsRealtime.TripUpdate.StopTimeEvent stopTimeEvent = GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder()
                .setTime(stopEventTimeInSeconds)
                .build();
        switch (stopEstimate.getType()) {
            case ARRIVAL:
                stopTimeUpdateBuilder.setArrival(stopTimeEvent);
                break;
            case DEPARTURE:
                stopTimeUpdateBuilder.setDeparture(stopTimeEvent);
                break;
        }

        return stopTimeUpdateBuilder.build();
    }

    public static long lastModified(InternalMessages.StopEstimate estimate) {
        return estimate.getLastModifiedUtcMs() / 1000;
    }

    public static GtfsRealtime.TripUpdate newTripUpdate(InternalMessages.StopEstimate estimate) {
        final int direction = PubtransFactory.joreDirectionToGtfsDirection(estimate.getTripInfo().getDirectionId());
        String routeId = estimate.getTripInfo().getRouteId();
        if (!ProcessorUtils.isMetroRoute(routeId)) {
            routeId = reformatRouteId(routeId);
        }

        GtfsRealtime.TripDescriptor.Builder tripDescriptor = GtfsRealtime.TripDescriptor.newBuilder()
                .setRouteId(routeId)
                .setDirectionId(direction)
                .setStartDate(estimate.getTripInfo().getOperatingDay()) // Local date as String
                .setStartTime(estimate.getTripInfo().getStartTime()) // Local time as String
                .setScheduleRelationship(mapInternalScheduleTypeToGtfsRt(estimate.getTripInfo().getScheduleType()));

        //Trips outside of static schedule need trip ID to be accepted by OTP
        if (estimate.getTripInfo().getScheduleType() != InternalMessages.TripInfo.ScheduleType.SCHEDULED) {
            tripDescriptor.setTripId(generateTripId(estimate.getTripInfo()));
        }

        GtfsRealtime.TripUpdate.Builder tripUpdateBuilder = GtfsRealtime.TripUpdate.newBuilder()
                .setTrip(tripDescriptor)
                .setTimestamp(lastModified(estimate));

        return tripUpdateBuilder.build();
    }

    private static GtfsRealtime.TripDescriptor.ScheduleRelationship mapInternalScheduleTypeToGtfsRt(InternalMessages.TripInfo.ScheduleType scheduleType) {
        switch (scheduleType) {
            case ADDED:
                return GtfsRealtime.TripDescriptor.ScheduleRelationship.ADDED;
            case UNSCHEDULED:
                return GtfsRealtime.TripDescriptor.ScheduleRelationship.UNSCHEDULED;
            case SCHEDULED:
            default:
                return GtfsRealtime.TripDescriptor.ScheduleRelationship.SCHEDULED;
        }
    }

    public static GtfsRealtime.TripUpdate newTripUpdate(InternalMessages.TripCancellation cancellation, long timestampMs) {
        final int gtfsRtDirection = PubtransFactory.joreDirectionToGtfsDirection(cancellation.getDirectionId());
        String routeId = cancellation.getRouteId();
        if (!ProcessorUtils.isMetroRoute(routeId)) {
            routeId = reformatRouteId(routeId);
        }

        GtfsRealtime.TripDescriptor tripDescriptor = GtfsRealtime.TripDescriptor.newBuilder()
                .setRouteId(routeId)
                .setDirectionId(gtfsRtDirection)
                .setStartDate(cancellation.getStartDate())
                .setStartTime(cancellation.getStartTime())
                .setScheduleRelationship(GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED)
                .build();

        GtfsRealtime.TripUpdate.Builder tripUpdateBuilder = GtfsRealtime.TripUpdate.newBuilder()
                .setTrip(tripDescriptor)
                .setTimestamp(timestampMs / 1000);

        return tripUpdateBuilder.build();
    }

    static String reformatRouteId(String routeId) {
        Matcher matcher = ROUTE_NUMBER_PATTERN.matcher(routeId);
        matcher.find();
        return matcher.group(1);
    }

    /**
     * Generates a trip ID for trips outside of static schedule from trip info
     * @param tripInfo
     * @return Trip ID
     */
    private static String generateTripId(InternalMessages.TripInfo tripInfo) {
        return tripInfo.getRouteId()+"_"+tripInfo.getOperatingDay()+"_"+tripInfo.getStartTime()+"_"+tripInfo.getDirectionId();
    }
}
