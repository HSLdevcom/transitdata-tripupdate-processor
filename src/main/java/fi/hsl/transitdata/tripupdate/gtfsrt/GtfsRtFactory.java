package fi.hsl.transitdata.tripupdate.gtfsrt;

import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.transitdata.PubtransFactory;
import fi.hsl.common.transitdata.RouteIdUtils;
import fi.hsl.common.transitdata.proto.InternalMessages;
import org.apache.pulsar.shade.org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GtfsRtFactory {
    
    private static final Logger log = LoggerFactory.getLogger(GtfsRtFactory.class);

    public static final int DIRECTION_ID_OUTBOUND = 0;
    public static final int DIRECTION_ID_INBOUND = 1;

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
            case NO_DATA:
                //If there is no data for current or previous stop time update, set ScheduleRelationship to NO_DATA
                if (previousUpdate == null || previousUpdate.getScheduleRelationship() == GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.NO_DATA) {
                    stopTimeUpdateBuilder.setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.NO_DATA);
                //Otherwise use ScheduleRelationship of previous stop time update
                } else {
                    stopTimeUpdateBuilder.setScheduleRelationship(previousUpdate.getScheduleRelationship());
                }
                break;
        }

        if (stopEstimate.hasEstimatedTimeUtcMs()) {
            // GTFS-RT treats times in seconds
            long stopEventTimeInSeconds = stopEstimate.getEstimatedTimeUtcMs() / 1000;

            GtfsRealtime.TripUpdate.StopTimeEvent.Builder stopTimeEvent = GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder()
                    .setTime(stopEventTimeInSeconds);

            //Whether the event was observed in real world (i.e. not an estimate)
            final boolean observedTime = stopEstimate.hasObservedTime() && stopEstimate.getObservedTime();
            if (observedTime) {
                stopTimeEvent.setUncertainty(0);
            }

            switch (stopEstimate.getType()) {
                case ARRIVAL:
                    stopTimeUpdateBuilder.setArrival(stopTimeEvent);
                    break;
                case DEPARTURE:
                    stopTimeUpdateBuilder.setDeparture(stopTimeEvent);
                    break;
            }
        }

        return stopTimeUpdateBuilder.build();
    }

    public static long lastModified(InternalMessages.StopEstimate estimate) {
        return estimate.getLastModifiedUtcMs() / 1000;
    }

    public static GtfsRealtime.TripUpdate newTripUpdate(InternalMessages.StopEstimate estimate) {
        final int direction = PubtransFactory.joreDirectionToGtfsDirection(estimate.getTripInfo().getDirectionId());
        String routeId = RouteIdUtils.normalizeRouteId(estimate.getTripInfo().getRouteId());
        
        GtfsRealtime.TripDescriptor.ScheduleRelationship scheduleType = mapInternalScheduleTypeToGtfsRt(estimate.getTripInfo().getScheduleType());
        
        GtfsRealtime.TripDescriptor.Builder tripDescriptor = GtfsRealtime.TripDescriptor.newBuilder()
                .setRouteId(routeId)
                .setDirectionId(direction)
                .setStartDate(estimate.getTripInfo().getOperatingDay()) // Local date as String
                .setStartTime(estimate.getTripInfo().getStartTime()) // Local time as String
                .setScheduleRelationship(scheduleType);
        
        //Trips outside of static schedule need trip ID to be accepted by OTP
        if (scheduleType != GtfsRealtime.TripDescriptor.ScheduleRelationship.SCHEDULED) {
            tripDescriptor.setTripId(generateTripId(estimate.getTripInfo()));
        }

        GtfsRealtime.TripUpdate.Builder tripUpdateBuilder = GtfsRealtime.TripUpdate.newBuilder()
                .setTrip(tripDescriptor)
                .setTimestamp(lastModified(estimate));
        
        if (StringUtils.isNotBlank(estimate.getTargetedStopId())
                && estimate.getTargetedStopId().equals(estimate.getStopId())) {
            log.info("Targeted stopId has changed. TimetabledStopId={}, TargetedStopId={}, RouteId={}, DirectionId={}, OperationDay={}, StartTime={}",
                    estimate.getStopId(), estimate.getTargetedStopId(), estimate.getTripInfo().getRouteId(),
                    estimate.getTripInfo().getDirectionId(), estimate.getTripInfo().getOperatingDay(),
                    estimate.getTripInfo().getStartTime());
            
            GtfsRealtime.TripUpdate.StopTimeEvent stopTimeEvent = GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder()
                    .setDelay(0)
                    .setTime(estimate.getEstimatedTimeUtcMs())
                    .setUncertainty(0)
                    .build();
            
            GtfsRealtime.TripUpdate.StopTimeProperties stopTimeProperties = GtfsRealtime.TripUpdate.StopTimeProperties.newBuilder()
                    .setAssignedStopId(estimate.getTargetedStopId())
                    .build();
            
            GtfsRealtime.TripUpdate.StopTimeUpdate.Builder stopTimeUpdate = GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder()
                    .setStopSequence(estimate.getStopSequence())
                    .setDeparture(stopTimeEvent)
                    .setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SCHEDULED)
                    .setStopTimeProperties(stopTimeProperties);
            
            tripUpdateBuilder = tripUpdateBuilder.addStopTimeUpdate(stopTimeUpdate);
        }
        
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
        String routeId = RouteIdUtils.normalizeRouteId(cancellation.getRouteId());

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

    /**
     * Generates a trip ID for trips outside of static schedule from trip info
     * @param tripInfo
     * @return Trip ID
     */
    private static String generateTripId(InternalMessages.TripInfo tripInfo) {
        return tripInfo.getRouteId()+"_"+tripInfo.getOperatingDay()+"_"+tripInfo.getStartTime()+"_"+tripInfo.getDirectionId();
    }
}
