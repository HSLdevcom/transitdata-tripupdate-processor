package fi.hsl.transitdata.tripupdate;

import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.proto.PubtransTableProtos;

import java.util.Map;

/**
 * StopEvent is an immutable (intermediate) data format.
 */
public class StopEvent {
    private StopEvent() {}

    private long dvjId;
    private EventType eventType;
    private long targetTime;
    private long stopId;
    private int stopSeq;

    public enum ScheduleRelationship {
        Scheduled, Skipped
    }
    private ScheduleRelationship scheduleRelationship;

    private final RouteData routeData = new RouteData();

    public static class RouteData {
        private String routeName;
        private int direction;
        private String operatingDay;
        private String startTime;

        public String getRouteName() {
            return routeName;
        }

        public int getDirection() {
            return direction;
        }

        public String getOperatingDay() {
            return operatingDay;
        }

        public String getStartTime() {
            return startTime;
        }
    }

    public enum EventType {
        Arrival, Departure
    }

    public static StopEvent newInstance(PubtransTableProtos.Common common, Map<String, String> properties, StopEvent.EventType type) {
        StopEvent event = new StopEvent();
        event.dvjId = common.getIsOnDatedVehicleJourneyId();
        event.eventType = type;
        event.stopId = common.getIsTargetedAtJourneyPatternPointGid();
        event.stopSeq = common.getJourneyPatternSequenceNumber();

        event.scheduleRelationship = (common.getState() == 3L) ? StopEvent.ScheduleRelationship.Skipped : StopEvent.ScheduleRelationship.Scheduled;
        //TODO Use java OffsetDateTime?
        event.targetTime = java.sql.Timestamp.valueOf(common.getTargetDateTime()).getTime(); //Don't set if skipped?

        if (properties != null) {
            event.getRouteData().direction = Integer.parseInt(properties.get(TransitdataProperties.KEY_DIRECTION));
            event.getRouteData().routeName = properties.get(TransitdataProperties.KEY_ROUTE_NAME);
            event.getRouteData().operatingDay = properties.get(TransitdataProperties.KEY_OPERATING_DAY);
            event.getRouteData().startTime = properties.get(TransitdataProperties.KEY_START_TIME);
        }

        return event;
    }

    public long getDatedVehicleJourneyId() {
        return dvjId;
    }

    public EventType getEventType() {
        return eventType;
    }

    public long getTargetTime() {
        return targetTime;
    }

    public long getStopId() {
        return stopId;
    }

    public int getStopSeq() {
        return stopSeq;
    }

    public ScheduleRelationship getScheduleRelationship() {
        return scheduleRelationship;
    }

    public RouteData getRouteData() {
        return routeData;
    }


}
