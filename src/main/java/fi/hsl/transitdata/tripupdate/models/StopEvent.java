package fi.hsl.transitdata.tripupdate.models;

import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.proto.PubtransTableProtos;
import fi.hsl.transitdata.tripupdate.gtfsrt.GtfsRtFactory;

import java.util.Map;

/**
 * StopEvent is an immutable (intermediate) data format.
 */
public class StopEvent {
    private StopEvent() {}

    private long dvjId;
    private EventType eventType;
    private long targetTime;
    private int stopSeq;
    private long lastModifiedTimestamp;

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
        private long stopId;

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

        public long getStopId() {
            return stopId;
        }
    }

    public enum EventType {
        Arrival, Departure
    }

    public static StopEvent newInstance(PubtransTableProtos.Common common, Map<String, String> properties, StopEvent.EventType type) {
        StopEvent event = new StopEvent();
        event.dvjId = common.getIsOnDatedVehicleJourneyId();
        event.eventType = type;
        event.stopSeq = common.getJourneyPatternSequenceNumber();

        event.scheduleRelationship = (common.getState() == 3L) ? StopEvent.ScheduleRelationship.Skipped : StopEvent.ScheduleRelationship.Scheduled;
        //Timestamps in GTFS need to be in seconds
        event.targetTime = java.sql.Timestamp.valueOf(common.getTargetDateTime()).getTime() / 1000; //Don't set if skipped?
        event.lastModifiedTimestamp = common.getLastModifiedUtcDateTime();

        if (properties != null) {
            event.getRouteData().stopId = Long.parseLong(properties.get(TransitdataProperties.KEY_STOP_ID));
            int pubtransDirection = Integer.parseInt(properties.get(TransitdataProperties.KEY_DIRECTION));
            event.getRouteData().direction = pubtransDirectionToGtfsDirection(pubtransDirection);
            event.getRouteData().routeName = properties.get(TransitdataProperties.KEY_ROUTE_NAME);
            event.getRouteData().operatingDay = properties.get(TransitdataProperties.KEY_OPERATING_DAY);
            event.getRouteData().startTime = properties.get(TransitdataProperties.KEY_START_TIME);
        }

        return event;
    }

    public static int pubtransDirectionToGtfsDirection(int pubtransDirection) {
        int gtfsDirection = pubtransDirection == 1 ? GtfsRtFactory.DIRECTION_ID_OUTBOUND : GtfsRtFactory.DIRECTION_ID_INBOUND;
        return gtfsDirection;
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

    public int getStopSeq() {
        return stopSeq;
    }

    public ScheduleRelationship getScheduleRelationship() {
        return scheduleRelationship;
    }

    public RouteData getRouteData() {
        return routeData;
    }

    public long getLastModifiedTimestamp() {
        return lastModifiedTimestamp;
    }
}
