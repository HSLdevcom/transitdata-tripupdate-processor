package fi.hsl.transitdata.tripupdate;

public class StopEvent {
    //TODO Add getters and factory method / builder

    long dated_vehicle_journey_id;
    public enum EventType {
        Arrival, Departure
    }
    EventType event_type;
    long target_time;
    long stop_id;
    int stop_seq;

    public enum ScheduleRelationship {
        Scheduled, Skipped
    }
    ScheduleRelationship schedule_relationship;

    final RouteData routeData = new RouteData();

    public static class RouteData {
        String route_name;
        int direction;
        String operating_day;
        String start_time;
    }

}
