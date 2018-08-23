package fi.hsl.transitdata.tripupdate;

public class StopEvent {

    long dated_vehicle_journey_id;
    public enum EventType {
        Arrival, Departure
    }
    EventType event_type;
    long target_time;
    int stop_id;
    int stop_seq;

    public enum ScheduleRelationship {
        Scheduled, Skipped
    }
    ScheduleRelationship schedule_relationship;
}
