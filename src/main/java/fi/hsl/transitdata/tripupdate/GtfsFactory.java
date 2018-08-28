package fi.hsl.transitdata.tripupdate;

import com.google.transit.realtime.GtfsRealtime;

public class GtfsFactory {
    private GtfsFactory() {}


    public static GtfsRealtime.FeedMessage newFeedMessage(GtfsRealtime.TripUpdate tripUpdate, long timestamp) {

        GtfsRealtime.FeedHeader header = GtfsRealtime.FeedHeader.newBuilder()
                .setGtfsRealtimeVersion("2.0")
                .setIncrementality(GtfsRealtime.FeedHeader.Incrementality.DIFFERENTIAL)
                .setTimestamp(timestamp)
                .build();

        GtfsRealtime.FeedEntity entity = GtfsRealtime.FeedEntity.newBuilder()
                .setTripUpdate(tripUpdate)
                .setId("test") //TODO fix?
                .build();

        return GtfsRealtime.FeedMessage.newBuilder().addEntity(entity).setHeader(header).build();
    }

    public static GtfsRealtime.TripUpdate.StopTimeUpdate newStopTimeUpdate(StopEvent stopEvent) {
        return newStopTimeUpdateFromPrevious(stopEvent, null);
    }

    public static GtfsRealtime.TripUpdate.StopTimeUpdate newStopTimeUpdateFromPrevious(StopEvent stopEvent, GtfsRealtime.TripUpdate.StopTimeUpdate previousUpdate) {

        GtfsRealtime.TripUpdate.StopTimeUpdate.Builder stopTimeUpdateBuilder = null;
        if (previousUpdate != null) {
            stopTimeUpdateBuilder = previousUpdate.toBuilder();
        }
        else {
            stopTimeUpdateBuilder = GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder()
                    .setStopId(String.valueOf(stopEvent.stop_id))
                    .setStopSequence(stopEvent.stop_seq);
        }

        switch (stopEvent.schedule_relationship) {
            case Skipped:
                stopTimeUpdateBuilder.setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED);
                break;
            case Scheduled:
                stopTimeUpdateBuilder.setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SCHEDULED);
                break;
        }

        GtfsRealtime.TripUpdate.StopTimeEvent stopTimeEvent = GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder()
                .setTime(stopEvent.target_time)
                .build();
        switch (stopEvent.event_type) {
            case Arrival:
                stopTimeUpdateBuilder.setArrival(stopTimeEvent);
                break;
            case Departure:
                stopTimeUpdateBuilder.setDeparture(stopTimeEvent);
                break;
        }

        return stopTimeUpdateBuilder.build();
    }
}
