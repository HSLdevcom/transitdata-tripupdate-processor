package fi.hsl.transitdata.tripupdate;

import com.google.transit.realtime.GtfsRealtime;

public class GtfsRtFactory {
    private GtfsRtFactory() {}


    public static GtfsRealtime.FeedMessage newFeedMessage(String id, GtfsRealtime.TripUpdate tripUpdate, long timestamp) {

        GtfsRealtime.FeedHeader header = GtfsRealtime.FeedHeader.newBuilder()
                .setGtfsRealtimeVersion("2.0")
                .setIncrementality(GtfsRealtime.FeedHeader.Incrementality.DIFFERENTIAL)
                .setTimestamp(timestamp)
                .build();

        GtfsRealtime.FeedEntity entity = GtfsRealtime.FeedEntity.newBuilder()
                .setTripUpdate(tripUpdate)
                .setId(id)
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
                .setRouteId(event.getRouteData().getRouteName())
                .setDirectionId(event.getRouteData().getDirection())
                .setStartDate(event.getRouteData().getOperatingDay())
                .setStartTime(event.getRouteData().getStartTime())
                .build();

        GtfsRealtime.TripUpdate.Builder tripUpdateBuilder = GtfsRealtime.TripUpdate.newBuilder()
                .setTrip(tripDescriptor);

        return tripUpdateBuilder.build();
    }

}