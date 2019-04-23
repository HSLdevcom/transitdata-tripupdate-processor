package fi.hsl.transitdata.tripupdate;

import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.transitdata.MockDataUtils;
import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.transitdata.tripupdate.gtfsrt.GtfsRtFactory;

public class MockDataFactory {

    public static GtfsRealtime.TripUpdate.StopTimeEvent mockStopTimeEvent(InternalMessages.StopEstimate.Type eventType, long startTimeEpoch) throws Exception {
        GtfsRealtime.TripUpdate.StopTimeUpdate update = mockStopTimeUpdate(eventType, startTimeEpoch);
        if (eventType == InternalMessages.StopEstimate.Type.ARRIVAL)
            return update.getArrival();
        else
            return update.getDeparture();
    }


    public static GtfsRealtime.TripUpdate.StopTimeUpdate mockStopTimeUpdate(InternalMessages.StopEstimate.Type eventType, long startTimeEpoch) throws Exception {
        InternalMessages.StopEstimate estimate = MockDataUtils.mockStopEstimate(eventType, startTimeEpoch);
        return GtfsRtFactory.newStopTimeUpdate(estimate);
    }

    public static GtfsRealtime.TripUpdate.StopTimeUpdate mockStopTimeUpdate(InternalMessages.StopEstimate arrival, InternalMessages.StopEstimate departure) {
        GtfsRealtime.TripUpdate.StopTimeUpdate arrivalUpdate = GtfsRtFactory.newStopTimeUpdate(arrival);
        GtfsRealtime.TripUpdate.StopTimeUpdate departureUpdate = GtfsRtFactory.newStopTimeUpdate(departure);
        //Merge these two
        return arrivalUpdate.toBuilder().setDeparture(departureUpdate.getDeparture()).build();
    }

    public static GtfsRealtime.TripUpdate.StopTimeUpdate mockStopTimeUpdate(String stopId, long arrivalTime,
                                                                         long departureTime) {

        GtfsRealtime.TripUpdate.StopTimeEvent arrival = GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder()
                .setTime(arrivalTime)
                .build();

        GtfsRealtime.TripUpdate.StopTimeEvent departure = GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder()
                .setTime(departureTime)
                .build();

        GtfsRealtime.TripUpdate.StopTimeUpdate.Builder stopTimeUpdateBuilder = GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder();

        if (arrivalTime != 0) {
            stopTimeUpdateBuilder.setArrival(arrival);
        }
        if (departureTime != 0) {
            stopTimeUpdateBuilder.setDeparture(departure);
        }

        return stopTimeUpdateBuilder.setStopId(stopId).build();
    }


    public static GtfsRealtime.TripUpdate mockTripUpdate(String routeId, int directionId,
                                                      String startDate, String startTime,
                                                      Iterable<GtfsRealtime.TripUpdate.StopTimeUpdate> stopTimeUpdateList) {

        GtfsRealtime.TripDescriptor tripDescriptor = GtfsRealtime.TripDescriptor.newBuilder()
                .setRouteId(routeId)
                .setDirectionId(directionId)
                .setStartDate(startDate)
                .setStartTime(startTime)
                .build();

        GtfsRealtime.TripUpdate.Builder tripUpdateBuilder = GtfsRealtime.TripUpdate.newBuilder().setTrip(tripDescriptor);

        if (stopTimeUpdateList != null) {
            tripUpdateBuilder.addAllStopTimeUpdate(stopTimeUpdateList);
        }

        return tripUpdateBuilder.build();
    }

    public static GtfsRealtime.TripUpdate mockTripUpdate(String routeId, int directionId, String startDate, String startTime) {
        return mockTripUpdate(routeId, directionId, startDate, startTime, null);
    }
}
