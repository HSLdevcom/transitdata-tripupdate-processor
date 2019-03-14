package fi.hsl.transitdata.tripupdate;

import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.transitdata.MockDataUtils;
import fi.hsl.common.transitdata.RouteData;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.common.transitdata.proto.PubtransTableProtos;
import fi.hsl.transitdata.tripupdate.gtfsrt.GtfsRtFactory;
import fi.hsl.transitdata.tripupdate.models.StopEvent;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class MockDataFactory {

    public static final long DEFAULT_DVJ_ID = 1234567890L;
    /*
    public static StopEvent mockStopEvent(PubtransTableProtos.Common common, Map<String, String> mockProps, StopEvent.EventType eventType) {
        return StopEvent.newInstance(common, mockProps, eventType);
    }

    public static StopEvent mockStopEvent(StopEvent.EventType eventType, long startTimeEpoch) {
        return mockStopEvent(eventType, 0, startTimeEpoch);
    }

    public static GtfsRealtime.TripUpdate.StopTimeEvent mockStopTimeEvent(StopEvent.EventType eventType, long startTimeEpoch) {
        GtfsRealtime.TripUpdate.StopTimeUpdate update = mockStopTimeUpdate(eventType, startTimeEpoch);
        if (eventType == StopEvent.EventType.Arrival)
            return update.getArrival();
        else
            return update.getDeparture();
    }*/

    public static GtfsRealtime.TripUpdate.StopTimeUpdate mockStopTimeUpdate(StopEvent.EventType eventType, long startTimeEpoch) {
        StopEvent event = MockDataFactory.mockStopEvent(eventType, startTimeEpoch);
        return GtfsRtFactory.newStopTimeUpdate(event);
    }

    public static GtfsRealtime.TripUpdate.StopTimeUpdate mockStopTimeUpdate(StopEvent arrival, StopEvent departure) {
        GtfsRealtime.TripUpdate.StopTimeUpdate arrivalUpdate = GtfsRtFactory.newStopTimeUpdate(arrival);
        GtfsRealtime.TripUpdate.StopTimeUpdate departureUpdate = GtfsRtFactory.newStopTimeUpdate(departure);
        //Merge these two
        return arrivalUpdate.toBuilder().setDeparture(departureUpdate.getDeparture()).build();
    }
    /*
    public static StopEvent mockStopEvent(StopEvent.EventType eventType, int stopSequence, long startTimeEpoch) {
        PubtransTableProtos.Common common = MockDataUtils.generateValidCommon(DEFAULT_DVJ_ID, stopSequence, startTimeEpoch * 1000).build();
        final int stopId = stopSequence;

        Map<String, String> props = new RouteData(stopId, GtfsRtFactory.DIRECTION_ID_INBOUND, "route-name", startTimeEpoch).toMap();
        return StopEvent.newInstance(common, props, eventType);
    }

    public static StopEvent mockStopEvent(String routeId) {

        Map<String, String> mockProperties = new RouteData(MockDataUtils.generateValidJoreId(), GtfsRtFactory.DIRECTION_ID_OUTBOUND, routeId, "20180101", "11:22:00").toMap();
        PubtransTableProtos.Common mockCommon = MockDataUtils.generateValidCommon().build();
        return StopEvent.newInstance(mockCommon, mockProperties, StopEvent.EventType.Arrival);
    }*/

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
