package fi.hsl.transitdata.tripupdate;

import com.google.transit.realtime.GtfsRealtime;
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
    public static final long DEFAULT_JPP_ID = 9876543210L;

    public static final SimpleDateFormat START_TIME_FORMAT = new SimpleDateFormat("yyyyMMdd HH:mm:ss");


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
    }

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

    public static String[] formatStopEventTargetDateTime(long startTimeEpoch) {
        //TODO refactor
        String startTimeAsString = START_TIME_FORMAT.format(new Date(startTimeEpoch * 1000));
        return startTimeAsString.split(" ");
    }

    public static StopEvent mockStopEvent(StopEvent.EventType eventType, int stopSequence, long startTimeEpoch) {
        PubtransTableProtos.Common common = mockCommon(DEFAULT_DVJ_ID, stopSequence, DEFAULT_JPP_ID, startTimeEpoch * 1000);
        //System.out.println(startTimeAsString);
        final int stopId = stopSequence;
        String[] dateAndTime = formatStopEventTargetDateTime(startTimeEpoch);

        Map<String, String> props = mockMessageProperties(stopId, GtfsRtFactory.DIRECTION_ID_INBOUND, "route-name", dateAndTime[0], dateAndTime[1]);
        return StopEvent.newInstance(common, props, eventType);
    }

    public static StopEvent mockStopEvent(String routeId) {

        Map<String, String> mockProperties = mockMessageProperties(1234567, 0, routeId, "20180101", "11:22:00");
        PubtransTableProtos.Common mockCommon = mockCommon(111, 2, 333);
        return StopEvent.newInstance(mockCommon, mockProperties, StopEvent.EventType.Arrival);
    }

    public static Map<String, String> mockMessageProperties(long stopId, int direction, String routeName, String operatingDay, String startTime) {

        Map<String, String> props = new HashMap<>();
        props.put(TransitdataProperties.KEY_DIRECTION, Integer.toString(direction));
        props.put(TransitdataProperties.KEY_ROUTE_NAME, routeName);
        props.put(TransitdataProperties.KEY_OPERATING_DAY, operatingDay);
        props.put(TransitdataProperties.KEY_START_TIME, startTime);
        props.put(TransitdataProperties.KEY_STOP_ID, Long.toString(stopId));
        return props;
    }

    public static PubtransTableProtos.Common mockCommon(long dvjId, int stopSequence, long jppId) {
        return mockCommon(dvjId, stopSequence, jppId, 1545674400000L);
    }


    public static PubtransTableProtos.Common mockCommon(long dvjId, int stopSequence, long jppId, long targetDateTimeMs) {
        PubtransTableProtos.Common.Builder commonBuilder = PubtransTableProtos.Common.newBuilder();
        commonBuilder.setIsOnDatedVehicleJourneyId(dvjId);
        commonBuilder.setIsTargetedAtJourneyPatternPointGid(jppId);
        commonBuilder.setJourneyPatternSequenceNumber(stopSequence);

        commonBuilder.setState(3L);
        commonBuilder.setTargetUtcDateTimeMs(targetDateTimeMs);
        commonBuilder.setSchemaVersion(commonBuilder.getSchemaVersion());

        commonBuilder.setId(987654321L);
        commonBuilder.setIsTimetabledAtJourneyPatternPointGid(1);
        commonBuilder.setVisitCountNumber(2);
        commonBuilder.setType(3);
        commonBuilder.setIsValidYesNo(true);
        commonBuilder.setLastModifiedUtcDateTimeMs(1536218315000L);

        return commonBuilder.build();
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

    public static InternalMessages.TripCancellation mockTripCancellation(String routeId, int directionId,
                                                                         String startDate, String startTime) {

        InternalMessages.TripCancellation.Builder tripCancellationBuilder = InternalMessages.TripCancellation.newBuilder();

        tripCancellationBuilder.setRouteId(routeId)
                .setDirectionId(directionId)
                .setStartDate(startDate)
                .setStartTime(startTime)
                .setSchemaVersion(tripCancellationBuilder.getSchemaVersion())
                .setStatus(InternalMessages.TripCancellation.Status.CANCELED);

        return tripCancellationBuilder.build();
    }

    public static InternalMessages.TripCancellation mockTripCancellation(String routeId) {
        return mockTripCancellation(routeId, 0, "20180101", "11:22:00");
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
