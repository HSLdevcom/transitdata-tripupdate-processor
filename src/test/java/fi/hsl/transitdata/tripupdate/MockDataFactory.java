package fi.hsl.transitdata.tripupdate;

import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.proto.PubtransTableProtos;
import fi.hsl.transitdata.tripupdate.gtfsrt.GtfsRtFactory;
import fi.hsl.transitdata.tripupdate.models.StopEvent;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class MockDataFactory {

    public static final long DEFAULT_DVJ_ID = 1234567890L;
    public static final long DEFAULT_JPP_ID = 9876543210L;

    public static final SimpleDateFormat START_TIME_FORMAT = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");


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

    public static StopEvent mockStopEvent(StopEvent.EventType eventType, int stopSequence, long startTimeEpoch) {
        String startTimeAsString = START_TIME_FORMAT.format(new Date());
        PubtransTableProtos.Common common = mockCommon(DEFAULT_DVJ_ID, stopSequence, DEFAULT_JPP_ID, startTimeEpoch * 1000);
        //System.out.println(startTimeAsString);
        final int stopId = stopSequence;
        String[] dateAndTime = startTimeAsString.split(" ");

        Map<String, String> props = mockMessageProperties(stopId, GtfsRtFactory.DIRECTION_ID_INBOUND, "route-name", dateAndTime[0], dateAndTime[1]);
        return StopEvent.newInstance(common, props, eventType);
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
}
