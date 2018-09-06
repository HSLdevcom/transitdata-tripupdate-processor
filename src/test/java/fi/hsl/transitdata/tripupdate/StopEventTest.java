package fi.hsl.transitdata.tripupdate;

import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.proto.PubtransTableProtos;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class StopEventTest {

    private static final long DVJ_ID = Long.MAX_VALUE - 1;
    private static final long STOP_ID = Long.MAX_VALUE;
    private static final int STOP_SEQ = Integer.MAX_VALUE;

    private static final int DIRECTION = 1;
    private static final String ROUTE_NAME = "route-abc";
    private static final String OPERATING_DAY = "monday";
    private static final String START_TIME = "2010-10-25 14:05:05";

    @Test
    public void instantiateWithoutProperties() {
        PubtransTableProtos.Common common = mockCommon(DVJ_ID, STOP_ID, STOP_SEQ);
        StopEvent stop = StopEvent.newInstance(common, null, StopEvent.EventType.Arrival);

        assertIds(stop);
        assertTrue(stop.getEventType() == StopEvent.EventType.Arrival);
        assertNull(stop.getRouteData().getOperatingDay());
        assertNull(stop.getRouteData().getRouteName());
        assertNull(stop.getRouteData().getStartTime());
    }

    @Test
    public void instantiateFully() {
        PubtransTableProtos.Common common = mockCommon(DVJ_ID, STOP_ID, STOP_SEQ);
        Map<String, String> props = mockMessageProperties();

        StopEvent stop = StopEvent.newInstance(common, props, StopEvent.EventType.Departure);

        assertIds(stop);
        assertTrue(stop.getEventType() == StopEvent.EventType.Departure);
        assertEquals(stop.getRouteData().getOperatingDay(), OPERATING_DAY);
        assertEquals(stop.getRouteData().getRouteName(), ROUTE_NAME);
        assertEquals(stop.getRouteData().getStartTime(), START_TIME);
        assertEquals(stop.getRouteData().getDirection(), DIRECTION);
    }

    private void assertIds(StopEvent stop) {
        assertTrue(stop.getStopId() == STOP_ID);
        assertTrue(stop.getDatedVehicleJourneyId() == DVJ_ID);
        assertTrue(stop.getStopSeq() == STOP_SEQ);
    }

    public static StopEvent mockStopEvent(long dvjId, long stopId, int stopSequence) {
        PubtransTableProtos.Common common = mockCommon(dvjId, stopId, stopSequence);
        return StopEvent.newInstance(common, null, StopEvent.EventType.Arrival);
    }

    public static Map<String, String> mockMessageProperties() {

        Map<String, String> props = new HashMap<>();
        props.put(TransitdataProperties.KEY_DIRECTION, Integer.toString(DIRECTION));
        props.put(TransitdataProperties.KEY_ROUTE_NAME, ROUTE_NAME);
        props.put(TransitdataProperties.KEY_OPERATING_DAY, OPERATING_DAY);
        props.put(TransitdataProperties.KEY_START_TIME, START_TIME);
        return props;
    }

    static PubtransTableProtos.Common mockCommon(long dvjId, long stopId, int stopSequence) {
        PubtransTableProtos.Common.Builder commonBuilder = PubtransTableProtos.Common.newBuilder();
        commonBuilder.setIsOnDatedVehicleJourneyId(dvjId);
        commonBuilder.setIsTargetedAtJourneyPatternPointGid(stopId);
        commonBuilder.setJourneyPatternSequenceNumber(stopSequence);

        commonBuilder.setState(3L);
        commonBuilder.setTargetDateTime("2018-12-24 18:00:00");
        commonBuilder.setSchemaVersion(commonBuilder.getSchemaVersion());

        commonBuilder.setId(987654321L);
        commonBuilder.setIsTimetabledAtJourneyPatternPointGid(1);
        commonBuilder.setVisitCountNumber(2);
        commonBuilder.setType(3);
        commonBuilder.setIsValidYesNo(true);
        commonBuilder.setLastModifiedUtcDateTime(1536218315L);

        return commonBuilder.build();
    }
}
