package fi.hsl.transitdata.tripupdate.models;

import fi.hsl.common.transitdata.MockDataUtils;
import fi.hsl.common.transitdata.RouteData;
import fi.hsl.common.transitdata.proto.PubtransTableProtos;
import fi.hsl.transitdata.tripupdate.MockDataFactory;
import fi.hsl.transitdata.tripupdate.models.StopEvent;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class StopEventTest {

    private static final long DVJ_ID = Long.MAX_VALUE - 1;
    private static final int STOP_SEQ = Integer.MAX_VALUE;

    private static final int DIRECTION = 1;
    private static final String ROUTE_NAME = "route-abc";
    private static final String OPERATING_DAY = "20181225";
    private static final String START_TIME = "14:05:05";
    private static final long STOP_ID = Long.MAX_VALUE;

    @Test
    public void instantiateWithoutProperties() {
        PubtransTableProtos.Common common = MockDataUtils.generateValidCommon(DVJ_ID, STOP_SEQ).build();
        StopEvent stop = StopEvent.newInstance(common, null, StopEvent.EventType.Arrival);

        assertIds(stop);
        assertTrue(stop.getEventType() == StopEvent.EventType.Arrival);
        assertNull(stop.getRouteData().getOperatingDay());
        assertNull(stop.getRouteData().getRouteName());
        assertNull(stop.getRouteData().getStartTime());
        assertEquals(stop.getRouteData().getStopId(), 0);
    }

    @Test
    public void instantiateFully() {
        PubtransTableProtos.Common common = MockDataUtils.generateValidCommon(DVJ_ID, STOP_SEQ).build();
        Map<String, String> props = new RouteData(STOP_ID, DIRECTION, ROUTE_NAME, OPERATING_DAY, START_TIME).toMap();

        StopEvent stop = StopEvent.newInstance(common, props, StopEvent.EventType.Departure);

        assertIds(stop);
        assertTrue(stop.getEventType() == StopEvent.EventType.Departure);
        assertEquals(stop.getRouteData().getOperatingDay(), OPERATING_DAY);
        assertEquals(stop.getRouteData().getRouteName(), ROUTE_NAME);
        assertEquals(stop.getRouteData().getStartTime(), START_TIME);
        assertEquals(stop.getRouteData().getDirection(), StopEvent.pubtransDirectionToGtfsDirection(DIRECTION));
        assertEquals(stop.getRouteData().getStopId(), STOP_ID);
    }

    @Test
    public void testTimestampConversion() {
        PubtransTableProtos.Common common = MockDataUtils.generateValidCommon(DVJ_ID, STOP_SEQ).build();
        final long lastModifiedMs = common.getLastModifiedUtcDateTimeMs();
        StopEvent stop = StopEvent.newInstance(common, null, StopEvent.EventType.Arrival);

        assertEquals(lastModifiedMs, stop.getLastModifiedTimestamp(TimeUnit.MILLISECONDS));
        assertEquals(lastModifiedMs / 1000, stop.getLastModifiedTimestamp(TimeUnit.SECONDS));
        assertEquals(lastModifiedMs / (60 * 1000), stop.getLastModifiedTimestamp(TimeUnit.MINUTES));
    }

    private void assertIds(StopEvent stop) {
        assertTrue(stop.getDatedVehicleJourneyId() == DVJ_ID);
        assertTrue(stop.getStopSeq() == STOP_SEQ);
    }

}
