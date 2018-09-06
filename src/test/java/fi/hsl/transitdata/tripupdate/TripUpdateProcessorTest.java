package fi.hsl.transitdata.tripupdate;

import com.google.transit.realtime.GtfsRealtime;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class TripUpdateProcessorTest {
    @Test
    void testStopTimeUpdateCache() throws Exception {
        final long dvjId = 99L;
        final String strDvjId = Long.toString(dvjId);
        final long stopId = 10L;

        int stopSequence = 1;
        TripUpdateProcessor processor = new TripUpdateProcessor(null);

        StopEvent first = StopEventTest.mockStopEvent(dvjId, stopId, stopSequence);
        processor.updateStopTimeUpdateLists(strDvjId, first);
        Map<Integer, GtfsRealtime.TripUpdate.StopTimeUpdate> updates = processor.getUpdatesForJourney(strDvjId);
        assertTrue(updates.size() == 1);

        stopSequence++;

        StopEvent second = StopEventTest.mockStopEvent(dvjId, stopId, stopSequence);
        processor.updateStopTimeUpdateLists(strDvjId, second);

        Map<Integer, GtfsRealtime.TripUpdate.StopTimeUpdate> updates2 = processor.getUpdatesForJourney(strDvjId);
        assertTrue(updates2.size() == 2);


    }

}
