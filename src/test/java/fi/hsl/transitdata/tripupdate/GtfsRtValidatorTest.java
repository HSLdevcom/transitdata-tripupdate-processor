package fi.hsl.transitdata.tripupdate;

import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

import static com.google.transit.realtime.GtfsRealtime.TripUpdate.*;
import static junit.framework.TestCase.assertTrue;

public class GtfsRtValidatorTest {
    @Test
    public void testStopSequences() {
        final long DVI_ID = 1234567890L;
        final long JPP_ID = 9876543210L;

        List<StopTimeUpdate> updates = new LinkedList<>();
        for (int stopSequence = 1; stopSequence < 100; stopSequence++) {
            //Let's switch types to make sure both work
            StopEvent.EventType type = stopSequence % 2 == 0 ? StopEvent.EventType.Arrival :StopEvent.EventType.Departure;
            StopEvent event = StopEventTest.mockStopEvent(DVI_ID, JPP_ID, stopSequence, type);
            StopTimeUpdate update = GtfsRtFactory.newStopTimeUpdate(event);
            updates.add(update);
        }

        List<StopTimeUpdate> stopSeqRemoved = GtfsRtValidator.removeStopSequences(updates);
        assertTrue(stopSeqRemoved.size() == updates.size());
        checkStopSequences(stopSeqRemoved);

        List<StopTimeUpdate> fullValidation = GtfsRtValidator.cleanStopTimeUpdates(updates, null);
        assertTrue(fullValidation.size() == updates.size());
        checkStopSequences(fullValidation);

        // Originals should not be modified
        updates.forEach(update -> assertTrue(update.hasStopSequence() && update.getStopSequence() > 0));

    }

    private void checkStopSequences(List<StopTimeUpdate> stopSeqRemoved) {
        stopSeqRemoved.forEach(update -> {
            assertTrue(!update.hasStopSequence());
            //Validate that other data hasn't been wiped
            assertTrue(update.hasArrival() | update.hasDeparture());
            if (update.hasArrival()) {
                assertTrue(update.getArrival().hasTime());
            }
            else if (update.hasDeparture()) {
                assertTrue(update.getDeparture().hasTime());
            }
        });
    }

}
