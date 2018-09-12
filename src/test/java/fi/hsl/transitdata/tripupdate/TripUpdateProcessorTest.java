package fi.hsl.transitdata.tripupdate;

import com.google.transit.realtime.GtfsRealtime;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TripUpdateProcessorTest {
    @Test
    void testStopTimeUpdateCache() throws Exception {
        TripUpdateProcessor processor = new TripUpdateProcessor(null);

        final long firstDvjId = 99L;
        final String strFirstDvjId = Long.toString(firstDvjId);

        final int amount = 20;
        addStops(firstDvjId, amount, processor);

        final long secondDvjId = 100L;
        final String strSecondDvjId = Long.toString(secondDvjId);
        final int secondAmount = amount - 1;
        addStops(secondDvjId, secondAmount, processor);

        validateStops(strFirstDvjId, amount, processor);
        validateStops(strSecondDvjId, secondAmount, processor);
        validateStops("does-not-exist", 0, processor);

        //Adding stops to existing StopTimeUpdates should not increase the count, nor affect the ordering.
        int existingId = 1;
        addStop(firstDvjId, existingId, existingId, processor);
        validateStops(strFirstDvjId, amount, processor);
        //Adding with new sequenceIDs should again increase it
        int newId = amount;
        addStop(firstDvjId, newId, newId, processor);
        validateStops(strFirstDvjId, amount + 1, processor);

    }

    private void addStops(final long dvjId, final int amount, TripUpdateProcessor processor) throws Exception {
        int counter = 0;
        while(counter < amount) {
            final int stopSequence = counter;
            final int stopId = stopSequence;
            counter++;

            final String strDvjId = Long.toString(dvjId);
            addStop(dvjId, stopId, stopSequence, processor);

            //Should reflect to cache size
            Map<Integer, GtfsRealtime.TripUpdate.StopTimeUpdate> updates = processor.getUpdatesForJourney(strDvjId);
            assertEquals(updates.size(), counter);
        }
    }

    private void addStop(long dvjId, long stopId, int stopSequence, TripUpdateProcessor processor) throws Exception {
        final String strDvjId = Long.toString(dvjId);
        StopEvent first = StopEventTest.mockStopEvent(dvjId, stopId, stopSequence);
        //Update cache
        processor.updateStopTimeUpdateLists(strDvjId, first);
    }

    private void validateStops(final String dvjId, final int correctAmount, TripUpdateProcessor processor) throws Exception {
        Map<Integer, GtfsRealtime.TripUpdate.StopTimeUpdate> updates = processor.getUpdatesForJourney(dvjId);
        assertEquals(updates.size(), correctAmount);

        //Validate that stopIds and seqIds match and the sorting order is correct, by seqId
        int index = 0;
        for (GtfsRealtime.TripUpdate.StopTimeUpdate update: updates.values()) {
            assertEquals(Integer.toString(update.getStopSequence()), update.getStopId());
            assertEquals(update.getStopSequence(), index);

            index++;
        }
    }

}
