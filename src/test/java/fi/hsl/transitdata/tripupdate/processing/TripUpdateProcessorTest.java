package fi.hsl.transitdata.tripupdate.processing;

import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.transitdata.MockDataUtils;
import fi.hsl.common.transitdata.RouteData;
import fi.hsl.common.transitdata.proto.PubtransTableProtos;
import fi.hsl.transitdata.tripupdate.MockDataFactory;
import fi.hsl.transitdata.tripupdate.models.StopEvent;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TripUpdateProcessorTest {
    @Test
    public void testStopTimeUpdateCache() throws Exception {
        TripUpdateProcessor processor = new TripUpdateProcessor(null);

        final long firstDvjId = 99L;

        final int amount = 20;
        addStops(firstDvjId, amount, processor);

        final long secondDvjId = 100L;
        final int secondAmount = amount - 1;
        addStops(secondDvjId, secondAmount, processor);

        validateStops(firstDvjId, amount, processor);
        validateStops(secondDvjId, secondAmount, processor);
        final long nonExistingId = 69;
        validateStops(nonExistingId, 0, processor);

        //Adding stops to existing StopTimeUpdates should not increase the count, nor affect the ordering.
        int existingId = 1;
        addStop(firstDvjId, existingId, existingId, processor);
        validateStops(firstDvjId, amount, processor);
        //Adding with new sequenceIDs should again increase it
        int newId = amount;
        addStop(firstDvjId, newId, newId, processor);
        validateStops(firstDvjId, amount + 1, processor);

    }

    private void addStops(final long dvjId, final int amount, TripUpdateProcessor processor) throws Exception {
        int counter = 0;
        while(counter < amount) {
            final int stopSequence = counter;
            final int stopId = stopSequence;
            counter++;

            addStop(dvjId, stopId, stopSequence, processor);

            //Should reflect to cache size
            List<GtfsRealtime.TripUpdate.StopTimeUpdate> updates = processor.getStopTimeUpdates(dvjId);
            assertEquals(updates.size(), counter);
        }
    }

    private void addStop(long dvjId, long stopId, int stopSequence, TripUpdateProcessor processor) throws Exception {
        PubtransTableProtos.Common common = MockDataUtils.generateValidCommon(dvjId, stopSequence).build();
        final int direction = 1;
        final String routeName = "69A";
        final String operatingDay = "monday";
        final String startTime = "2010-10-25 14:05:05";

        Map<String, String> props = new RouteData(stopId, direction, routeName, operatingDay, startTime).toMap();
        StopEvent first = StopEvent.newInstance(common, props, StopEvent.EventType.Arrival);

        //Update cache
        processor.updateStopTimeUpdateCache(first);
    }

    private void validateStops(final long dvjId, final int correctAmount, TripUpdateProcessor processor) throws Exception {
        List<GtfsRealtime.TripUpdate.StopTimeUpdate> updates = processor.getStopTimeUpdates(dvjId);
        assertEquals(updates.size(), correctAmount);

        //Validate that stopIds and seqIds match and the sorting order is correct, by seqId
        int index = 0;
        for (GtfsRealtime.TripUpdate.StopTimeUpdate update: updates) {
            assertEquals(Integer.toString(update.getStopSequence()), update.getStopId());
            assertEquals(update.getStopSequence(), index);

            index++;
        }
    }

}
