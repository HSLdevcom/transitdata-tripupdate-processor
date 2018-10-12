package fi.hsl.transitdata.tripupdate;

import org.junit.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import static com.google.transit.realtime.GtfsRealtime.TripUpdate.*;
import static junit.framework.TestCase.assertTrue;

public class GtfsRtValidatorTest {
    final static long[] ARRIVALS =   new long[] { 1545674400L, 1545674500L, 1545674600L };
    final static long[] DEPARTURES = new long[] { 1545674450L, 1545674550L, 1545674650L };

    final static long DVI_ID = 1234567890L;
    final static long JPP_ID = 9876543210L;

    @Test
    public void testValidateTime() {
        long epoch = ARRIVALS[1];
        final Optional<StopTimeEvent> stopTimeEvent = Optional.of(MockDataFactory.mockStopTimeEvent(StopEvent.EventType.Arrival, epoch));

        Optional<Long> laterMinTime = Optional.of(ARRIVALS[2]);
        Optional<StopTimeEvent> shouldBeChanged = GtfsRtValidator.validateTime(stopTimeEvent, laterMinTime);
        assertTrue(stopTimeEvent.get().getTime() < shouldBeChanged.get().getTime());
        assertTrue(laterMinTime.get() == shouldBeChanged.get().getTime());

        Optional<Long> sameMinTime = Optional.of(ARRIVALS[1]);
        Optional<StopTimeEvent> shouldBeSame = GtfsRtValidator.validateTime(stopTimeEvent, sameMinTime);
        assertTrue(stopTimeEvent.get().getTime() == shouldBeSame.get().getTime());

        Optional<Long> earlierMinTime = Optional.of(ARRIVALS[0]);
        Optional<StopTimeEvent> shouldBeSameAgain = GtfsRtValidator.validateTime(stopTimeEvent, earlierMinTime);
        assertTrue(stopTimeEvent.get().getTime() == shouldBeSameAgain.get().getTime());

        //If timestamp is empty, we should receive the original
        Optional<StopTimeEvent> shouldBeOriginal = GtfsRtValidator.validateTime(stopTimeEvent, Optional.empty());
        assertTrue(stopTimeEvent.get().getTime() == shouldBeOriginal.get().getTime());

        //If event is empty we should get nothing
        Optional<StopTimeEvent> nothing = GtfsRtValidator.validateTime(Optional.empty(), Optional.of(ARRIVALS[0]));
        assertTrue(!nothing.isPresent());
        Optional<StopTimeEvent> stillNothing = GtfsRtValidator.validateTime(Optional.empty(), Optional.empty());
        assertTrue(!stillNothing.isPresent());


    }

    @Test
    public void testStopSequences() {

        List<StopTimeUpdate> updates = new LinkedList<>();
        for (int stopSequence = 1; stopSequence < 100; stopSequence++) {
            //Let's switch types to make sure both work
            StopEvent.EventType type = stopSequence % 2 == 0 ? StopEvent.EventType.Arrival :StopEvent.EventType.Departure;
            StopEvent event = MockDataFactory.mockStopEvent(MockDataFactory.mockCommon(DVI_ID, stopSequence, JPP_ID), null, type);
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
