package fi.hsl.transitdata.tripupdate.gtfsrt;

import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.transitdata.MockDataUtils;
import fi.hsl.transitdata.tripupdate.MockDataFactory;
import fi.hsl.transitdata.tripupdate.models.StopEvent;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import static com.google.transit.realtime.GtfsRealtime.TripUpdate.*;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class GtfsRtValidatorTest {
    final static long[] ARRIVALS =   new long[] { 1545674400L, 1545674500L, 1545674600L };
    final static long[] DEPARTURES = new long[] { 1545674450L, 1545674550L, 1545674650L };

    final static long DVI_ID = 1234567890L;
    final static long JPP_ID = 9876543210L;
    /*
    TODO ENABLE BACK
    @Test
    public void testValidateTime() {
        long epoch = ARRIVALS[1];
        final Optional<StopTimeEvent> stopTimeEvent = Optional.of(MockDataFactory.mockStopTimeEvent(StopEvent.EventType.Arrival, epoch));

        Optional<Long> laterMinTime = Optional.of(ARRIVALS[2]);
        Optional<StopTimeEvent> shouldBeChanged = GtfsRtValidator.validateMinTime(stopTimeEvent, laterMinTime);
        assertTrue(stopTimeEvent.get().getTime() < shouldBeChanged.get().getTime());
        assertTrue(laterMinTime.get() == shouldBeChanged.get().getTime());

        Optional<Long> sameMinTime = Optional.of(ARRIVALS[1]);
        Optional<StopTimeEvent> shouldBeSame = GtfsRtValidator.validateMinTime(stopTimeEvent, sameMinTime);
        assertTrue(stopTimeEvent.get().getTime() == shouldBeSame.get().getTime());

        Optional<Long> earlierMinTime = Optional.of(ARRIVALS[0]);
        Optional<StopTimeEvent> shouldBeSameAgain = GtfsRtValidator.validateMinTime(stopTimeEvent, earlierMinTime);
        assertTrue(stopTimeEvent.get().getTime() == shouldBeSameAgain.get().getTime());

        //If timestamp is empty, we should receive the original
        Optional<StopTimeEvent> shouldBeOriginal = GtfsRtValidator.validateMinTime(stopTimeEvent, Optional.empty());
        assertTrue(stopTimeEvent.get().getTime() == shouldBeOriginal.get().getTime());

        //If event is empty we should get nothing
        Optional<StopTimeEvent> nothing = GtfsRtValidator.validateMinTime(Optional.empty(), Optional.of(ARRIVALS[0]));
        assertTrue(!nothing.isPresent());
        Optional<StopTimeEvent> stillNothing = GtfsRtValidator.validateMinTime(Optional.empty(), Optional.empty());
        assertTrue(!stillNothing.isPresent());
    }

    @Test
    public void testValidateTimestampsOnlyArrival() {
        long[] timestamps = ARRIVALS;
        final StopTimeUpdate firstArrival = MockDataFactory.mockStopTimeUpdate(StopEvent.EventType.Arrival, timestamps[0]);
        final StopTimeUpdate secondArrival = MockDataFactory.mockStopTimeUpdate(StopEvent.EventType.Arrival, timestamps[1]);

        validateTimestamps(firstArrival, secondArrival, GtfsRtValidator.OnConflict.ArrivalWins, timestamps[1], 0);
        validateTimestamps(secondArrival, firstArrival, GtfsRtValidator.OnConflict.ArrivalWins, timestamps[1], 0);
        validateTimestamps(firstArrival, firstArrival, GtfsRtValidator.OnConflict.ArrivalWins, timestamps[0], 0);
        validateTimestamps(secondArrival, firstArrival, GtfsRtValidator.OnConflict.DepartureWins, timestamps[1], 0);
    }

    @Test
    public void testValidateTimestampsBothArrivalAndDeparture() {

        final StopTimeUpdate first = MockDataFactory.mockStopTimeUpdate(
                MockDataFactory.mockStopEvent(StopEvent.EventType.Arrival, ARRIVALS[0]),
                MockDataFactory.mockStopEvent(StopEvent.EventType.Departure, DEPARTURES[0]));

        final StopTimeUpdate second = MockDataFactory.mockStopTimeUpdate(
                MockDataFactory.mockStopEvent(StopEvent.EventType.Arrival, ARRIVALS[1]),
                MockDataFactory.mockStopEvent(StopEvent.EventType.Departure, DEPARTURES[1]));

        validateTimestamps(first, second, GtfsRtValidator.OnConflict.ArrivalWins, ARRIVALS[1], DEPARTURES[1]);
        validateTimestamps(first, second, GtfsRtValidator.OnConflict.DepartureWins, ARRIVALS[1], DEPARTURES[1]);

    }


    void validateTimestamps(StopTimeUpdate prev, StopTimeUpdate cur, GtfsRtValidator.OnConflict onConflict, long expectedArrival, long expectedDeparture) {

        StopTimeUpdate validated = GtfsRtValidator.validateTimestamps(prev, cur, onConflict);
        assertTrue(validated.hasArrival() == cur.hasArrival());
        assertTrue(validated.hasDeparture() == cur.hasDeparture());
        if (validated.hasArrival()) {
            assertEquals(validated.getArrival().getTime(), expectedArrival);
        }
        if (validated.hasDeparture()) {
            assertEquals(validated.getDeparture().getTime(), expectedDeparture);
        }
    }

    @Test
    public void testArrivalAndDepartureSequences() {
        {
            //Best case scenario, all in correct order
            // A0  D0  A1  D1  A2  D2
            LinkedList<StopTimeUpdate> raw = createStopTimeUpdateSequence(ARRIVALS, DEPARTURES);
            List<StopTimeUpdate> same = GtfsRtValidator.validateArrivalsAndDepartures(raw, raw.getLast());

            for (int n = 0; n < raw.size(); n++) {
                StopTimeUpdate orig = raw.get(n);
                StopTimeUpdate updated = same.get(n);
                assertTrue(orig.getArrival().getTime() == updated.getArrival().getTime());
                assertTrue(orig.getDeparture().getTime() == updated.getDeparture().getTime());
            }
        }
        {
            //Swap timestamps between first and second timestamp. first ones should be raised to later departure
            // A1  D1  A0  D0  A2  D2 => A1 D1 D1  D1 A2  D2
            long[] newArrivals = copyAndSwap(ARRIVALS, 0, 1);
            long[] newDepartures = copyAndSwap(DEPARTURES, 0, 1);
            LinkedList<StopTimeUpdate> raw = createStopTimeUpdateSequence(newArrivals, newDepartures);

            List<StopTimeUpdate> updated = GtfsRtValidator.validateArrivalsAndDepartures(raw, raw.getLast());

            StopTimeUpdate updatedFirst = updated.get(0);
            assertTrue(updatedFirst.getArrival().getTime() == ARRIVALS[1]);
            assertTrue(updatedFirst.getDeparture().getTime() == DEPARTURES[1]);

            StopTimeUpdate updatedSecond = updated.get(1);
            assertTrue(updatedSecond.getArrival().getTime() == DEPARTURES[1]);
            assertTrue(updatedSecond.getDeparture().getTime() == DEPARTURES[1]);

            StopTimeUpdate updatedThird = updated.get(2);
            assertTrue(updatedThird.getArrival().getTime() == ARRIVALS[2]);
            assertTrue(updatedThird.getDeparture().getTime() == DEPARTURES[2]);
        }
        {
            //Swap timestamps between first and last timestamp. all previous should be raised to the last departure
            // A2 D2 A1  D1  A0  D0   => A2 D2 D2 D2 D2 D2
            long[] newArrivals = copyAndSwap(ARRIVALS, 0, 2);
            long[] newDepartures = copyAndSwap(DEPARTURES, 0, 2);
            LinkedList<StopTimeUpdate> raw = createStopTimeUpdateSequence(newArrivals, newDepartures);

            List<StopTimeUpdate> updated = GtfsRtValidator.validateArrivalsAndDepartures(raw, raw.getLast());

            StopTimeUpdate updatedFirst = updated.get(0);
            assertTrue(updatedFirst.getArrival().getTime() == ARRIVALS[2]);
            assertTrue(updatedFirst.getDeparture().getTime() == DEPARTURES[2]);

            StopTimeUpdate updatedSecond = updated.get(1);
            assertTrue(updatedSecond.getArrival().getTime() == DEPARTURES[2]);
            assertTrue(updatedSecond.getDeparture().getTime() == DEPARTURES[2]);

            StopTimeUpdate updatedThird = updated.get(2);
            assertTrue(updatedThird.getArrival().getTime() == DEPARTURES[2]);
            assertTrue(updatedThird.getDeparture().getTime() == DEPARTURES[2]);
        }
        {
            // First departure to delay after second arrival so second arrival needs to be moved
            // A0 A1 D0 D1 A2  D2 => A0 D0 D0 D1 A2 D2
            long[] newDepartures = Arrays.copyOf(DEPARTURES, DEPARTURES.length);
            newDepartures[0] = ARRIVALS[1] + 1;
            LinkedList<StopTimeUpdate> raw = createStopTimeUpdateSequence(ARRIVALS, newDepartures);

            List<StopTimeUpdate> updated = GtfsRtValidator.validateArrivalsAndDepartures(raw, raw.getLast());

            StopTimeUpdate updatedFirst = updated.get(0);
            assertTrue(updatedFirst.getArrival().getTime() == ARRIVALS[0]);
            assertTrue(updatedFirst.getDeparture().getTime() == newDepartures[0]);

            StopTimeUpdate updatedSecond = updated.get(1);
            assertTrue(updatedSecond.getArrival().getTime() == newDepartures[0]);
            assertTrue(updatedSecond.getDeparture().getTime() == newDepartures[1]);

            StopTimeUpdate updatedThird = updated.get(2);
            assertTrue(updatedThird.getArrival().getTime() == ARRIVALS[2]);
            assertTrue(updatedThird.getDeparture().getTime() == newDepartures[2]);
        }
    }

    long[] copyAndSwap(long[] original, int index1, int index2) {
        long[] copy = Arrays.copyOf(original, original.length);
        copy[index1] = original[index2];
        copy[index2] = original[index1];
        return copy;
    }

    LinkedList<StopTimeUpdate> createStopTimeUpdateSequence(long[] arrivals, long[] departures) {
        LinkedList<StopTimeUpdate> updates = new LinkedList<>();
        for (int n = 0; n < arrivals.length; n++) {
            final GtfsRealtime.TripUpdate.StopTimeUpdate update = MockDataFactory.mockStopTimeUpdate(
                    MockDataFactory.mockStopEvent(StopEvent.EventType.Arrival, arrivals[n]),
                    MockDataFactory.mockStopEvent(StopEvent.EventType.Departure, departures[n]));
            updates.add(update);
        }
        return updates;
    }

    @Test
    public void testStopSequences() {

        List<StopTimeUpdate> updates = new LinkedList<>();
        for (int stopSequence = 1; stopSequence < 100; stopSequence++) {
            //Let's switch types to make sure both work
            StopEvent.EventType type = stopSequence % 2 == 0 ? StopEvent.EventType.Arrival :StopEvent.EventType.Departure;
            StopEvent event = MockDataFactory.mockStopEvent(MockDataUtils.generateValidCommon(DVI_ID, stopSequence).build(), null, type);
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
        //Make sure stop sequences are all removed but other data is still there.
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

    @Test
    public void testArrivalAndDepartureFilling() {

        LinkedList<StopTimeUpdate> both = createStopTimeUpdateSequence(ARRIVALS, DEPARTURES);
        List<StopTimeUpdate> same = GtfsRtValidator.fillMissingArrivalsAndDepartures(both);

        Consumer<StopTimeUpdate> validator = (update) -> {
            assertTrue(update.hasArrival());
            assertTrue(update.hasDeparture());
            assertTrue(update.getArrival().getTime() > 0);
            assertTrue(update.getDeparture().getTime() > 0);
        };
        same.forEach(validator);

        Consumer<StopTimeUpdate> sameTimeValidator = (update) -> {
            assertTrue(update.hasArrival());
            assertTrue(update.hasDeparture());
            assertEquals(update.getArrival().getTime(), update.getDeparture().getTime());
        };

        LinkedList<StopTimeUpdate> onlyDepartures = new LinkedList<>();
        for (int n = 0; n < DEPARTURES.length; n++) {
            final GtfsRealtime.TripUpdate.StopTimeUpdate update = MockDataFactory.mockStopTimeUpdate(
                    StopEvent.EventType.Departure, DEPARTURES[n]);
            onlyDepartures.add(update);
        }
        List<StopTimeUpdate> filledArrivals = GtfsRtValidator.fillMissingArrivalsAndDepartures(onlyDepartures);
        filledArrivals.forEach(sameTimeValidator);

        LinkedList<StopTimeUpdate> onlyArrivals = new LinkedList<>();
        for (int n = 0; n < ARRIVALS.length; n++) {
            final GtfsRealtime.TripUpdate.StopTimeUpdate update = MockDataFactory.mockStopTimeUpdate(
                    StopEvent.EventType.Arrival, ARRIVALS[n]);
            onlyArrivals.add(update);
        }
        List<StopTimeUpdate> filledDepartures = GtfsRtValidator.fillMissingArrivalsAndDepartures(onlyArrivals);
        filledDepartures.forEach(sameTimeValidator);
    }     */

}
