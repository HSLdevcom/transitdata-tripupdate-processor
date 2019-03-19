package fi.hsl.transitdata.tripupdate.gtfsrt;

import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.transitdata.MockDataUtils;
import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.transitdata.tripupdate.MockDataFactory;
import org.junit.Test;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import static com.google.transit.realtime.GtfsRealtime.TripUpdate.*;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class GtfsRtValidatorTest {
    final static long[] SRC_ARRIVALS_MS =   new long[] { 1545674400000L, 1545674500000L, 1545674600000L };
    final static long[] SRC_DEPARTURES_MS = new long[] { 1545674450000L, 1545674550000L, 1545674650000L };

    final static long[] DST_ARRIVALS = Arrays.stream(SRC_ARRIVALS_MS).map(ms -> ms / 1000).toArray();
    final static long[] DST_DEPARTURES = Arrays.stream(SRC_DEPARTURES_MS).map(ms -> ms / 1000).toArray();


    final static long DVI_ID = 1234567890L;
    final static long JPP_ID = 9876543210L;

    @Test
    public void testValidateTime() throws Exception {
        long epoch = SRC_ARRIVALS_MS[1];

        final Optional<StopTimeEvent> stopTimeEvent = Optional.of(MockDataFactory.mockStopTimeEvent(InternalMessages.StopEstimate.Type.ARRIVAL, epoch));

        Optional<Long> laterMinTime = Optional.of(DST_ARRIVALS[2]);
        Optional<StopTimeEvent> shouldBeChanged = GtfsRtValidator.validateMinTime(stopTimeEvent, laterMinTime);
        assertTrue(stopTimeEvent.get().getTime() < shouldBeChanged.get().getTime());
        assertTrue(laterMinTime.get() == shouldBeChanged.get().getTime());

        Optional<Long> sameMinTime = Optional.of(DST_ARRIVALS[1]);
        Optional<StopTimeEvent> shouldBeSame = GtfsRtValidator.validateMinTime(stopTimeEvent, sameMinTime);
        assertTrue(stopTimeEvent.get().getTime() == shouldBeSame.get().getTime());

        Optional<Long> earlierMinTime = Optional.of(DST_ARRIVALS[0]);
        Optional<StopTimeEvent> shouldBeSameAgain = GtfsRtValidator.validateMinTime(stopTimeEvent, earlierMinTime);
        assertTrue(stopTimeEvent.get().getTime() == shouldBeSameAgain.get().getTime());

        //If timestamp is empty, we should receive the original
        Optional<StopTimeEvent> shouldBeOriginal = GtfsRtValidator.validateMinTime(stopTimeEvent, Optional.empty());
        assertTrue(stopTimeEvent.get().getTime() == shouldBeOriginal.get().getTime());

        //If event is empty we should get nothing
        Optional<StopTimeEvent> nothing = GtfsRtValidator.validateMinTime(Optional.empty(), Optional.of(DST_ARRIVALS[0]));
        assertTrue(!nothing.isPresent());
        Optional<StopTimeEvent> stillNothing = GtfsRtValidator.validateMinTime(Optional.empty(), Optional.empty());
        assertTrue(!stillNothing.isPresent());
    }

    @Test
    public void testValidateTimestampsOnlyArrival() throws Exception {
        final StopTimeUpdate firstArrival = MockDataFactory.mockStopTimeUpdate(InternalMessages.StopEstimate.Type.ARRIVAL, SRC_ARRIVALS_MS[0]);
        final StopTimeUpdate secondArrival = MockDataFactory.mockStopTimeUpdate(InternalMessages.StopEstimate.Type.ARRIVAL, SRC_ARRIVALS_MS[1]);

        validateTimestamps(firstArrival, secondArrival, GtfsRtValidator.OnConflict.ArrivalWins, DST_ARRIVALS[1], 0);
        validateTimestamps(secondArrival, firstArrival, GtfsRtValidator.OnConflict.ArrivalWins, DST_ARRIVALS[1], 0);
        validateTimestamps(firstArrival, firstArrival, GtfsRtValidator.OnConflict.ArrivalWins, DST_ARRIVALS[0], 0);
        validateTimestamps(secondArrival, firstArrival, GtfsRtValidator.OnConflict.DepartureWins, DST_ARRIVALS[1], 0);
    }

    @Test
    public void testValidateTimestampsBothArrivalAndDeparture() throws Exception {

        final StopTimeUpdate first = MockDataFactory.mockStopTimeUpdate(
                MockDataUtils.mockStopEstimate(InternalMessages.StopEstimate.Type.ARRIVAL, SRC_ARRIVALS_MS[0]),
                MockDataUtils.mockStopEstimate(InternalMessages.StopEstimate.Type.DEPARTURE, SRC_DEPARTURES_MS[0]));

        final StopTimeUpdate second = MockDataFactory.mockStopTimeUpdate(
                MockDataUtils.mockStopEstimate(InternalMessages.StopEstimate.Type.ARRIVAL, SRC_ARRIVALS_MS[1]),
                MockDataUtils.mockStopEstimate(InternalMessages.StopEstimate.Type.DEPARTURE, SRC_DEPARTURES_MS[1]));

        validateTimestamps(first, second, GtfsRtValidator.OnConflict.ArrivalWins, DST_ARRIVALS[1], DST_DEPARTURES[1]);
        validateTimestamps(first, second, GtfsRtValidator.OnConflict.DepartureWins, DST_ARRIVALS[1], DST_DEPARTURES[1]);

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
    public void testArrivalAndDepartureSequences() throws Exception {
        {
            //Best case scenario, all in correct order
            // A0  D0  A1  D1  A2  D2
            LinkedList<StopTimeUpdate> raw = createStopTimeUpdateSequence(SRC_ARRIVALS_MS, SRC_DEPARTURES_MS);
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
            long[] newArrivals = copyAndSwap(SRC_ARRIVALS_MS, 0, 1);
            long[] newDepartures = copyAndSwap(SRC_DEPARTURES_MS, 0, 1);
            LinkedList<StopTimeUpdate> raw = createStopTimeUpdateSequence(newArrivals, newDepartures);

            List<StopTimeUpdate> updated = GtfsRtValidator.validateArrivalsAndDepartures(raw, raw.getLast());

            StopTimeUpdate updatedFirst = updated.get(0);
            assertEquals(DST_ARRIVALS[1], updatedFirst.getArrival().getTime());
            assertEquals(DST_DEPARTURES[1], updatedFirst.getDeparture().getTime() );

            StopTimeUpdate updatedSecond = updated.get(1);
            assertEquals(DST_DEPARTURES[1], updatedSecond.getArrival().getTime());
            assertEquals(DST_DEPARTURES[1], updatedSecond.getDeparture().getTime() );

            StopTimeUpdate updatedThird = updated.get(2);
            assertEquals(DST_ARRIVALS[2], updatedThird.getArrival().getTime());
            assertEquals(DST_DEPARTURES[2], updatedThird.getDeparture().getTime() );
        }
        {
            //Swap timestamps between first and last timestamp. all previous should be raised to the last departure
            // A2 D2 A1  D1  A0  D0   => A2 D2 D2 D2 D2 D2
            long[] newArrivals = copyAndSwap(SRC_ARRIVALS_MS, 0, 2);
            long[] newDepartures = copyAndSwap(SRC_DEPARTURES_MS, 0, 2);
            LinkedList<StopTimeUpdate> raw = createStopTimeUpdateSequence(newArrivals, newDepartures);

            List<StopTimeUpdate> updated = GtfsRtValidator.validateArrivalsAndDepartures(raw, raw.getLast());

            StopTimeUpdate updatedFirst = updated.get(0);
            assertEquals(DST_ARRIVALS[2], updatedFirst.getArrival().getTime());
            assertEquals(DST_DEPARTURES[2], updatedFirst.getDeparture().getTime() );

            StopTimeUpdate updatedSecond = updated.get(1);
            assertEquals(DST_DEPARTURES[2], updatedSecond.getArrival().getTime());
            assertEquals(DST_DEPARTURES[2], updatedSecond.getDeparture().getTime() );

            StopTimeUpdate updatedThird = updated.get(2);
            assertEquals(DST_DEPARTURES[2], updatedThird.getArrival().getTime());
            assertEquals(DST_DEPARTURES[2], updatedThird.getDeparture().getTime() );
        }
        {
            // First departure to delay after second arrival so second arrival needs to be moved
            // A0 A1 D0 D1 A2  D2 => A0 D0 D0 D1 A2 D2
            long[] newDeparturesMs = Arrays.copyOf(SRC_DEPARTURES_MS, SRC_DEPARTURES_MS.length);
            newDeparturesMs[0] = SRC_ARRIVALS_MS[1] + 1;
            long[] newDeparturesSecs = Arrays.stream(newDeparturesMs).map(ms -> ms / 1000).toArray();

            LinkedList<StopTimeUpdate> raw = createStopTimeUpdateSequence(SRC_ARRIVALS_MS, newDeparturesMs);

            List<StopTimeUpdate> updated = GtfsRtValidator.validateArrivalsAndDepartures(raw, raw.getLast());

            StopTimeUpdate updatedFirst = updated.get(0);
            assertEquals(DST_ARRIVALS[0], updatedFirst.getArrival().getTime());
            assertEquals(newDeparturesSecs[0], updatedFirst.getDeparture().getTime() );

            StopTimeUpdate updatedSecond = updated.get(1);
            assertEquals(newDeparturesSecs[0], updatedSecond.getArrival().getTime());
            assertEquals(newDeparturesSecs[1], updatedSecond.getDeparture().getTime() );

            StopTimeUpdate updatedThird = updated.get(2);
            assertEquals(DST_ARRIVALS[2], updatedThird.getArrival().getTime());
            assertEquals(newDeparturesSecs[2], updatedThird.getDeparture().getTime() );
        }
    }

    long[] copyAndSwap(long[] original, int index1, int index2) {
        long[] copy = Arrays.copyOf(original, original.length);
        copy[index1] = original[index2];
        copy[index2] = original[index1];
        return copy;
    }

    LinkedList<StopTimeUpdate> createStopTimeUpdateSequence(long[] arrivals, long[] departures) throws Exception {
        LinkedList<StopTimeUpdate> updates = new LinkedList<>();
        for (int n = 0; n < arrivals.length; n++) {
            final GtfsRealtime.TripUpdate.StopTimeUpdate update = MockDataFactory.mockStopTimeUpdate(
                    MockDataUtils.mockStopEstimate(InternalMessages.StopEstimate.Type.ARRIVAL, arrivals[n]),
                    MockDataUtils.mockStopEstimate(InternalMessages.StopEstimate.Type.DEPARTURE, departures[n]));
            updates.add(update);
        }
        return updates;
    }

    @Test
    public void testStopSequences() throws Exception {
        long startTime = System.currentTimeMillis();

        List<StopTimeUpdate> updates = new LinkedList<>();
        for (int stopSequence = 1; stopSequence < 100; stopSequence++) {
            //Let's switch types to make sure both work
            InternalMessages.StopEstimate.Type type = stopSequence % 2 == 0 ? InternalMessages.StopEstimate.Type.ARRIVAL : InternalMessages.StopEstimate.Type.DEPARTURE;
            long targetTime = startTime++; //Let's modify this a bit
            InternalMessages.StopEstimate event = MockDataUtils.mockStopEstimate(DVI_ID, type, 0L, stopSequence, targetTime); //MockDataFactory.mockStopEvent(MockDataUtils.generateValidCommon(DVI_ID, stopSequence).build(), null, type);
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
    public void testArrivalAndDepartureFilling() throws Exception {

        LinkedList<StopTimeUpdate> both = createStopTimeUpdateSequence(SRC_ARRIVALS_MS, SRC_DEPARTURES_MS);
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
        for (int n = 0; n < SRC_DEPARTURES_MS.length; n++) {
            final GtfsRealtime.TripUpdate.StopTimeUpdate update = MockDataFactory.mockStopTimeUpdate(
                    InternalMessages.StopEstimate.Type.DEPARTURE, SRC_DEPARTURES_MS[n]);
            onlyDepartures.add(update);
        }
        List<StopTimeUpdate> filledArrivals = GtfsRtValidator.fillMissingArrivalsAndDepartures(onlyDepartures);
        filledArrivals.forEach(sameTimeValidator);

        LinkedList<StopTimeUpdate> onlyArrivals = new LinkedList<>();
        for (int n = 0; n < SRC_ARRIVALS_MS.length; n++) {
            final GtfsRealtime.TripUpdate.StopTimeUpdate update = MockDataFactory.mockStopTimeUpdate(
                    InternalMessages.StopEstimate.Type.ARRIVAL, SRC_ARRIVALS_MS[n]);
            onlyArrivals.add(update);
        }
        List<StopTimeUpdate> filledDepartures = GtfsRtValidator.fillMissingArrivalsAndDepartures(onlyArrivals);
        filledDepartures.forEach(sameTimeValidator);
    }

}
