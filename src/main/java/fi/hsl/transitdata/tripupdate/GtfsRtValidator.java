package fi.hsl.transitdata.tripupdate;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.transit.realtime.GtfsRealtime.TripUpdate.*;

public class GtfsRtValidator {
    private GtfsRtValidator() {}


    /**
     * Our data source might report inconsistencies with arrival and departure dwell times (departure happening before arrival),
     * and also to running times (current arrival before previous departure).
     * OpenTripPlanner won't accept this, so we'll try to fix the timestamps by adjusting them appropriately.
     *
     * We'll also remove the optional stopSequenceId's from them because they have extra via-points there which can confuse the clients.
     */
    public static List<StopTimeUpdate> cleanStopTimeUpdates(List<StopTimeUpdate> rawEstimates, StopTimeUpdate latest) {
        List<StopTimeUpdate> fixedTimestamps = validateArrivalsAndDepartures(rawEstimates, latest);
        List<StopTimeUpdate> removedStops = removeStopSequences(fixedTimestamps);
        return removedStops;
    }

    static List<StopTimeUpdate> removeStopSequences(List<StopTimeUpdate> updates) {
        return updates.stream().map(update ->
            update.toBuilder().clearStopSequence().build()
        ).collect(Collectors.toList());
    }

    static List<StopTimeUpdate> validateArrivalsAndDepartures(List<StopTimeUpdate> rawEstimates, StopTimeUpdate latest) {
        LinkedList<StopTimeUpdate> validList = new LinkedList<>();
        StopTimeUpdate previous = null;
        for (StopTimeUpdate unvalidated: rawEstimates) {
            // If this is the latest message and it happens to be arrival, we want to use that timestamp.
            // otherwise always use departure.
            OnConflict conflictBehavior = OnConflict.DepartureWins;
            if (unvalidated == latest && latest.hasArrival()) {
                conflictBehavior = OnConflict.ArrivalWins;
            }

            StopTimeUpdate validated = validateTimestamps(previous, unvalidated, conflictBehavior);
            validList.add(validated);
            previous = validated;
        }
        return validList;
    }

    enum OnConflict {
        DepartureWins,
        ArrivalWins
    }

    private static StopTimeUpdate validateTimestamps(StopTimeUpdate prev, StopTimeUpdate cur, OnConflict conflictBehavior) {
        // We need to make sure current timestamps are > previous ones
        // and arrivals cannot be later than departures

        Optional<Long> maybePrevTimestamp = Optional.empty();
        if (prev != null) {
            if (prev.hasDeparture()) {
                maybePrevTimestamp = Optional.of(prev.getDeparture().getTime());
            }
            else if (prev.hasArrival()) {
                maybePrevTimestamp = Optional.of(prev.getArrival().getTime());
            }
        }

        final Optional<StopTimeEvent> curArrival = cur.hasArrival() ? Optional.of(cur.getArrival()) : Optional.empty();
        Optional<StopTimeEvent> newArrival = validateTime(curArrival, maybePrevTimestamp);

        final Optional<StopTimeEvent> curDeparture = cur.hasDeparture() ? Optional.of(cur.getDeparture()) : Optional.empty();
        Optional<StopTimeEvent> newDeparture = validateTime(curDeparture, maybePrevTimestamp);

        // Now both are at least >= then previous timestamp.
        // Next let's resolve possible conflict at current stop
        if (conflictBehavior == OnConflict.ArrivalWins) {
            Optional<Long> maybeArrivalTimestamp = newArrival.map(StopTimeEvent::getTime);
            newDeparture = validateTime(newDeparture, maybeArrivalTimestamp);
        }
        else if (conflictBehavior == OnConflict.DepartureWins) {
            Optional<Long> maybeDepartureTimestamp = newDeparture.map(StopTimeEvent::getTime);
            newArrival = validateTime(newArrival, maybeDepartureTimestamp);
        }

        StopTimeUpdate.Builder builder = cur.toBuilder();
        newArrival.map(builder::setArrival);
        newDeparture.map(builder::setDeparture);
        return builder.build();
    }

    /**
     * Either return the same valid StopTimeEvent or create a copy with time adjusted to minimum
     */
    static Optional<StopTimeEvent> validateTime(final Optional<StopTimeEvent> maybeEvent, final Optional<Long> maybeMinTime) {
        return maybeEvent.map(event ->
           maybeMinTime.map(minTimestamp -> {
               if (event.getTime() < minTimestamp) {
                   return event.toBuilder().setTime(minTimestamp).build();
               } else {
                   return event;
               }
           }).orElse(event));
    }
}
