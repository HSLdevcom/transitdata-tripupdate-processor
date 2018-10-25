package fi.hsl.transitdata.tripupdate.gtfsrt;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.transit.realtime.GtfsRealtime.TripUpdate.*;

public class GtfsRtValidator {
    private GtfsRtValidator() {}

    public static List<StopTimeUpdate> cleanStopTimeUpdates(List<StopTimeUpdate> rawEstimates, StopTimeUpdate latest) {
        List<StopTimeUpdate> fixedTimestamps = validateArrivalsAndDepartures(rawEstimates, latest);
        List<StopTimeUpdate> removedStops = removeStopSequences(fixedTimestamps);
        List<StopTimeUpdate> filledEvents = fillMissingArrivalsAndDepartures(removedStops);
        return filledEvents;
    }

    /**
     * Removes the optional stopSequenceId's from StopTimeUpdates.
     * Our updates might have extra via-points there which can confuse the clients.
     */
    static List<StopTimeUpdate> removeStopSequences(List<StopTimeUpdate> updates) {
        return updates.stream().map(update ->
            update.toBuilder().clearStopSequence().build()
        ).collect(Collectors.toList());
    }

    /**
     * OpenTripPlanner caches arrival and departure times & estimates. Sometimes this can cause problems if we
     * serve update containing only one of them, f.ex in a case where arrival moves to later time than cached departure.
     *
     * We'll try to fix this by always sending both arrival and departure times.
     */
    static List<StopTimeUpdate> fillMissingArrivalsAndDepartures(List<StopTimeUpdate> updates) {
        return updates.stream().map(update -> {
            if (update.hasArrival() && !update.hasDeparture()) {
                StopTimeEvent newDeparture = StopTimeEvent.newBuilder()
                        .setTime(update.getArrival().getTime())
                        .build();
                return update.toBuilder()
                        .setDeparture(newDeparture)
                        .build();
            }
            else if (update.hasDeparture() && !update.hasArrival()) {
                StopTimeEvent newArrival = StopTimeEvent.newBuilder()
                        .setTime(update.getDeparture().getTime())
                        .build();
                return update.toBuilder()
                        .setArrival(newArrival)
                        .build();
            }
            else {
                return update;
            }
        }).collect(Collectors.toList());
    }

    /**
     * Our data source might report inconsistencies with arrival and departure dwell times (departure happening before arrival),
     * and also to running times (current arrival before previous departure).
     * OpenTripPlanner won't accept this, so we'll try to fix the timestamps by adjusting them appropriately.
     */
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

    static StopTimeUpdate validateTimestamps(StopTimeUpdate prev, StopTimeUpdate cur, OnConflict conflictBehavior) {
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
        Optional<StopTimeEvent> newArrival = validateMinTime(curArrival, maybePrevTimestamp);

        final Optional<StopTimeEvent> curDeparture = cur.hasDeparture() ? Optional.of(cur.getDeparture()) : Optional.empty();
        Optional<StopTimeEvent> newDeparture = validateMinTime(curDeparture, maybePrevTimestamp);

        // Now both are at least >= then previous timestamp.
        // Next let's resolve possible conflict at current stop
        if (conflictBehavior == OnConflict.ArrivalWins) {
            Optional<Long> maybeArrivalTimestamp = newArrival.map(StopTimeEvent::getTime);
            newDeparture = validateMinTime(newDeparture, maybeArrivalTimestamp);
        }
        else if (conflictBehavior == OnConflict.DepartureWins) {
            Optional<Long> maybeDepartureTimestamp = newDeparture.map(StopTimeEvent::getTime);
            newArrival = validateMaxTime(newArrival, maybeDepartureTimestamp);
        }

        StopTimeUpdate.Builder builder = cur.toBuilder();
        newArrival.map(builder::setArrival);
        newDeparture.map(builder::setDeparture);

        return builder.build();
    }

    /**
     * Either return the same valid StopTimeEvent or create a copy with time adjusted to minimum
     */
    static Optional<StopTimeEvent> validateMinTime(final Optional<StopTimeEvent> maybeEvent, final Optional<Long> maybeMinTime) {
        return maybeEvent.map(event ->
           maybeMinTime.map(minTimestamp -> {
               if (event.getTime() < minTimestamp) {
                   return event.toBuilder().setTime(minTimestamp).build();
               } else {
                   return event;
               }
           }).orElse(event));
    }

    /**
     * Either return the same valid StopTimeEvent or create a copy with time adjusted to maximum
     */
    static Optional<StopTimeEvent> validateMaxTime(final Optional<StopTimeEvent> maybeEvent, final Optional<Long> maybeMaxTime) {
        return maybeEvent.map(event ->
            maybeMaxTime.map(maxTimestamp -> {
                if (event.getTime() > maxTimestamp) {
                    return event.toBuilder().setTime(maxTimestamp).build();
                } else {
                    return event;
                }
            }).orElse(event));
    }
}
