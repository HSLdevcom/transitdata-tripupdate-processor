package fi.hsl.transitdata.tripupdate.filters;

import com.google.transit.realtime.GtfsRealtime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.*;
import java.time.format.DateTimeFormatter;

public class ValidatePrematureDepartures implements ITripUpdateValidator {

    private static final Logger log = LoggerFactory.getLogger(ValidatePrematureDepartures.class);

    private long tripUpdateMinTimeBeforeDeparture;

    public ValidatePrematureDepartures(long tripUpdateMinTimeBeforeDeparture) {
        this.tripUpdateMinTimeBeforeDeparture = tripUpdateMinTimeBeforeDeparture;
    }

    @Override
    public boolean validate(GtfsRealtime.TripUpdate tripUpdate) {
        //If a TripUpdate has no StopTimeUpdates, it is most likely represents a trip that has been cancelled
        //Current hypothesis is that these messages should always be relevant and thus routed through
        if (tripUpdate.getStopTimeUpdateList().isEmpty()) {
            return true;
        }

        //TODO: Fix this for daylight saving time
        String[] tripStartTimeArray = tripUpdate.getTrip().getStartTime().split(":");

        if (tripStartTimeArray.length != 3) {
            log.error("Invalid start time for trip update");
            return false;
        }

        LocalDate tripOperatingDayDate = LocalDate.parse(tripUpdate.getTrip().getStartDate(), DateTimeFormatter.BASIC_ISO_DATE);
        OffsetDateTime tripStartTimeOffSetDateTime = OffsetDateTime.of(LocalDateTime.of(tripOperatingDayDate, LocalTime.MIDNIGHT), ZoneOffset.UTC);
        long tripStartTimePosix = tripStartTimeOffSetDateTime.toEpochSecond() +
                Long.parseLong(tripStartTimeArray[0]) * 60*60 + Long.parseLong(tripStartTimeArray [1]) * 60;

        long tripFirstStopTimeEventTime;

        if (tripUpdate.getStopTimeUpdate(0).hasDeparture()) {
            tripFirstStopTimeEventTime = tripUpdate.getStopTimeUpdate(0).getDeparture().getTime();
        } else {
            tripFirstStopTimeEventTime = tripUpdate.getStopTimeUpdate(0).getArrival().getTime();
        }

        //Filter out premature departures, where the departure time for the first StopTimeUpdate is more than the
        //configured amount of seconds before the scheduled departure time of the trip
        if (tripStartTimePosix - tripFirstStopTimeEventTime > tripUpdateMinTimeBeforeDeparture ) {
            return false;
        }

        return true;
    }
}
