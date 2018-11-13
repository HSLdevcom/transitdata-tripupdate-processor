package fi.hsl.transitdata.tripupdate.validators;

import com.google.transit.realtime.GtfsRealtime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.*;
import java.time.format.DateTimeFormatter;

public class PrematureDeparturesValidator implements ITripUpdateValidator {

    private static final Logger log = LoggerFactory.getLogger(PrematureDeparturesValidator.class);

    private long tripUpdateMinTimeBeforeDeparture;
    private ZoneId zoneId;

    public PrematureDeparturesValidator(long tripUpdateMinTimeBeforeDeparture, String zoneIdString) {
        this.tripUpdateMinTimeBeforeDeparture = tripUpdateMinTimeBeforeDeparture;
        this.zoneId = ZoneId.of(zoneIdString);
    }

    @Override
    public boolean validate(GtfsRealtime.TripUpdate tripUpdate) {
        //If a TripUpdate has no StopTimeUpdates, it is most likely represents a trip that has been cancelled
        //Current hypothesis is that these messages should always be relevant and thus routed through
        if (tripUpdate.getStopTimeUpdateList().isEmpty()) {
            return true;
        }

        String[] tripStartTimeArray = tripUpdate.getTrip().getStartTime().split(":");

        if (tripStartTimeArray.length != 3) {
            log.error("Invalid start time for trip update");
            return false;
        }

        long tripFirstStopTimeEventTime;

        if (tripUpdate.getStopTimeUpdate(0).hasDeparture()) {
            tripFirstStopTimeEventTime = tripUpdate.getStopTimeUpdate(0).getDeparture().getTime();
        } else {
            tripFirstStopTimeEventTime = tripUpdate.getStopTimeUpdate(0).getArrival().getTime();
        }

        long tripStartTimePosix = tripStartTimeToPosixTime(tripUpdate);
        //Filter out premature departures, where the departure time for the first StopTimeUpdate is more than the
        //configured amount of seconds before the scheduled departure time of the trip
        if (tripStartTimePosix - tripFirstStopTimeEventTime > tripUpdateMinTimeBeforeDeparture ) {
            return false;
        }

        return true;
    }

    long tripStartTimeToPosixTime(GtfsRealtime.TripUpdate tripUpdate) {

        String[] tripStartTimeArray = tripUpdate.getTrip().getStartTime().split(":");

        boolean over24Hours = false;
        if (Integer.parseInt(tripStartTimeArray[0]) > 23) {
            over24Hours = true;
        }

        LocalTime tripStartTimeLocal;

        if (!over24Hours) {
            tripStartTimeLocal = LocalTime.parse(tripUpdate.getTrip().getStartTime());
        } else {
            int hours = Integer.parseInt(tripStartTimeArray[0]) - 24;
            String hoursString;
            if (hours > 9) {
                hoursString = hours + "";
            } else {
                hoursString = "0" + hours;
            }
            tripStartTimeLocal = LocalTime.parse(hoursString + ":" + tripStartTimeArray[1] + ":" + tripStartTimeArray[2]);
        }

        LocalDate tripStartDateLocal = LocalDate.parse(tripUpdate.getTrip().getStartDate(), DateTimeFormatter.BASIC_ISO_DATE);
        if (over24Hours) {
            tripStartDateLocal = tripStartDateLocal.plusDays(1);
        }

        long tripStartTimeEpoch = LocalDateTime.of(tripStartDateLocal, tripStartTimeLocal).atZone(zoneId).toInstant().getEpochSecond();

        return tripStartTimeEpoch;
    }

}
