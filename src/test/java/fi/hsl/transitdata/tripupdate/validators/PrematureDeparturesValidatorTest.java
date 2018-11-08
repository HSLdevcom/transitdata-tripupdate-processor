package fi.hsl.transitdata.tripupdate.validators;

import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.transitdata.tripupdate.MockDataFactory;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

public class PrematureDeparturesValidatorTest {

    private static final String TIMEZONE = "Europe/Helsinki";

    @Test
    public void tripUpdateWithPrematureDepartureIsDiscarded() {

        //More than three minutes is a premature prediction
        PrematureDeparturesValidator validator = new PrematureDeparturesValidator(180, TIMEZONE);

        //2018-11-07T17:10:00 in Helsinki/Europe
        Collection<GtfsRealtime.TripUpdate.StopTimeUpdate> stopTimeUpdates = new ArrayList<>();
        stopTimeUpdates.add(MockDataFactory.mockStopTimeUpdate("A", 0, 1541603400));

        GtfsRealtime.TripUpdate tripUpdate = MockDataFactory.mockTripUpdate("1010", 0, "20181107",
                "17:14:00", stopTimeUpdates);

        assertEquals(false, validator.validate(tripUpdate));

    }

    @Test
    public void tripUpdateWithPrematureArrivalIsDiscarded() {

        //More than three minutes is a premature prediction
        PrematureDeparturesValidator validator = new PrematureDeparturesValidator(180, TIMEZONE);

        //2018-11-07T17:10:00 in Helsinki/Europe
        Collection<GtfsRealtime.TripUpdate.StopTimeUpdate> stopTimeUpdates = new ArrayList<>();
        stopTimeUpdates.add(MockDataFactory.mockStopTimeUpdate("A", 1541603400, 0));

        GtfsRealtime.TripUpdate tripUpdate = MockDataFactory.mockTripUpdate("1010", 0, "20181107",
                "17:14:00", stopTimeUpdates);

        assertEquals(false, validator.validate(tripUpdate));

    }

    @Test
    public void tripUpdateWithValidDepartureIsAccepted() {

        //More than three minutes is a premature prediction
        PrematureDeparturesValidator validator = new PrematureDeparturesValidator(180, TIMEZONE);

        //2018-11-07T17:10:00 in Helsinki/Europe
        Collection<GtfsRealtime.TripUpdate.StopTimeUpdate> stopTimeUpdates = new ArrayList<>();
        stopTimeUpdates.add(MockDataFactory.mockStopTimeUpdate("A", 0, 1541603400));

        GtfsRealtime.TripUpdate tripUpdate = MockDataFactory.mockTripUpdate("1010", 0, "20181107", "17:11:00", stopTimeUpdates);

        assertEquals(true, validator.validate(tripUpdate));

    }

    @Test
    public void tripUpdateStartTimeInDayTimeIsParsedCorrectly() {

        PrematureDeparturesValidator validator = new PrematureDeparturesValidator(180, TIMEZONE);

        GtfsRealtime.TripUpdate tripUpdate = MockDataFactory.mockTripUpdate("1010", 0, "20181107", "17:10:00");

        assertEquals(1541603400, validator.tripStartTimeToPosixTime(tripUpdate));

    }

    @Test
    public void tripUpdateStartTimeAfterMidnightIsParsedCorrectly() {

        PrematureDeparturesValidator validator = new PrematureDeparturesValidator(180, TIMEZONE);

        //2018-11-08T03:50:00
        GtfsRealtime.TripUpdate tripUpdate = MockDataFactory.mockTripUpdate("1010", 0, "20181107", "27:50:00");

        assertEquals(1541641800, validator.tripStartTimeToPosixTime(tripUpdate));

    }

    @Test
    public void tripUpdateStartTimeJustAfterMidnightIsParsedCorrectly() {

        PrematureDeparturesValidator validator = new PrematureDeparturesValidator(180, TIMEZONE);

        //2018-11-08T00:01:00
        GtfsRealtime.TripUpdate tripUpdate = MockDataFactory.mockTripUpdate("1010", 0, "20181107", "24:01:00");

        assertEquals(1541628060, validator.tripStartTimeToPosixTime(tripUpdate));
    }


}
