package fi.hsl.transitdata.tripupdate.validators;

import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.transitdata.tripupdate.MockDataFactory;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

public class TripUpdateMaxAgeValidatorTest {

    @Test
    public void tripUpdateWithNoStopTimeUpdatesIsAccepted() {

        //2 hours max age
        TripUpdateMaxAgeValidator validator = new TripUpdateMaxAgeValidator(7200);

        GtfsRealtime.TripUpdate tripUpdate = MockDataFactory.mockTripUpdate("1010", 0, "20181107", "17:14:00");

        //Validate with the public interface method
        assertEquals(true, validator.validate(tripUpdate));

    }

    @Test
    public void tripUpdateWithOveragedLastStopDepartureTimeIsDiscarded() {

        //2 hours max age
        TripUpdateMaxAgeValidator validator = new TripUpdateMaxAgeValidator(7200);

        Collection<GtfsRealtime.TripUpdate.StopTimeUpdate> stopTimeUpdates = new ArrayList<>();
        //2018-11-07T17:10:00 in Helsinki/Europe
        stopTimeUpdates.add(MockDataFactory.mockStopTimeUpdate("A", 0, 1541603400));
        //2018-11-07T17:30:00 in Helsinki/Europe
        stopTimeUpdates.add(MockDataFactory.mockStopTimeUpdate("B", 0, 1541604600));

        GtfsRealtime.TripUpdate tripUpdate = MockDataFactory.mockTripUpdate("1010", 0, "20181107", "17:14:00", stopTimeUpdates);

        //Validate with 2018-11-07T19:31:00
        assertEquals(false, validator.validateWithCurrentTime(tripUpdate, 1541611860));

    }

    @Test
    public void tripUpdateWithValidLastStopDepartureTimeIsAccepted() {

        //2 hours max age
        TripUpdateMaxAgeValidator validator = new TripUpdateMaxAgeValidator(7200);

        Collection<GtfsRealtime.TripUpdate.StopTimeUpdate> stopTimeUpdates = new ArrayList<>();
        //2018-11-07T17:10:00 in Helsinki/Europe
        stopTimeUpdates.add(MockDataFactory.mockStopTimeUpdate("A", 0, 1541603400));
        //2018-11-07T17:30:00 in Helsinki/Europe
        stopTimeUpdates.add(MockDataFactory.mockStopTimeUpdate("B", 0, 1541604600));

        GtfsRealtime.TripUpdate tripUpdate = MockDataFactory.mockTripUpdate("1010", 0, "20181107", "17:14:00", stopTimeUpdates);

        //Validate with 2018-11-07T19:31:00
        assertEquals(true, validator.validateWithCurrentTime(tripUpdate, 1541611740));

    }
}
