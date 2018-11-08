package fi.hsl.transitdata.tripupdate.validators;

import com.google.transit.realtime.GtfsRealtime;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

public class TripUpdateMaxAgeValidatorTest {

    @Test
    public void tripUpdateWithPrematureDepartureIsDiscarded() {

        //More than three minutes is a premature prediction
        PrematureDeparturesValidator validator = new PrematureDeparturesValidator(180);

        //2018-11-07T17:10:00 in Helsinki/Europe
        Collection<GtfsRealtime.TripUpdate.StopTimeUpdate> stopTimeUpdates = new ArrayList<>();
        stopTimeUpdates.add(newMockStopTimeUpdate("A", 0, 1541603400));

        GtfsRealtime.TripUpdate tripUpdate = newMockTripUpdate("1010", 0, "20181107",
                "17:14:00", stopTimeUpdates);

        assertEquals(false, validator.validate(tripUpdate));

    }

    @Test
    public void tripUpdateWithPrematureArrivalIsDiscarded() {

        //More than three minutes is a premature prediction
        PrematureDeparturesValidator validator = new PrematureDeparturesValidator(180);

        //2018-11-07T17:10:00 in Helsinki/Europe
        Collection<GtfsRealtime.TripUpdate.StopTimeUpdate> stopTimeUpdates = new ArrayList<>();
        stopTimeUpdates.add(newMockStopTimeUpdate("A", 1541603400, 0));

        GtfsRealtime.TripUpdate tripUpdate = newMockTripUpdate("1010", 0, "20181107",
                "17:14:00", stopTimeUpdates);

        assertEquals(false, validator.validate(tripUpdate));

    }

    @Test
    public void tripUpdateWithValidDepartureIsAccepted() {

        //More than three minutes is a premature prediction
        PrematureDeparturesValidator validator = new PrematureDeparturesValidator(180);

        //2018-11-07T17:10:00 in Helsinki/Europe
        Collection<GtfsRealtime.TripUpdate.StopTimeUpdate> stopTimeUpdates = new ArrayList<>();
        stopTimeUpdates.add(newMockStopTimeUpdate("A", 0, 1541603400));

        GtfsRealtime.TripUpdate tripUpdate = newMockTripUpdate("1010", 0, "20181107",
                "17:11:00", stopTimeUpdates);

        assertEquals(true, validator.validate(tripUpdate));

    }

    private GtfsRealtime.TripUpdate.StopTimeUpdate newMockStopTimeUpdate(String stopId, long arrivalTime,
                                                                         long departureTime) {

        GtfsRealtime.TripUpdate.StopTimeEvent arrival = GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder()
                .setTime(arrivalTime)
                .build();

        GtfsRealtime.TripUpdate.StopTimeEvent departure = GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder()
                .setTime(departureTime)
                .build();

        GtfsRealtime.TripUpdate.StopTimeUpdate.Builder stopTimeUpdateBuilder = GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder();

        if (arrivalTime != 0) {
           stopTimeUpdateBuilder.setArrival(arrival);
        }
        if (departureTime != 0) {
            stopTimeUpdateBuilder.setDeparture(departure);
        }

        return stopTimeUpdateBuilder.setStopId(stopId).build();
    }

    private GtfsRealtime.TripUpdate newMockTripUpdate(String routeId, int directionId,
                                                      String startDate, String startTime,
                                                      Iterable<GtfsRealtime.TripUpdate.StopTimeUpdate> stopTimeUpdateList) {

        GtfsRealtime.TripDescriptor tripDescriptor = GtfsRealtime.TripDescriptor.newBuilder()
                .setRouteId(routeId)
                .setDirectionId(directionId)
                .setStartDate(startDate)
                .setStartTime(startTime)
                .build();

        return GtfsRealtime.TripUpdate.newBuilder()
                .setTrip(tripDescriptor)
                .addAllStopTimeUpdate(stopTimeUpdateList)
                .build();

    }
}
