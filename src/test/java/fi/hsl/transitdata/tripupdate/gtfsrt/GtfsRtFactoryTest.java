package fi.hsl.transitdata.tripupdate.gtfsrt;

import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.transitdata.tripupdate.MockDataFactory;
import fi.hsl.transitdata.tripupdate.models.StopEvent;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

public class GtfsRtFactoryTest {

    @Test
    public void newTripUpdateFromStopEventWithLetterAndNumberIsRenamedProperly() {

        StopEvent stopEvent = MockDataFactory.mockStopEvent("1010H4");
        GtfsRealtime.TripUpdate tripUpdate = GtfsRtFactory.newTripUpdate(stopEvent);

        assertEquals("1010H", tripUpdate.getTrip().getRouteId());
    }

    @Test
    public void newTripUpdateFromStopEventWithSpaceAndNumberIsRenamedProperly() {

        StopEvent stopEvent = MockDataFactory.mockStopEvent("1010 3");
        GtfsRealtime.TripUpdate tripUpdate = GtfsRtFactory.newTripUpdate(stopEvent);

        assertEquals("1010", tripUpdate.getTrip().getRouteId());
    }

    @Test
    public void newTripUpdateFromStopEventWithFourCharactersIsNotRenamed() {

        StopEvent stopEvent = MockDataFactory.mockStopEvent("1010");
        GtfsRealtime.TripUpdate tripUpdate = GtfsRtFactory.newTripUpdate(stopEvent);

        assertEquals("1010", tripUpdate.getTrip().getRouteId());
    }

    @Test
    public void newTripUpdateFromStopEventWithOneLetterIsNotRenamed() {

        StopEvent stopEvent = MockDataFactory.mockStopEvent("1010H");
        GtfsRealtime.TripUpdate tripUpdate = GtfsRtFactory.newTripUpdate(stopEvent);

        assertEquals("1010H", tripUpdate.getTrip().getRouteId());
    }

    @Test
    public void newTripUpdateFromStopEventWithTwoLettersIsNotRenamed() {

        StopEvent stopEvent = MockDataFactory.mockStopEvent("1010HK");
        GtfsRealtime.TripUpdate tripUpdate = GtfsRtFactory.newTripUpdate(stopEvent);

        assertEquals("1010HK", tripUpdate.getTrip().getRouteId());
    }

    @Test
    public void newTripUpdateFromTripCancellationWithLetterAndNumberIsRenamedProperly() {

        InternalMessages.TripCancellation tripCancellation = MockDataFactory.mockTripCancellation("1010H4");
        GtfsRealtime.TripUpdate tripUpdate = GtfsRtFactory.newTripUpdate(tripCancellation, 1542096708);

        assertEquals("1010H", tripUpdate.getTrip().getRouteId());
    }

    @Test
    public void newTripUpdateFromTripCancellationWithSpaceAndNumberIsRenamedProperly() {

        InternalMessages.TripCancellation tripCancellation = MockDataFactory.mockTripCancellation("1010 3");
        GtfsRealtime.TripUpdate tripUpdate = GtfsRtFactory.newTripUpdate(tripCancellation, 1542096708);

        assertEquals("1010", tripUpdate.getTrip().getRouteId());
    }

    @Test
    public void newTripUpdateFromTripCancellationWithFourCharactersIsNotRenamed() {

        InternalMessages.TripCancellation tripCancellation = MockDataFactory.mockTripCancellation("1010");
        GtfsRealtime.TripUpdate tripUpdate = GtfsRtFactory.newTripUpdate(tripCancellation, 1542096708);

        assertEquals("1010", tripUpdate.getTrip().getRouteId());
    }

    @Test
    public void newTripUpdateFromTripCancellationWithOneLetterIsNotRenamed() {

        InternalMessages.TripCancellation tripCancellation = MockDataFactory.mockTripCancellation("1010H");
        GtfsRealtime.TripUpdate tripUpdate = GtfsRtFactory.newTripUpdate(tripCancellation, 1542096708);

        assertEquals("1010H", tripUpdate.getTrip().getRouteId());
    }

    @Test
    public void newTripUpdateFromTripCancellationWithTwoLettersIsNotRenamed() {

        InternalMessages.TripCancellation tripCancellation = MockDataFactory.mockTripCancellation("1010HK");
        GtfsRealtime.TripUpdate tripUpdate = GtfsRtFactory.newTripUpdate(tripCancellation, 1542096708);

        assertEquals("1010HK", tripUpdate.getTrip().getRouteId());
    }
}
