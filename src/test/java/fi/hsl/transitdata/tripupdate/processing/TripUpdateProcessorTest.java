package fi.hsl.transitdata.tripupdate.processing;

import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.transitdata.MockDataUtils;
import fi.hsl.common.transitdata.proto.InternalMessages;
import org.junit.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TripUpdateProcessorTest {
    @Test
    public void testStopTimeUpdateCache() throws Exception {
        TripUpdateProcessor processor = new TripUpdateProcessor(null);

        final long baseDvjId = MockDataUtils.generateValidJoreId();
        final long firstDvjId = baseDvjId;

        final int amount = 20;
        addStops(firstDvjId, amount, processor);

        final long secondDvjId = baseDvjId + 1;
        final int secondAmount = amount - 1;
        addStops(secondDvjId, secondAmount, processor);

        validateStops(firstDvjId, amount, processor);
        validateStops(secondDvjId, secondAmount, processor);
        final long nonExistingId = baseDvjId + 42;
        validateStops(nonExistingId, 0, processor);

        //Adding stops to existing StopTimeUpdates should not increase the count, nor affect the ordering.
        int existingId = 1;
        addStop(firstDvjId, existingId, existingId, processor);
        validateStops(firstDvjId, amount, processor);
        //Adding with new sequenceIDs should again increase it
        int newId = amount;
        addStop(firstDvjId, newId, newId, processor);
        validateStops(firstDvjId, amount + 1, processor);

    }

    private void addStops(final long dvjId, final int amount, TripUpdateProcessor processor) throws Exception {
        int counter = 0;
        while(counter < amount) {
            final int stopSequence = counter;
            final int stopId = stopSequence; // we can just use the same id here
            counter++;

            addStop(dvjId, stopId, stopSequence, processor);

            //Should reflect to cache size
            final String cacheKey = Long.toString(dvjId);
            List<GtfsRealtime.TripUpdate.StopTimeUpdate> updates = processor.getStopTimeUpdates(cacheKey);
            assertEquals(updates.size(), counter);
        }
    }

    private void addStop(long dvjId, long stopId, int stopSequence, TripUpdateProcessor processor) throws Exception {
        final long fixedTargetDateTime = 1545692705000L;
        InternalMessages.StopEstimate estimate = MockDataUtils.mockStopEstimate(dvjId,
                InternalMessages.StopEstimate.Type.ARRIVAL, stopId, stopSequence, fixedTargetDateTime);
        //Update cache
        processor.updateStopTimeUpdateCache(estimate);
    }

    private void validateStops(final long dvjId, final int correctAmount, TripUpdateProcessor processor) throws Exception {
        final String cacheKey = Long.toString(dvjId);
        List<GtfsRealtime.TripUpdate.StopTimeUpdate> updates = processor.getStopTimeUpdates(cacheKey);
        assertEquals(updates.size(), correctAmount);

        //Validate that stopIds and seqIds match and the sorting order is correct, by seqId
        int index = 0;
        for (GtfsRealtime.TripUpdate.StopTimeUpdate update: updates) {
            assertEquals(Integer.toString(update.getStopSequence()), update.getStopId());
            assertEquals(update.getStopSequence(), index);

            index++;
        }
    }

    @Test
    public void testCorrectScheduleTypeIsRestoredAfterCancellationOfCancellation() {
        TripUpdateProcessor processor = new TripUpdateProcessor(null);

        InternalMessages.TripInfo tripInfo = InternalMessages.TripInfo.newBuilder()
                .setTripId("trip_1")
                .setDirectionId(1)
                .setOperatingDay("20200101")
                .setStartTime("00:00:00")
                .setRouteId("2550")
                .setScheduleType(InternalMessages.TripInfo.ScheduleType.ADDED)
                .build();

        Optional<GtfsRealtime.TripUpdate> tripUpdate = processor.processStopEstimate(InternalMessages.StopEstimate.newBuilder()
                .setSchemaVersion(1)
                .setStopId("1")
                .setStopSequence(1)
                .setEstimatedTimeUtcMs(0)
                .setScheduledTimeUtcMs(0)
                .setLastModifiedUtcMs(0)
                .setType(InternalMessages.StopEstimate.Type.ARRIVAL)
                .setStatus(InternalMessages.StopEstimate.Status.SCHEDULED)
                .setTripInfo(tripInfo)
                .build());

        assertTrue(tripUpdate.isPresent());
        assertEquals(GtfsRealtime.TripDescriptor.ScheduleRelationship.ADDED, tripUpdate.get().getTrip().getScheduleRelationship());

        // 'messageKey' is trip ID
        GtfsRealtime.TripUpdate tripCancellation = processor.processTripCancellation("trip_1", 0, InternalMessages.TripCancellation.newBuilder()
                .setSchemaVersion(1)
                .setTripId("trip_1")
                .setDirectionId(1)
                .setRouteId("2550")
                .setStartDate("20200101")
                .setStartTime("00:00:00")
                .setStatus(InternalMessages.TripCancellation.Status.CANCELED)
                .build());

        assertEquals(GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED, tripCancellation.getTrip().getScheduleRelationship());

        GtfsRealtime.TripUpdate cancellationOfCancellation = processor.processTripCancellation("trip_1", 0, InternalMessages.TripCancellation.newBuilder()
                .setSchemaVersion(1)
                .setTripId("trip_1")
                .setDirectionId(1)
                .setRouteId("2550")
                .setStartDate("20200101")
                .setStartTime("00:00:00")
                .setStatus(InternalMessages.TripCancellation.Status.RUNNING)
                .build());

        assertEquals(GtfsRealtime.TripDescriptor.ScheduleRelationship.ADDED, cancellationOfCancellation.getTrip().getScheduleRelationship());
    }
    
    @Test
    public void testAssignedStopIdIsSet() {
        TripUpdateProcessor processor = new TripUpdateProcessor(null);
        
        InternalMessages.TripInfo tripInfo = InternalMessages.TripInfo.newBuilder()
                .setTripId("trip_1")
                .setDirectionId(1)
                .setOperatingDay("20240315")
                .setStartTime("13:43:00")
                .setRouteId("2015")
                .setScheduleType(InternalMessages.TripInfo.ScheduleType.ADDED)
                .build();
        
        Optional<GtfsRealtime.TripUpdate> tripUpdate = processor.processStopEstimate(InternalMessages.StopEstimate.newBuilder()
                .setSchemaVersion(1)
                .setStopId("1")
                .setTargetedStopId("2")
                .setStopSequence(1)
                .setEstimatedTimeUtcMs(0)
                .setScheduledTimeUtcMs(0)
                .setLastModifiedUtcMs(0)
                .setType(InternalMessages.StopEstimate.Type.DEPARTURE)
                .setStatus(InternalMessages.StopEstimate.Status.SCHEDULED)
                .setTripInfo(tripInfo)
                .build());
        
        assertTrue(tripUpdate.isPresent());
        assertEquals(1, tripUpdate.get().getStopTimeUpdate(0).getStopSequence());
        assertEquals("2", tripUpdate.get().getStopTimeUpdate(0).getStopTimeProperties().getAssignedStopId());
    }

    @Test
    public void testCancellationOfCancellationWithoutPreviousStopTimeUpdates() {
        TripUpdateProcessor processor = new TripUpdateProcessor(null);

        // first, cancel a trip
        GtfsRealtime.TripUpdate tripCancellation = processor.processTripCancellation("trip_1", 0, InternalMessages.TripCancellation.newBuilder()
                .setSchemaVersion(1)
                .setTripId("trip_1")
                .setDirectionId(1)
                .setRouteId("2550")
                .setStartDate("20200101")
                .setStartTime("00:00:00")
                .setStatus(InternalMessages.TripCancellation.Status.CANCELED)
                .build());

        // then, cancel the cancellation
        GtfsRealtime.TripUpdate tu = processor.processTripCancellation("trip_1", 0, InternalMessages.TripCancellation.newBuilder()
                .setSchemaVersion(1)
                .setTripId("trip_1")
                .setDirectionId(1)
                .setRouteId("2550")
                .setStartDate("20200101")
                .setStartTime("00:00:00")
                .setStatus(InternalMessages.TripCancellation.Status.RUNNING)
                .build());

        // check that cancellation was cancelled
        assertEquals(GtfsRealtime.TripDescriptor.ScheduleRelationship.SCHEDULED, tu.getTrip().getScheduleRelationship());
        // check that one stopTimeUpdate was added to the cancellation of cancellation
        assertEquals(1, tu.getStopTimeUpdateCount());
        assertEquals(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.NO_DATA, tu.getStopTimeUpdate(0).getScheduleRelationship());
        assertEquals(1, tu.getStopTimeUpdate(0).getStopSequence());
        assertEquals(false, tu.getStopTimeUpdate(0).hasArrival());
        assertEquals(false, tu.getStopTimeUpdate(0).hasDeparture());
        assertEquals(false, tu.getStopTimeUpdate(0).hasStopId());
    }

    @Test
    public void testCancellationOfCancellationWithPreviousStopTimeUpdates() {
        TripUpdateProcessor processor = new TripUpdateProcessor(null);

        // first, add trip with a stop time update
        InternalMessages.TripInfo tripInfo = InternalMessages.TripInfo.newBuilder()
                .setTripId("trip_1")
                .setDirectionId(1)
                .setOperatingDay("20200101")
                .setStartTime("00:00:00")
                .setRouteId("2550")
                .setScheduleType(InternalMessages.TripInfo.ScheduleType.SCHEDULED)
                .build();

        Optional<GtfsRealtime.TripUpdate> tripUpdate = processor.processStopEstimate(InternalMessages.StopEstimate.newBuilder()
                .setSchemaVersion(1)
                .setStopId("1")
                .setStopSequence(1)
                .setEstimatedTimeUtcMs(0)
                .setScheduledTimeUtcMs(0)
                .setLastModifiedUtcMs(0)
                .setType(InternalMessages.StopEstimate.Type.ARRIVAL)
                .setStatus(InternalMessages.StopEstimate.Status.SCHEDULED)
                .setTripInfo(tripInfo)
                .build());

        // then, cancel the trip
        GtfsRealtime.TripUpdate tripCancellation = processor.processTripCancellation("trip_1", 0, InternalMessages.TripCancellation.newBuilder()
                .setSchemaVersion(1)
                .setTripId("trip_1")
                .setDirectionId(1)
                .setStartDate("20200101")
                .setStartTime("00:00:00")
                .setRouteId("2550")
                .setStatus(InternalMessages.TripCancellation.Status.CANCELED)
                .build());

        // cancel the cancellation
        GtfsRealtime.TripUpdate tu = processor.processTripCancellation("trip_1", 0, InternalMessages.TripCancellation.newBuilder()
                .setSchemaVersion(1)
                .setTripId("trip_1")
                .setDirectionId(1)
                .setStartDate("20200101")
                .setStartTime("00:00:00")
                .setRouteId("2550")
                .setStatus(InternalMessages.TripCancellation.Status.RUNNING)
                .build());

        // check that the cancellation has one stopTimeUpdate and its the one that was added before the first cancellation
        assertEquals(GtfsRealtime.TripDescriptor.ScheduleRelationship.SCHEDULED, tu.getTrip().getScheduleRelationship());
        assertEquals(1, tu.getStopTimeUpdateCount());
        assertEquals(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SCHEDULED, tu.getStopTimeUpdate(0).getScheduleRelationship());
        assertEquals(true, tu.getStopTimeUpdate(0).hasStopId());
        assertEquals(true, tu.getStopTimeUpdate(0).hasArrival());
        assertEquals(true, tu.getStopTimeUpdate(0).hasDeparture());
        assertEquals("1", tu.getStopTimeUpdate(0).getStopId());
    }
}
