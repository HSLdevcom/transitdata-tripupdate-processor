package fi.hsl.transitdata.tripupdate.models;

import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.common.transitdata.proto.PubtransTableProtos;
import fi.hsl.transitdata.tripupdate.processing.BaseProcessor;

/**
 * Intermediate data format which will be refactored out soon, we'll just read StopEstimates directly.
 */
public class PubtransData {

    public PubtransTableProtos.Common common;
    public PubtransTableProtos.DOITripInfo tripInfo;
    public BaseProcessor.EventType eventType;

    public PubtransData(BaseProcessor.EventType eventType, PubtransTableProtos.Common common, PubtransTableProtos.DOITripInfo tripInfo) {
        this.tripInfo = tripInfo;
        this.common = common;
        this.eventType = eventType;
    }

    /**
     * StopEstimate is the data-model that we're going to use in the future so let's refactor this codebase already to use it internally.
     * Then it's easy to swap to new input schema.
     *
     * Throws RuntimeException if data is not valid.
     */
    public InternalMessages.StopEstimate toStopEstimate() throws Exception {
        InternalMessages.StopEstimate.Builder builder = InternalMessages.StopEstimate.newBuilder();
        builder.setSchemaVersion(builder.getSchemaVersion());

        InternalMessages.TripInfo.Builder tripBuilder = InternalMessages.TripInfo.newBuilder();
        tripBuilder.setTripId(Long.toString(tripInfo.getDvjId()));
        tripBuilder.setOperatingDay(tripInfo.getOperatingDay());
        tripBuilder.setRouteId(tripInfo.getRouteId());
        tripBuilder.setDirectionId(tripInfo.getDirectionId());//Jore format
        tripBuilder.setStartTime(tripInfo.getStartTime());
        builder.setTripInfo(tripBuilder.build());
        //NOTICE!!! Previous implementation of Internal model (StopEvent) converted Jore dir to GTFS-RT direction already here!!
        builder.setStopId(tripInfo.getStopId()); //Use to be Long in old internal model
        builder.setStopSequence(common.getJourneyPatternSequenceNumber());

        InternalMessages.StopEstimate.Status scheduledStatus = (common.getState() == 3L) ?
                InternalMessages.StopEstimate.Status.SKIPPED :
                InternalMessages.StopEstimate.Status.SCHEDULED;

        builder.setStatus(scheduledStatus);

        builder.setType(eventType == BaseProcessor.EventType.Arrival ?
                InternalMessages.StopEstimate.Type.ARRIVAL:
                InternalMessages.StopEstimate.Type.DEPARTURE);
        builder.setEstimatedTimeUtcMs(common.getTargetUtcDateTimeMs()); //NOTICE!! Previous impl used seconds as targettime!! GTFS-RT demands that also
        // builder.setScheduledTimeUtcMs(..); // This we don't have here atm, and it's optional.
        builder.setLastModifiedUtcMs(common.getLastModifiedUtcDateTimeMs());
        return builder.build();
    }

}
