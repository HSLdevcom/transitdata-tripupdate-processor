package fi.hsl.transitdata.tripupdate.application;

import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.common.transitdata.proto.PubtransTableProtos;
import fi.hsl.transitdata.tripupdate.MockDataFactory;
import fi.hsl.transitdata.tripupdate.models.StopEvent;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

/**
 * Generates mock data that should be identical as our Source-components for TripUpdateProcessor
 */
public class ITMockDataSource {
    private ITMockDataSource() {}

    static class SourceMessage {
        public SourceMessage(byte[] payload, TransitdataProperties.ProtobufSchema schema, String dvjId, long timestamp) {
            this(payload, schema, dvjId, timestamp, new HashMap<>());
        }

        public SourceMessage(byte[] payload, TransitdataProperties.ProtobufSchema schema, String dvjId, long timestamp, Map<String, String> props) {
            this.payload = payload;
            this.schema = schema;
            this.timestamp = timestamp;
            this.dvjId = dvjId;
            this.props = props;
        }
        byte[] payload;
        TransitdataProperties.ProtobufSchema schema;
        long timestamp;
        String dvjId;
        Map<String, String> props;
    }

    static class CancellationSourceMessage extends SourceMessage {
        InternalMessages.TripCancellation cancellation;

        public CancellationSourceMessage(InternalMessages.TripCancellation cancellation, String dvjId, long timestamp) {
            super(cancellation.toByteArray(), TransitdataProperties.ProtobufSchema.InternalMessagesTripCancellation, dvjId, timestamp);
            this.cancellation = cancellation;
        }
    }

    static class ArrivalSourceMessage extends SourceMessage {
        PubtransTableProtos.ROIArrival arrival;

        public ArrivalSourceMessage(PubtransTableProtos.ROIArrival arrival, String dvjId, long timestamp, Map<String, String> props) {
            super(arrival.toByteArray(), TransitdataProperties.ProtobufSchema.PubtransRoiArrival, dvjId, timestamp, props);
            this.arrival = arrival;
        }
    }

    public static CancellationSourceMessage newCancellationMessage(String dvjId, String routeId, int direction, LocalDateTime startTime) {
        return newCancellationMessage(dvjId, routeId, direction, startTime, InternalMessages.TripCancellation.Status.CANCELED);
    }

    public static CancellationSourceMessage newCancellationMessage(String dvjId, String routeId, int direction,
                                                                   LocalDateTime startTime,
                                                                   InternalMessages.TripCancellation.Status status) {

        final long timestamp = System.currentTimeMillis();

        InternalMessages.TripCancellation.Builder builder = InternalMessages.TripCancellation.newBuilder();
        String date = DateTimeFormatter.ofPattern("yyyyMMdd").format(startTime);
        String time = DateTimeFormatter.ofPattern("HH:mm:ss").format(startTime);

        builder.setRouteId(routeId);
        builder.setDirectionId(direction);
        builder.setStartDate(date);
        builder.setStartTime(time);
        //Version number is defined in the proto file as default value but we still need to set it since it's a required field
        builder.setSchemaVersion(builder.getSchemaVersion());
        builder.setStatus(status);

        final InternalMessages.TripCancellation cancellation = builder.build();
        return new CancellationSourceMessage(cancellation, dvjId, timestamp);
    }

    public static ArrivalSourceMessage newArrivalMessage(int startTimeOffsetInSeconds, long dvjId, String routeId, int direction, int stopSeqAndId) {
        return newArrivalMessage(startTimeOffsetInSeconds, dvjId, routeId, direction, stopSeqAndId, false);
    }

    public static ArrivalSourceMessage newArrivalMessage(int startTimeOffsetInSeconds, long dvjId, String routeId, int direction, int stopSeqAndId, boolean isViaPoint) {
        final long nowMs = System.currentTimeMillis();
        final long targetTimeEpochMs = nowMs + startTimeOffsetInSeconds * 1000;

        PubtransTableProtos.Common.Builder commonBuilder = PubtransTableProtos.Common.newBuilder();
        //We're hardcoding the version number to proto file to ease syncing with changes, however we still need to set it since it's a required field
        commonBuilder.setSchemaVersion(commonBuilder.getSchemaVersion());
        commonBuilder.setId(11111);
        commonBuilder.setIsOnDatedVehicleJourneyId(dvjId);
        //To ease testing let's use same numbers for stopSeq and stopId's.
        commonBuilder.setJourneyPatternSequenceNumber(stopSeqAndId);
        commonBuilder.setIsTimetabledAtJourneyPatternPointGid(22222);
        commonBuilder.setIsTargetedAtJourneyPatternPointGid(33333);
        commonBuilder.setVisitCountNumber(99);

        commonBuilder.setTargetUtcDateTimeMs(targetTimeEpochMs);

        commonBuilder.setState(2L); // 3L == Skipped
        commonBuilder.setType(isViaPoint ? 0 : 1);
        commonBuilder.setIsValidYesNo(true);

        commonBuilder.setLastModifiedUtcDateTimeMs(nowMs);
        PubtransTableProtos.Common common = commonBuilder.build();
        PubtransTableProtos.ROIArrival arrival = PubtransTableProtos.ROIArrival
                .newBuilder()
                .setCommon(common)
                .build();

        final String[] dateAndTime = MockDataFactory.formatStopEventTargetDateTime(targetTimeEpochMs / 1000); //TODO refactor sec <-> ms in some consistent way
        Map<String, String> props = MockDataFactory.mockMessageProperties(stopSeqAndId, direction, routeId, dateAndTime[0], dateAndTime[1]);
        return new ArrivalSourceMessage(arrival, Long.toString(dvjId), nowMs, props);
    }

    public static void sendPulsarMessage(Producer<byte[]> producer, SourceMessage msg) throws PulsarClientException {
        TypedMessageBuilder<byte[]> builder = producer.newMessage().value(msg.payload)
                .eventTime(msg.timestamp)
                .key(msg.dvjId)
                .property(TransitdataProperties.KEY_DVJ_ID, msg.dvjId)
                .property(TransitdataProperties.KEY_PROTOBUF_SCHEMA, msg.schema.toString());

        msg.props.forEach((key, value) -> builder.property(key, value));
        builder.send();
    }
}
