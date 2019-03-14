package fi.hsl.transitdata.tripupdate.application;

import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.gtfsrt.FeedMessageFactory;
import fi.hsl.common.transitdata.MockDataUtils;
import fi.hsl.common.transitdata.RouteData;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.common.transitdata.proto.PubtransTableProtos;
import fi.hsl.transitdata.tripupdate.MockDataFactory;
import fi.hsl.transitdata.tripupdate.gtfsrt.GtfsRtFactory;
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
        public SourceMessage(byte[] payload, TransitdataProperties.ProtobufSchema schema, long dvjId, long timestamp) {
            this(payload, schema, dvjId, timestamp, new HashMap<>());
        }

        public SourceMessage(byte[] payload, TransitdataProperties.ProtobufSchema schema, long dvjId, long timestamp, Map<String, String> props) {
            this.payload = payload;
            this.schema = schema;
            this.timestamp = timestamp;
            this.dvjId = dvjId;
            this.props = props;
        }
        byte[] payload;
        TransitdataProperties.ProtobufSchema schema;
        long timestamp;
        long dvjId;
        Map<String, String> props;
    }

    static class CancellationSourceMessage extends SourceMessage {
        InternalMessages.TripCancellation cancellation;

        public CancellationSourceMessage(InternalMessages.TripCancellation cancellation, long dvjId, long timestamp) {
            super(cancellation.toByteArray(), TransitdataProperties.ProtobufSchema.InternalMessagesTripCancellation, dvjId, timestamp);
            this.cancellation = cancellation;
        }
    }

    static class ArrivalSourceMessage extends SourceMessage {
        PubtransTableProtos.ROIArrival arrival;

        public ArrivalSourceMessage(PubtransTableProtos.ROIArrival arrival, long dvjId, long timestamp, Map<String, String> props) {
            super(arrival.toByteArray(), TransitdataProperties.ProtobufSchema.PubtransRoiArrival, dvjId, timestamp, props);
            this.arrival = arrival;
        }

        public GtfsRealtime.FeedMessage toGtfsRt() {
            StopEvent event = StopEvent.newInstance(arrival.getCommon(), this.props, StopEvent.EventType.Arrival);
            GtfsRealtime.TripUpdate tu = GtfsRtFactory.newTripUpdate(event);
            GtfsRealtime.FeedMessage feedMessage = FeedMessageFactory.createDifferentialFeedMessage(Long.toString(dvjId), tu, timestamp);
            return feedMessage;
        }
    }

    public static CancellationSourceMessage newCancellationMessage(long dvjId, String routeId, int joreDirection, LocalDateTime startTime) {
        return newCancellationMessage(dvjId, routeId, joreDirection, startTime, InternalMessages.TripCancellation.Status.CANCELED);
    }

    public static CancellationSourceMessage newCancellationMessage(long dvjId, String routeId, int joreDirection,
                                                                   LocalDateTime startTime,
                                                                   InternalMessages.TripCancellation.Status status) {

        final long timestamp = System.currentTimeMillis();

        InternalMessages.TripCancellation.Builder builder = InternalMessages.TripCancellation.newBuilder();
        String date = DateTimeFormatter.ofPattern("yyyyMMdd").format(startTime);
        String time = DateTimeFormatter.ofPattern("HH:mm:ss").format(startTime);

        builder.setRouteId(routeId);
        builder.setDirectionId(joreDirection);
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

        PubtransTableProtos.Common.Builder commonBuilder = MockDataUtils.generateValidCommon(dvjId, stopSeqAndId, targetTimeEpochMs, nowMs);
        MockDataUtils.setIsViaPoint(commonBuilder, isViaPoint);

        PubtransTableProtos.Common common = commonBuilder.build();
        PubtransTableProtos.ROIArrival arrival = PubtransTableProtos.ROIArrival
                .newBuilder()
                .setCommon(common)
                .build();

        //To ease testing let's use same numbers for stopSeq and stopId's.
        Map<String, String> props = new RouteData(stopSeqAndId, direction, routeId, targetTimeEpochMs / 1000).toMap();
        return new ArrivalSourceMessage(arrival, dvjId, nowMs, props);
    }

    public static void sendPulsarMessage(Producer<byte[]> producer, SourceMessage msg) throws PulsarClientException {
        String dvjIdAsString = Long.toString(msg.dvjId);
        TypedMessageBuilder<byte[]> builder = producer.newMessage().value(msg.payload)
                .eventTime(msg.timestamp)
                .key(dvjIdAsString)
                .property(TransitdataProperties.KEY_DVJ_ID, dvjIdAsString)
                .property(TransitdataProperties.KEY_PROTOBUF_SCHEMA, msg.schema.toString());

        msg.props.forEach((key, value) -> builder.property(key, value));
        builder.send();
    }
}
