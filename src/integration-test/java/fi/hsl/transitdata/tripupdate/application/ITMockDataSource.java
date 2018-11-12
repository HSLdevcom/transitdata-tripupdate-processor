package fi.hsl.transitdata.tripupdate.application;

import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.proto.InternalMessages;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Generates mock data that should be identical as our Source-components for TripUpdateProcessor
 */
public class ITMockDataSource {
    private ITMockDataSource() {}

    static class SourceMessage {
        public SourceMessage(byte[] payload, TransitdataProperties.ProtobufSchema schema, String dvjId, long timestamp) {
            this.payload = payload;
            this.schema = schema;
            this.timestamp = timestamp;
            this.dvjId = dvjId;
        }
        byte[] payload;
        TransitdataProperties.ProtobufSchema schema;
        long timestamp;
        String dvjId;
    }

    static class CancellationSourceMessage extends SourceMessage {
        InternalMessages.TripCancellation cancellation;

        public CancellationSourceMessage(InternalMessages.TripCancellation cancellation, String dvjId, long timestamp) {
            super(cancellation.toByteArray(), TransitdataProperties.ProtobufSchema.InternalMessagesTripCancellation, dvjId, timestamp);
            this.cancellation = cancellation;
        }
    }

    public static CancellationSourceMessage newCancellationMessage(String dvjId, String routeId, int direction, LocalDateTime startTime, long timestamp) {
        return newCancellationMessage(dvjId, routeId, direction, startTime, timestamp, InternalMessages.TripCancellation.Status.CANCELED);
    }

    public static CancellationSourceMessage newCancellationMessage(String dvjId, String routeId, int direction,
                                                                   LocalDateTime startTime, long timestamp,
                                                                   InternalMessages.TripCancellation.Status status) {
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

    public static void sendPulsarMessage(Producer<byte[]> producer, SourceMessage msg) throws PulsarClientException {
        producer.newMessage().value(msg.payload)
                .eventTime(msg.timestamp)
                .key(msg.dvjId)
                .property(TransitdataProperties.KEY_DVJ_ID, msg.dvjId)
                .property(TransitdataProperties.KEY_PROTOBUF_SCHEMA, msg.schema.toString())
                .send();
    }
}
