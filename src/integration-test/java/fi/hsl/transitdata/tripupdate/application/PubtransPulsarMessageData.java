package fi.hsl.transitdata.tripupdate.application;

import fi.hsl.common.pulsar.PulsarMessageData;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.common.transitdata.proto.PubtransTableProtos;

import java.util.HashMap;

public class PubtransPulsarMessageData<T extends com.google.protobuf.GeneratedMessageV3> extends PulsarMessageData {
    T actualPayload;
    long dvjId;
    TransitdataProperties.ProtobufSchema schema;

    public PubtransPulsarMessageData(T actualPayload, TransitdataProperties.ProtobufSchema schema, Long eventTime, long dvjId) {
        super(actualPayload.toByteArray(), eventTime, Long.toString(dvjId), new HashMap<>());
        this.dvjId = dvjId;
        this.actualPayload = actualPayload;
        this.schema = schema;
        this.properties.put(TransitdataProperties.KEY_PROTOBUF_SCHEMA, schema.toString());
        this.properties.put(TransitdataProperties.KEY_DVJ_ID, Long.toString(dvjId));
    }

    public static class ArrivalPulsarMessageData extends PubtransPulsarMessageData<PubtransTableProtos.ROIArrival> {
        public ArrivalPulsarMessageData(PubtransTableProtos.ROIArrival actualPayload, Long eventTime, long dvjId) {
            super(actualPayload, TransitdataProperties.ProtobufSchema.PubtransRoiArrival, eventTime, dvjId);
        }
    }

    public static class DeparturePulsarMessageData extends PubtransPulsarMessageData<PubtransTableProtos.ROIDeparture> {
        public DeparturePulsarMessageData(PubtransTableProtos.ROIDeparture actualPayload, Long eventTime, long dvjId) {
            super(actualPayload, TransitdataProperties.ProtobufSchema.PubtransRoiDeparture, eventTime, dvjId);
        }
    }

    public static class CancellationPulsarMessageData extends PubtransPulsarMessageData<InternalMessages.TripCancellation> {
        public CancellationPulsarMessageData(InternalMessages.TripCancellation actualPayload, Long eventTime, long dvjId) {
            super(actualPayload, TransitdataProperties.ProtobufSchema.InternalMessagesTripCancellation, eventTime, dvjId);
        }
    }

}
