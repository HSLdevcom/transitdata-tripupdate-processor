package fi.hsl.transitdata.tripupdate.processing;

import com.google.protobuf.InvalidProtocolBufferException;
import fi.hsl.common.transitdata.proto.PubtransTableProtos;
import fi.hsl.transitdata.tripupdate.models.PubtransData;
import org.apache.pulsar.client.api.Message;

public class ArrivalProcessor extends BaseProcessor {

    public ArrivalProcessor(TripUpdateProcessor processor) {
        super(EventType.Arrival, processor);
    }

    protected PubtransData parseSharedData(Message msg) throws InvalidProtocolBufferException {
        PubtransTableProtos.ROIArrival roiMessage = PubtransTableProtos.ROIArrival.parseFrom(msg.getData());
        return new PubtransData(eventType, roiMessage.getCommon(), roiMessage.getTripInfo());
    }

}
