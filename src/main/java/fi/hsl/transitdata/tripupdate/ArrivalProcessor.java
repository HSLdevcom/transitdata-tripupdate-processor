package fi.hsl.transitdata.tripupdate;

import com.google.protobuf.InvalidProtocolBufferException;
import fi.hsl.common.transitdata.proto.PubtransTableProtos;
import org.apache.pulsar.client.api.Message;

public class ArrivalProcessor extends BaseProcessor {

    public ArrivalProcessor(TripUpdateProcessor processor) {
        super(StopEvent.EventType.Arrival, processor);
    }

    protected PubtransTableProtos.Common parseSharedDataFromMessage(Message msg) throws InvalidProtocolBufferException {
        PubtransTableProtos.ROIArrival roiMessage = PubtransTableProtos.ROIArrival.parseFrom(msg.getData());
        return roiMessage.getCommon();
    }

}
