package fi.hsl.transitdata.tripupdate.processing;

import com.google.protobuf.InvalidProtocolBufferException;
import fi.hsl.common.transitdata.proto.PubtransTableProtos;
import fi.hsl.transitdata.tripupdate.models.StopEvent;
import org.apache.pulsar.client.api.Message;

public class DepartureProcessor extends BaseProcessor {

    public DepartureProcessor(TripUpdateProcessor processor) {
        super(EventType.Departure, processor);
    }

    protected PubtransData parseSharedData(Message msg) throws InvalidProtocolBufferException {
        PubtransTableProtos.ROIDeparture roiMessage = PubtransTableProtos.ROIDeparture.parseFrom(msg.getData());
        return new PubtransData(eventType, roiMessage.getCommon(), roiMessage.getTripInfo());
    }

}
