package fi.hsl.transitdata.tripupdate;

import com.google.protobuf.InvalidProtocolBufferException;
import fi.hsl.common.transitdata.proto.PubtransTableProtos;
import org.apache.pulsar.client.api.Message;
import redis.clients.jedis.Jedis;

public class ArrivalProcessor extends BaseProcessor {

    public ArrivalProcessor(Jedis jedis, TripUpdateProcessor processor) {
        super(jedis, StopEvent.EventType.Arrival, processor);
    }

    protected PubtransTableProtos.ROIBase parseBaseFromMessage(Message msg) throws InvalidProtocolBufferException {
        PubtransTableProtos.ROIArrival roiMessage = PubtransTableProtos.ROIArrival.parseFrom(msg.getData());
        return roiMessage.getBase();
    }

}
