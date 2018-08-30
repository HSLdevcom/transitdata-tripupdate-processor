package fi.hsl.transitdata.tripupdate;

import com.google.protobuf.InvalidProtocolBufferException;
import fi.hsl.common.transitdata.proto.PubtransTableProtos;
import org.apache.pulsar.client.api.Message;
import redis.clients.jedis.Jedis;

public class DepartureProcessor extends BaseProcessor {

    public DepartureProcessor(Jedis jedis, TripUpdateProcessor processor) {
        super(jedis, StopEvent.EventType.Departure, processor);
    }

    protected PubtransTableProtos.Common parseSharedDataFromMessage(Message msg) throws InvalidProtocolBufferException {
        PubtransTableProtos.ROIDeparture roiMessage = PubtransTableProtos.ROIDeparture.parseFrom(msg.getData());
        return roiMessage.getCommon();
    }

}
