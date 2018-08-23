package fi.hsl.transitdata.tripupdate;

import com.google.protobuf.InvalidProtocolBufferException;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.proto.PubtransTableProtos;
import org.apache.pulsar.client.api.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

public class ArrivalProcessor implements IMessageProcessor {
    private static final Logger log = LoggerFactory.getLogger(ArrivalProcessor.class);

    private Jedis jedis;

    public ArrivalProcessor(Jedis jedis) {
        this.jedis = jedis;
    }

    @Override
    public boolean validateMessage(Message msg) {
        try {
            PubtransTableProtos.ROIArrival roiMessage = PubtransTableProtos.ROIArrival.parseFrom(msg.getData());

            String stopIdString = jedis.get(TransitdataProperties.REDIS_PREFIX_JPP + roiMessage.getIsTargetedAtJourneyPatternPointGid());
            if (stopIdString == null) {
                log.error("No stop id found for JourneyPatternPoint Gid " + roiMessage.getIsTargetedAtJourneyPatternPointGid());
                return false;
            }
            if (roiMessage.getTargetDateTime() == null || roiMessage.getTargetDateTime().isEmpty()) {
                log.error("No TargetDatetime, message discarded");
                return false;
            }
        }
        catch (InvalidProtocolBufferException e) {
            log.error("Failed to parse ROIArrival from message payload", e);
            return false;
        }
        return true;
    }

    @Override
    public void processMessage(Message msg) {

    }
}
