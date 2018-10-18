package fi.hsl.transitdata.tripupdate;

import org.apache.pulsar.client.api.Message;

public class CancellationProcessor implements IMessageProcessor {

    private final TripUpdateProcessor tripUpdateProcessor;

    public CancellationProcessor(TripUpdateProcessor tripUpdateProcessor) {
        this.tripUpdateProcessor = tripUpdateProcessor;
    }

    @Override
    public boolean validateMessage(Message msg) {
        return false;
    }

    @Override
    public void processMessage(Message msg) {

    }
}
