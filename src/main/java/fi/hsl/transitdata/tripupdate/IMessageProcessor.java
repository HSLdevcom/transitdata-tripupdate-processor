package fi.hsl.transitdata.tripupdate;

import org.apache.pulsar.client.api.Message;

public interface IMessageProcessor {
    /**
     * Check the data within the payload
     *
     * @param msg
     * @return true if we can proceed, false if we want to ignore this message
     */
    boolean validateMessage(Message msg);

    /**
     * Invoked if message goes through the validation
     * @param msg
     */
    void processMessage(Message msg);
}
