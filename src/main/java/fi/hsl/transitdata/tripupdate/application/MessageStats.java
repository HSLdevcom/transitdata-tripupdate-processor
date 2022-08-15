package fi.hsl.transitdata.tripupdate.application;


import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class MessageStats {
    private long startTime = System.nanoTime();

    private int messagesReceived = 0;
    private int messagesSent = 0;
    private int invalidTripUpdates = 0;

    private final Map<String, Integer> invalidTripUpdateReasons = new HashMap<>();

    public int getMessagesReceived() {
        return messagesReceived;
    }

    public void incrementMessagesReceived() {
        messagesReceived++;
    }

    public int getMessagesSent() {
        return messagesSent;
    }

    public void incrementMessagesSent() {
        messagesSent++;
    }

    public int getInvalidTripUpdates() {
        return invalidTripUpdates;
    }

    public void incrementInvalidTripUpdates(final String validator) {

        invalidTripUpdates++;

        if (validator != null) {
            invalidTripUpdateReasons.compute(validator, (key, prev) -> (prev != null ? prev : 0) + 1);
        }
    }

    public long getStartTime() {
        return startTime;
    }

    public long getDurationSecs() {
        return (System.nanoTime() - startTime) / 1_000_000_000;
    }

    public void reset() {
        startTime = System.nanoTime();

        messagesReceived = 0;
        messagesSent = 0;
        invalidTripUpdates = 0;

        invalidTripUpdateReasons.clear();
    }

    public void logAndReset(Logger logger) {
        logger.info(toString());
        reset();
    }

    @Override
    public String toString() {
        final String reasonsText = invalidTripUpdateReasons.entrySet().stream().map(entry -> entry.getKey() + ": " + entry.getValue()).collect(Collectors.joining(", "));

        return "Message stats:\n"+
                "\tStart time: " + getDurationSecs() + " seconds ago\n" +
                "\tMessages received: " + messagesReceived + "\n" +
                "\tMessages sent: " + messagesSent + "\n" +
                "\tInvalid trip updates: " + invalidTripUpdates + "(" + reasonsText + ")";
    }
}
