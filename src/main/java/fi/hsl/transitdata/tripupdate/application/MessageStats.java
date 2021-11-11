package fi.hsl.transitdata.tripupdate.application;


import org.slf4j.Logger;

public class MessageStats {
    private long startTime = System.nanoTime();

    private int messagesReceived = 0;
    private int messagesSent = 0;
    private int invalidTripUpdates = 0;

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

    public void incrementInvalidTripUpdates() {
        invalidTripUpdates++;
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
    }

    public void logAndReset(Logger logger) {
        logger.info(toString());
        reset();
    }

    @Override
    public String toString() {
        return "Message stats:\n"+
                "\tStart time: " + getDurationSecs() + " seconds ago\n" +
                "\tMessages received: " + messagesReceived + "\n" +
                "\tMessages sent: " + messagesSent + "\n" +
                "\tInvalid trip updates: " + invalidTripUpdates;
    }
}
