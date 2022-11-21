package cs451.types;

import java.time.Duration;
import java.time.Instant;

public class PendingAck {
    private final byte type;
    private final int destPort;
    private Instant start;
    private int timeout;
    private int attemptNumber;
    public static final int ACK_TIMEOUT = 500; // in milliseconds
    public static final int BACK_OFF = 10; // in milliseconds

    public PendingAck(byte type, int destPort) {
        this.type = type;
        this.destPort = destPort;
        this.start = Instant.now();
        this.timeout = ACK_TIMEOUT;
        this.attemptNumber = 1;
    }

    public byte getType() {
        return type;
    }

    public int getDestPort() {
        return destPort;
    }

    public int getTimeOut() {
        return timeout;
    }

    public boolean hasTimedOut() {
        return Duration.between(start, Instant.now()).toMillis() >= timeout;
    }

    public void resetTimeout() {
        this.attemptNumber += 1;
        this.timeout += (BACK_OFF * attemptNumber);
        this.start = Instant.now();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + destPort;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof PendingAck) {
            return ((PendingAck) obj).getDestPort() == destPort;
        }

        return false;
    }

    @Override
    public String toString() {
        return String.valueOf(destPort);
    }
}
