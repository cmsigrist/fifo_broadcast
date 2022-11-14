package cs451.types;

import java.time.Duration;
import java.time.Instant;

public class PendingAck {
    private final String destIP;
    private final int destPort;
    private final Instant start;
    private final int timeout;
    private final int attemptNumber;
    public static final int ACK_TIMEOUT = 500; // in milliseconds

    public PendingAck(String destIP, int destPort) {
        this.destIP = destIP;
        this.destPort = destPort;
        this.start = Instant.now();
        this.timeout = ACK_TIMEOUT;
        this.attemptNumber = 1;
    }

    public PendingAck(PendingAck pendingAck) {
        this.destIP = pendingAck.getDestIP();
        this.destPort = pendingAck.getDestPort();
        this.start = Instant.now();
        this.attemptNumber = pendingAck.getAttemptNumber() + 1;
        this.timeout = pendingAck.getTimeout() * attemptNumber;
    }

    public String getDestIP() {
        return destIP;
    }

    public int getDestPort() {
        return destPort;
    }

    public int getTimeout() {
        return timeout;
    }

    public int getAttemptNumber() {
        return attemptNumber;
    }

    public boolean hasTimedOut() {
        return Duration.between(start, Instant.now()).toMillis() >= timeout;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((destIP == null) ? 0 : destIP.hashCode());
        result = prime * result + destPort;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof PendingAck) {
            return ((PendingAck) obj).getDestIP().equals(destIP) &&
                    ((PendingAck) obj).getDestPort() == destPort;
        }

        return false;
    }

    @Override
    public String toString() {
        return destIP + ":" + destPort;
    }
}
