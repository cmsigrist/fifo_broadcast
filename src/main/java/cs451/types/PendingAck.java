package cs451.types;

import java.time.Duration;
import java.time.Instant;

public class PendingAck {
    private final String destIP;
    private final int destPort;
    private Instant start;
    private int timeout;
    private int attemptNumber;
    public static final int ACK_TIMEOUT = 500; // in milliseconds
    public static final int BACK_OFF = 100; // in milliseconds

    public PendingAck(String destIP, int destPort) {
        this.destIP = destIP;
        this.destPort = destPort;
        this.start = Instant.now();
        this.timeout = ACK_TIMEOUT;
        this.attemptNumber = 1;
    }

    public String getDestIP() {
        return destIP;
    }

    public int getDestPort() {
        return destPort;
    }

    public boolean hasTimedOut() {
        return Duration.between(start, Instant.now()).toMillis() >= timeout;
    }

    public void resetTimeout() {
        this.attemptNumber += 1;
        this.timeout += (BACK_OFF * attemptNumber);
        this.start = Instant.now();
    }

    public String getKey() {
        return destIP + ":" + destPort;
    }

    public static String getKey(String destIP, int destPort) {
        return destIP + ":" + destPort;
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
