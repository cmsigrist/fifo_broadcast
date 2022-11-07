package cs451.types;

import java.time.Duration;
import java.time.Instant;

import cs451.messages.Message;

public class PendingAck {
    Message message;
    private Instant start;
    // The address of the node from which we wait an ack
    private final String destIP;
    private final int destPort;
    private boolean isAcked = false;
    public static final int ACK_TIMEOUT = 500; // in milliseconds

    public PendingAck(Message message, Instant start) {
        this.message = message;
        this.start = start;
        this.destIP = message.getDestIP();
        this.destPort = message.getDestPort();
    }

    public Message getMessage() {
        return message;
    }

    public void setAcked() {
        this.isAcked = true;
    }

    public boolean isAcked() {
        return isAcked;
    }

    public boolean hasTimedOut() {
        return Duration.between(start, Instant.now()).toMillis() >= ACK_TIMEOUT;
    }

    public void resetTimeout() {
        this.start = Instant.now();
    }

    public String getKey() {
        return message.getSeqNum() + destIP + destPort;
    }

    public static String makeKey(int seqNum, String destIP, int destPort) {
        return seqNum + destIP + destPort;
    }
}
