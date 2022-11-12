package cs451.types;

import java.time.Duration;
import java.time.Instant;

import cs451.messages.Packet;

public class PendingAck {
    Packet packet;
    private Instant start;
    private boolean isAcked = false;
    public static final int ACK_TIMEOUT = 500; // in milliseconds

    public PendingAck(Packet packet, Instant start) {
        this.packet = packet;
        this.start = start;
    }

    public Packet getPacket() {
        return packet;
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

    // public String getKey() {
    // Message message = packet.getMessage();
    // return message.getPid() + message.getSeqNum() + packet.getDestIP() +
    // packet.getDestPort();
    // }

    // public static String makeKey(byte pid, int seqNum, String srcIP, int srcPort)
    // {
    // return pid + seqNum + srcIP + srcPort;
    // }

    public String toString() {
        return packet.toString();
    }
}
