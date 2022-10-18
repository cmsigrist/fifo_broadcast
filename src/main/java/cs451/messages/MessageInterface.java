package cs451.messages;

import java.net.DatagramPacket;

public interface MessageInterface {
    public byte getPid();
    public int getSeqNum();
    public String getDestIP();
    public int getDestPort();
    public String getPayload();
    public MessageType getType();
    public byte[] serialize();
}

