package cs451.messages;

import java.net.DatagramPacket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static cs451.network.UDPChannel.MAX_SIZE;

public class Message implements MessageInterface{
    // process that sent the message
    private final byte pid;
    private final int seqNum;
    private final String destIP;
    private final int destPort;
    private final String payload;
    private final MessageType type;
    public static final int typeIndex = 0;
    public static final int pidIndex = 1;
    public static final int payloadIndex = 2;
    public static final int packetSizeOffset = 3;
    public static final int headerSize = 5;

    public Message(byte pid, int seqNum, String destIP, int destPort, String payload, MessageType type) {
        this.pid = pid;
        this.seqNum = seqNum;
        this.destIP = destIP;
        this.destPort = destPort;
        this.payload = payload;
        this.type = type;
    }
    public Message(byte pid, int seqNum, String destIP, int destPort, String payload) {
        this.pid = pid;
        this.seqNum = seqNum;
        this.destIP = destIP;
        this.destPort = destPort;
        this.payload = payload;
        this.type = MessageType.CHAT_MESSAGE;
    }

    public Message(byte pid, int seqNum, String destIP, int destPort) {
        this.pid = pid;
        this.seqNum = seqNum;
        this.destIP = destIP;
        this.destPort = destPort;
        this.payload = "";
        this.type = MessageType.ACK_MESSAGE;
    }

    @Override
    public byte getPid() {
        return pid;
    }

    @Override
    public int getSeqNum() {
        return seqNum;
    }

    public String getDestIP() {
        return destIP;
    }

    public int getDestPort() {
        return destPort;
    }

    public String getPayload() {
        return payload;
    }

    public MessageType getType() {
        return type;
    }

    // packet[0] : type (1 byte)
    // packet[1] : pid (1 byte)
    // packet[2] : start of payload (1 byte)
    // packet[3-4] : packetSize (2 byte)
    // packet[5 up to 12] : seqNum (up to 8 bytes)
    public byte[] serialize() {
        byte[] s = Integer.toHexString(seqNum).getBytes();
        byte[] p = payload.getBytes(StandardCharsets.UTF_8);

        short packetSize = (short) (headerSize + s.length + p.length);
        assert packetSize < MAX_SIZE;
        byte[] size = ByteBuffer.allocate(packetSizeOffset).putShort(packetSize).array();

        byte[] packet = new byte[packetSize];
        packet[typeIndex] = (byte) type.ordinal();
        packet[pidIndex] = pid;
        packet[payloadIndex] = (byte) (headerSize + s.length);
        packet[packetSizeOffset] = size[0];
        packet[packetSizeOffset + 1] = size[1];

        System.arraycopy(s, 0, packet, headerSize, s.length);
        System.arraycopy(p, 0, packet, headerSize + s.length, p.length);

        return packet;
    }

    public static Message deSerialize(DatagramPacket d) {
        byte[] packet = d.getData();

        int startPayload = packet[payloadIndex];
        short packetSize = ByteBuffer.wrap(packet, packetSizeOffset, packetSizeOffset).getShort();

        int seqNum = Integer.parseUnsignedInt(
                new String(
                        Arrays.copyOfRange(packet, headerSize, startPayload)),
                16);

        String payload = new String(Arrays.copyOfRange(packet, startPayload, packetSize));

        MessageType type = MessageType.values()[packet[typeIndex]];

        return new Message(packet[pidIndex], seqNum, d.getAddress().toString(), d.getPort(), payload, type);
    }

    public boolean equals(Object o) {
        if (o instanceof Message) {
            return ((Message) o).getPid() == pid &&
                    ((Message) o).getSeqNum() == seqNum &&
                     ((Message) o).getPayload().equals(payload);
        }

        return false;
    }

    public String toString() {
        return "pid: " + pid + " seqNum: " + seqNum + "\n";
    }

    public String delivered() {
        return "d " + this.getPid() + " " + this.getSeqNum() + "\n";
    }

    public String broadcast() {
        return "b " + this.getSeqNum() + "\n";
    }
}
