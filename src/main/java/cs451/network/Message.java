package cs451.network;

import java.io.Serializable;
import java.net.DatagramPacket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static cs451.network.UDPChannel.MAX_SIZE;

public class Message implements Serializable {
    // process that sent the message
    private final byte pid;
    private final int seqNum;
    private final String payload;

    private static final int pidIndex = 0;
    private static final int payloadIndex = 1;
    private static final int packetSizeOffset = 2;
    private static final int headerSize = 4;

    public Message(byte pid, int seqNum, String payload) {
        this.pid = pid;
        this.seqNum = seqNum;
        this.payload = payload;
    }

    public byte getPid() {
        return pid;
    }

    public int getSeqNum() {
        return seqNum;
    }

    public String getPayload() {
        return payload;
    }

    // packet[0] : pid (1 byte)
    // packet[1] : start of payload (1 byte)
    // packet[2-3] : packetSize (2 bytes)
    // packet[4 up to 11] : seqNum (up to 8 bytes)
    public byte[] serialize() {
        byte[] s = Integer.toHexString(seqNum).getBytes();
        byte[] p = payload.getBytes(StandardCharsets.UTF_8);

        short packetSize = (short) (headerSize + s.length + p.length);
        assert packetSize < MAX_SIZE;
        byte[] size = ByteBuffer.allocate(packetSizeOffset).putShort(packetSize).array();

        byte[] packet = new byte[packetSize];
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

        return new Message(packet[pidIndex], seqNum, payload);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof Message) {
            return ((Message) o).getPid() == pid &&
                    ((Message) o).getPayload().equals(payload) &&
                    ((Message) o).getSeqNum() == seqNum;
        }

        return false;
    }

    @Override
    public String toString() {
        return "SeqNum: " + seqNum + " pid: " + pid + " payload: " + payload;
    }

    public String delivered() {
        return "d " + pid + " " + seqNum + "\n";
    }

    public String broadcast() {
        return "b " + seqNum + "\n";
    }
}
