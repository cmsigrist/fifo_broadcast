package cs451.messages;

import java.net.DatagramPacket;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.StringJoiner;

public class Message {
    // process that sent the message
    private final byte pid;
    private final int seqNum;
    private final int originPort;

    private byte type;
    private int relayPort;
    private int destPort;

    public Message(byte type, byte pid, int seqNum, int originPort) {
        this.type = type;
        this.pid = pid;
        this.seqNum = seqNum;
        this.originPort = originPort;
    }

    public byte getType() {
        return type;
    }

    public byte getPid() {
        return pid;
    }

    public int getSeqNum() {
        return seqNum;
    }

    public int getOriginPort() {
        return originPort;
    }

    public int getRelayPort() {
        return relayPort;
    }

    public int getDestPort() {
        return destPort;
    }

    public void setType(byte type) {
        this.type = type;
    }

    public void setRelayPort(int relayPort) {
        this.relayPort = relayPort;
    }

    public void setDestPort(int destPort) {
        this.destPort = destPort;
    }

    public void setRelayMessage(byte type, int relayPort, int destPort) {
        this.type = type;
        this.relayPort = relayPort;
        this.destPort = destPort;
    }

    public byte[] marshall() {
        StringJoiner stringJoiner = new StringJoiner(":");

        stringJoiner
                .add(Byte.toString(type))
                .add(Byte.toString(pid))
                .add(Integer.toString(seqNum))
                .add(Integer.toString(originPort));
        byte[] payload = stringJoiner.toString().getBytes();
        short packetLength = (short) payload.length;

        byte[] size = ByteBuffer.allocate(2).putShort(packetLength).array();

        byte[] packet = new byte[packetLength + 2];
        packet[0] = size[0];
        packet[1] = size[1];

        System.arraycopy(payload, 0, packet, 2, packetLength);

        return packet;
    }

    public static Message unmarshall(DatagramPacket d) {
        byte[] packet = d.getData();
        short packetLength = ByteBuffer.wrap(packet, 0, 2).getShort();

        String payload = new String(Arrays.copyOfRange(packet, 2, packetLength + 2));
        String[] fields = payload.split(":");

        byte type = Byte.valueOf(fields[0]);
        byte pid = Byte.valueOf(fields[1]);
        int seqNum = Integer.parseInt(fields[2]);
        int originPort = Integer.parseInt(fields[3]);

        Message message = new Message(type, pid, seqNum, originPort);
        message.setRelayPort(d.getPort());

        return message;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder
                .append(" pid: ")
                .append(Integer.valueOf(pid + 1).toString())
                .append(" seqNum: ")
                .append(seqNum);

        return "{" + type + " : pid: " + Integer.valueOf(pid + 1).toString() + " seqNum: " + seqNum + " from: "
                + relayPort + "}";
    }

    public String delivered() {
        return "d " + Integer.valueOf(pid + 1).toString() + " " + seqNum + "\n";
    }

    public static String delivered(byte pid, int seqNum) {
        return "d " + Integer.valueOf(pid + 1).toString() + " " + seqNum + "\n";
    }

    public String broadcast() {
        return "b " + seqNum + "\n";
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof Message) {
            return ((Message) o).getPid() == pid &&
                    ((Message) o).getSeqNum() == seqNum;
        }

        return false;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + pid;
        result = prime * result + seqNum;
        return result;
    }

    public static int hashCode(byte pid, int seqNum) {
        final int prime = 31;
        int result = 1;
        result = prime * result + pid;
        result = prime * result + seqNum;
        return result;
    }
}
