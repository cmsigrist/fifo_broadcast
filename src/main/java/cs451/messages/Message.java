package cs451.messages;

import java.util.StringJoiner;

public class Message {
    // process that sent the message
    private final byte pid;
    private final int seqNum;
    private final String payload;
    private final MessageType type;
    private final String originIP;
    private final int originPort;

    public static String ACK_PAYLOAD = "";

    // Constructor for Messages of type type
    public Message(byte pid, int seqNum, String originIP, int originPort, String payload, MessageType type) {
        this.pid = pid;
        this.seqNum = seqNum;
        this.originIP = originIP;
        this.originPort = originPort;
        this.payload = payload;
        this.type = type;
    }

    // Constructor for ChatMessage
    public Message(byte pid, int seqNum, String originIP, int originPort, String payload) {
        this.pid = pid;
        this.seqNum = seqNum;
        this.originIP = originIP;
        this.originPort = originPort;
        this.payload = payload;
        this.type = MessageType.CHAT_MESSAGE;
    }

    // Constructor for AckMessage
    public Message(byte pid, int seqNum, String originIP, int originPort) {
        this.pid = pid;
        this.seqNum = seqNum;
        this.originIP = originIP;
        this.originPort = originPort;
        this.payload = ACK_PAYLOAD;
        this.type = MessageType.ACK_MESSAGE;
    }

    public byte getPid() {
        return pid;
    }

    public int getSeqNum() {
        return seqNum;
    }

    public String getOriginIP() {
        return originIP;
    }

    public int getOriginPort() {
        return originPort;
    }

    public String getPayload() {
        return payload;
    }

    public MessageType getType() {
        return type;
    }

    public String marshall() {
        StringJoiner stringJoiner = new StringJoiner(":");

        stringJoiner
                .add(Integer.toString(type.ordinal()))
                .add(Byte.toString(pid))
                .add(Integer.toString(seqNum))
                .add(originIP)
                .add(Integer.toString(originPort))
                .add(payload);

        return stringJoiner.toString();
    }

    public static Message unmarshall(String m) {
        String[] fields = m.split(":");

        MessageType messageType = MessageType.values()[Integer.parseInt(fields[0])];
        byte pid = Byte.valueOf(fields[1]);
        int seqNum = Integer.parseInt(fields[2]);
        String originIP = fields[3];
        int originPort = Integer.parseInt(fields[4]);
        String payload = Message.ACK_PAYLOAD;

        if (messageType == MessageType.CHAT_MESSAGE) {
            payload = fields[5];
        }

        return new Message(pid, seqNum, originIP, originPort, payload, messageType);
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder
                .append(type)
                .append(" pid: ")
                .append(Integer.valueOf(pid + 1).toString())
                .append(" seqNum: ")
                .append(seqNum);
        // .append(" payload: ")
        // .append(payload)
        // .append(" originIP: ")
        // .append(originIP)
        // .append(" originPort: ")
        // .append(originPort);
        return stringBuilder.toString();
    }

    public String delivered() {
        return "d " + Integer.valueOf(pid + 1).toString() + " " + seqNum + "\n";
    }

    public String broadcast() {
        return "b " + seqNum + "\n";
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof Message) {
            return ((Message) o).getPid() == pid &&
                    ((Message) o).getSeqNum() == seqNum &&
                    ((Message) o).getPayload().equals(payload) &&
                    ((Message) o).getOriginIP().equals(originIP) &&
                    ((Message) o).getOriginPort() == originPort;
        }

        return false;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + pid;
        result = prime * result + seqNum;
        result = prime * result + ((payload == null) ? 0 : payload.hashCode());
        result = prime * result + ((type == null) ? 0 : type.hashCode());
        result = prime * result + ((originIP == null) ? 0 : originIP.hashCode());
        result = prime * result + originPort;
        return result;
    }

}
