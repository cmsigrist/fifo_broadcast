package cs451.messages;

import java.util.StringJoiner;

public class Message {
    // process that sent the message
    private final byte pid;
    private final int seqNum;
    private final String payload;
    private final String originIP;
    private final int originPort;

    public static String ACK_PAYLOAD = "";

    public Message(byte pid, int seqNum, String payload, String originIP, int originPort) {
        this.pid = pid;
        this.seqNum = seqNum;
        this.payload = payload;
        this.originIP = originIP;
        this.originPort = originPort;
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

    public String getOriginIP() {
        return originIP;
    }

    public int getOriginPort() {
        return originPort;
    }

    public String marshall() {
        StringJoiner stringJoiner = new StringJoiner(":");

        stringJoiner
                .add(Byte.toString(pid))
                .add(Integer.toString(seqNum))
                .add(payload)
                .add(originIP)
                .add(Integer.toString(originPort));

        return stringJoiner.toString();
    }

    public static Message unmarshall(String m) {
        String[] fields = m.split(":");

        byte pid = Byte.valueOf(fields[0]);
        int seqNum = Integer.parseInt(fields[1]);
        String payload = fields[2];
        String originIP = fields[3];
        int originPort = Integer.parseInt(fields[4]);

        return new Message(pid, seqNum, payload, originIP, originPort);
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder
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
        result = prime * result + ((originIP == null) ? 0 : originIP.hashCode());
        result = prime * result + originPort;
        return result;
    }

}
