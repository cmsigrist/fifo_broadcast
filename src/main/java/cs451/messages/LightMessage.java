package cs451.messages;

import java.util.ArrayList;

public class LightMessage {
    private final int seqNum;
    private final String payload;
    private final String srcIP;
    private final int srcPort;
    private final ArrayList<String> past;

    public LightMessage(int seqNum, String payload, String srcIP, int srcPort, ArrayList<String> past) {
        this.seqNum = seqNum;
        this.payload = payload;
        this.srcIP = srcIP;
        this.srcPort = srcPort;
        this.past = past;
    }

    public int getSeqNum() {
        return seqNum;
    }

    public String getPayload() {
        return payload;
    }

    public String getSrcIP() {
        return srcIP;
    }

    public int getSrcPort() {
        return srcPort;
    }

    public ArrayList<String> getPast() {
        return past;
    }
}
