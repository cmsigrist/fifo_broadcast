package cs451.messages;

public class LightMessage {
    private final String payload;
    private final String destIP;
    private final int destPort;

    public LightMessage(String payload, String destIP, int destPort) {
        this.payload = payload;
        this.destIP = destIP;
        this.destPort = destPort;
    }

    public String getPayload() {
        return payload;
    }

    public String getDestIP() {
        return destIP;
    }

    public int getDestPort() {
        return destPort;
    }
}
