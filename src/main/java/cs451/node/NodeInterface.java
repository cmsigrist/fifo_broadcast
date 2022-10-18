package cs451.node;

public interface NodeInterface {
    void start();
    void sendNewMessage(String payload, String destIP, int destPort);
    public void writeOutput();
}
