package cs451.node;

public interface NodeInterface {
    void start();

    void broadcastNewMessage(String payload);

    public void writeOutput();
}
