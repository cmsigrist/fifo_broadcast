package cs451.node;

public interface NodeInterface {
    void start();
    void sendNewMessage(String payload);
    public void writeOutput();
}
