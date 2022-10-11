package cs451.link;

import java.io.IOException;
import java.util.ArrayList;

public interface LinkInterface {
    void send(String payload) throws IOException;
    void sendAll() throws IOException;
    void deliver() throws IOException;
    ArrayList<String> getLogs();
}
