package cs451.link;

import cs451.messages.LightMessage;

import java.io.IOException;
import java.util.ArrayList;

public interface LinkInterface {
    void send(LightMessage m) throws IOException;
    void deliver() throws IOException;
    ArrayList<String> getLogs();
}
