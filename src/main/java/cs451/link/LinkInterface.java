package cs451.link;

import java.io.IOException;
import java.util.ArrayList;

public interface LinkInterface {
    void send(String m, String destIp, int destPort, ArrayList<String> past) throws IOException;

    void deliver() throws IOException;

    ArrayList<String> getLogs();
}
