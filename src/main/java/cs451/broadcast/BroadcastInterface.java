package cs451.broadcast;

import java.io.IOException;
import java.util.ArrayList;

public interface BroadcastInterface {
  // void broadcast(String m, String past) throws IOException;

  // void broadcast(String m) throws IOException;

  void deliver() throws IOException;

  void waitForAck() throws IOException, InterruptedException;

  ArrayList<String> getLogs();
}
