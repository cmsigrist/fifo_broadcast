package cs451.network;

import java.io.IOException;
import java.net.DatagramPacket;

public interface ChannelInterface {
  public void send(DatagramPacket d) throws IOException;

  public DatagramPacket receive() throws IOException;

}
