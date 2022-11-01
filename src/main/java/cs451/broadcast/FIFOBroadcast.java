package cs451.broadcast;

import java.io.IOException;
import java.net.SocketException;
import java.util.ArrayList;

import cs451.node.Host;

public class FIFOBroadcast implements BroadcastInterface {
  private final byte pid;
  private final String srcIP;
  private final int srcPort;
  private final ArrayList<Host> peers;

  private final ReliableBroadcast reliableBroadcast;

  public FIFOBroadcast(byte pid, String srcIP, int srcPort, ArrayList<Host> peers) throws SocketException {
    this.pid = pid;
    this.srcIP = srcIP;
    this.srcPort = srcPort;
    this.peers = peers;

    try {
      this.reliableBroadcast = new ReliableBroadcast(pid, srcIP, srcPort, peers);
    } catch (SocketException e) {
      throw new SocketException("Error while creating node: " + e.getMessage());
    }
  }

  public byte getPid() {
    return pid;
  }

  public String getSrcIP() {
    return srcIP;
  }

  public int getSrcPort() {
    return srcPort;
  }

  public ArrayList<Host> getPeers() {
    return peers;
  }

  public ReliableBroadcast getReliableBroadcast() {
    return reliableBroadcast;
  }

  @Override
  public void broadcast(String m) throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void deliver() throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void waitForAck() throws IOException, InterruptedException {
    // TODO Auto-generated method stub

  }

  @Override
  public ArrayList<String> getLogs() {
    return reliableBroadcast.getLogs();
  }
}
