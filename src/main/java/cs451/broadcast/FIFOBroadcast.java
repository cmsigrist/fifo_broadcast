package cs451.broadcast;

import java.io.IOException;
import java.net.SocketException;
import java.util.ArrayList;

import cs451.node.Host;
import cs451.types.AtomicArrayList;

public class FIFOBroadcast implements BroadcastInterface {
  private final byte pid;
  private final String srcIP;
  private final int srcPort;
  private final ArrayList<Host> peers;

  private final UniformReliableBroadcast uniformReliableBroadcast;

  private final AtomicArrayList<String> past;

  public FIFOBroadcast(byte pid, String srcIP, int srcPort, ArrayList<Host> peers) throws SocketException {
    this.pid = pid;
    this.srcIP = srcIP;
    this.srcPort = srcPort;
    this.peers = peers;

    this.past = new AtomicArrayList<>();

    try {
      this.uniformReliableBroadcast = new UniformReliableBroadcast(pid, srcIP, srcPort, peers);
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

  public UniformReliableBroadcast getReliableBroadcast() {
    return uniformReliableBroadcast;
  }

  @Override
  public void broadcast(String m) throws IOException {
    // TODO Auto-generated method stub
    uniformReliableBroadcast.broadcast(m);

  }

  @Override
  public void deliver() throws IOException {
    // TODO Auto-generated method stub
    uniformReliableBroadcast.deliver();

  }

  @Override
  public void waitForAck() throws IOException, InterruptedException {
    // TODO Auto-generated method stub
    uniformReliableBroadcast.waitForAck();

  }

  @Override
  public ArrayList<String> getLogs() {
    return uniformReliableBroadcast.getLogs();
  }
}
