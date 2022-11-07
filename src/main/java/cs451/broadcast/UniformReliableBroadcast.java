package cs451.broadcast;

import java.io.IOException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashSet;

import cs451.node.Host;
import cs451.types.AtomicArrayList;

public class UniformReliableBroadcast implements BroadcastInterface {
  private final byte pid;
  private final String srcIP;
  private final int srcPort;
  private final ArrayList<Host> peers;
  private final HashSet<String> delivered;

  private final BestEffortBroadcast bebBroadcast;

  private final AtomicArrayList<String> forwarded;

  public UniformReliableBroadcast(byte pid, String srcIP, int srcPort, ArrayList<Host> peers) throws SocketException {
    this.pid = pid;
    this.srcIP = srcIP;
    this.srcPort = srcPort;
    this.peers = peers;
    this.delivered = new HashSet<>();
    this.forwarded = new AtomicArrayList<>();

    try {
      this.bebBroadcast = new BestEffortBroadcast(pid, srcIP, srcPort, peers);
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

  public BestEffortBroadcast getBebBroadcast() {
    return bebBroadcast;
  }

  @Override
  public void broadcast(String m) throws IOException {
    // TODO deliver message to the node
    // delivered.add(m);
    // deliver();
    bebBroadcast.broadcast(m);
  }

  @Override
  public void deliver() throws IOException {
    bebBroadcast.deliver();
  }

  @Override
  public void waitForAck() throws IOException, InterruptedException {
    // TODO Auto-generated method stub
    bebBroadcast.waitForAck();
  }

  @Override
  public ArrayList<String> getLogs() {
    return bebBroadcast.getLogs();
  }
}
