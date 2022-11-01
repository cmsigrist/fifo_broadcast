package cs451.broadcast;

import java.io.IOException;
import java.net.SocketException;
import java.util.ArrayList;

import cs451.link.PerfectLink;
import cs451.messages.LightMessage;
import cs451.node.Host;

public class BestEffortBroadcast implements BroadcastInterface {
  private final byte pid;
  private final String srcIP;
  private final int srcPort;
  private final ArrayList<Host> peers;

  private final PerfectLink p2pLink;

  public BestEffortBroadcast(byte pid, String srcIP, int srcPort, ArrayList<Host> peers) throws SocketException {
    this.pid = pid;
    this.srcIP = srcIP;
    this.srcPort = srcPort;
    this.peers = peers;

    try {
      this.p2pLink = new PerfectLink(pid, srcIP, srcPort);
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

  public PerfectLink getP2PLink() {
    return p2pLink;
  }

  @Override
  public void broadcast(String m) throws IOException {
    for (Host peer : peers) {
      p2pLink.send(new LightMessage(m, peer.getIp(), peer.getPort()));
    }
  }

  @Override
  public void deliver() throws IOException {
    p2pLink.deliver();
  }

  @Override
  public void waitForAck() throws IOException, InterruptedException {
    p2pLink.waitForAck();
  }

  @Override
  public ArrayList<String> getLogs() {
    return p2pLink.getLogs();
  }
}
