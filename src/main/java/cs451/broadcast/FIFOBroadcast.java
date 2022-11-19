package cs451.broadcast;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import cs451.link.PerfectLink;
import cs451.messages.Message;
import cs451.messages.MessageType;
import cs451.node.Host;
import cs451.types.AtomicArrayList;
import cs451.types.AtomicMap;
import cs451.types.AtomicSet;
import cs451.types.PendingAck;

public class FIFOBroadcast {
  private final byte pid;
  private final int srcPort;
  private final ArrayList<Host> peers;
  private final PerfectLink p2pLink;

  private final AtomicMap<Integer, Integer> ackedMessage;
  private final AtomicMap<Message, PendingAck> pendingAcks;
  private final AtomicSet<Message> forwarded;
  private final HashSet<Integer> delivered;

  // private final Queue<Message> buffer;
  private final Queue<Message> deliverQueue;

  private int seqNum = 0;
  private final AtomicArrayList<String> logs;

  private final int majority;
  // private final int BUFFER_THRESHOLD;

  public FIFOBroadcast(byte pid, String srcIP, int srcPort, ArrayList<Host> peers)
      throws IOException {
    this.pid = pid;
    this.srcPort = srcPort;
    this.peers = peers;

    ackedMessage = new AtomicMap<>();
    this.pendingAcks = new AtomicMap<>();
    forwarded = new AtomicSet<>();
    delivered = new HashSet<>();

    deliverQueue = new ConcurrentLinkedQueue<>();
    // buffer = new ConcurrentLinkedQueue<>();

    this.p2pLink = new PerfectLink(pid, srcIP, srcPort, pendingAcks, deliverQueue);
    logs = new AtomicArrayList<>();

    this.majority = 1 + (peers.size() / 2);
    // TODO tweak threshold to respect memory limitations
    // this.BUFFER_THRESHOLD = peers.size() * 10;
  }

  public void broadcast(String payload) throws IOException {
    // TODO buffer payloads
    seqNum += 1;
    Message message = new Message(MessageType.CHAT_MESSAGE, pid, seqNum, srcPort);

    System.out.println("Fifo broadcast seqNum " + seqNum);

    urbBroadcast(message);
    // fifoBroadcast
    logs.add(message.broadcast());

    ackedMessage.put(message.hashCode(), srcPort);
  }

  public void urbBroadcast(Message message) throws IOException {
    forwarded.add(message);

    bebBroadcast(message);
  }

  public void bebBroadcast(Message message) throws IOException {
    for (Host peer : peers) {
      message.setDestPort(peer.getPort());

      System.out.println("Beb broadcast packet: " +
          message.toString() + " to : " + peer.getPort());

      p2pLink.send(message);
    }
  }

  public void channelDeliver() throws IOException {
    try {
      p2pLink.channelDeliver();
    } catch (IOException e) {
      throw new IOException(e.getMessage());
    }
  }

  public void deliver(byte pid, int seqNum) {
    System.out.println("FIFO delivering packet pid: " + pid + " seqNum: " + seqNum);

    delivered.add(Message.hashCode(pid, seqNum));
    logs.addIfNotInArray(Message.delivered(pid, seqNum));

    // System.out.println("FIFO deliver finished");
  }

  public void urbDeliver(Message message) {
    System.out.println("Urb delivering packet: " + message.toString());

    if (!delivered.contains(message.hashCode())) {
      int seqNum = message.getSeqNum();
      int s = seqNum;
      boolean d = false;
      byte pid = message.getPid();

      while (s > 1 && !d) {
        s--;
        if (delivered.contains(Message.hashCode(pid, s))) {
          d = true;
        }
      }

      while (s < seqNum) {
        deliver(pid, s);
        s++;
      }

      deliver(message.getPid(), message.getSeqNum());
    }

    // Clean up the structure if everyone delivered the packet
    int numAcks = ackedMessage.get(message.hashCode()).size();
    if (numAcks == peers.size()) {
      cleanUp(message);
    }
  }

  public void heartbeat() {
    ArrayList<Message> f = new ArrayList<>(forwarded.snapshot());
    System.out.println("Heartbeat forwarded: " + f);

    for (Message message : f) {
      // if majority have acked m and m not delivered
      System.out
          .println("Heartbeat  " + message.toString() + " acks: "
              + ackedMessage.get(message.hashCode()) + " majority: "
              + (ackedMessage.get(message.hashCode()).size() >= majority));

      if (ackedMessage.get(message.hashCode()).size() >= majority &&
          !delivered.contains(message.hashCode())) {
        System.out.println("Heartbeat urbDeliver packet: " + message.toString());
        urbDeliver(message);
      }
    }

    System.out.println("Heartbeat finished check");
  }

  public void bebDeliver(Message message) throws IOException {
    // Send ack if first time received an ack for the packet from the source
    Integer[] ackValues = {
        message.getRelayPort(),
        message.getOriginPort(),
        srcPort
    };

    if (message.getType() == MessageType.ACK_MESSAGE) {
      HashSet<Integer> acks = ackedMessage.get(message.hashCode());

      if (acks != null && !acks.contains(message.getRelayPort())) {
        message.setRelayMessage(MessageType.ACK_MESSAGE, srcPort, message.getRelayPort());

        System.out.println("beb deliver P2P sending ACK: " + message.toString()
            + " to: " + message.getDestPort());
        p2pLink.P2PSend(message);
      }
    }

    // TODO check if need to do all the time ?
    ackedMessage.put(message.hashCode(), ackValues);

    System.out.println("bebDeliver: " + message.toString());
    System.out
        .println("bebDeliver ackedMessage for: " + message.toString() + " acks update: "
            + ackedMessage.get(message.hashCode()));

    // Forward message with ACK
    if (!forwarded.contains(message)) {
      // set yourself as the relay and forward (broadcast) once the message
      // TODO don't put if already delivered ?
      forwarded.add(message);
      System.out.println("bebDeliver forwarded: " + forwarded.snapshot());
      message.setRelayMessage(MessageType.ACK_MESSAGE, srcPort, message.getRelayPort());

      bebBroadcast(message);
      // Make sure heartbeat is run sometimes (if the node is flooded with
      // deliver, it sometimes never context switch to heartbeatThread)
      heartbeat();
    }
  }

  public void waitForAck() throws IOException, InterruptedException {
    p2pLink.waitForAck();
  }

  // TODO clean up seqNum 1 -> n
  private void cleanUp(Message message) {
    System.out.println("Urb deliver cleaning up delivered: " + message.toString());

    delivered.remove(message.hashCode());
    forwarded.remove(message);
    ackedMessage.remove(message.hashCode());
    pendingAcks.remove(message);
  }

  public ArrayList<String> getLogs() {
    return logs.nonAtomicSnapshot();
  }

  public Queue<Message> getDeliverQueue() {
    return deliverQueue;
  }
}
