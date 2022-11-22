package cs451.broadcast;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import cs451.link.PerfectLink;
import cs451.messages.Message;
import cs451.messages.MessageType;
import cs451.node.Host;
import cs451.types.AckMap;
import cs451.types.DeliveredMap;
import cs451.types.Forwarded;
import cs451.types.ForwardedMap;
import cs451.types.Logs;
import cs451.types.PendingMap;

public class FIFOBroadcast {
  private final byte pid;
  private final int srcPort;
  private final ArrayList<Host> peers;
  private final PerfectLink p2pLink;

  private final AckMap ackedMessage;
  private final PendingMap pendingAcks;
  private final ForwardedMap forwarded;
  private final DeliveredMap delivered;

  private final Queue<Message> deliverQueue;

  private final Logs logs;

  private final int majority;

  private int seqNum = 0;
  private final AtomicInteger toSend;
  private int pendingMessage = 0;
  private final int MAX_PENDING;

  public FIFOBroadcast(byte pid, String srcIP, int srcPort, ArrayList<Host> peers, AtomicInteger toSend)
      throws IOException {
    this.pid = pid;
    this.srcPort = srcPort;
    this.peers = peers;

    ackedMessage = new AckMap();
    this.pendingAcks = new PendingMap();
    forwarded = new ForwardedMap();
    forwarded.put(pid, 1);
    delivered = new DeliveredMap();

    deliverQueue = new ConcurrentLinkedQueue<>();

    this.p2pLink = new PerfectLink(pid, srcIP, srcPort, pendingAcks, deliverQueue);
    logs = new Logs();

    this.majority = 1 + (peers.size() / 2);
    this.toSend = toSend;

    int inFlightSize = peers.size() * peers.size();
    MAX_PENDING = inFlightSize > 100 ? 1 : (100 / inFlightSize);
    System.out.println("MAX_PENDING: " + MAX_PENDING);
  }

  public void broadcast() throws IOException, InterruptedException {
    // No new message to send
    if (toSend.get() <= 0) {
      return;
    }

    int del = delivered.get(pid);
    toSend.decrementAndGet();
    seqNum += 1;

    Message message = new Message(MessageType.CHAT_MESSAGE, pid, seqNum);

    // System.out.println("Fifo broadcast seqNum " + seqNum);

    urbBroadcast(message);
    // fifoBroadcast
    logs.add(message.broadcast());
    ackedMessage.put(message.hashCode(), srcPort);

    pendingMessage += 1;

    // Wait if MAX_PENDING messages are in flight and half have not been delivered
    if (pendingMessage == MAX_PENDING) {
      pendingMessage = 0;

      int numTry = MAX_PENDING - 1;

      while ((pendingAcks.size() > MAX_PENDING || delivered.get(pid) < del + MAX_PENDING) && numTry > 0) {
        Thread.sleep(500);
        numTry--;
      }
    }
  }

  public void urbBroadcast(Message message) throws IOException {
    forwarded.put(pid, message.getSeqNum());
    bebBroadcast(message);
  }

  public void bebBroadcast(Message message) throws IOException {
    for (Host peer : peers) {
      message.setDestPort(peer.getPort());
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
    // System.out.println("FIFO delivering message: " + "{pid: " + (pid + 1)
    // + " seqNum: " + seqNum + "}");

    delivered.put(pid, seqNum);
    logs.addIfNotInArray(Message.delivered(pid, seqNum));
  }

  public void urbDeliver(Message message) {
    if (!delivered.contains(message.getPid(), message.getSeqNum())) {
      int s = message.getSeqNum() - 1;
      byte pid = message.getPid();
      boolean d = false;

      while (s > 0 && !d) {
        if (delivered.contains(pid, s)) {
          d = true;
        } else {
          s--;
        }
      }

      s += 1; // last s was delivered, deliver from [s+1, seqNum]
      while (s < message.getSeqNum()) {
        deliver(pid, s);
        // Might not have seen it, but put in anyway
        forwarded.put(pid, s);
        s++;
      }

      deliver(message.getPid(), message.getSeqNum());
    }
  }

  public void heartbeat() {
    HashMap<Byte, Forwarded> forwardedSnapshot = new HashMap<>(forwarded.snapshot());

    for (Byte pid : forwardedSnapshot.keySet()) {
      Forwarded f = new Forwarded(forwardedSnapshot.get(pid));
      ArrayList<Integer> seqNums = new ArrayList<>(f.getSeqNums());
      int range = f.getRange();

      // Need to iterate over all to know eventually clean up
      for (int i = 1; i <= range; i++) {
        seqNums.add(i);
      }

      for (int seqNum : seqNums) {
        int numAcks = ackedMessage.size(Message.hashCode(pid, seqNum));

        if (numAcks >= majority) {
          if (!delivered.contains(pid, seqNum)) {
            Message message = new Message(pid, seqNum);
            urbDeliver(message);
          }

          if (numAcks == peers.size() + 1) {
            cleanUp(pid, seqNum);
          }
        }
      }
    }
  }

  public void bebDeliver(Message message) throws IOException {
    // Send ack if first time received an ack for the packet from the source
    int[] ackValues = {
        message.getRelayPort(),
        srcPort
    };

    // In response to broadcast ack / or new message
    // should receive acks from everyone
    if (message.getType() == MessageType.ACK_MESSAGE) {
      message.setRelayMessage(MessageType.ACK_MESSAGE, srcPort, message.getRelayPort());
      p2pLink.P2PSend(message);
      ackedMessage.put(message.hashCode(), ackValues);
    }

    // Forward message with ACK (the first time receives this message)
    if (!forwarded.contains(message.getPid(), message.getSeqNum())) {
      ackedMessage.put(message.hashCode(), ackValues);
      // set yourself as the relay and forward (broadcast) once the message
      forwarded.put(message.getPid(), message.getSeqNum());
      message.setRelayMessage(MessageType.ACK_MESSAGE, srcPort, message.getRelayPort());
      bebBroadcast(message);
    }
  }

  public void waitForAck() throws IOException, InterruptedException {
    p2pLink.waitForAck();
  }

  private void cleanUp(byte pid, int seqNum) {
    Message message = new Message(pid, seqNum);
    ackedMessage.remove(message.hashCode());
    pendingAcks.remove(pid, seqNum);
    int range = delivered.get(pid);
    forwarded.cleanUp(pid, range);
  }

  public ArrayList<String> getLogs() {
    return logs.nonAtomicSnapshot();
  }

  public Queue<Message> getDeliverQueue() {
    return deliverQueue;
  }
}
