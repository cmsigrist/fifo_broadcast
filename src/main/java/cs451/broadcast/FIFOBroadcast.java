package cs451.broadcast;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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

  final Lock lock = new ReentrantLock();
  final Condition notFull = lock.newCondition();
  private final Queue<Message> deliverQueue;

  private int seqNum = 0;
  private final Logs logs;

  private final int majority;

  private final int PENDING_THRESHOLD;

  public FIFOBroadcast(byte pid, String srcIP, int srcPort, ArrayList<Host> peers)
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

    this.p2pLink = new PerfectLink(pid, srcIP, srcPort, pendingAcks, deliverQueue, lock, notFull);
    logs = new Logs();

    this.majority = 1 + (peers.size() / 2);
    // Up to BUFFER_THRESHOLD on the fly pending messages;
    // 100 * 5 * 5 = 2500 on the fly -> 74MiB
    // 2000 -> 74MiB
    // x * peer * peer = 1000
    this.PENDING_THRESHOLD = 5; // 10 * 25 = 250 on the fly
  }

  public synchronized void broadcast(String payload) throws IOException, InterruptedException {
    lock.lock();
    try {
      seqNum += 1;
      Message message = new Message(MessageType.CHAT_MESSAGE, pid, seqNum);

      System.out.println("Fifo broadcast seqNum " + seqNum + " pendingAcks.size: " + pendingAcks.size());

      while (pendingAcks.size() > PENDING_THRESHOLD) {
        // System.out.println("FIFO broadcast waiting");
        notFull.await();
      }

      urbBroadcast(message);
      // fifoBroadcast
      logs.add(message.broadcast());

      ackedMessage.put(message.hashCode(), srcPort);

    } finally {
      lock.unlock();
    }
  }

  public void urbBroadcast(Message message) throws IOException {
    forwarded.get(pid).setRange(message.getSeqNum());
    bebBroadcast(message);
  }

  public void bebBroadcast(Message message) throws IOException {
    for (Host peer : peers) {
      message.setDestPort(peer.getPort());

      // System.out.println("Beb broadcast packet: " +
      // message.toString() + " to : " + peer.getPort());

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
    System.out.println("FIFO delivering message: " + "{pid: " + (pid + 1)
        + " seqNum: " + seqNum + "}");

    delivered.put(pid, seqNum);
    // System.err.println("FIFO delivered[" + (pid + 1) + "]: " +
    // delivered.get(pid));
    logs.addIfNotInArray(Message.delivered(pid, seqNum));

    // System.out.println("FIFO deliver finished");
  }

  public void urbDeliver(Message message) {
    // System.out.println("urbDeliver message: " + message.toString());
    // System.err.println("urbdeliver delivered[" + (message.getPid() + 1) + "]: " +
    // delivered.get(message.getPid()));

    if (!delivered.contains(message.getPid(), message.getSeqNum())) {
      int s = message.getSeqNum() - 1;
      byte pid = message.getPid();
      boolean d = false;
      // TODO bug
      while (s > 0 && !d) {
        // System.out.println("urbdeliver delivered message: " + "{ pid: " + (pid + 1)
        // + " seqNum: " + s + "}" + " delivered.contains: "
        // + delivered.contains(pid, s));
        if (delivered.contains(pid, s)) {
          d = true;
        } else {
          s--;
        }
      }

      s += 1; // last s was delivered, deliver from [s+1, seqNum]
      // System.out.println("urbDeliver s = " + s);
      while (s < message.getSeqNum()) {
        // System.out.println("urbDeliver past message: " + "{pid: " + (pid + 1)
        // + " seqNum: " + s + "}");
        deliver(pid, s);

        forwarded.put(pid, s);
        s++;
      }

      deliver(message.getPid(), message.getSeqNum());
    }
  }

  public void heartbeat() {
    HashMap<Byte, Forwarded> forwardedSnapshot = new HashMap<>(forwarded.snapshot());
    // System.out.println("Heartbeat forwarded: " + forwardedSnapshot);

    for (Byte pid : forwardedSnapshot.keySet()) {
      Forwarded f = new Forwarded(forwardedSnapshot.get(pid));
      ArrayList<Integer> seqNums = new ArrayList<>(f.getSeqNums());
      int range = f.getRange();

      // TODO bugged, can still cleanup ?
      // should it only retry for delivered up to ?
      // if (delivered.contains(pid, range)) {
      // continue;
      // }

      // int del = delivered.get(pid);
      // for (int i = del; i <= range; i++) {
      // seqNums.add(i);
      // }

      for (int i = 1; i <= range; i++) {
        seqNums.add(i);
      }

      for (int seqNum : seqNums) {
        int numAcks = ackedMessage.size(Message.hashCode(pid, seqNum));
        if (numAcks >= majority) {
          // Message message = new Message(pid, seqNum);
          // System.out.println("Heartbeat urbDeliver packet: "
          // + message.toString() + " numAcks: " + numAcks
          // + " delivered: " + delivered.contains(pid, seqNum));
          if (!delivered.contains(pid, seqNum)) {
            Message message = new Message(pid, seqNum);
            // System.out.println("Heartbeat urbDeliver packet: "
            // + message.toString() + " numAcks: " + numAcks
            // + " delivered: " + delivered.contains(pid, seqNum));
            urbDeliver(message);
          }

          if (numAcks == peers.size() + 1) {
            cleanUp(pid, seqNum);
          }
        }
      }
    }

    // System.out.println("Heartbeat finished check");
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
      // HashSet<Integer> acks = ackedMessage.get(message.hashCode());
      // Send ack again ?

      // if (acks != null && !acks.contains(message.getRelayPort())) {
      message.setRelayMessage(MessageType.ACK_MESSAGE, srcPort, message.getRelayPort());

      // System.out.println("beb deliver P2P sending ACK: " + message.toString()
      // + " to: " + message.getDestPort());
      p2pLink.P2PSend(message);

      ackedMessage.put(message.hashCode(), ackValues);
      // }
    }

    // Forward message with ACK (the first time receives this message)
    if (!forwarded.contains(message.getPid(), message.getSeqNum())) {
      // System.out.println("bebDeliver: " + message.toString());
      ackedMessage.put(message.hashCode(), ackValues);
      // System.out
      // .println("bebDeliver ackedMessage for: " + message.toString() + " acksupdate:
      // "
      // + ackedMessage.get(message.hashCode()));

      // set yourself as the relay and forward (broadcast) once the message
      forwarded.put(message.getPid(), message.getSeqNum());
      // var f = new Forwarded(forwarded.get(message.getPid()));
      // System.out.println("bebDeliver message: " + message.toString()
      // + " forwarded: " + f);
      message.setRelayMessage(MessageType.ACK_MESSAGE, srcPort, message.getRelayPort());

      bebBroadcast(message);
    }
  }

  public void waitForAck() throws IOException, InterruptedException {
    p2pLink.waitForAck();
  }

  private void cleanUp(byte pid, int seqNum) {
    Message message = new Message(pid, seqNum);
    // System.out.println("Cleaning up: " + message.toString());
    // boolean finished = false;

    // Don't remove last message from delivered keep the last message in the
    // history !

    /* finished = ! */ackedMessage.remove(message.hashCode());
    pendingAcks.remove(pid, seqNum);
    int range = delivered.get(pid);
    forwarded.cleanUp(pid, range);

    // seqNum--;
    // // TODO create new object ?
    // message.setSeqNum(seqNum);

    // while (seqNum > 0 && !finished) {
    // // remove returns true if it hasn't been removed yet
    // finished = !ackedMessage.remove(message.hashCode());
    // pendingAcks.remove(pid, seqNum);

    // seqNum--;
    // message.setSeqNum(seqNum);
    // }

    // var f = new Forwarded(forwarded.get(pid));
    // System.err.println("cleanUp forwarded: " + f);
  }

  public ArrayList<String> getLogs() {
    return logs.nonAtomicSnapshot();
  }

  public Queue<Message> getDeliverQueue() {
    return deliverQueue;
  }
}
