package cs451.broadcast;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import cs451.link.PerfectLink;
import cs451.messages.Message;
import cs451.messages.MessageType;
import cs451.node.Host;
import cs451.types.AtomicArrayList;
import cs451.types.AtomicMapOfSet;
import cs451.types.AtomicSet;
import cs451.types.PendingAck;

public class FIFOBroadcast {
  private final byte pid;
  private final int srcPort;
  private final ArrayList<Host> peers;
  private final PerfectLink p2pLink;

  private final AtomicMapOfSet<Integer, Integer> ackedMessage;
  private final AtomicMapOfSet<Message, PendingAck> pendingAcks;
  // private final AtomicMap forwarded;
  private final AtomicSet<Message> forwarded;
  private final AtomicSet<Integer> delivered;

  final Lock lock = new ReentrantLock();
  final Condition notFull = lock.newCondition();
  private final Queue<Message> buffer;
  private final Queue<Message> deliverQueue;

  private int seqNum = 0;
  private final AtomicArrayList<String> logs;

  private final int majority;
  // Up to BUFFER_THRESHOLD on the fly pending messages;
  // 100 * 5 * 5 = 2500 on the fly -> 74MiB
  // 2000 -> 74MiB
  // x * peer * peer = 1000
  private final int PENDING_THRESHOLD;

  public FIFOBroadcast(byte pid, String srcIP, int srcPort, ArrayList<Host> peers)
      throws IOException {
    this.pid = pid;
    this.srcPort = srcPort;
    this.peers = peers;

    ackedMessage = new AtomicMapOfSet<>();
    this.pendingAcks = new AtomicMapOfSet<>();
    forwarded = new AtomicSet<>();
    // forwarded = new AtomicMap();
    // forwarded.put(pid, 1);
    delivered = new AtomicSet<>();

    deliverQueue = new ConcurrentLinkedQueue<>();
    buffer = new ConcurrentLinkedQueue<>();

    this.p2pLink = new PerfectLink(pid, srcIP, srcPort, pendingAcks, deliverQueue, lock, notFull);
    logs = new AtomicArrayList<>();

    this.majority = 1 + (peers.size() / 2);
    // TODO tweak threshold to respect memory limitations
    this.PENDING_THRESHOLD = 10;
  }

  public synchronized void broadcast(String payload) throws IOException, InterruptedException {
    lock.lock();
    try {
      seqNum += 1;
      Message message = new Message(MessageType.CHAT_MESSAGE, pid, seqNum, srcPort);

      System.out.println("Fifo broadcast seqNum " + seqNum);
      buffer.offer(message);

      while (pendingAcks.size() > PENDING_THRESHOLD) {
        System.out.println("FIFO broadcast waiting");
        notFull.await();
      }

      buffer.poll();
      urbBroadcast(message);
      // fifoBroadcast
      logs.add(message.broadcast());

      ackedMessage.put(message.hashCode(), srcPort);

    } finally {
      lock.unlock();
    }
  }

  public void urbBroadcast(Message message) throws IOException {
    // forwarded.get(pid).setRange(message.getSeqNum());
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
    System.out.println("FIFO delivering packet pid: " + (pid + 1) + " seqNum: " + seqNum);

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
  }

  public void heartbeat() {
    ArrayList<Message> f = new ArrayList<>(forwarded.snapshot());
    System.out.println("Heartbeat forwarded: " + f);

    for (Message message : f) {
      // if majority have acked m and m not delivered
      // System.out
      // .println("Heartbeat " + message.toString() + " acks: "
      // + ackedMessage.get(message.hashCode()) + " majority: "
      // + (ackedMessage.size(message.hashCode()) >= majority));

      int numAcks = ackedMessage.size(message.hashCode());
      if (numAcks >= majority) {
        if (!delivered.contains(message.hashCode())) {
          System.out.println("Heartbeat urbDeliver packet: " + message.toString());
          urbDeliver(message);
        }

        if (numAcks == peers.size() + 1) {
          cleanUp(message);
        }
      }
    }

    // HashMap<Byte, Forwarded> forwardedSnapshot = new
    // HashMap<>(forwarded.snapshot());
    // System.out.println("Heartbeat forwarded: " + forwardedSnapshot);
    // for (Byte pid : forwardedSnapshot.keySet()) {
    // Forwarded f = forwardedSnapshot.get(pid);
    // ArrayList<Integer> seqNums = new ArrayList<>(f.getSeqNums());
    // int range = f.getRange();

    // for (int i = 1; i <= range; i++) {
    // seqNums.add(i);
    // }

    // for (int seqNum : seqNums) {
    // int numAcks = ackedMessage.size(Message.hashCode(pid, seqNum));
    // if (numAcks >= majority) {
    // if (!delivered.contains(Message.hashCode(pid, seqNum))) {
    // System.out.println("Heartbeat urbDeliver packet: { pid: " + pid + " seqNum: "
    // + seqNum + " }");
    // urbDeliver(pid, seqNum);
    // }

    // if (numAcks == peers.size() + 1) {
    // cleanUp(message);
    // }
    // }
    // }
    // }

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

      ackedMessage.put(message.hashCode(), ackValues);
    }

    // TODO check if need to do all the time ?
    // ackedMessage.put(message.hashCode(), ackValues);

    System.out.println("bebDeliver: " + message.toString());
    System.out
        .println("bebDeliver ackedMessage for: " + message.toString() + " acks update: "
            + ackedMessage.get(message.hashCode()));

    // Forward message with ACK
    // if (!forwarded.contains(message.getPid(), message.getSeqNum())) {
    if (!forwarded.contains(message)) {
      // set yourself as the relay and forward (broadcast) once the message
      forwarded.add(message);
      // forwarded.put(message.getPid(), message.getSeqNum());
      System.out.println("bebDeliver forwarded: " + forwarded.snapshot());
      message.setRelayMessage(MessageType.ACK_MESSAGE, srcPort, message.getRelayPort());

      ackedMessage.put(message.hashCode(), ackValues);
      bebBroadcast(message);
    }
  }

  public void waitForAck() throws IOException, InterruptedException {
    p2pLink.waitForAck();
  }

  private void cleanUp(Message message) {
    System.out.println("Cleaning up: " + message.toString());

    delivered.remove(message.hashCode());
    ackedMessage.remove(message.hashCode());
    pendingAcks.remove(message);

    int s = message.getSeqNum() - 1;
    boolean finished = false;
    Message m = new Message(pid, s);

    while (s > 0 && !finished) {
      // remove returns true if it hasn't been removed yet
      finished = !delivered.remove(m.hashCode());
      ackedMessage.remove(m.hashCode());
      pendingAcks.remove(m);

      s--;
      m.setSeqNum(s);
    }
  }

  public ArrayList<String> getLogs() {
    return logs.nonAtomicSnapshot();
  }

  public Queue<Message> getDeliverQueue() {
    return deliverQueue;
  }
}
