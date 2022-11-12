package cs451.broadcast;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import cs451.link.PerfectLink;
import cs451.messages.Message;
import cs451.messages.MessageType;
import cs451.messages.Packet;
import cs451.node.Host;
import cs451.types.AtomicArrayList;
import cs451.types.AtomicMap;
import cs451.types.AtomicSet;
import cs451.utils.Keys;

public class FIFOBroadcast {
  protected final byte pid;
  protected final String srcIP;
  protected final int srcPort;
  protected final ArrayList<Host> peers;
  protected final PerfectLink p2pLink;

  protected final AtomicMap<Message, HashSet<String>> ackedMessage;
  protected final AtomicSet<Packet> forwarded;
  protected final HashSet<Integer> delivered;
  protected final AtomicArrayList<Message> past;

  protected final Queue<Packet> deliverQueue;

  protected int seqNum = 0;
  protected final AtomicArrayList<String> logs;

  public FIFOBroadcast(byte pid, String srcIP, int srcPort, ArrayList<Host> peers)
      throws IOException {
    this.pid = pid;
    this.srcIP = srcIP;
    this.srcPort = srcPort;
    this.peers = peers;

    ackedMessage = new AtomicMap<>();
    forwarded = new AtomicSet<>();
    delivered = new HashSet<>();
    past = new AtomicArrayList<>();

    deliverQueue = new ConcurrentLinkedQueue<>();

    this.p2pLink = new PerfectLink(pid, srcIP, srcPort, deliverQueue);
    logs = new AtomicArrayList<>();
  }

  public void broadcast(String m) throws IOException {
    seqNum += 1;
    Message message = new Message(pid, seqNum, srcIP, srcPort, m);
    Packet packet = new Packet(message, past.snapshot());

    System.out.println("Fifo broadcast seqNum " + seqNum);

    urbBroadcast(packet);
    // fifoBroadcast
    logs.add(message.broadcast());
    past.add(message);

    delivered.add(message.hashCode());
    logs.addIfNotInArray(message.delivered());

    String[] ackValues = { Keys.ackValues(srcIP, srcPort) };
    updateAck(message, ackValues);

    // HashSet<String> values = new HashSet<>();
    // values.add(Keys.ackValues(srcIP, srcPort));
    // ackedMessage.put(message, values);
  }

  public void urbBroadcast(Packet packet) throws IOException {
    forwarded.add(packet);

    bebBroadcast(packet);
  }

  public void bebBroadcast(Packet packet) throws IOException {
    packet.setRelayIP(srcIP);
    packet.setRelayPort(srcPort);

    for (Host peer : peers) {
      System.out.println("Beb broadcast packet seqNum: " +
          packet.getMessage().getSeqNum() + " to : " + peer.getPort());
      Packet p = new Packet(packet, peer.getIp(), peer.getPort());
      p2pLink.send(p);
    }
  }

  public void channelDeliver() throws IOException {
    try {
      p2pLink.channelDeliver();
    } catch (IOException e) {
      throw new IOException(e.getMessage());
    }
  }

  public void deliver(Message message) throws IOException {
    System.out.println("FIFO delivering packet: " + message.toString());

    delivered.add(message.hashCode());
    logs.addIfNotInArray(message.delivered());

    if (!ackedMessage.get(message).contains(Keys.ackValues(srcIP, srcPort))) {
      System.out.println("Fifo deliver broadcasting ack");
      // updateAck(message, srcIP, srcPort);

      Message ackMessage = new Message(pid, message.getSeqNum(), srcIP, srcPort);
      Packet packet = new Packet(ackMessage);

      urbBroadcast(packet);
    }
  }

  public void urbDeliver(Packet packet) throws IOException {
    Message message = packet.getMessage();

    System.out.println("Urb delivering packet: " + message.toString());

    if (!delivered.contains(message.hashCode()) && message.getType() == MessageType.CHAT_MESSAGE) {
      ArrayList<Message> past = packet.getPast();

      for (Message m : past) {
        if (!delivered.contains(m.hashCode())) {
          deliver(m);
          past.add(m);
        }
      }

      deliver(message);
      past.add(message);
    }

    if (message.getType() == MessageType.ACK_MESSAGE) {
      // HashSet<String> acks = updateAck(message, packet.getRelayIP(),
      // packet.getRelayPort());
      HashSet<String> acks = ackedMessage.get(message);
      // Garbage collect if majority received message
      if (acks.size() >= (1 + peers.size() / 2)) {
        past.remove(message);
      }
    }
  }

  public void bebDeliver(Packet packet) throws IOException {
    Message message = packet.getMessage();
    String[] ackValues = {
        Keys.ackValues(packet.getRelayIP(), packet.getRelayPort()),
        Keys.ackValues(message.getOriginIP(), message.getOriginPort()),
        Keys.ackValues(srcIP, srcPort)
    };

    // ackedMessage.copy().forEach((Message k, HashSet<String> v) -> {
    // System.out.printf(k + ": " + v);
    // });
    // System.out.println("\n");
    System.out.println("bebDeliver: " + packet.toString() + " from: " + packet.getRelayPort());
    System.out.println("bebDeliver forwarded: " + forwarded.snapshot());

    updateAck(message, ackValues);
    // HashSet<String> values = ackedMessage.get(message);

    // if (values == null) {
    // values = new HashSet<>();
    // }

    // for (String v : ackValues) {
    // values.add(v);
    // }

    // ackedMessage.put(message, values);

    System.out
        .println("bebDeliver ackedMessage for: " + message.toString() + " acks update: " + ackedMessage.get(message));

    if (!forwarded.contains(packet)) {
      // set yourself as the relay and forward (broadcast) once the message
      System.out.println("bebDeliver bebBroadcast packet: " +
          message.toString() + " from " + message.getOriginPort());
      forwarded.add(packet);
      bebBroadcast(packet);
    }
  }

  public void heartbeat() throws IOException, InterruptedException {
    ArrayList<Packet> f = new ArrayList<>(forwarded.snapshot());
    System.out.println("Heartbeat forwarded: " + f);

    for (Packet packet : f) {
      Message message = packet.getMessage();
      // if majority have acked m and m not delivered
      System.out.println("Heartbeat  " + message.toString() + " acks: " + ackedMessage.get(message) + " majority: "
          + (ackedMessage.get(message).size() >= (1 + peers.size() / 2)));

      if (ackedMessage.get(message).size() >= (1 + peers.size() / 2) && !delivered.contains(message.hashCode())) {
        System.out.println("urbDeliver packet: " + packet.getMessage());
        urbDeliver(packet);
      }
    }
  }

  public void waitForAck() throws IOException, InterruptedException {
    p2pLink.waitForAck();
  }

  protected HashSet<String> updateAck(Message message, String[] ackValues) {
    HashSet<String> acks;

    ackedMessage.lock.lock();

    try {
      acks = ackedMessage.nonAtomicGet(message);

      if (acks == null) {
        acks = new HashSet<>();
      }

      for (String ackValue : ackValues) {
        acks.add(ackValue);
      }

      ackedMessage.nonAtomicPut(message, acks);
    } finally {
      ackedMessage.lock.unlock();
    }

    return acks;
  }

  public ArrayList<String> getLogs() {
    return logs.nonAtomicSnapshot();
  }

  public Queue<Packet> getDeliverQueue() {
    return deliverQueue;
  }
}
