package cs451.broadcast;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
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
import cs451.types.PendingAck;

public class FIFOBroadcast {
  private final byte pid;
  private final String srcIP;
  private final int srcPort;
  private final ArrayList<Host> peers;
  private final PerfectLink p2pLink;

  private final AtomicMap<Message, String, String> ackedMessage;
  private final AtomicMap<Packet, String, PendingAck> pendingAcks;
  private final AtomicSet<Packet> forwarded;
  private final HashSet<Integer> delivered;
  private final AtomicArrayList<Message> past;

  // private final Queue<Packet> buffer;
  private final Queue<Packet> deliverQueue;

  private int seqNum = 0;
  private final AtomicArrayList<String> logs;

  private final int majority;

  public FIFOBroadcast(byte pid, String srcIP, int srcPort, ArrayList<Host> peers)
      throws IOException {
    this.pid = pid;
    this.srcIP = srcIP;
    this.srcPort = srcPort;
    this.peers = peers;

    ackedMessage = new AtomicMap<>();
    this.pendingAcks = new AtomicMap<>();
    forwarded = new AtomicSet<>();
    delivered = new HashSet<>();
    past = new AtomicArrayList<>();

    deliverQueue = new ConcurrentLinkedQueue<>();
    // buffer = new ConcurrentLinkedQueue<>();

    this.p2pLink = new PerfectLink(pid, srcIP, srcPort, pendingAcks, deliverQueue);
    logs = new AtomicArrayList<>();

    this.majority = 1 + (peers.size() / 2);
  }

  public void broadcast(String payload) throws IOException {
    // TODO buffer payloads
    seqNum += 1;
    Message message = new Message(pid, seqNum, payload, srcIP, srcPort);
    Packet packet = new Packet(message, past.snapshot());

    System.out.println("Fifo broadcast seqNum " + seqNum);

    urbBroadcast(packet);
    // fifoBroadcast
    logs.add(message.broadcast());
    past.add(message);

    String[] ackValues = { Packet.getKey(srcIP, srcPort) };
    ackedMessage.put(message, ackValues);

    // HashSet<String> values = new HashSet<>();
    // values.add(Keys.ackValues(srcIP, srcPort));
    // ackedMessage.put(message, values);
  }

  public void urbBroadcast(Packet packet) throws IOException {
    forwarded.add(packet);

    bebBroadcast(packet);
  }

  public void bebBroadcast(Packet packet) throws IOException {
    for (Host peer : peers) {
      packet.setRelayPacket(MessageType.CHAT_MESSAGE, srcIP, srcPort, peer.getIp(), peer.getPort());
      System.out.println("Beb broadcast packet: " +
          packet.toString() + " to : " + peer.getPort());
      p2pLink.send(packet);
    }
  }

  public void channelDeliver() throws IOException {
    try {
      p2pLink.channelDeliver();
    } catch (IOException e) {
      throw new IOException(e.getMessage());
    }
  }

  public void deliver(Message message) {
    System.out.println("FIFO delivering packet: " + message.toString());

    delivered.add(message.hashCode());
    logs.addIfNotInArray(message.delivered());

    // System.out.println("FIFO deliver finished");
  }

  public void urbDeliver(Packet packet) {
    Message message = packet.getMessage();

    System.out.println("Urb delivering packet: " + packet.toString());

    if (!delivered.contains(message.hashCode())) {
      ArrayList<Message> packetPast = packet.getPast();
      System.out.println("Urb deliver past: " + packetPast);

      for (Message m : packetPast) {
        if (!delivered.contains(m.hashCode())) {
          deliver(m);
        }
      }

      deliver(message);

      // Broadcast ack
      // packet.setRelayPacket(MessageType.ACK_MESSAGE, srcIP, srcPort,
      // packet.getRelayIP(), packet.getRelayPort());
      // try {
      // System.out.println("Urb deliver broadcasting ack");
      // urbBroadcast(packet);
      // } catch (IOException e) {
      // System.out.println("Urb deliver Error while broadcasting ack");
      // }
    }

    // Clean up the structure if everyone delivered the packet
    // TODO maybe change to majority have acked
    if (ackedMessage.get(message).size() == peers.size()) {
      System.out.println("Urb deliver cleaning up: " + packet.toString());
      forwarded.remove(packet);
      ackedMessage.remove(message);
      delivered.remove(message.hashCode());
      past.remove(message);
    }
  }

  public void heartbeat() {
    ArrayList<Packet> f = new ArrayList<>(forwarded.snapshot());
    System.out.println("Heartbeat forwarded: " + f);

    for (Packet packet : f) {
      Message message = packet.getMessage();
      // if majority have acked m and m not delivered
      System.out.println("Heartbeat  " + packet.toString() + " acks: " + ackedMessage.get(message) + " majority: "
          + (ackedMessage.get(message).size() >= majority));

      if (ackedMessage.get(message).size() >= majority && !delivered.contains(message.hashCode())) {
        System.out.println("Heartbeat urbDeliver packet: " + packet.toString());
        urbDeliver(packet);
      }
    }

    System.out.println("Heartbeat finished check");
  }

  public void bebDeliver(Packet packet) throws IOException {
    Message message = packet.getMessage();
    String packetKey = packet.getKey();

    // Send ack if first time received an ack for the packet from the source
    if (packet.getType() == MessageType.ACK_MESSAGE) {
      HashMap<String, String> acks = ackedMessage.get(message);

      if (acks != null && !acks.containsKey(packetKey)) {
        packet.setRelayPacket(MessageType.ACK_MESSAGE, srcIP, srcPort, packet.getRelayIP(), packet.getRelayPort());
        System.out.println("beb deliver P2P sending ACK: " + packet.toString() + " to: " + packet.getDestPort());
        p2pLink.P2PSend(packet);
      }
    }

    String[] ackValues = {
        packetKey,
        Packet.getKey(message.getOriginIP(), message.getOriginPort()),
        Packet.getKey(srcIP, srcPort)
    };

    ackedMessage.put(message, ackValues);

    System.out.println("bebDeliver: " + packet.toString());
    System.out
        .println("bebDeliver ackedMessage for: " + packet.toString() + " acks update: " + ackedMessage.get(message));

    // Forward message with ACK
    if (!forwarded.contains(packet)) {
      // set yourself as the relay and forward (broadcast) once the message
      forwarded.add(packet);
      System.out.println("bebDeliver forwarded: " + forwarded.snapshot());
      packet.setRelayPacket(MessageType.ACK_MESSAGE, srcIP, srcPort, packet.getRelayIP(), packet.getRelayPort());

      bebBroadcast(packet);
    }
  }

  public void waitForAck() throws IOException, InterruptedException {
    p2pLink.waitForAck();
  }

  public ArrayList<String> getLogs() {
    return logs.nonAtomicSnapshot();
  }

  public Queue<Packet> getDeliverQueue() {
    return deliverQueue;
  }
}
