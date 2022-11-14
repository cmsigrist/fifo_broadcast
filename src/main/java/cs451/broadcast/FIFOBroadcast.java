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

public class FIFOBroadcast {
  private final byte pid;
  private final String srcIP;
  private final int srcPort;
  private final ArrayList<Host> peers;
  private final PerfectLink p2pLink;

  private final AtomicMap<Message, String> ackedMessage;
  private final AtomicSet<Packet> forwarded;
  private final HashSet<Integer> delivered;
  private final AtomicArrayList<Message> past;

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
    forwarded = new AtomicSet<>();
    delivered = new HashSet<>();
    past = new AtomicArrayList<>();

    deliverQueue = new ConcurrentLinkedQueue<>();

    this.p2pLink = new PerfectLink(pid, srcIP, srcPort, deliverQueue);
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
      Packet p = new Packet(packet.getType(), packet, srcIP, srcPort, peer.getIp(), peer.getPort());
      System.out.println("Beb broadcast packet: " +
          p.toString() + " to : " + peer.getPort());
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

  public void deliver(Message message) {
    System.out.println("FIFO delivering packet: " + message.toString());

    delivered.add(message.hashCode());
    logs.addIfNotInArray(message.delivered());

    // TODO does it need to broadcast an ack ?
    // if (!ackedMessage.get(message).contains(Packet.getKey(srcIP, srcPort))) {
    // System.out.println("Fifo deliver broadcasting ack");
    // // updateAck(message, srcIP, srcPort);

    // Packet ackPacket = new Packet(message, srcIP, srcPort, message.getOriginIP(),
    // message.getOriginPort());
    // urbBroadcast(ackPacket);
    // }

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
          // TODO only add you message to your local past ?
          // past.add(m);
        }
      }

      deliver(message);
      // TODO add a past per pid ?
      // past.add(message);
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
    MessageType type = packet.getType();
    Packet ackPacket = new Packet(MessageType.ACK_MESSAGE, packet, srcIP, srcPort, packet.getRelayIP(),
        packet.getRelayPort());

    // Send ack if first time received an ack for the packet from the source
    if (type == MessageType.ACK_MESSAGE) {
      HashSet<String> acks = ackedMessage.get(message);

      if (acks != null && !acks.contains(packetKey)) {
        System.out.println("beb deliver P2P sending ACK: " + ackPacket.toString() + " to: " + ackPacket.getDestPort());
        p2pLink.P2PSend(ackPacket);
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

    // Only forward if it's a chat message, answer with an ack
    if (type == MessageType.CHAT_MESSAGE) {
      if (!forwarded.contains(packet)) {
        // set yourself as the relay and forward (broadcast) once the message
        forwarded.add(packet);
        System.out.println("bebDeliver forwarded: " + forwarded.snapshot());

        bebBroadcast(ackPacket);
      }
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
