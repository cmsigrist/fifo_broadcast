package cs451.messages;

import java.net.DatagramPacket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringJoiner;

public class Packet {
  private MessageType type;
  private final Message message;
  private final ArrayList<Message> past;
  private String relayIP;
  private int relayPort;
  private String destIP;
  private int destPort;

  public static int startPacket = 2;

  public Packet(MessageType type, Message message, ArrayList<Message> past, String relayIP, int relayPort,
      String destIP, int destPort) {
    this.type = type;
    this.message = message;
    this.past = new ArrayList<>(past);
    this.relayIP = relayIP;
    this.relayPort = relayPort;
    this.destIP = destIP;
    this.destPort = destPort;
  }

  // For CHAT_MESSAGE
  public Packet(Message message, ArrayList<Message> past) {
    this.type = MessageType.CHAT_MESSAGE;
    this.message = message;
    this.past = new ArrayList<>(past);
  }

  // Change the relay, dest, and the type
  public Packet(MessageType type, Packet packet, String relayIP, int relayPort, String destIP, int destPort) {
    this.type = type;
    this.message = packet.getMessage();
    this.relayIP = relayIP;
    this.relayPort = relayPort;
    this.destIP = destIP;
    this.destPort = destPort;
    this.past = new ArrayList<>(packet.getPast());
  }

  public MessageType getType() {
    return type;
  }

  public Message getMessage() {
    return message;
  }

  public ArrayList<Message> getPast() {
    return past;
  }

  public String getRelayIP() {
    return relayIP;
  }

  public int getRelayPort() {
    return relayPort;
  }

  public String getDestIP() {
    return destIP;
  }

  public int getDestPort() {
    return destPort;
  }

  public void setType(MessageType type) {
    this.type = type;
  }

  public void setRelayIP(String relayIP) {
    this.relayIP = relayIP;
  }

  public void setRelayPort(int relayPort) {
    this.relayPort = relayPort;
  }

  public void setDestIP(String destIP) {
    this.destIP = destIP;
  }

  public void setDestPort(int destPort) {
    this.destPort = destPort;
  }

  public String getKey() {
    return relayIP + "," + relayPort;
  }

  public static String getKey(String srcIP, int srcPort) {
    return srcIP + "," + srcPort;
  }

  public void setRelayPacket(MessageType type, String relayIP, int relayPort, String destIP, int destPort) {
    this.type = type;
    this.relayIP = relayIP;
    this.relayPort = relayPort;
    this.destIP = destIP;
    this.destPort = destPort;
  }

  public byte[] marshall() {
    StringJoiner stringJoiner = new StringJoiner("|");

    stringJoiner.add(message.marshall());

    for (Message m : past) {
      stringJoiner.add(m.marshall());
    }

    stringJoiner
        .add(type.toString())
        .add(relayIP)
        .add(Integer.toString(relayPort))
        .add(destIP)
        .add(Integer.toString(destPort));

    byte[] packet = stringJoiner.toString().getBytes();
    short packetLength = (short) packet.length;

    byte[] size = ByteBuffer.allocate(startPacket).putShort(packetLength).array();

    byte[] packetWithSize = new byte[packetLength + startPacket];
    packetWithSize[0] = size[0];
    packetWithSize[1] = size[1];

    System.arraycopy(packet, 0, packetWithSize, startPacket, packetLength);

    return packetWithSize;
  }

  public static Packet unmarshall(DatagramPacket d) {
    byte[] packetBytes = d.getData();
    short packetLength = ByteBuffer.wrap(packetBytes, 0, startPacket).getShort();

    String packet = new String(Arrays.copyOfRange(packetBytes, startPacket, packetLength));
    String[] fields = packet.split("\\|");

    Message message = Message.unmarshall(fields[0]);

    ArrayList<Message> past = new ArrayList<>();
    int length = fields.length;

    MessageType type = MessageType.valueOf(fields[length - 5]);

    if (type == MessageType.CHAT_MESSAGE) {
      for (int i = 1; i < length - 5; i++) {
        past.add(Message.unmarshall(fields[i]));
      }
    }

    String relayIP = fields[length - 4];
    int relayPort = Integer.parseInt(fields[length - 3]);
    String destIP = fields[length - 2];
    int destPort = Integer.parseInt(fields[length - 1]);

    return new Packet(type, message, past, relayIP, relayPort, destIP, destPort);
  }

  // Packets are wrapper for message
  @Override
  public boolean equals(Object o) {
    if (o instanceof Packet) {
      return ((Packet) o).getMessage().equals(message);
    }

    return false;
  }

  // Packets are wrapper for message
  @Override
  public int hashCode() {
    return message.hashCode();
  }

  @Override
  public String toString() {
    return "{" + type + " : " + message.toString() + " from: " + relayPort + "}";
  }
}
