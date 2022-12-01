package cs451.messages;

import java.net.DatagramPacket;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.StringJoiner;

public class ProposalMessage {
  private byte type;
  // process that sent the message
  private final byte pid;
  private final int seqNum;

  private int relayPort;
  private int destPort;
  private String[] proposal;
  private final int proposalNumber;
  private final int step;

  public ProposalMessage(
      byte type,
      byte pid,
      int seqNum,
      String[] proposal,
      int proposalNumber,
      int step) {
    this.type = type;
    this.pid = pid;
    this.seqNum = seqNum;

    this.proposal = proposal;
    this.proposalNumber = proposalNumber;
    this.step = step;
  }

  public ProposalMessage(byte pid, int seqNum, int proposalNumber, int step, int relayPort,
      int destPort) {
    this.type = MessageType.ACK_MESSAGE;
    this.pid = pid;
    this.seqNum = seqNum;
    this.proposal = new String[0];
    this.proposalNumber = proposalNumber;
    this.step = step;

    this.relayPort = relayPort;
    this.destPort = destPort;
  }

  public ProposalMessage(
      byte pid,
      int seqNum,
      String[] proposal,
      int proposalNumber,
      int step,
      int relayPort,
      int destPort) {
    this.type = MessageType.NACK_MESSAGE;
    this.pid = pid;
    this.seqNum = seqNum;
    this.proposal = proposal;
    this.proposalNumber = proposalNumber;
    this.step = step;

    this.relayPort = relayPort;
    this.destPort = destPort;
  }

  public byte getType() {
    return type;
  }

  public byte getPid() {
    return pid;
  }

  public int getSeqNum() {
    return seqNum;
  }

  public int getRelayPort() {
    return relayPort;
  }

  public int getDestPort() {
    return destPort;
  }

  public void setRelayPort(int relayPort) {
    this.relayPort = relayPort;
  }

  public void setDestPort(int destPort) {
    this.destPort = destPort;
  }

  // For and ACK_RESPONSE_MESSAGE
  public void setRelayMessage(int relayPort, int destPort) {
    this.type = MessageType.ACK_RESPONSE_MESSAGE;
    this.relayPort = relayPort;
    this.destPort = destPort;
  }

  public String[] getProposal() {
    return proposal;
  }

  public int getProposalNumber() {
    return proposalNumber;
  }

  public int getStep() {
    return step;
  }

  public void setProposal(String[] proposal) {
    this.proposal = proposal;
  }

  public byte[] marshall() {
    StringJoiner stringJoiner = new StringJoiner(":");

    stringJoiner
        .add(Byte.toString(type))
        .add(Byte.toString(pid))
        .add(Integer.toString(seqNum))
        .add(Integer.toString(step))
        .add(Integer.toString(proposalNumber))
        .add(Integer.toString(proposal.length));
    for (String proposal : proposal) {
      stringJoiner.add(proposal);
    }

    byte[] payload = stringJoiner.toString().getBytes();
    short packetLength = (short) payload.length;

    byte[] size = ByteBuffer.allocate(2).putShort(packetLength).array();

    byte[] packet = new byte[packetLength + 2];
    packet[0] = size[0];
    packet[1] = size[1];

    System.arraycopy(payload, 0, packet, 2, packetLength);

    return packet;
  }

  public static ProposalMessage unmarshall(DatagramPacket d) {
    byte[] packet = d.getData();
    short packetLength = ByteBuffer.wrap(packet, 0, 2).getShort();

    String payload = new String(Arrays.copyOfRange(packet, 2, packetLength + 2));
    String[] fields = payload.split(":");

    byte type = Byte.valueOf(fields[0]);
    byte pid = Byte.valueOf(fields[1]);
    int seqNum = Integer.parseInt(fields[2]);
    int step = Integer.parseInt(fields[3]);
    int proposalNumber = Integer.parseInt(fields[4]);

    int proposalLength = Integer.parseInt(fields[5]);
    String[] proposal = new String[proposalLength];

    for (int i = 0; i < proposalLength; i++) {
      proposal[i] = fields[6 + i];
    }

    ProposalMessage proposalMessage = new ProposalMessage(
        type, pid, seqNum, proposal, proposalNumber, step);
    proposalMessage.setRelayPort(d.getPort());

    return proposalMessage;
  }

  public static String decide(String[] accepted) {
    StringJoiner stringJoiner = new StringJoiner(" ");

    for (String a : accepted) {
      stringJoiner.add(a);
    }

    return stringJoiner.toString() + "\n";
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder
        .append("{type: ");
    if (type == MessageType.PROPOSAL_MESSAGE) {
      stringBuilder.append("PROPOSAL");
    }
    if (type == MessageType.ACK_MESSAGE) {
      stringBuilder.append("ACK");
    }
    if (type == MessageType.NACK_MESSAGE) {
      stringBuilder.append("NACK");
    }
    if (type == MessageType.ACK_RESPONSE_MESSAGE) {
      stringBuilder.append("ACKRESP");
    }
    stringBuilder
        .append(" pid: ")
        .append(Integer.valueOf(pid + 1).toString())
        .append(" seqNum: ")
        .append(seqNum)
        .append(" proposal: ");

    for (String p : proposal) {
      stringBuilder.append(p).append(",");
    }

    stringBuilder
        .append(" proposalNumber: ")
        .append(proposalNumber)
        .append(" step: ")
        .append(step)
        .append(" from: ")
        .append(relayPort)
        .append(" to: ")
        .append(destPort)
        .append("}");

    return stringBuilder.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof Message) {
      return ((Message) o).getPid() == pid &&
          ((Message) o).getSeqNum() == seqNum;
    }

    return false;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + pid;
    result = prime * result + seqNum;
    return result;
  }

  public static int hashCode(byte pid, int seqNum) {
    final int prime = 31;
    int result = 1;
    result = prime * result + pid;
    result = prime * result + seqNum;
    return result;
  }
}
