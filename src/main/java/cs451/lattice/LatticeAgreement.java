package cs451.lattice;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import cs451.link.PerfectLink;
import cs451.messages.MessageType;
import cs451.messages.ProposalMessage;
import cs451.node.Host;
import cs451.types.AckCount;
import cs451.types.Logs;
import cs451.types.PendingMap;
import cs451.types.ValueSet;

public class LatticeAgreement {
  private final byte pid;
  private final int srcPort;
  private final ArrayList<Host> peers;
  private final PerfectLink p2pLink;

  // Ack and nack count for a step
  private final AckCount proposalNumbers;
  private final AckCount ackCount;
  private final AckCount nackCount;
  // TODO the sets are not updating correctly
  // private final ValueSet proposedValue;
  private final ValueSet acceptedValue;
  private AtomicInteger step = new AtomicInteger();

  private final PendingMap pendingAcks;
  private final Queue<ProposalMessage> deliverQueue;

  private final Logs logs;
  private final int majority;

  private AtomicInteger seqNum = new AtomicInteger();
  private int pendingMessage = 0;
  private final int MAX_PENDING;

  final Lock lock = new ReentrantLock();
  final Condition full = lock.newCondition();

  public LatticeAgreement(
      byte pid,
      String srcIP,
      int srcPort,
      ArrayList<Host> peers,
      int numProposal) throws IOException {
    this.pid = pid;
    this.srcPort = srcPort;
    this.peers = peers;

    this.seqNum.set(0);

    this.proposalNumbers = new AckCount();
    this.ackCount = new AckCount();
    this.nackCount = new AckCount();

    // For current step
    // this.proposalNumber.set(0);
    // this.proposedValue = new ValueSet();
    this.acceptedValue = new ValueSet();
    this.step.set(0);

    this.pendingAcks = new PendingMap();
    this.deliverQueue = new ConcurrentLinkedQueue<>();

    this.p2pLink = new PerfectLink(pid, srcIP, srcPort, pendingAcks, deliverQueue);
    logs = new Logs(numProposal);

    this.majority = 1 + (peers.size() / 2);

    int inFlightSize = peers.size() * peers.size();
    MAX_PENDING = inFlightSize > 100 ? 1 : (100 / inFlightSize);
    System.out.println("MAX_PENDING: " + MAX_PENDING);
  }

  // Uses beBroadcast
  public void propose(String proposal) throws IOException, InterruptedException {
    int currentStep = step.incrementAndGet();
    int currentProposalNumber = proposalNumbers.incrementAndGet(currentStep);

    String[] proposed = proposal.split(" ");

    // proposedValue.add(currentStep, proposed);
    acceptedValue.add(currentStep, proposed);

    // I have seen the proposedValue
    ackCount.set(currentStep, 1);
    nackCount.set(currentStep, 0);

    broadcast(proposed, currentProposalNumber, currentStep);
  }

  public void broadcast(String[] proposal, int proposalNumber, int step)
      throws IOException, InterruptedException {
    int s = seqNum.incrementAndGet();

    ProposalMessage proposalMessage = new ProposalMessage(
        MessageType.PROPOSAL_MESSAGE, pid, s, proposal, proposalNumber, step);

    proposalMessage.setRelayPort(srcPort);
    System.out.println("sending proposal message:" + proposalMessage.toString());

    // BestEffortBroadcast
    for (Host peer : peers) {
      proposalMessage.setDestPort(peer.getPort());

      p2pLink.send(proposalMessage);
    }

    pendingMessage += 1;

    // Wait if MAX_PENDING messages are in flight
    if (pendingMessage == MAX_PENDING) {
      pendingMessage = 0;

      int numTry = MAX_PENDING - 1;

      lock.lock();
      try {
        while ((pendingAcks.size() > MAX_PENDING) && numTry > 0) {
          full.await();
          // Thread.sleep(500);
          numTry--;
        }
      } finally {
        lock.unlock();
      }
    }
  }

  public void decide(int currentStep) {
    // String decided =
    // ProposalMessage.decide(proposedValue.getToList(currentStep));
    String decided = ProposalMessage.decide(acceptedValue.getToList(currentStep));
    System.out.println("step: " + currentStep + " decidedValue: " + decided);
    logs.add(currentStep - 1, decided);
  }

  public void deliver(ProposalMessage message) throws IOException, InterruptedException {
    int currentStep = message.getStep();
    int currentProposalNumber = proposalNumbers.get(currentStep);
    byte type = message.getType();

    System.out.println("received: " + message.toString() + " from: " + message.getRelayPort());

    if (type == MessageType.PROPOSAL_MESSAGE) {
      processProposal(message, currentStep, currentProposalNumber);
    } else if (type == MessageType.ACK_MESSAGE) {
      processAck(message, currentStep, currentProposalNumber);
    } else if (type == MessageType.NACK_MESSAGE) {
      processNack(message, currentStep, currentProposalNumber);
    }

    // Send ack for message (does not expect a new ack for this message)
    if (type != MessageType.ACK_RESPONSE_MESSAGE) {
      // System.out.println("sending ack response: " + message.toString());
      message.setRelayMessage(srcPort, message.getRelayPort());
      p2pLink.P2PSend(message);
    }

    lock.lock();
    try {
      full.signal();
    } finally {
      lock.unlock();
    }
  }

  // TODO ignore message not in the same step ?
  public void processProposal(ProposalMessage proposalMessage, int currentStep, int currentProposalNumber)
      throws IOException {
    System.out.println(Arrays.toString(proposalMessage.getProposal()) + " containsAll"
        + acceptedValue.get(currentStep) + " ? : " + Arrays.asList(proposalMessage.getProposal())
            .containsAll(acceptedValue.get(currentStep)));
    if (Arrays.asList(proposalMessage.getProposal())
        .containsAll(acceptedValue.get(currentStep))) {
      acceptedValue.add(currentStep, proposalMessage.getProposal());
      // Send ack to src
      int s = seqNum.incrementAndGet();

      ProposalMessage ackMessage = new ProposalMessage(
          pid,
          s,
          proposalMessage.getProposalNumber(),
          proposalMessage.getStep(),
          srcPort,
          proposalMessage.getRelayPort());

      System.out.println(
          "sending: " + ackMessage.toString() + " for proposal: " + Arrays.toString(proposalMessage.getProposal()));
      p2pLink.send(ackMessage);
    } else {
      acceptedValue.add(currentStep, proposalMessage.getProposal());
      // Send nack
      int s = seqNum.incrementAndGet();

      ProposalMessage nackMessage = new ProposalMessage(
          pid,
          s,
          acceptedValue.getToList(currentStep),
          proposalMessage.getProposalNumber(),
          proposalMessage.getStep(),
          srcPort,
          proposalMessage.getRelayPort());
      System.out.println("sending: " + nackMessage.toString());
      p2pLink.send(nackMessage);
    }
  }

  // Only process if in the same step
  public void processAck(ProposalMessage ackMessage, int currentStep, int currentProposalNumber) {
    System.out.println("processAck: " + ackMessage.getProposalNumber() + " current: " + currentProposalNumber);

    if (ackMessage.getProposalNumber() == currentProposalNumber) {
      int newAckCount = ackCount.incrementAndGet(currentStep);
      System.out
          .println("step: " + currentStep + " proposalNumber: " + currentProposalNumber + " ackCount: " + newAckCount);
      if (newAckCount >= majority) {
        decide(currentStep);

        // +1 since node is not in peer
        if (ackCount.get(currentStep) == (peers.size() + 1)) {
          cleanUp(currentStep);
        }
      }
    }

  }

  public void processNack(ProposalMessage nackMessage, int currentStep, int currentProposalNumber)
      throws IOException, InterruptedException {
    System.out.println("processNack: " + nackMessage.getProposalNumber() + " current: " + currentProposalNumber);

    if (nackMessage.getProposalNumber() == currentProposalNumber) {
      int newNackCount = nackCount.incrementAndGet(currentStep);

      // proposedValue.add(currentStep, nackMessage.getProposal());
      acceptedValue.add(currentStep, nackMessage.getProposal());

      System.out.println("step: " + step + " proposalNumber: " + currentProposalNumber + " nackCount: " + newNackCount);

      if (newNackCount > 0 && ackCount.get(currentStep) + newNackCount >= majority) {
        currentProposalNumber = proposalNumbers.incrementAndGet(currentStep);
        // String[] newProposal = proposedValue.getToList(currentStep);
        String[] newProposal = acceptedValue.getToList(currentStep);
        // acceptedValue.add(currentStep, newProposal);

        ackCount.set(currentStep, 1);
        nackCount.set(currentStep, 0);

        System.out.println("step: " + currentStep + " proposalNumber: " + currentProposalNumber + " reviewed proposal: "
            + Arrays.toString(newProposal));
        broadcast(newProposal, currentProposalNumber, currentStep);
      }
    }
  }

  public void channelDeliver() throws IOException {
    try {
      p2pLink.channelDeliver();
    } catch (IOException e) {
      throw new IOException(e.getMessage());
    }
  }

  private void cleanUp(int step) {
    System.out.println("cleaning up step: " + step);
    ackCount.remove(step);
    nackCount.remove(step);
    acceptedValue.remove(step);
    // proposedValue.remove(step);
    proposalNumbers.remove(step);
  }

  public void waitForAck() throws IOException, InterruptedException {
    p2pLink.waitForAck();
  }

  public ArrayList<String> getLogs() {
    return logs.nonAtomicSnapshot();
  }

  public Queue<ProposalMessage> getDeliverQueue() {
    return deliverQueue;
  }
}
