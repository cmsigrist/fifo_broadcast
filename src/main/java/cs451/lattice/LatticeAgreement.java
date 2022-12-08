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
import cs451.types.Logs;
import cs451.types.PendingMap;
import cs451.types.StepMap;
import cs451.types.ValueSet;

public class LatticeAgreement {
  private final byte pid;
  private final int srcPort;
  private final ArrayList<Host> peers;
  private final PerfectLink p2pLink;

  // Ack and nack count for a step
  private final StepMap proposalNumbers;
  private final StepMap ackCount;
  // private final StepMap nackCount;

  private final ValueSet acceptedValue;
  private AtomicInteger step;

  private final PendingMap pendingAcks;
  private final Queue<ProposalMessage> deliverQueue;

  private final Logs logs;
  private final int majority;

  private AtomicInteger seqNum = new AtomicInteger();

  final Lock lock;
  final Condition full;

  public LatticeAgreement(
      byte pid,
      String srcIP,
      int srcPort,
      ArrayList<Host> peers,
      int numProposal,
      AtomicInteger step,
      ReentrantLock lock,
      Condition full) throws IOException {
    this.pid = pid;
    this.srcPort = srcPort;
    this.peers = peers;

    this.seqNum.set(0);

    this.proposalNumbers = new StepMap();
    this.ackCount = new StepMap();
    // this.nackCount = new StepMap();
    this.step = step;

    this.acceptedValue = new ValueSet();

    this.pendingAcks = new PendingMap();
    this.deliverQueue = new ConcurrentLinkedQueue<>();

    this.p2pLink = new PerfectLink(pid, srcIP, srcPort, pendingAcks, deliverQueue);
    logs = new Logs(numProposal);

    this.majority = 1 + (peers.size() / 2);

    this.lock = lock;
    this.full = full;
  }

  // Uses beBroadcast
  public void propose(String proposal) throws IOException, InterruptedException {
    int currentStep = step.get();
    int currentProposalNumber = proposalNumbers.incrementAndGet(currentStep);

    String[] proposed = proposal.split(" ");

    acceptedValue.add(currentStep, proposed);

    // I have seen the proposedValue
    ackCount.set(currentStep, 1);
    // nackCount.set(currentStep, 0);

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
  }

  public void decide(int currentStep) {
    String decided = ProposalMessage.decide(acceptedValue.getToList(currentStep));
    System.out.println("step: " + currentStep + " decidedValue: " + decided);
    logs.add(currentStep - 1, decided);

    step.incrementAndGet();

    lock.lock();
    try {
      full.signal();
      System.out.println("WAKE UP!");
    } finally {
      lock.unlock();
    }
  }

  public void deliver(ProposalMessage message) throws IOException, InterruptedException {
    int messageStep = message.getStep();
    int currentProposalNumber = proposalNumbers.get(messageStep);
    byte type = message.getType();

    System.out.println("received: " + message.toString() + " from: " + message.getRelayPort());

    if (type == MessageType.PROPOSAL_MESSAGE) {
      processProposal(message, messageStep, currentProposalNumber);
    }

    if (type == MessageType.ACK_MESSAGE) {
      processAck(message, messageStep, currentProposalNumber);
      message.setRelayMessage(srcPort, message.getRelayPort());
      p2pLink.P2PSend(message);
    }

    if (type == MessageType.NACK_MESSAGE) {
      processNack(message, messageStep, currentProposalNumber);
      message.setRelayMessage(srcPort, message.getRelayPort());
      p2pLink.P2PSend(message);
    }
  }

  // TODO ignore message not in the same step ?
  public void processProposal(ProposalMessage proposalMessage, int messageStep, int currentProposalNumber)
      throws IOException {
    if (Arrays.asList(proposalMessage.getProposal())
        .containsAll(acceptedValue.get(messageStep))) {
      acceptedValue.add(messageStep, proposalMessage.getProposal());
      // Send ack to src
      ProposalMessage ackMessage = new ProposalMessage(
          pid,
          proposalMessage.getSeqNum(),
          proposalMessage.getProposalNumber(),
          proposalMessage.getStep(),
          srcPort,
          proposalMessage.getRelayPort());

      System.out.println(
          "sending: " + ackMessage.toString() + " for proposal: " + Arrays.toString(proposalMessage.getProposal()));
      p2pLink.send(ackMessage);
    } else {
      acceptedValue.add(messageStep, proposalMessage.getProposal());
      // Send nack
      ProposalMessage nackMessage = new ProposalMessage(
          pid,
          proposalMessage.getSeqNum(),
          acceptedValue.getToList(messageStep),
          proposalMessage.getProposalNumber(),
          proposalMessage.getStep(),
          srcPort,
          proposalMessage.getRelayPort());
      System.out.println("sending: " + nackMessage.toString());
      p2pLink.send(nackMessage);
    }
  }

  // Only decide if in the same step (process for clean up)
  public void processAck(ProposalMessage ackMessage, int messageStep, int currentProposalNumber) {
    System.out.println("processAck: " + ackMessage.getProposalNumber() + " current: " + currentProposalNumber);

    if (ackMessage.getProposalNumber() == currentProposalNumber) {
      int newAckCount = ackCount.incrementAndGet(messageStep);
      System.out
          .println("step: " + messageStep + " proposalNumber: " + currentProposalNumber + " ackCount: " + newAckCount);

      if (newAckCount >= majority) {
        int currentStep = step.get();

        System.out.println("Ack majority for messageStep: " + messageStep + " currentStep: " + currentStep);
        if (messageStep == step.get()) {
          decide(messageStep);
        }

        // +1 since node is not in peers
        if (ackCount.get(messageStep) == (peers.size() + 1)) {
          cleanUp(messageStep);
        }
      }
    }
  }

  // Only process if in the same step (else could retry to send a new proposal
  // when value has already been decided)
  public void processNack(ProposalMessage nackMessage, int messageStep, int currentProposalNumber)
      throws IOException, InterruptedException {
    System.out.println("step: " + messageStep + " processNack: " + nackMessage.getProposalNumber() + " current: "
        + currentProposalNumber);

    int currentStep = step.get();

    if (messageStep == currentStep && nackMessage.getProposalNumber() == currentProposalNumber) {
      // int newNackCount = nackCount.incrementAndGet(messageStep);

      acceptedValue.add(messageStep, nackMessage.getProposal());

      // Send new proposal as soon as received a nack ?
      // Clean up pendingAcks !
      pendingAcks.removeStep(messageStep);
      // if (newNackCount > 0 && ackCount.get(currentStep) + newNackCount >= majority)
      // {
      currentProposalNumber = proposalNumbers.incrementAndGet(messageStep);
      String[] newProposal = acceptedValue.getToList(messageStep);

      ackCount.set(messageStep, 1);
      // nackCount.set(messageStep, 0);

      System.out.println("step: " + messageStep + " proposalNumber: " + currentProposalNumber + " reviewed proposal: "
          + Arrays.toString(newProposal));
      broadcast(newProposal, currentProposalNumber, messageStep);
      // }
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
    // nackCount.remove(step);
    acceptedValue.remove(step);
    proposalNumbers.remove(step);
  }

  public void waitForAck() throws IOException, InterruptedException {
    p2pLink.waitForAck();
  }

  public String[] getLogs() {
    return logs.snapshot();
  }

  public Queue<ProposalMessage> getDeliverQueue() {
    return deliverQueue;
  }
}
