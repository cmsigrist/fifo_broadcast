package cs451.link;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Queue;

import cs451.messages.MessageType;
import cs451.messages.ProposalMessage;
import cs451.network.UDPChannel;
import cs451.types.PendingAck;
import cs451.types.PendingMap;

public class PerfectLink {
    private final byte pid;
    private final String srcIP;
    private final int srcPort;

    // UDP channel associated to the link
    private final UDPChannel UDPChannel;
    private final PendingMap pendingAcks;

    // List containing the keys of messages that were acked
    private final Queue<ProposalMessage> bebDeliverQueue;

    public PerfectLink(byte pid, String srcIp, int srcPort, PendingMap pendingAcks,
            Queue<ProposalMessage> bebDeliverQueue) throws SocketException {
        try {
            this.UDPChannel = new UDPChannel(srcIp, srcPort);
        } catch (SocketException e) {
            throw new SocketException(e.getMessage());
        }
        this.pid = pid;
        this.srcIP = srcIp;
        this.srcPort = srcPort;
        this.pendingAcks = pendingAcks;
        this.bebDeliverQueue = bebDeliverQueue;
    }

    // Sends a single message
    public void send(ProposalMessage message) throws IOException {
        try {
            PendingAck pendingAck = new PendingAck(message);
            pendingAcks.put(message.getSeqNum(), pendingAck);

            P2PSend(message);
        } catch (IOException e) {
            throw new IOException("Error while sending message: " + e.getMessage());
        }
    }

    public void P2PSend(ProposalMessage message) throws IOException {
        byte[] p = message.marshall();
        DatagramPacket d = new DatagramPacket(p, p.length, InetAddress.getByName(srcIP),
                message.getDestPort());

        try {
            UDPChannel.send(d);
        } catch (IOException ignored) {
            // UDPChannel.send() error is ignored, on error just send it again anyway
        }
    }

    public void channelDeliver() throws IOException {
        try {
            DatagramPacket d = UDPChannel.receive();

            if (d == null) {
                return;
            }
            if (d.getPort() < 11000 || d.getPort() > 11999) {
                return;
            }

            ProposalMessage message = ProposalMessage.unmarshall(d);
            deliver(message);

        } catch (IOException e) {
            throw new IOException(e.getMessage());
        }
    }

    private void deliver(ProposalMessage message) throws IOException {
        // System.out.println("P2P deliver: " + message.toString());
        // var copy = pendingAcks.snapshot();
        // pendingAcks.acquireLock();
        // System.out.println("current pendingAcks: " + copy);
        // pendingAcks.releaseLock();

        if (message.getType() == MessageType.ACK_MESSAGE ||
                message.getType() == MessageType.NACK_MESSAGE) {
            pendingAcks.removePendingAck(
                    message.getSeqNum(),
                    (PendingAck p) -> p.getDestPort() == message.getRelayPort()
                            && p.getType() == MessageType.PROPOSAL_MESSAGE);
        }

        if (message.getType() == MessageType.ACK_RESPONSE_MESSAGE) {
            pendingAcks.removePendingAck(
                    message.getSeqNum(),
                    (PendingAck p) -> p.getDestPort() == message.getRelayPort()
                            && (p.getType() == MessageType.ACK_MESSAGE
                                    || p.getType() == MessageType.NACK_MESSAGE));
            return;
        }

        // var copy = pendingAcks.snapshot();
        // pendingAcks.acquireLock();
        // System.out.println("update pendingAcks: " + copy);
        // pendingAcks.releaseLock();

        bebDeliverQueue.offer(message);
    }

    // Sends all the message
    public void waitForAck() throws IOException, InterruptedException {
        HashMap<Integer, ArrayList<PendingAck>> pendingAcksCopy = pendingAcks.snapshot();
        // Snapshot

        for (var seqNumEntry : pendingAcksCopy.entrySet()) {
            ArrayList<PendingAck> pending = seqNumEntry.getValue();
            int seqNum = seqNumEntry.getKey();

            for (PendingAck p : pending) {
                if (p.hasTimedOut()) {
                    pendingAcks.acquireLock();
                    try {
                        // Check if it hasn't been acked in the meantime
                        boolean stillPending = pendingAcks.nonAtomicContains(seqNum, p);

                        if (stillPending) {
                            try {
                                ProposalMessage message = p.getMessage(pid, seqNum);
                                message.setRelayPort(srcPort);
                                message.setDestPort(p.getDestPort());

                                System.out.println("ack timeout resending: " + message.toString());

                                P2PSend(message);
                            } catch (IOException e) {
                                System.out.println("Error while sending message: " + e.getMessage());
                            }

                            pendingAcks.nonAtomicRemove(seqNum, p);
                            p.resetTimeout();
                            pendingAcks.put(seqNum, p);
                        }
                    } finally {
                        pendingAcks.releaseLock();
                    }
                }
            }
        }

    }
}