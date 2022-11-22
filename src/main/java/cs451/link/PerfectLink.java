package cs451.link;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Queue;

import cs451.messages.Message;
import cs451.messages.MessageType;
import cs451.network.UDPChannel;
import cs451.types.PendingAck;
import cs451.types.PendingMap;

public class PerfectLink {
    private final String srcIP;
    private final int srcPort;
    // UDP channel associated to the link
    private final UDPChannel UDPChannel;
    private final PendingMap pendingAcks;

    // List containing the keys of messages that were acked
    private final Queue<Message> bebDeliverQueue;

    public PerfectLink(byte pid, String srcIp, int srcPort, PendingMap pendingAcks,
            Queue<Message> bebDeliverQueue) throws SocketException {
        try {
            this.UDPChannel = new UDPChannel(srcIp, srcPort);
        } catch (SocketException e) {
            throw new SocketException(e.getMessage());
        }

        this.srcIP = srcIp;
        this.srcPort = srcPort;
        this.pendingAcks = pendingAcks;
        this.bebDeliverQueue = bebDeliverQueue;
    }

    // Sends a single message
    public void send(Message message) throws IOException {
        try {
            PendingAck pendingAck = new PendingAck(message.getType(), message.getDestPort());
            pendingAcks.put(message.getPid(), message.getSeqNum(), pendingAck);

            P2PSend(message);
        } catch (IOException e) {
            throw new IOException("Error while sending message: " + e.getMessage());
        }
    }

    public void P2PSend(Message message) throws IOException {
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

            Message message = Message.unmarshall(d);
            deliver(message);

        } catch (IOException e) {
            throw new IOException(e.getMessage());
        }
    }

    private void deliver(Message message) throws IOException {
        pendingAcks.removePendingAck(
                message.getPid(),
                message.getSeqNum(),
                (PendingAck p) -> p.getDestPort() == message.getRelayPort());

        bebDeliverQueue.offer(message);
    }

    // Sends all the message
    public void waitForAck() throws IOException, InterruptedException {
        HashMap<Byte, HashMap<Integer, ArrayList<PendingAck>>> pendingAcksCopy = pendingAcks.snapshot();
        // Snapshot

        // System.out.println("WaitForAck pendingAcks.size(): " + pendingAcks.size());

        for (var pidEntry : pendingAcksCopy.entrySet()) {
            for (var seqNumEntry : pidEntry.getValue().entrySet()) {
                ArrayList<PendingAck> pending = seqNumEntry.getValue();
                byte pid = pidEntry.getKey();
                int seqNum = seqNumEntry.getKey();
                Message message = new Message(MessageType.ACK_MESSAGE, pid, seqNum);
                message.setRelayPort(srcPort);

                for (PendingAck p : pending) {
                    if (p.hasTimedOut()) {

                        pendingAcks.acquireLock();
                        try {
                            // Check if it hasn't been acked in the meantime
                            boolean stillPending = pendingAcks.nonAtomicContains(pid, seqNum, p);

                            if (stillPending) {
                                try {
                                    message.setDestPort(p.getDestPort());

                                    P2PSend(message);
                                } catch (IOException e) {
                                    System.out.println("Error while sending message: " + e.getMessage());
                                }

                                pendingAcks.nonAtomicRemove(pid, seqNum, p);
                                p.resetTimeout();
                                pendingAcks.put(pid, seqNum, p);
                            }
                        } finally {
                            pendingAcks.releaseLock();
                        }
                    }
                }
            }
        }
    }
}