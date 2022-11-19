package cs451.link;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Queue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import cs451.messages.Message;
import cs451.network.UDPChannel;
import cs451.types.AtomicMapOfSet;
import cs451.types.PendingAck;

public class PerfectLink {
    private final String srcIP;
    private final int srcPort;
    // UDP channel associated to the link
    private final UDPChannel UDPChannel;
    // TODO optimise such that message are resent in seqNum order
    private final AtomicMapOfSet<Message, PendingAck> pendingAcks;
    // List containing the keys of messages that were acked
    private final Queue<Message> bebDeliverQueue;

    private final Lock lock;
    private final Condition notFull;

    public PerfectLink(byte pid, String srcIp, int srcPort, AtomicMapOfSet<Message, PendingAck> pendingAcks,
            Queue<Message> bebDeliverQueue, Lock lock, Condition notFull) throws SocketException {
        try {
            this.UDPChannel = new UDPChannel(srcIp, srcPort);
        } catch (SocketException e) {
            throw new SocketException(e.getMessage());
        }

        this.srcIP = srcIp;
        this.srcPort = srcPort;
        this.pendingAcks = pendingAcks;
        this.bebDeliverQueue = bebDeliverQueue;
        this.lock = lock;
        this.notFull = notFull;
    }

    // Sends a single message
    public void send(Message message) throws IOException {
        try {
            // System.out.println("sending: " + message.toString() + " to : " +
            // message.getDestPort());

            PendingAck pendingAck = new PendingAck(message.getDestPort());
            pendingAcks.put(message, pendingAck);

            P2PSend(message);
        } catch (IOException e) {
            throw new IOException("Error while sending message: " + e.getMessage());
        }
    }

    // Sends all the message
    public void waitForAck() throws IOException, InterruptedException {
        HashMap<Message, HashSet<PendingAck>> pendingAcksCopy = pendingAcks.snapshot();
        // Snapshot

        System.out.println("WaitForAck pendingAcks: " + pendingAcksCopy);

        ArrayList<Message> messages = new ArrayList<>(pendingAcksCopy.keySet());

        for (Message message : messages) {
            ArrayList<PendingAck> pending = new ArrayList<>(pendingAcksCopy.get(message));

            message.setRelayPort(srcPort);

            for (PendingAck p : pending) {
                if (p.hasTimedOut()) {
                    pendingAcks.acquireLock();

                    try {
                        // Check if it hasn't been acked in the meantime
                        boolean stillPending = pendingAcks.nonAtomicGet(message).contains(p);

                        if (stillPending) {
                            try {
                                message.setDestPort(p.getDestPort());

                                System.out.println(
                                        "WaitForAck resending: " + message.toString() + " to: " +
                                                message.getDestPort());

                                P2PSend(message);
                            } catch (IOException e) {
                                System.out.println("Error while sending message: " + e.getMessage());
                            }

                            pendingAcks.get(message).remove(p);
                            p.resetTimeout();
                            pendingAcks.put(message, p);
                        }
                    } finally {
                        pendingAcks.releaseLock();
                    }
                }
            }
        }
    }

    public void P2PSend(Message message) throws IOException {
        // System.out.println("P2P sending message: " + message + " to: " +
        // message.getDestPort());
        byte[] p = message.marshall();
        DatagramPacket d = new DatagramPacket(p, p.length, InetAddress.getByName(srcIP),
                message.getDestPort());

        try {
            UDPChannel.send(d);
        } catch (IOException ignored) {
            // UDPChannel.send() error is ignored, on error just send it again anyway
        }
    }

    // This thread loops until SIGTERM or SIGSTOP
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

    private synchronized void deliver(Message message) throws IOException {
        System.out.println("P2P deliver: " + message.toString());
        pendingAcks.removeElem(
                message,
                (PendingAck p) -> p.getDestPort() == message.getRelayPort());

        lock.lock();
        try {

            notFull.signal();
        } finally {
            lock.unlock();
        }

        System.out.println("P2P deliver pendingAcks.size: " + pendingAcks.size());
        bebDeliverQueue.offer(message);
    }
}