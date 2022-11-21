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
import cs451.messages.MessageType;
import cs451.network.UDPChannel;
import cs451.types.PendingAck;
import cs451.types.PendingMap;

public class PerfectLink {
    private final String srcIP;
    private final int srcPort;
    // UDP channel associated to the link
    private final UDPChannel UDPChannel;
    // TODO optimise such that message are resent in seqNum order
    // private final AtomicMapOfSet<Message, PendingAck> pendingAcks;
    private final PendingMap pendingAcks;

    // List containing the keys of messages that were acked
    private final Queue<Message> bebDeliverQueue;

    private final Lock lock;
    private final Condition notFull;

    public PerfectLink(byte pid, String srcIp, int srcPort, PendingMap pendingAcks,
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

            PendingAck pendingAck = new PendingAck(message.getType(), message.getDestPort());
            pendingAcks.put(message.getPid(), message.getSeqNum(), pendingAck);

            P2PSend(message);
        } catch (IOException e) {
            throw new IOException("Error while sending message: " + e.getMessage());
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
        // System.out.println("P2P deliver: " + message.toString());
        pendingAcks.removePendingAck(
                message.getPid(),
                message.getSeqNum(),
                (PendingAck p) -> p.getDestPort() == message.getRelayPort());

        lock.lock();
        try {
            // System.err.println("deliver WakeUp !");
            notFull.signal();
        } finally {
            lock.unlock();
        }

        // System.out.println("P2P deliver pendingAcks.size: " + pendingAcks.size());
        bebDeliverQueue.offer(message);
    }

    // Sends all the message
    public void waitForAck() throws IOException, InterruptedException {
        HashMap<Byte, HashMap<Integer, ArrayList<PendingAck>>> pendingAcksCopy = new HashMap<>(pendingAcks.snapshot());
        // Snapshot

        System.out.println("WaitForAck pendingAcks.size(): " + pendingAcks.size());
        HashSet<Byte> pids = new HashSet<>(pendingAcksCopy.keySet());

        for (byte pid : pids) {
            HashMap<Integer, ArrayList<PendingAck>> h = new HashMap<>(pendingAcksCopy.get(pid));
            HashSet<Integer> seqNums = new HashSet<>(h.keySet());

            for (int seqNum : seqNums) {
                ArrayList<PendingAck> pending = new ArrayList<>(h.get(seqNum));
                Message message = new Message(MessageType.ACK_MESSAGE, pid, seqNum);
                message.setRelayPort(srcPort);

                for (PendingAck p : pending) { // TODO Concurrent modif
                    if (p.hasTimedOut()) {

                        pendingAcks.acquireLock();

                        try {
                            // Check if it hasn't been acked in the meantime
                            boolean stillPending = pendingAcks.nonAtomicContains(pid, seqNum, p);

                            if (stillPending) {
                                try {
                                    // message.setType(p.getType());
                                    message.setDestPort(p.getDestPort());

                                    // System.out.println(
                                    // "WaitForAck resending: " + message.toString() + " to: " +
                                    // message.getDestPort());

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