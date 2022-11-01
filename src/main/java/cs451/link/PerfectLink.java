package cs451.link;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.SocketException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.locks.ReentrantLock;

import cs451.messages.LightMessage;
import cs451.messages.Message;
import cs451.messages.MessageType;
import cs451.messages.PendingAckMessage;
import cs451.network.UDPChannel;

public class PerfectLink implements LinkInterface {
    // pid associated with the source of the link
    private final byte pid;
    // UDP channel associated to the link
    private final UDPChannel UDPChannel;
    private final HashMap<String, PendingAckMessage> pendingAcks;
    // List containing the keys of messages that were acked
    private final HashSet<String> ackedMessage;

    private final ReentrantLock PLock;
    private final ArrayList<String> logs;
    private final ReentrantLock LLock;
    private int seqNum = 0;

    public PerfectLink(byte pid, String srcIp, int srcPort) throws SocketException {
        try {
            this.UDPChannel = new UDPChannel(srcIp, srcPort);
        } catch (SocketException e) {
            throw new SocketException(e.getMessage());
        }

        this.pid = pid;
        this.pendingAcks = new HashMap<>();
        this.ackedMessage = new HashSet<>();
        this.logs = new ArrayList<>();

        this.PLock = new ReentrantLock();
        this.LLock = new ReentrantLock();
    }

    // Sends a single message
    public void send(LightMessage m) throws IOException {
        seqNum += 1;
        Message message = new Message(pid, seqNum, m.getDestIP(), m.getDestPort(), m.getPayload());

        try {
            P2PSend(message);
        } catch (IOException e) {
            throw new IOException("Error while sending message: " + e.getMessage());
        } finally {
            LLock.lock();
            try {
                String log = message.broadcast();
                logs.add(log);
            } finally {
                LLock.unlock();
            }

            PLock.lock();

            try {
                PendingAckMessage pendingAckMessage = new PendingAckMessage(message, Instant.now());
                pendingAcks.put(pendingAckMessage.getKey(), pendingAckMessage);

                // System.out.println("Sent packet: " + seqNum);
            } finally {
                PLock.unlock();
            }
        }
    }

    // Sends all the message
    public void waitForAck() throws IOException, InterruptedException {
        ArrayList<PendingAckMessage> pending;
        while (true) {
            // Snapshot
            PLock.lock();
            try {
                pending = new ArrayList<>(pendingAcks.values());
            } finally {
                PLock.unlock();
            }

            for (PendingAckMessage p : pending) {
                Message message = p.getMessage();

                if (p.isAcked()) {
                    ackedMessage.add(p.getKey());

                    PLock.lock();
                    try {
                        pendingAcks.remove(p.getKey());
                    } finally {
                        PLock.unlock();
                    }
                    continue;
                }

                if (p.hasTimedOut()) {
                    PLock.lock();
                    try {
                        // Check if it hasn't been acked in the meantime
                        PendingAckMessage pendingAckMessage = pendingAcks.get(p.getKey());

                        if (pendingAckMessage != null) {
                            try {
                                P2PSend(message);
                            } catch (IOException e) {
                                throw new IOException("Error while sending message: " + e.getMessage());
                            }

                            pendingAckMessage.resetTimeout();
                        }
                        // System.out.println("Sent packet: " + seqNum);
                    } finally {
                        PLock.unlock();
                    }
                }
            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new InterruptedException(e.getMessage());
            }
        }
    }

    private void P2PSend(Message message) throws IOException {
        byte[] packet = message.serialize();
        DatagramPacket d = new DatagramPacket(packet, packet.length, InetAddress.getByName(message.getDestIP()),
                message.getDestPort());

        try {
            UDPChannel.send(d);
        } catch (IOException ignored) {
            // UDPChannel.send() error is ignored, on error just send it again anyway
        }
    }

    // This thread loops until SIGTERM or SIGSTOP
    public void deliver() throws IOException {
        while (true) {
            try {
                DatagramPacket d = UDPChannel.receive();

                if (d == null) {
                    continue;
                }
                if (d.getPort() < 11000 || d.getPort() > 11999) {
                    continue;
                }

                String srcIP = d.getAddress().toString().substring(1);
                int srcPort = d.getPort();

                Message message = Message.deSerialize(d);

                if (message.getType() == MessageType.CHAT_MESSAGE) {
                    P2PDeliver(message, srcIP, srcPort);
                }

                if (message.getType() == MessageType.ACK_MESSAGE) {
                    // Received ack for message, no need to try to send it anymore
                    String key = PendingAckMessage.makeKey(message.getSeqNum(), srcIP, srcPort);
                    PLock.lock();

                    try {
                        PendingAckMessage pendingAckMessage = pendingAcks.get(key);

                        if (pendingAckMessage != null) {
                            pendingAckMessage.setAcked();
                        }

                    } finally {
                        PLock.unlock();
                    }
                }

            } catch (IOException e) {
                throw new IOException(e.getMessage());
            }
        }
    }

    private void P2PDeliver(Message message, String srcIP, int srcPort) throws IOException {
        LLock.lock();
        try {
            String log = message.delivered();
            if (!logs.contains(log)) {
                logs.add(log);
            }
        } finally {
            LLock.unlock();
        }

        Message ackMessage = new Message(pid, message.getSeqNum(), srcIP, srcPort);

        try {
            // System.out.println("Sending ack for seqNum: " + message.getSeqNum() + " to: "
            // + srcPort);
            P2PSend(ackMessage);
        } catch (IOException e) {
            // System.out.println("P2PDeliver Error when sending ACK: " + e.getMessage());
            throw new IOException(e.getMessage());
        }
    }

    public ArrayList<String> getLogs() {
        return new ArrayList<>(logs);
    }

    public HashSet<String> getAckedMessage() {
        return ackedMessage;
    }
}