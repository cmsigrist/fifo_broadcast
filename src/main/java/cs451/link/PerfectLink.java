package cs451.link;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.SocketException;
import java.time.Instant;
import java.util.ArrayList;

import cs451.messages.LightMessage;
import cs451.messages.Message;
import cs451.messages.MessageType;
import cs451.network.UDPChannel;
import cs451.types.AtomicArrayList;
import cs451.types.AtomicMap;
import cs451.types.PendingAck;

public class PerfectLink implements LinkInterface {
    // pid associated with the source of the link
    private final byte pid;
    // UDP channel associated to the link
    private final UDPChannel UDPChannel;
    private final AtomicMap<String, PendingAck> pendingAcks;
    private final AtomicMap<String, String[]> ackedMessage;
    // List containing the keys of messages that were acked

    private final AtomicArrayList<String> logs;
    private int seqNum = 0;

    public PerfectLink(byte pid, String srcIp, int srcPort) throws SocketException {
        try {
            this.UDPChannel = new UDPChannel(srcIp, srcPort);
        } catch (SocketException e) {
            throw new SocketException(e.getMessage());
        }

        this.pid = pid;
        this.pendingAcks = new AtomicMap<>();
        this.ackedMessage = new AtomicMap<>();
        this.logs = new AtomicArrayList<>();
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
            System.out.println("sending: " + seqNum);
            logs.add(message.broadcast());

            PendingAck pendingAck = new PendingAck(message, Instant.now());
            pendingAcks.add(pendingAck.getKey(), pendingAck);
        }
    }

    // Sends all the message
    public void waitForAck() throws IOException, InterruptedException {
        ArrayList<PendingAck> pending;

        while (true) {
            // Snapshot

            // TODO try to snapshot, iterate over all the pendingAcks
            // If has TO then check if in between not acked,
            // if not resend else continue
            // Access ackedMessage with no lock ?

            pending = pendingAcks.snapshot();
            System.out.println("New snapshot");
            for (PendingAck p : pending) {

                if (p.hasTimedOut()) {
                    pendingAcks.lock.lock();

                    try {
                        // Check if it hasn't been acked in the meantime
                        PendingAck pendingAck = pendingAcks.get(p.getKey());

                        if (pendingAck != null) {
                            try {
                                Message m = p.getMessage();
                                System.out.println("resending: " + m.getSeqNum());
                                P2PSend(m);
                            } catch (IOException e) {
                                throw new IOException("Error while sending message: " + e.getMessage());
                            }

                            pendingAck.resetTimeout();
                        }
                        // System.out.println("Sent packet: " + seqNum);
                    } finally {
                        pendingAcks.lock.unlock();
                    }
                }
            }

            try {
                Thread.sleep(PendingAck.ACK_TIMEOUT - 100);
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
                    String key = PendingAck.makeKey(message.getSeqNum(), srcIP, srcPort);
                    pendingAcks.remove(key);
                    // TODO
                    ackedMessage.add(key, null);

                    System.out.println("received ack: " + message.getSeqNum());
                }

            } catch (IOException e) {
                throw new IOException(e.getMessage());
            }
        }
    }

    private void P2PDeliver(Message message, String srcIP, int srcPort) throws IOException {
        logs.addIfNotInArray(message.delivered());

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
        return logs.nonAtomicSnapshot();
    }

}