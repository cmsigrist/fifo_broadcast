package cs451.link;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.SocketException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Queue;

import cs451.messages.Message;
import cs451.messages.MessageType;
import cs451.messages.Packet;
import cs451.network.UDPChannel;
import cs451.types.AtomicMap;
import cs451.types.PendingAck;

public class PerfectLink {
    // pid associated with the source of the link
    private final byte pid;
    private final String srcIP;
    private final int srcPort;
    // UDP channel associated to the link
    private final UDPChannel UDPChannel;
    // TODO optimise such that message are resent in seqNum order
    // TODO map not working, change hashCode of packets st sent packet
    // With Chat Message and received packet with ACK_Message have same hash!
    // Or change such that process simply send back the same packet (modulo dest)
    // to src for ack
    private final AtomicMap<Integer, PendingAck> pendingAcks;
    // List containing the keys of messages that were acked
    private final Queue<Packet> bebDeliverQueue;

    public PerfectLink(byte pid, String srcIp, int srcPort, Queue<Packet> bebDeliverQueue) throws SocketException {
        try {
            this.UDPChannel = new UDPChannel(srcIp, srcPort);
        } catch (SocketException e) {
            throw new SocketException(e.getMessage());
        }

        this.pid = pid;
        this.srcIP = srcIp;
        this.srcPort = srcPort;
        this.pendingAcks = new AtomicMap<>();
        this.bebDeliverQueue = bebDeliverQueue;
    }

    // Sends a single message
    public void send(Packet packet) throws IOException {
        try {
            P2PSend(packet);
        } catch (IOException e) {
            throw new IOException("Error while sending message: " + e.getMessage());
        } finally {
            Message message = packet.getMessage();
            System.out.println("sending: " + message.getSeqNum() + " to : " +
                    packet.getDestPort());

            if (message.getType() == MessageType.CHAT_MESSAGE) {
                PendingAck pendingAck = new PendingAck(packet, Instant.now());
                pendingAcks.put(message.hashCode(), pendingAck);
            }
        }
    }

    // Sends all the message
    public void waitForAck() throws IOException, InterruptedException {
        ArrayList<PendingAck> pending = pendingAcks.snapshot();
        // Snapshot

        System.out.println("WaitForAck pendingAcks: " + pending);
        for (PendingAck p : pending) {

            if (p.hasTimedOut()) {
                pendingAcks.lock.lock();

                try {
                    // Check if it hasn't been acked in the meantime
                    PendingAck pendingAck = pendingAcks.nonAtomicGet(p.getPacket().getMessage().hashCode());
                    System.out.println(
                            "WaifForAck TO ! pendingAck null ? " + pendingAck);
                    if (pendingAck != null) {
                        try {
                            Packet packet = pendingAck.getPacket();
                            System.out.println(
                                    "WaifForAck resending: " + packet.getMessage().toString() + " to: " +
                                            packet.getDestPort());
                            P2PSend(packet);
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
    }

    private void P2PSend(Packet packet) throws IOException {
        byte[] p = packet.marshall();
        DatagramPacket d = new DatagramPacket(p, p.length, InetAddress.getByName(packet.getDestIP()),
                packet.getDestPort());

        try {
            UDPChannel.send(d);
        } catch (IOException ignored) {
            // UDPChannel.send() error is ignored, on error just send it again anyway
        }
    }

    // This thread loops until SIGTERM or SIGSTOP
    public void channelDeliver() throws IOException {
        while (true) {
            try {
                DatagramPacket d = UDPChannel.receive();

                if (d == null) {
                    continue;
                }
                if (d.getPort() < 11000 || d.getPort() > 11999) {
                    continue;
                }

                Packet packet = Packet.unmarshall(d);
                deliver(packet);

            } catch (IOException e) {
                throw new IOException(e.getMessage());
            }
        }
    }

    private void deliver(Packet packet) throws IOException {
        // logs.addIfNotInArray(message.delivered());
        Message message = packet.getMessage();

        if (message.getType() == MessageType.CHAT_MESSAGE) {
            Message ackMessage = new Message(pid, message.getSeqNum(), srcIP, srcPort);
            Packet ackPacket = new Packet(ackMessage);

            try {
                P2PSend(ackPacket);
            } catch (IOException e) {
                throw new IOException(e.getMessage());
            }
        }

        if (message.getType() == MessageType.ACK_MESSAGE) {
            // Received ack for message, no need to try to send it anymore
            System.out.println("P2P received ack packet: " + packet.getMessage().toString());
            System.out.println("pending acks: " + pendingAcks.snapshot());
            // pendingAcks.remove(message.hashCode());
        }
        // System.out.println("P2P delivering packet: " +
        // packet.getMessage().toString());
        bebDeliverQueue.offer(packet);
    }
}