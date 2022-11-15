package cs451.link;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Queue;

import cs451.messages.Packet;
import cs451.network.UDPChannel;
import cs451.types.AtomicMap;
import cs451.types.PendingAck;

public class PerfectLink {
    // pid associated with the source of the link
    // private final byte pid;
    private final String srcIP;
    private final int srcPort;
    // UDP channel associated to the link
    private final UDPChannel UDPChannel;
    // TODO optimise such that message are resent in seqNum order
    // TODO map not working, change hashCode of packets st sent packet
    // With Chat Message and received packet with ACK_Message have same hash!
    // Or change such that process simply send back the same packet (modulo dest)
    // to src for ack
    private final AtomicMap<Packet, String, PendingAck> pendingAcks;
    // List containing the keys of messages that were acked
    private final Queue<Packet> bebDeliverQueue;

    public PerfectLink(byte pid, String srcIp, int srcPort, AtomicMap<Packet, String, PendingAck> pendingAcks,
            Queue<Packet> bebDeliverQueue) throws SocketException {
        try {
            this.UDPChannel = new UDPChannel(srcIp, srcPort);
        } catch (SocketException e) {
            throw new SocketException(e.getMessage());
        }

        // this.pid = pid;
        this.srcIP = srcIp;
        this.srcPort = srcPort;
        this.pendingAcks = pendingAcks;
        this.bebDeliverQueue = bebDeliverQueue;
    }

    // Sends a single message
    public void send(Packet packet) throws IOException {
        try {
            System.out.println("sending: " + packet.toString() + " to : " +
                    packet.getDestPort());

            PendingAck pendingAck = new PendingAck(packet.getDestIP(), packet.getDestPort());
            pendingAcks.put(packet, pendingAck.getKey(), pendingAck);

            P2PSend(packet);
        } catch (IOException e) {
            throw new IOException("Error while sending message: " + e.getMessage());
        }
    }

    // Sends all the message
    public void waitForAck() throws IOException, InterruptedException {
        HashMap<Packet, HashMap<String, PendingAck>> pendingAcksCopy = pendingAcks.copy();
        // Snapshot

        System.out.println("WaitForAck pendingAcks: " + pendingAcksCopy);

        ArrayList<Packet> packets = new ArrayList<>(pendingAcksCopy.keySet());

        for (Packet packet : packets) {
            ArrayList<PendingAck> pending = new ArrayList<>(pendingAcksCopy.get(packet).values());

            packet.setRelayIP(srcIP);
            packet.setRelayPort(srcPort);

            for (PendingAck p : pending) {
                if (p.hasTimedOut()) {
                    pendingAcks.acquireLock();

                    try {
                        // Check if it hasn't been acked in the meantime
                        boolean stillPending = pendingAcks.nonAtomicGet(packet).containsKey(p.getKey());

                        if (stillPending) {
                            try {
                                // Avoid creating a new packet
                                // Packet newPacket = new Packet(packet.getType(), packet, srcIP, srcPort,
                                // p.getDestIP(),
                                // p.getDestPort());

                                packet.setDestIP(p.getDestIP());
                                packet.setDestPort(p.getDestPort());

                                System.out.println(
                                        "WaitForAck resending: " + packet.toString() + " to: " +
                                                packet.getDestPort());

                                P2PSend(packet);
                            } catch (IOException e) {
                                System.out.println("Error while sending message: " + e.getMessage());
                            }

                            pendingAcks.get(packet).get(p.getKey()).resetTimeout();
                        }
                        // System.out.println("Sent packet: " + seqNum);
                    } finally {
                        pendingAcks.releaseLock();
                    }
                }
            }
        }
    }

    public void P2PSend(Packet packet) throws IOException {
        System.out.println("P2P sending packet: " + packet + " to: " + packet.getDestPort());
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
        // Message message = packet.getMessage();
        System.out.println("P2P deliver: " + packet.toString());
        pendingAcks.acquireLock();

        try {
            if (pendingAcks.nonAtomicHasKey(packet)) {
                // System.out.println("P2P deliver received ack packet: " +
                // packet.toString());

                pendingAcks.nonAtomicGet(packet)
                        .remove(PendingAck.getKey(packet.getRelayIP(), packet.getRelayPort()));

                // System.out.println("P2P deliver updated pending acks: " +
                // pendingAcks.nonAtomicGet(packet));
            }
        } finally {
            pendingAcks.releaseLock();
        }

        bebDeliverQueue.offer(packet);

        // if (pendingAcks.get(packet)) {
        // // Received ack for message, no need to try to send it anymore
        // System.out.println("P2P received ack packet: " +
        // packet.getMessage().toString() + " from: " + packet.getRelayPort());
        // System.out.println("pending acks: " + pendingAcks.snapshot());
        // pendingAcks.remove(packet.hashCode());
        // } else {
        // Packet ackPacket = new Packet(message, srcIP, srcPort, packet.getRelayIP(),
        // packet.getRelayPort());
        // System.out.println("P2P sending ack packet: " +
        // packet.getMessage().toString() + " to: " + packet.getRelayPort());
        // try {
        // P2PSend(ackPacket);
        // } catch (IOException e) {
        // throw new IOException(e.getMessage());
        // }
        // }

    }
}