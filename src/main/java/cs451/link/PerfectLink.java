package cs451.link;

import cs451.network.Message;
import cs451.network.UDPChannel;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

// A perfect link connects two nodes together
public class PerfectLink implements LinkInterface {
    // pid associated with the source of the link
    private final byte pid;
    private final String destIP;
    private final int destPort;
    // UDP channel associated to the link
    private final UDPChannel UDPChannel;
    private final Set<Message> sent;
    private final ReentrantLock SLock;
    private final ArrayList<String> logs;
    private final ReentrantLock LLock;

    private int seqNum = 0;

    public PerfectLink(byte pid, String srcIp, int srcPort, String destIP, int destPort) throws SocketException {
        try {

            this.UDPChannel = new UDPChannel(srcIp, srcPort, destIP, destPort);
        } catch (SocketException e) {
            throw new SocketException(e.getMessage());
        }

        this.pid = pid;
        this.destIP = destIP;
        this.destPort = destPort;
        this.sent = new HashSet<>();
        this.logs = new ArrayList<>();

        this.SLock = new ReentrantLock();
        this.LLock = new ReentrantLock();
    }

    // Sends a single message
    public void send(String payload) throws IOException {
        seqNum += 1;
        Message message = new Message(pid, seqNum, payload);

        try {
            P2PSend(message);

            LLock.lock();
            try {
                logs.add("b " + seqNum + "\n");
            } finally {
                LLock.unlock();
            }
        } catch (IOException e) {
            throw new IOException("Error while sending message: " + e.getMessage());
        } finally {
            SLock.lock();

            try {
                sent.add(message);
                System.out.println("Pid: " + pid + " sent packet: " + seqNum);
            } finally {
                SLock.unlock();
            }
        }
    }

    // Sends all the message
    public void sendAll() throws IOException {
        ArrayList<Message> toSend;

        while (true) {
            // Get the latest copy of sent
            SLock.lock();
            try {
                toSend = new ArrayList<>(sent);
            } finally {
                SLock.unlock();
            }

            for (Message m : toSend) {
                try {
                    P2PSend(m);

                    LLock.lock();
                    try {
                        String log = m.broadcast();
                        if (!logs.contains(log)) {
                            logs.add(log);
                        }
                    } finally {
                        LLock.unlock();
                    }
                } catch (IOException e) {
                    throw new IOException("Error while sending message: " + e.getMessage());
                }
            }
        }
    }

    private void P2PSend(Message message) throws IOException {
        byte[] packet = message.serialize();
        DatagramPacket d = new DatagramPacket(packet, packet.length, InetAddress.getByName(destIP), destPort);

        try {
            UDPChannel.send(d);
        } catch (IOException e) {
            throw new IOException(e.getMessage());
        }
    }

    // This thread loops until SIGTERM or SIGSTOP
    public void deliver() throws IOException {
        while (true) {
            try {
                try {
                    DatagramPacket p = UDPChannel.receive();

                    if (p == null) {
                        continue;
                    }
                    if (p.getPort() < 11000 || p.getPort() > 11999) {
                        continue;
                    }

                    Message message = Message.deSerialize(p);

                    P2PDeliver(message);
                } catch (IOException e) {
                    throw new IOException(e.getMessage());
                }

            } catch (IOException e) {
                throw new IOException("Error while delivering message: " + e.getMessage());
            }
        }
    }

    private void P2PDeliver(Message message) throws IOException {
        LLock.lock();
        try {
            String log = message.delivered();
            if (!logs.contains(log)) {
                logs.add(log);
            }
        } finally {
            LLock.unlock();
        }
    }

    public ArrayList<String> getLogs() {
        return new ArrayList<>(logs);
    }
}
