package cs451.link;

import cs451.messages.LightMessage;
import cs451.messages.Message;
import cs451.messages.MessageType;
import cs451.network.UDPChannel;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

public class PerfectLink implements LinkInterface {
    // pid associated with the source of the link
    private final byte pid;
    // UDP channel associated to the link
    private final UDPChannel UDPChannel;
    private final HashMap<Integer, Message> sent;
    private final ReentrantLock SLock;
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
        this.sent = new HashMap<>();
        this.logs = new ArrayList<>();

        this.SLock = new ReentrantLock();
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
            SLock.lock();

            try {
                sent.put(seqNum, message);
                System.out.println("Pid: " + pid + " sent packet: " + seqNum);
            } finally {
                SLock.unlock();
            }
        }
    }

    // Sends all the message
    public void sendAll() throws IOException, InterruptedException {
        ArrayList<Message> toSend;

        while (true) {
            // Get the latest copy of sent
            SLock.lock();
            try {
                toSend = new ArrayList<>(sent.values());
            } finally {
                SLock.unlock();
            }

            System.out.println("toSend: " + toSend);

            for (Message m : toSend) {
                try {
                    P2PSend(m);
                } catch (IOException e) {
                    throw new IOException("Error while sending message: " + e.getMessage());
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
        DatagramPacket d = new DatagramPacket(packet, packet.length, InetAddress.getByName(message.getDestIP()), message.getDestPort());

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
                    SLock.lock();
                    try {
                        sent.remove(message.getSeqNum());
                    } finally {
                        SLock.unlock();
                    }

                    LLock.lock();
                    try {
                        String log = message.broadcast();
                        if (!logs.contains(log)) {
                            logs.add(log);
                        }
                    } finally {
                        LLock.unlock();
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
            P2PSend(ackMessage);
        } catch (IOException e) {
            System.out.println("Error: " + e.getMessage());
            throw new IOException(e.getMessage());
        }
    }

    public ArrayList<String> getLogs() {
        return new ArrayList<>(logs);
    }
}
