package cs451.node;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import cs451.broadcast.FIFOBroadcast;
import cs451.messages.Packet;

public class Node implements NodeInterface {
    private final byte pid;
    private final String outputPath;
    // private final int numMessage;
    private final Queue<String> newMessages;

    private final FIFOBroadcast fifoBroadcast;

    Thread deliverThread;
    Thread sendThread;
    Thread IPCThread;
    Thread heartbeatThread;
    Thread waitForAckThread;

    // Can be extended using a list of hosts, instead of a single receiver (destIP,
    // destPort)
    public Node(Host host, String outputPath, ArrayList<Host> peers, int numMessage) throws IOException {
        // pid in [1, 128] shift of -1 so that it fits in a byte
        this.pid = Integer.valueOf(host.getId() - 1).byteValue();
        this.newMessages = new ConcurrentLinkedQueue<>();
        this.outputPath = outputPath;

        String srcIP = host.getIp();
        int srcPort = host.getPort();

        System.out.println("Node IP: " + srcIP + " port: " + srcPort);

        try {
            this.fifoBroadcast = new FIFOBroadcast(pid, srcIP, srcPort, peers);
        } catch (SocketException e) {
            throw new SocketException("Error while creating node: " + e.getMessage());
        }

        deliverThread = new Thread(() -> {
            System.out.println("Pid: " + Integer.valueOf(pid + 1).toString() + " starting to listen");

            try {
                fifoBroadcast.channelDeliver();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        sendThread = new Thread(() -> {
            while (true) {
                // pop last message
                String newMessage = newMessages.poll();

                // send last message
                if (newMessage != null) {
                    try {
                        fifoBroadcast.broadcast(newMessage);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        });

        IPCThread = new Thread(() -> {
            while (true) {
                Packet packet = fifoBroadcast.getDeliverQueue().poll();

                if (packet != null) {
                    try {
                        fifoBroadcast.bebDeliver(packet);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        });

        heartbeatThread = new Thread(() -> {
            while (true) {
                fifoBroadcast.heartbeat();

                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                }
            }
        });

        waitForAckThread = new Thread(() -> {
            while (true) {
                try {
                    fifoBroadcast.waitForAck();

                    Thread.sleep(500);
                } catch (IOException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        initSignalHandlers();
    }

    // Num threads: 7
    // Main
    // Interrupt
    // Send
    // Deliver
    // Heartbeat
    // Ack
    // IPC

    @Override
    public void start() {
        System.out.println("Starting node");
        // Main thread (application thread)
        // Interrupt thread
        sendThread.start();
        deliverThread.start();
        IPCThread.start();
        heartbeatThread.start();
        waitForAckThread.start();
    }

    @Override
    public void broadcastNewMessage(String payload) {
        newMessages.offer(payload);
    }

    public void initSignalHandlers() {
        Runtime.getRuntime().addShutdownHook(new Thread(this::writeOutput));
    }

    @Override
    public void writeOutput() {
        System.out.println("Immediately stopping network packet processing.");

        // write/flush output file if necessary
        System.out.println("Writing output.");
        ArrayList<String> logs = fifoBroadcast.getLogs();

        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath));

            for (String s : logs) {
                writer.write(s);
            }

            writer.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
