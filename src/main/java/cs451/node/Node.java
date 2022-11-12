package cs451.node;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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

    // Can be extended using a list of hosts, instead of a single receiver (destIP,
    // destPort)
    public Node(Host host, String outputPath, ArrayList<Host> peers, int numMessage) throws IOException {
        // pid in [1, 128] shift of -1 so that it fits in a byte
        this.pid = Integer.valueOf(host.getId() - 1).byteValue();
        this.newMessages = new ConcurrentLinkedQueue<>();
        this.outputPath = outputPath;
        // this.numMessage = numMessage;

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

        // TODO comment to use only 4 threads
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

        initSignalHandlers();
    }

    // Num threads: 6 (-> can be reduced to 5 if combining Main & Send)
    // Main
    // Interrupt
    // Send
    // Deliver
    // Heartbeat & Ack
    // Subscriber & Publisher

    @Override
    public void start() {
        System.out.println("Starting node");

        sendThread.start();
        deliverThread.start();
        IPCThread.start();

        ScheduledExecutorService service;
        service = Executors.newSingleThreadScheduledExecutor();
        service.scheduleAtFixedRate(() -> {
            try {
                fifoBroadcast.waitForAck();
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }, 500, 400, TimeUnit.MILLISECONDS);

        // TODO parameterize rate (1 milli -> executes after each send)
        service.scheduleAtFixedRate(() -> {
            try {
                fifoBroadcast.heartbeat();
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }, 0, 500, TimeUnit.MILLISECONDS);

        // TODO uncomment to use only 4 threads
        // for (int i = 1; i < numMessage + 1; i++) {
        // newMessages.offer(String.valueOf(i));
        // }

        // System.out.println("Broadcasting and delivering messages...\n");
        // Send on main thread
        // while (true) {
        // // pop last message
        // String newMessage = newMessages.poll();

        // // send last message
        // if (newMessage != null) {
        // try {
        // fifoBroadcast.broadcast(newMessage);
        // } catch (IOException e) {
        // throw new RuntimeException(e);
        // }
        // }
        // }

        // Interrupt thread
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
