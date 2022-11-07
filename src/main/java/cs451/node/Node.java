package cs451.node;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import cs451.broadcast.FIFOBroadcast;

public class Node implements NodeInterface {
    private final byte pid;
    private final String outputPath;
    private final Queue<String> newMessages;

    private final FIFOBroadcast fifoBroadcast;

    Thread listeningThread;
    Thread sendThread;
    Thread waitForAckThread;

    // Can be extended using a list of hosts, instead of a single receiver (destIP,
    // destPort)
    public Node(Host host, String outputPath, ArrayList<Host> peers) throws SocketException {
        System.out.println("Node IP: " + host.getIp() + " port: " + host.getPort());
        this.outputPath = outputPath;

        // pid in [1, 128] shift of -1 so that it fits in a byte
        this.pid = Integer.valueOf(host.getId() - 1).byteValue();
        this.newMessages = new ConcurrentLinkedQueue<>();

        try {
            this.fifoBroadcast = new FIFOBroadcast(pid, host.getIp(), host.getPort(), peers);
        } catch (SocketException e) {
            throw new SocketException("Error while creating node: " + e.getMessage());
        }

        listeningThread = new Thread(() -> {
            System.out.println("Pid: " + Integer.valueOf(pid + 1).toString() + " starting to listen");

            try {
                fifoBroadcast.deliver();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        });

        waitForAckThread = new Thread(() -> {
            System.out.println("Pid: " + Integer.valueOf(pid + 1).toString() + " starting broadcast");

            try {
                fifoBroadcast.waitForAck();
            } catch (IOException | InterruptedException e) {
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

        initSignalHandlers();
    }

    public void start() {
        System.out.println("Starting node");
        listeningThread.start();

        System.out.println("pid: " + pid);
        if (pid != 1) {
            sendThread.start();
            waitForAckThread.start();
        }
    }

    @Override
    public void broadcastNewMessage(String payload) {
        newMessages.offer(payload);
    }

    public void initSignalHandlers() {
        Runtime.getRuntime().addShutdownHook(new Thread(this::writeOutput));
    }

    public void writeOutput() {
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
