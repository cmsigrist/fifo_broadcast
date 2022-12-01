package cs451.node;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import cs451.lattice.LatticeAgreement;
import cs451.messages.ProposalMessage;

public class Node {
    private final byte pid;
    private final String outputPath;
    private final Queue<String> newMessages;

    private final LatticeAgreement latticeAgreement;

    Thread deliverThread;
    Thread sendThread;
    Thread IPCThread;
    Thread heartbeatThread;
    Thread waitForAckThread;

    // Can be extended using a list of hosts, instead of a single receiver (destIP,
    // destPort)
    public Node(Host host, String outputPath, ArrayList<Host> peers, int numProposal) throws IOException {
        // pid in [1, 128] shift of -1 so that it fits in a byte
        this.pid = Integer.valueOf(host.getId() - 1).byteValue();
        this.outputPath = outputPath;
        // this.toSend = new AtomicInteger();
        this.newMessages = new ConcurrentLinkedQueue<>();

        String srcIP = host.getIp();
        int srcPort = host.getPort();

        System.out.println("Node IP: " + srcIP + " port: " + srcPort);

        try {
            this.latticeAgreement = new LatticeAgreement(pid, srcIP, srcPort, peers, numProposal);
        } catch (SocketException e) {
            throw new SocketException("Error while creating node: " + e.getMessage());
        }

        deliverThread = new Thread(() -> {
            System.out.println("Pid: " + (pid + 1) + " starting to listen");

            while (true) {
                try {
                    latticeAgreement.channelDeliver();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        sendThread = new Thread(() -> {
            while (true) {
                // pop last message
                String newMessage = newMessages.poll();

                // send last message
                if (newMessage != null) {
                    try {
                        latticeAgreement.propose(newMessage);
                    } catch (IOException | InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        });

        IPCThread = new Thread(() -> {
            while (true) {
                ProposalMessage message = latticeAgreement.getDeliverQueue().poll();

                if (message != null) {
                    try {
                        latticeAgreement.deliver(message);
                    } catch (IOException | InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        });

        // heartbeatThread = new Thread(() -> {
        // while (true) {
        // fifoBroadcast.heartbeat();

        // try {
        // Thread.sleep(300);
        // } catch (InterruptedException ignored) {
        // }
        // }
        // });

        waitForAckThread = new Thread(() -> {
            while (true) {
                try {
                    latticeAgreement.waitForAck();

                    Thread.sleep(400);
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

    public void start() {
        System.out.println("Starting node");
        // Main thread (application thread)
        // Interrupt thread
        deliverThread.start();
        sendThread.start();
        IPCThread.start();
        // heartbeatThread.start(); -> Not needed in lattice agreement
        waitForAckThread.start();
    }

    public void broadcastNewMessage(String proposal) {
        // toSend.incrementAndGet();
        newMessages.offer(proposal);
    }

    public void initSignalHandlers() {
        Runtime.getRuntime().addShutdownHook(new Thread(this::writeOutput));
    }

    public void writeOutput() {
        System.out.println("Immediately stopping network packet processing.");

        // write/flush output file if necessary
        System.out.println("Writing output.");
        ArrayList<String> logs = latticeAgreement.getLogs();

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
