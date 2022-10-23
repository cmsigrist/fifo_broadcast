package cs451.node;

import cs451.link.PerfectLink;
import cs451.messages.LightMessage;
import cs451.utils.AckTimer;
import cs451.utils.AckTimerTask;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Node implements NodeInterface {
    private final byte pid;
    private final PerfectLink p2pLink;
    private final boolean isSender;
    private final String outputPath;
    private final Queue<LightMessage> newMessages;

    AckTimerTask ackTimerTask;
    Thread listeningThread;
    Thread sendThread;

    // Can be extended using a list of hosts, instead of a single receiver (destIP,
    // destPort)
    public Node(Host host, int destID, String outputPath) throws SocketException {
        System.out.println("Node IP: " + host.getIp() + " port: " + host.getPort());
        this.outputPath = outputPath;
        this.pid = Integer.valueOf(host.getId()).byteValue();
        this.isSender = pid != destID;
        this.newMessages = new ConcurrentLinkedQueue<>();
        this.ackTimerTask = new AckTimerTask();

        try {
            this.p2pLink = new PerfectLink(pid, host.getIp(), host.getPort(), ackTimerTask);
        } catch (SocketException e) {
            throw new SocketException("Error while creating node: " + e.getMessage());
        }

        listeningThread = new Thread(() -> {
            System.out.println("Pid: " + pid + " starting to listen");

            try {
                p2pLink.deliver();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        });

        sendThread = new Thread(() -> {
            while (true) {
                // pop last message
                LightMessage newMessage = newMessages.poll();

                // send last message
                if (newMessage != null) {
                    try {
                        p2pLink.send(newMessage);
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
        AckTimer.startTimer(ackTimerTask);
        listeningThread.start();

        if (isSender) {
            sendThread.start();
        }
    }

    public void sendNewMessage(String payload, String destIP, int destPort) {
        newMessages.offer(new LightMessage(payload, destIP, destPort));
    }

    public void initSignalHandlers() {
        Runtime.getRuntime().addShutdownHook(new Thread(this::writeOutput));
    }

    public void writeOutput() {
        ArrayList<String> logs = p2pLink.getLogs();

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
