package cs451;

import java.io.File;
import java.io.IOException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import cs451.node.Host;
import cs451.node.Node;
import cs451.parser.Parser;

public class Main {

    public static void main(String[] args) throws InterruptedException, SocketException {
        Parser parser = new Parser(args);
        parser.parse();

        System.out.println("Doing some initialization\n");

        Host host = parser.hosts().get(parser.myId() - 1);

        // Exclude the node itself ---- Don't deliver locally
        ArrayList<Host> peers = new ArrayList<>(parser.hosts());
        peers.remove(host);

        final ReentrantLock lock = new ReentrantLock();
        final Condition full = lock.newCondition();
        final AtomicInteger step = new AtomicInteger();
        step.set(1);

        System.out.println("Initializing node\n");
        Node node;

        try {
            File myObj = new File(parser.config());
            Scanner scanner = new Scanner(myObj);
            // config
            String conf = scanner.nextLine();
            String[] config = conf.split(" ");
            int numProposal = Integer.parseInt(config[0]);

            node = new Node(host, parser.output(), peers, numProposal, step, lock, full);
            node.start();

            System.out.println("Broadcasting and delivering messages...\n");

            while (scanner.hasNextLine()) {
                int currentStep = step.get();
                node.broadcastNewMessage(scanner.nextLine());

                lock.lock();
                try {
                    while (currentStep == step.get()) {
                        full.await(1, TimeUnit.SECONDS);
                        System.out.println("WOKE UP!");
                    }
                } finally {
                    lock.unlock();
                }
            }
            scanner.close();

            while (true) {
                // Sleep for 1 hour
                Thread.sleep(60 * 60 * 1000);
            }
        } catch (IOException e) {
            System.out.println("Error while creating the node");
        }
    }
}
