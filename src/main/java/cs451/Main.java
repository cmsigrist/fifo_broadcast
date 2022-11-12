package cs451;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Scanner;

import cs451.node.Host;
import cs451.node.Node;
import cs451.parser.Parser;

public class Main {
    private static final int MAX_THREADS = 170; // Each node uses 6 threads: floor(1024 / 6) = 170 nodes

    // private static void handleSignal() {
    // // immediately stop network packet processing
    // System.out.println("Immediately stopping network packet processing.");

    // // write/flush output file if necessary
    // System.out.println("Writing output.");
    // }

    // private static void initSignalHandlers() {
    // Runtime.getRuntime().addShutdownHook(new Thread() {
    // @Override
    // public void run() {
    // handleSignal();
    // }
    // });
    // }

    public static void main(String[] args) throws InterruptedException, SocketException {
        Parser parser = new Parser(args);
        parser.parse();

        // initSignalHandlers();

        // Give 6 threads per node (1 to send, 1 to listen, 1 for sig handler, 1 for
        // acks, 1 for the main, 1 for heartbeat)
        assert parser.hosts().size() <= MAX_THREADS;

        System.out.println("Doing some initialization\n");

        String conf = "";
        try {
            File myObj = new File(parser.config());
            Scanner scanner = new Scanner(myObj);
            while (scanner.hasNextLine()) {
                conf = scanner.nextLine();
            }
            scanner.close();
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }

        String[] config = conf.split(" ");

        int numMessage = Integer.parseInt(config[0]);

        Host host = parser.hosts().get(parser.myId() - 1);

        // Exclude the node itself ---- Don't deliver locally
        ArrayList<Host> peers = new ArrayList<>(parser.hosts());
        peers.remove(host);

        System.out.println("Initializing node\n");
        Node node;
        try {
            node = new Node(host, parser.output(), peers, numMessage);

            node.start();

            for (int i = 1; i < numMessage + 1; i++) {
                node.broadcastNewMessage(String.valueOf(i));
            }

            System.out.println("Broadcasting and delivering messages...\n");

            while (true) {
                // Sleep for 1 hour
                Thread.sleep(60 * 60 * 1000);
            }
        } catch (IOException e) {
            System.out.println("Error while creating the node");
        }
    }
}
