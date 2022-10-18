package cs451;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.SocketException;
import java.util.Scanner;

import cs451.node.Node;
import cs451.parser.Parser;

public class Main {
    private static final int MAX_THREADS = 256;

    private static void handleSignal() {
        // immediately stop network packet processing
        System.out.println("Immediately stopping network packet processing.");

        // write/flush output file if necessary
        System.out.println("Writing output.");
    }

    private static void initSignalHandlers() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                handleSignal();
            }
        });
    }

    public static void main(String[] args) throws InterruptedException, SocketException {
        Parser parser = new Parser(args);
        parser.parse();

        initSignalHandlers();

        // Give 4 threads per node (2 to send, 1 to listen, 1 for the main)
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
        int destID = Integer.parseInt(config[1]);
        String destIP = parser.hosts().get(destID - 1).getIp();
        int destPort = parser.hosts().get(destID - 1).getPort();

        System.out.println("Initializing node\n");
        Node node = new Node(parser.hosts().get(parser.myId() - 1), destID, parser.output());

        node.start();

        for (int i = 1; i < numMessage + 1; i++) {
            node.sendNewMessage(String.valueOf(i), destIP, destPort);
        }

        System.out.println("Broadcasting and delivering messages...\n");

        while (true) {
            // Sleep for 1 hour
            Thread.sleep(60 * 60 * 1000);
        }
    }
}
