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

    public static void main(String[] args) throws InterruptedException, SocketException {
        Parser parser = new Parser(args);
        parser.parse();

        System.out.println("Doing some initialization\n");

        String conf = "";
        ArrayList<String> proposals = new ArrayList<>();

        try {
            File myObj = new File(parser.config());
            Scanner scanner = new Scanner(myObj);
            conf = scanner.nextLine();

            while (scanner.hasNextLine()) {
                proposals.add(scanner.nextLine());
            }
            scanner.close();
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }

        String[] config = conf.split(" ");

        int numProposal = Integer.parseInt(config[0]);
        // int vs = Integer.parseInt(config[1]);
        // int ds = Integer.parseInt(config[2]);

        Host host = parser.hosts().get(parser.myId() - 1);

        // Exclude the node itself ---- Don't deliver locally
        ArrayList<Host> peers = new ArrayList<>(parser.hosts());
        peers.remove(host);

        System.out.println("Initializing node\n");
        Node node;

        try {
            node = new Node(host, parser.output(), peers, numProposal);

            node.start();

            for (int i = 1; i < numProposal + 1; i++) {
                node.broadcastNewMessage(proposals.get(i - 1));
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
