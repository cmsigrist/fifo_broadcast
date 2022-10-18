package cs451.network;

import java.io.IOException;
import java.net.*;

public class UDPChannel {
    public static final int MAX_SIZE = 1400;
    private final DatagramSocket socket;

    public UDPChannel(String srcIp, int srcPort) throws SocketException {
        try {
            this.socket = new DatagramSocket(srcPort, InetAddress.getByName(srcIp));
        } catch (SocketException e) {
            throw new SocketException(e.getMessage());
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    public void send(DatagramPacket d) throws IOException {
        try {
            this.socket.send(d);
        } catch (IOException ignored) {
        }
    }

    public DatagramPacket receive() throws IOException {
        byte[] buf = new byte[MAX_SIZE];
        DatagramPacket d = new DatagramPacket(buf, MAX_SIZE);

        try {
            this.socket.receive(d);
        } catch (IOException e) {
            return null;
        }

        return d;
    }
}
