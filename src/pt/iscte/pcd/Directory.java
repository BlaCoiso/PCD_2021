package pt.iscte.pcd;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class Directory {
    private final Set<NodeEntry> nodes;
    private final ServerSocket socket;

    public Directory(int port) throws IOException {
        socket = new ServerSocket(port);
        nodes = new HashSet<>();
    }

    public void start() {
        System.out.println("Starting directory on port " + socket.getLocalPort());
        //noinspection InfiniteLoopStatement
        while (true) {
            try {
                Socket s = socket.accept();
                new NodeConnectionThread(s).start();
            } catch (IOException e) {
                System.err.println("Failed to accept connection from client");
                e.printStackTrace();
            }
        }
    }

    public static class NodeEntry {
        public final String address;
        public final int port;

        private NodeEntry(String address, int port) {
            if (address == null || address.isEmpty() || address.startsWith("/"))
                throw new IllegalArgumentException("Invalid address");
            this.address = address;
            if (port <= 0 || port > 0xFFFF) throw new IllegalArgumentException("Invalid port number");
            this.port = port;
        }

        @Override
        public String toString() {
            return "node " + address + ' ' + port;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            NodeEntry nodeEntry = (NodeEntry) o;
            return port == nodeEntry.port && address.equals(nodeEntry.address);
        }

        @Override
        public int hashCode() {
            return Objects.hash(address, port);
        }
    }

    private class NodeConnectionThread extends Thread {
        private final Socket socket;

        public NodeConnectionThread(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            NodeEntry entry = null;
            try {
                System.out.println("New connection from " + socket.getInetAddress().getHostAddress());
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                String registerMessage = in.readLine();
                if (registerMessage == null || registerMessage.isEmpty()) return;
                String[] registerSplit = registerMessage.split(" ");
                if (registerSplit.length != 3 || !registerSplit[0].equalsIgnoreCase("INSC")) {
                    System.err.println("Node sent incorrect register message: got " + registerMessage);
                    return;
                }
                String nodeAddress = registerSplit[1];
                int nodePort = Integer.parseInt(registerSplit[2]);
                entry = new NodeEntry(nodeAddress, nodePort);
                synchronized (nodes) {
                    if (nodes.contains(entry)) {
                        System.err.println("Node with address " + entry.address + " and port " + entry.port + " was already registered!");
                        entry = null;
                        return;
                    }
                    nodes.add(entry);
                    System.out.println("Registered " + entry);
                }
                String line;
                while ((line = in.readLine()) != null) {
                    System.out.println("Message from node: " + line);
                    if (line.equalsIgnoreCase("nodes")) {
                        synchronized (nodes) {
                            for (NodeEntry node : nodes) out.println(node.toString());
                        }
                        out.println("end");
                    }
                }
            } catch (IOException e) {
                System.err.println("Failed to process node connection");
                e.printStackTrace();
            } catch (IllegalArgumentException e) {
                System.err.println("Node sent invalid value: " + e.getMessage());
            } finally {
                synchronized (nodes) {
                    if (entry != null) {
                        nodes.remove(entry);
                        System.out.println("Removed " + entry);
                    }
                }
                if (!socket.isClosed()) {
                    try {
                        socket.close();
                    } catch (IOException ignored) {
                    }
                }
            }
        }
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Required argument: port number");
            return;
        }
        try {
            int port = Integer.parseInt(args[0]);
            if (port < 0 || port > 0xFFFF) {
                System.err.println("Port numbers must be between 0 and " + 0xFFFF);
                return;
            }
            new Directory(port).start();
        } catch (NumberFormatException e) {
            System.err.println("Port number must be an integer");
        } catch (IOException e) {
            System.err.println("Failed to start directory (port might be in use)");
            e.printStackTrace();
        }
    }
}
