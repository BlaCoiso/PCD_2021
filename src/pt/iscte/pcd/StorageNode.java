package pt.iscte.pcd;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Queue;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;

public class StorageNode {
    private static final int DATA_SIZE = 1000000;
    private final String directoryAddress;
    private final int directoryPort;
    private int nodePort;
    private DirectoryClient directory = null;
    private final CloudByte[] data = new CloudByte[DATA_SIZE];
    private boolean dataInitialized = false;

    public StorageNode(String directoryAddress, int directoryPort, int nodePort, String dataFilePath) {
        this.directoryAddress = directoryAddress;
        this.directoryPort = directoryPort;
        this.nodePort = nodePort;

        //nodePort == 0 -> system assigned port
        if (directoryPort <= 0 || nodePort < 0 || directoryPort > 0xFFFF || nodePort > 0xFFFF) {
            throw new IllegalArgumentException("Port numbers must be between 0 and " + 0xFFFF);
        }
        if (dataFilePath != null) {
            try {
                readData(dataFilePath);
            } catch (IOException e) {
                e.printStackTrace();
                throw new IllegalArgumentException("Couldn't read data file");
            }
        }
    }

    private void requestData() {
        requestData(0, DATA_SIZE);
    }

    //TODO: Refactor to outer class
    private void requestData(int start, int length) {
        InetSocketAddress[] nodes = directory.getNodes();
        if (nodes == null || nodes.length == 0) throw new IllegalStateException("Failed to find nodes");
        Queue<ByteBlockRequest> requests;
        if (length < ByteBlockRequest.BLOCK_LENGTH) {
            ByteBlockRequest req = new ByteBlockRequest(start, length);
            requests = new ArrayDeque<>(1);
            requests.add(req);
        } else {
            requests = new ArrayDeque<>(length / ByteBlockRequest.BLOCK_LENGTH);
            int end = start + length;
            for (int i = start; i < end; i += ByteBlockRequest.BLOCK_LENGTH) {
                ByteBlockRequest req = new ByteBlockRequest(i, Integer.min(end - start, ByteBlockRequest.BLOCK_LENGTH));
                requests.add(req);
            }
        }
        System.out.println("Sending " + requests.size() + " requests to " + nodes.length + " nodes");
        CountDownLatch requestLatch = new CountDownLatch(requests.size());

        for (InetSocketAddress node : nodes) {
            System.out.println("Starting download thread for node " + node.getAddress().getHostAddress() + ":" + node.getPort());
            DownloaderThread thread = new DownloaderThread(node, requestLatch, requests, data);
            thread.start();
        }

        try {
            requestLatch.await();
        } catch (InterruptedException e) {
            //This should never throw since there's nothing interrupting this call
            throw new IllegalStateException("requestData interrupted");
        }
    }

    private void register() throws IOException {
        directory = new DirectoryClient(directoryAddress, directoryPort, nodePort);
    }

    private void readData(String dataFilePath) throws IOException {
        File dataFile = new File(dataFilePath);
        if (!dataFile.exists() || !dataFile.isFile() || !dataFile.canRead()) {
            throw new IllegalArgumentException("File is invalid or cannot be read");
        }
        byte[] bytes = Files.readAllBytes(dataFile.toPath());
        if (bytes.length != DATA_SIZE) throw new AssertionError("Expected bytes.length == " + DATA_SIZE);
        for (int i = 0; i < DATA_SIZE; ++i) {
            data[i] = new CloudByte(bytes[i]);
        }
        dataInitialized = true;
        System.out.println("Loaded data from file: \"" + dataFilePath + "\"");
    }

    private void start() {
        try (ServerSocket nodeSocket = new ServerSocket(nodePort)) {
            nodePort = nodeSocket.getLocalPort();
            register();
            if (!dataInitialized) requestData();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                if (directory != null) {
                    try {
                        System.out.println("Closing sockets...");
                        directory.close();
                        nodeSocket.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }));

            ErrorInjectionThread errorInjectionThread = new ErrorInjectionThread();
            errorInjectionThread.start();

            //noinspection InfiniteLoopStatement
            while (true) {
                Socket sock = nodeSocket.accept();
                try {
                    Thread nodeThread = new NodeThread(sock);
                    nodeThread.start();
                } catch (IOException e) {
                    System.err.println("Failed to start node thread");
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            System.err.println("Failed to start node socket");
            e.printStackTrace();
        }
        try {
            directory.close();
        } catch (IOException e) {
            System.err.println("Failed to close directory client");
            e.printStackTrace();
        }
    }

    private class NodeThread extends Thread {
        private final Socket socket;
        private final ObjectOutputStream outStream;
        private final ObjectInputStream inStream;

        public NodeThread(Socket socket) throws IOException {
            this.socket = socket;
            try {
                this.outStream = new ObjectOutputStream(socket.getOutputStream());
                this.inStream = new ObjectInputStream(socket.getInputStream());
            } catch (IOException e) {
                socket.close();
                throw e;
            }
        }

        @Override
        public void run() {
            while (true) {
                try {
                    Object req = inStream.readObject();
                    boolean sentData = false;
                    if (req instanceof ByteBlockRequest) {
                        ByteBlockRequest request = (ByteBlockRequest) req;
                        int start = request.startIndex;
                        int end = start + request.length;
                        if (start >= 0 && start < end && end <= data.length) {
                            CloudByte[] dataToSend = Arrays.copyOfRange(data, start, end);
                            outStream.writeObject(dataToSend);
                            sentData = true;
                        }
                    }
                    //Prevent remote from blocking if incorrect request received
                    if (!sentData) outStream.writeObject(null);
                } catch (IOException | ClassNotFoundException e) {
                    // EOFException = no more requests
                    if (!(e instanceof EOFException)) e.printStackTrace();
                    break;
                }
            }
            try {
                socket.close();
            } catch (IOException e) {
                System.err.println("Failed to close socket");
                e.printStackTrace();
            }
        }
    }

    private class ErrorInjectionThread extends Thread {
        private final Scanner scanner = new Scanner(System.in);

        public void run() {
            //noinspection InfiniteLoopStatement
            while (true) {
                String input = scanner.nextLine();
                if (input == null || input.isEmpty()) continue;
                String[] inputSplit = input.split(" ");
                if (inputSplit.length != 2 || !inputSplit[0].equalsIgnoreCase("ERROR")) {
                    System.out.println("Invalid input, please insert ERROR <byte_num>");
                    continue;
                }
                try {
                    int errorPosition = Integer.parseInt(inputSplit[1]);
                    if (errorPosition < 0 || errorPosition >= DATA_SIZE) {
                        System.err.println("Please enter a position between 0 and " + (DATA_SIZE - 1));
                        continue;
                    }
                    data[errorPosition].makeByteCorrupt();
                    System.out.println("Is parity ok? " + data[errorPosition].isParityOk());
                    System.out.println("Successful error insertion");
                } catch (NumberFormatException e) {
                    System.err.println("Invalid position for error insertion");
                }
            }
        }
    }

    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Usage: StorageNode <directoryAddr> <directoryPort> <nodePort> [file]");
            return;
        }
        String addr = args[0];
        String dirPortString = args[1];
        String nodePortString = args[2];
        String dataFilePath = args.length > 3 ? args[3] : null;
        if (dataFilePath != null && dataFilePath.isEmpty()) dataFilePath = null;

        try {
            int dirPort = Integer.parseInt(dirPortString);
            int nodePort = Integer.parseInt(nodePortString);
            StorageNode node = new StorageNode(addr, dirPort, nodePort, dataFilePath);
            node.start();
        } catch (NumberFormatException e) {
            System.err.println("Port numbers must be integers");
        }
    }
}
