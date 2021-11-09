package pt.iscte.pcd;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class StorageNode {
    private static final int DATA_SIZE = 1000000;
    private final String directoryAddress;
    private final int directoryPort;
    private final int nodePort;
    private Socket directorySocket = null;
    private PrintWriter directoryWriter = null;
    private BufferedReader directoryReader = null;
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
        InetSocketAddress[] nodes = getNodes();
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
            DownloaderThread thread = new DownloaderThread(node, requestLatch, requests);
            thread.start();
        }

        try {
            requestLatch.await();
        } catch (InterruptedException e) {
            //This should never throw since there's nothing interrupting this call
            throw new IllegalStateException("requestData interrupted");
        }
    }

    //TODO: Refactor to outer class
    private InetSocketAddress[] getNodes() {
        directoryWriter.println("nodes");
        List<String> results = new ArrayList<>();
        String line;
        try {
            while ((line = directoryReader.readLine()).compareToIgnoreCase("END") != 0) results.add(line);
        } catch (IOException e) {
            System.err.println("Failed to get list of nodes");
            e.printStackTrace();
            return null;
        }
        List<InetSocketAddress> addresses = new ArrayList<>(results.size());
        for (String v : results) {
            String[] split = v.split(" ");
            if (split.length == 3 && split[0].compareToIgnoreCase("node") == 0) {
                try {
                    InetSocketAddress addr = new InetSocketAddress(split[1], Integer.parseInt(split[2]));
                    // If no address or this node is self then ignore
                    if (addr.isUnresolved() || (addr.getPort() == nodePort && addr.getAddress().equals(directorySocket.getLocalAddress())))
                        continue;
                    addresses.add(addr);
                } catch (NumberFormatException ignored) {
                }
            }
        }
        return addresses.toArray(new InetSocketAddress[0]);
    }

    private void register() {
        try {
            directorySocket = new Socket(directoryAddress, directoryPort);
            directoryWriter = new PrintWriter(new OutputStreamWriter(directorySocket.getOutputStream()), true);
            directoryWriter.printf("INSC %s %d\n", directorySocket.getLocalAddress().getHostAddress(), nodePort);
            directoryReader = new BufferedReader(new InputStreamReader(directorySocket.getInputStream()));
        } catch (IOException e) {
            e.printStackTrace();
            if (directorySocket != null && !directorySocket.isClosed()) {
                try {
                    directorySocket.close();
                } catch (IOException ignored) {
                }
            }
            throw new IllegalArgumentException("Failed to register in directory");
        }
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
    }

    private void start() {
        register();
        if (!dataInitialized) requestData();
        try {
            ServerSocket nodeSocket = new ServerSocket(nodePort);
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
                    if (req instanceof ByteBlockRequest) {
                        ByteBlockRequest request = (ByteBlockRequest) req;
                        int start = request.startIndex;
                        int end = start + request.length;
                        if (start >= 0 && start < end) {
                            CloudByte[] dataToSend = Arrays.copyOfRange(data, start, end);
                            outStream.writeObject(dataToSend);
                        }
                    }
                } catch (IOException | ClassNotFoundException e) {
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

    private class DownloaderThread extends Thread {
        private final InetSocketAddress nodeAddr;
        private final CountDownLatch requestLatch;
        private final Queue<ByteBlockRequest> requestQueue;

        private DownloaderThread(InetSocketAddress nodeAddr, CountDownLatch requestLatch, Queue<ByteBlockRequest> requestQueue) {
            this.nodeAddr = nodeAddr;
            this.requestLatch = requestLatch;
            this.requestQueue = requestQueue;
        }

        @Override
        public void run() {
            try (Socket nodeSocket = new Socket(nodeAddr.getAddress(), nodeAddr.getPort())) {
                ObjectInputStream inStream = new ObjectInputStream(nodeSocket.getInputStream());
                ObjectOutputStream outStream = new ObjectOutputStream(nodeSocket.getOutputStream());

                int transferCount = 0;
                //TODO: Implement concurrent queue
                ByteBlockRequest request;
                do {
                    synchronized (requestQueue) {
                        request = requestQueue.isEmpty() ? null : requestQueue.poll();
                    }
                    if (request == null) break;
                    //FIXME: Deadlock when a thread fails to download a chunk
                    try {
                        outStream.writeObject(request);
                        //FIXME: Possible crash when remote sends wrong type data
                        CloudByte[] result = (CloudByte[]) inStream.readObject();
                        synchronized (data) {
                            System.arraycopy(result, 0, data, request.startIndex, request.length);
                        }
                        requestLatch.countDown();
                        transferCount++;
                    } catch (IOException | ClassNotFoundException e) {
                        System.err.println("Failed to receive request");
                        e.printStackTrace();
                        break;
                    }
                } while (true);
                System.out.println("transferCount = " + transferCount);
            } catch (IOException e) {
                System.err.println("Failed to open object streams for downloader thread");
                e.printStackTrace();
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
            //TODO: Launch another thread for console input for error injection
            node.start();
        } catch (NumberFormatException e) {
            System.err.println("Port numbers must be integers");
        }
    }
}
