package pt.iscte.pcd;

import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;

public class StorageClient implements Closeable {
    private final Socket nodeSocket;
    private final ObjectInputStream inStream;
    private final ObjectOutputStream outStream;

    public StorageClient(String nodeAddress, int nodePort) throws IOException {
        this(new Socket(nodeAddress, nodePort));
    }

    public StorageClient(InetAddress nodeAddress, int nodePort) throws IOException {
        this(new Socket(nodeAddress, nodePort));
    }

    public StorageClient(InetSocketAddress nodeAddress) throws IOException {
        this(nodeAddress.getAddress(), nodeAddress.getPort());
    }

    private StorageClient(Socket nodeSocket) throws IOException {
        this.nodeSocket = nodeSocket;
        try {
            this.inStream = new ObjectInputStream(nodeSocket.getInputStream());
            this.outStream = new ObjectOutputStream(nodeSocket.getOutputStream());
        } catch (IOException e) {
            nodeSocket.close();
            throw e;
        }
    }

    @Override
    public void close() throws IOException {
        nodeSocket.close();
    }

    public CloudByte[] requestData(int start, int length) throws IOException {
        return requestData(new ByteBlockRequest(start, length));
    }

    public CloudByte[] requestData(ByteBlockRequest request) throws IOException {
        outStream.writeObject(request);
        try {
            Object result = inStream.readObject();
            if (result instanceof CloudByte[]) {
                CloudByte[] data = (CloudByte[]) result;
                if (data.length != request.length)
                    throw new IOException("Expected " + request.length + " bytes, got " + data.length + " bytes");
                return data;
            }
        } catch (ClassNotFoundException ignored) {
        }
        return null;
    }
}
