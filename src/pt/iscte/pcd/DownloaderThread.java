package pt.iscte.pcd;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;

public class DownloaderThread extends Thread {
    private final InetSocketAddress nodeAddr;
    private final CountDownLatch requestLatch;
    private final Queue<ByteBlockRequest> requestQueue;
    private final CloudByte[] data;
    private int completedRequests = 0;

    public DownloaderThread(InetSocketAddress nodeAddr, CountDownLatch requestLatch, Queue<ByteBlockRequest> requestQueue, CloudByte[] data) {
        this.nodeAddr = nodeAddr;
        this.requestLatch = requestLatch;
        this.requestQueue = requestQueue;
        this.data = data;
    }

    @Override
    public void run() {
        try (StorageClient client = new StorageClient(nodeAddr)) {
            //TODO: Implement concurrent queue
            ByteBlockRequest request;
            do {
                synchronized (requestQueue) {
                    request = requestQueue.isEmpty() ? null : requestQueue.poll();
                }
                if (request == null) break;
                //FIXME: Deadlock when a thread fails to download a chunk
                try {
                    CloudByte[] result = client.requestData(request);
                    if (result == null) {
                        System.err.println("Failed to process data from node");
                        break;
                    }
                    synchronized (data) {
                        System.arraycopy(result, 0, data, request.startIndex, request.length);
                    }
                    requestLatch.countDown();
                    completedRequests++;
                } catch (IOException e) {
                    System.err.println("Failed to receive requested data");
                    e.printStackTrace();
                    break;
                }
            } while (true);
            System.out.println("completedRequests = " + completedRequests);
        } catch (IOException e) {
            System.err.println("Failed to open object streams for downloader thread");
            e.printStackTrace();
        }
    }

    public int getCompletedRequests() {
        return completedRequests;
    }
}