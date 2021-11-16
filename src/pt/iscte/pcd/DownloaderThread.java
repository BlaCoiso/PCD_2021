package pt.iscte.pcd;

import java.io.IOException;
import java.net.InetSocketAddress;

public class DownloaderThread extends Thread {
    private final InetSocketAddress nodeAddr;
    private final SynchronizedRequestQueue<ByteBlockRequest> requestQueue;
    private final CloudByte[] data;
    private int completedRequests = 0;

    public DownloaderThread(InetSocketAddress nodeAddr, SynchronizedRequestQueue<ByteBlockRequest> requestQueue, CloudByte[] data) {
        this.nodeAddr = nodeAddr;
        this.requestQueue = requestQueue;
        this.data = data;
    }

    @Override
    public void run() {
        try (StorageClient client = new StorageClient(nodeAddr)) {
            ByteBlockRequest request;
            while ((request = requestQueue.poll()) != null) {
                try {
                    CloudByte[] result = client.requestData(request);
                    if (result == null) {
                        System.err.println("Failed to process data from node");
                        requestQueue.retryFailed(request);
                        break;
                    }
                    requestQueue.notifyComplete();
                    synchronized (data) {
                        System.arraycopy(result, 0, data, request.startIndex, request.length);
                    }
                    completedRequests++;
                } catch (IOException e) {
                    System.err.println("Failed to receive requested data");
                    e.printStackTrace();
                    requestQueue.retryFailed(request);
                    break;
                }
            }
            System.out.println("Completed requests for node " + client + ": " + completedRequests);
        } catch (IOException e) {
            System.err.println("Failed to connect storage client for downloader thread");
            e.printStackTrace();
        } catch (InterruptedException e) {
            System.err.println("Downloader thread interrupted");
            e.printStackTrace();
        }
        requestQueue.notifyWorkerEnd();
    }

    public int getCompletedRequests() {
        return completedRequests;
    }
}