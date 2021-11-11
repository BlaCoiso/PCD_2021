package pt.iscte.pcd;

import java.util.Queue;
import java.util.concurrent.CountDownLatch;

public class SynchronizedRequestQueue<T> {
    private final Queue<T> requests;
    private final CountDownLatch latch;
    private int pendingRequests;
    private final int totalRequests;

    public SynchronizedRequestQueue(Queue<T> requests) {
        this.requests = requests;
        this.totalRequests = requests.size();
        this.latch = new CountDownLatch(totalRequests);
    }

    public synchronized int getCompletedRequests() {
        return totalRequests - requests.size() + pendingRequests;
    }

    public synchronized void notifyComplete() {
        if (pendingRequests <= 0)
            throw new IllegalStateException("Tried to notify complete without any pending requests");
        --pendingRequests;
        if (isComplete()) notifyAll();
        latch.countDown();
    }

    public synchronized boolean isComplete() {
        return pendingRequests + requests.size() == 0;
    }

    public synchronized T poll() throws InterruptedException {
        while (!isComplete() && requests.isEmpty()) wait();
        if (requests.isEmpty()) return null;
        pendingRequests++;
        return requests.poll();
    }

    public synchronized void retryFailed(T req) {
        requests.add(req);
        --pendingRequests;
        notify();
    }

    public void await() throws InterruptedException {
        latch.await();
    }
}
