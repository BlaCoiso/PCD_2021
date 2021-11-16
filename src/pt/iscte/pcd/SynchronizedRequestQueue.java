package pt.iscte.pcd;

import java.util.Queue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class SynchronizedRequestQueue<T> {
    private final Queue<T> requests;
    private int pendingRequests;
    private final int totalRequests;
    private final Lock lock;
    private final Condition requestCondition;
    private final Condition completeCondition;
    private int currentWorkers;
    
    public SynchronizedRequestQueue(Queue<T> requests, int workerCount) {
        this.requests = requests;
        this.totalRequests = requests.size();
        currentWorkers = workerCount;
        this.lock = new ReentrantLock();
        requestCondition = lock.newCondition();
        completeCondition = lock.newCondition();
    }

    public int getCompletedRequests() {
        lock.lock();
        try {
            return totalRequests - requests.size() + pendingRequests;
        } finally {
            lock.unlock();
        }
    }

    public void notifyComplete() {
        lock.lock();
        try {
            if (pendingRequests <= 0)
                throw new IllegalStateException("Tried to notify complete without any pending requests");
            --pendingRequests;
            if (isComplete()) {
                completeCondition.signalAll();
                requestCondition.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    public boolean isComplete() {
        lock.lock();
        try {
            return pendingRequests + requests.size() == 0;
        } finally {
            lock.unlock();
        }
    }

    public T poll() throws InterruptedException {
        lock.lock();
        try {
            while (!isComplete() && requests.isEmpty()) requestCondition.await();
            if (requests.isEmpty()) return null;
            pendingRequests++;
            return requests.poll();
        } finally {
            lock.unlock();
        }
    }

    public void retryFailed(T req) {
        lock.lock();
        try {
            requests.add(req);
            --pendingRequests;
            requestCondition.signal();
        } finally {
            lock.unlock();
        }
    }

    public void await() throws InterruptedException {
        lock.lock();
        try {
            while (!isComplete() && currentWorkers > 0) completeCondition.await();
        } finally {
            lock.unlock();
        }
    }

    public void notifyWorkerEnd() {
        lock.lock();
        try {
            if (currentWorkers <= 0) {
                throw new IllegalStateException("Tried to notify worker end when no workers are running");
            }
            currentWorkers--;
            if (currentWorkers == 0) completeCondition.signalAll();
        } finally {
            lock.unlock();
        }
    }
}
