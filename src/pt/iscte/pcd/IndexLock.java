package pt.iscte.pcd;

import java.util.HashMap;
import java.util.Map;

public class IndexLock {
    private final Map<Integer, IndexEntry> indices = new HashMap<>();

    private static class IndexEntry {
        private final Thread lockerThread;
        private int lockCount;

        private IndexEntry(Thread lockerThread) {
            this.lockerThread = lockerThread;
            this.lockCount = 1;
        }
    }

    public synchronized boolean tryLock(int index) {
        IndexEntry entry = indices.get(index);
        if (entry == null) {
            indices.put(index, new IndexEntry(Thread.currentThread()));
            return true;
        } else if (entry.lockerThread.equals(Thread.currentThread())) {
            entry.lockCount++;
            return true;
        }
        return false;
    }

    public synchronized void lock(int index) throws InterruptedException {
        while (!tryLock(index)) wait();
    }

    public synchronized boolean isLocked(int index) {
        return indices.containsKey(index);
    }

    public synchronized void unlock(int index) {
        IndexEntry entry = indices.get(index);
        if (entry == null) throw new IllegalStateException("Tried to unlock already unlocked index " + index);
        if (!entry.lockerThread.equals(Thread.currentThread()))
            throw new IllegalStateException("Thread tried to unlock index " + index + " locked by another thread");
        entry.lockCount--;
        if (entry.lockCount <= 0) indices.remove(index);
        notifyAll();
    }
}
