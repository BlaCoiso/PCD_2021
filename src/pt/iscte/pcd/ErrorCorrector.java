package pt.iscte.pcd;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class ErrorCorrector {
    private final CloudByte[] data;
    private final IndexLock lock;
    private final DirectoryClient directory;

    public ErrorCorrector(CloudByte[] data, DirectoryClient directory) {
        this.data = data;
        this.directory = directory;
        lock = new IndexLock();
    }

    public boolean tryCorrect(int index) {
        if (data[index].isParityOk()) return true;
        if (lock.tryLock(index)) {
            try {
                return startCorrection(index);
            } catch (InterruptedException e) {
                return false;
            } finally {
                lock.unlock(index);
            }
        }
        return false;
    }

    public boolean correct(int index) throws InterruptedException {
        if (data[index].isParityOk()) return true;
        try {
            lock.lock(index);
            if (data[index].isParityOk()) return true;
            return startCorrection(index);
        } finally {
            lock.unlock(index);
        }
    }

    public boolean isCorrecting(int index) {
        return lock.isLocked(index);
    }

    private boolean startCorrection(int index) throws InterruptedException {
        System.out.println("Finding nodes to correct byte " + index);
        InetSocketAddress[] nodes = directory.getNodes();
        if (nodes == null || nodes.length < 2) return false;
        return correctFrom(index, nodes);
    }

    public boolean correctFrom(int index, InetSocketAddress[] nodes) throws InterruptedException {
        ByteCorrectionThread[] threads = new ByteCorrectionThread[nodes.length];
        CountDownLatch latch = new CountDownLatch(2);
        for (int i = 0; i < nodes.length; i++) {
            System.out.println("Starting ByteCorrectionThread for node " + nodes[i]);
            threads[i] = new ByteCorrectionThread(nodes[i], index, latch);
            threads[i].start();
        }
        //FIXME: Deadlock when threads die
        latch.await();
        List<CloudByte> results = new ArrayList<>();
        for (ByteCorrectionThread thread : threads) {
            CloudByte result = thread.getData();
            if (result != null && result.isParityOk()) results.add(result);
            thread.interrupt();
        }
        CloudByte first = results.remove(0);
        CloudByte second = results.remove(0);
        if (first.value != second.value) {
            System.out.println("Bytes for correction have different values! first = " + first + ", second = " + second);
            return false;
        }
        data[index].value = first.value;
        System.out.println("Thread " + Thread.currentThread().getName() + " corrected error in position: " + index);
        return true;
    }
}
