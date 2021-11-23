package pt.iscte.pcd;

import java.io.IOException;
import java.net.InetSocketAddress;

public class ByteCorrectionThread extends Thread {
    private final InetSocketAddress nodeAddr;
    private CloudByte data = null;
    private final int index;
    private final CountDownLatch latch;

    public ByteCorrectionThread(InetSocketAddress nodeAddr, int index, CountDownLatch latch) {
        this.nodeAddr = nodeAddr;
        this.index = index;
        this.latch = latch;
    }

    @Override
    public void run() {
        try (StorageClient client = new StorageClient(nodeAddr)) {
            CloudByte[] result = client.requestData(index, 1);
            if (result != null) {
                data = result[0];
                if (data.isParityOk()) latch.countDown();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public CloudByte getData() {
        return data;
    }
}
