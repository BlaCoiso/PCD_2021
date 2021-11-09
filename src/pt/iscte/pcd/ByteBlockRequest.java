package pt.iscte.pcd;

import java.io.Serializable;

public class ByteBlockRequest implements Serializable {
    public static final int BLOCK_LENGTH = 100;
    public final int startIndex;
    public final int length;

    public ByteBlockRequest(int startIndex, int length) {
        this.startIndex = startIndex;
        this.length = length;
    }
}
