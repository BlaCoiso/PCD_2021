package pt.iscte.pcd;

import java.io.Serializable;

/**
 * Class to represent 7-bit bytes. Extra bit is for parity, and is set in constructor.
 *
 * @author luismota
 */
public class CloudByte implements Serializable {
    byte value;

    public CloudByte(byte value) {
        if (value < 0)
            throw new IllegalArgumentException("Invalid value for CloudByte");
        if (evenParity(value))
            this.value = value;
        else
            this.value = (byte) -value;
    }

    private static boolean evenParity(byte value) {
        return Integer.bitCount(value) % 2 == 0;
    }

    public byte getValue() {
        return (byte) Math.abs(value);
    }

    /**
     * Test if parity bit has expected value
     *
     * @return parity checks
     */
    public boolean isParityOk() {
        if (value < 0)
            return !evenParity((byte) -value);
        else
            return evenParity(value);
    }

    /**
     * Only for testing: force parity to be incorrect
     */
    public void makeByteCorrupt() {
        //general inversion did not work for 0...
        if (value == 0)
            value = 1;// 1 is invalid, should be -1
        else
            value = (byte) -value;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + value;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        CloudByte other = (CloudByte) obj;
        return value == other.value;
    }

    @Override
    public String toString() {
        return "CloudByte [value=" + getValue() + "]" + (!isParityOk() ? "->ERROR" : "");
    }
}
