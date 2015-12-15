package edu.purdue.jpgs.testUtil;

/**
 *
 * @author Lorenzo Bossi [lbossi@purdue.edu]
 */
public class SimpleConversion {

    public static byte[] getByteArray(byte... values) {
        return values;
    }

    public static byte[] getByteArray(int... values) {
        byte[] ret = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            ret[i] = (byte) values[i];
        }
        return ret;
    }
}
