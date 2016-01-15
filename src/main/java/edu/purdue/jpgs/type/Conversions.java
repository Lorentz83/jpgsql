package edu.purdue.jpgs.type;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collection;

/**
 * Implements conversions between formats.
 *
 * @author Lorenzo Bossi [lbossi@purdue.edu]
 */
public class Conversions {

    public static final Charset charset = StandardCharsets.UTF_8;

    /**
     * Converts a list of bytes into a string. With respect of the charset
     * specified by {@link #charset}.
     *
     * @param value the byte list.
     * @return the string.
     * @throws IllegalArgumentException
     */
    public static String toString(Collection<Byte> value) throws IllegalArgumentException {
        byte[] arr = new byte[value.size()];
        int i = 0;
        for (byte val : value) {
            arr[i++] = val;
        }
        return new String(arr, charset);
    }

    /**
     * Converts a list of bytes to an integer. Bytes are concatenated according
     * to the list order.
     *
     * @param value the list of bytes.
     * @return the integer value.
     * @throws IllegalArgumentException if value is longer than 4 bytes or is an
     * empty list.
     */
    public static int toInt(Collection<Byte> value) throws IllegalArgumentException {
        if (value.size() > 4) {
            throw new IllegalArgumentException("Cannot convert more than 4 bytes to integer");
        }
        if (value.isEmpty()) {
            throw new IllegalArgumentException("Nothing to convert");
        }

        int val = 0;
        for (int b : value) {
            val = val << 8;
            val += b & 0xFF; //this is required to convert the byte to unsiged byte
        }
        return val;
    }

    /**
     * Converts a string to a non null terminated array of bytes. With respect
     * of the charset specified by {@link #charset}.
     *
     * @param str the string to convert.
     * @return the byte array.
     */
    public static byte[] getBytes(String str) {
        return str.getBytes(charset);
    }

}
