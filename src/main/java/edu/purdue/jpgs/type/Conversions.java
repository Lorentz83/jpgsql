package edu.purdue.jpgs.type;

import java.nio.charset.Charset;
import java.util.Collection;
import java.util.List;

/**
 *
 * @author Lorenzo Bossi [lbossi@purdue.edu]
 */
public class Conversions {

    public static final Charset charset = Charset.forName("UTF-8");

    public static String toString(Collection<Byte> value) throws IllegalArgumentException {
        byte[] arr = new byte[value.size()];
        int i = 0;
        for (byte val : value) {
            arr[i++] = val;
        }
        return new String(arr, charset);
    }

    public static int toInt(List<Byte> value) throws IllegalArgumentException {
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

}
