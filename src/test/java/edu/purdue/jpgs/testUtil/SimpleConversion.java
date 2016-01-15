package edu.purdue.jpgs.testUtil;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

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

    public static List<String> row(String... cells) {
        return Arrays.asList(cells);
    }

    public static Iterator<List<String>> table(List<String>... rows) {
        List<List<String>> tbl = Arrays.asList(rows);
        return tbl.iterator();
    }
}
