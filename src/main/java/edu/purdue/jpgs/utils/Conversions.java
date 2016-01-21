package edu.purdue.jpgs.utils;

import edu.purdue.jpgs.PgProtocolException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;

/**
 * Implements conversions between formats.
 *
 * @author Lorenzo Bossi [lbossi@purdue.edu]
 */
public class Conversions {

    /**
     * Contains the standard charset used to communicate with the client.
     */
    public static final Charset charset = StandardCharsets.UTF_8;

    /**
     * Converts a list of bytes into a string. With respect of the charset
     * specified by {@link #charset}.
     *
     * @param value the byte list.
     * @return the string.
     */
    public static String toString(Collection<Byte> value) {
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
     * @throws IllegalArgumentException if value contains more than 4 bytes or
     * is an empty collection.
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

    /**
     * Binds a prepared statement to its actual values. This function takes care
     * of the quoted strings.
     *
     * @param preparedStatement the SQL with place-holders in the form of $n,
     * starting from 1.
     * @param values the values to be inserted.
     * @return the actual SQL.
     * @throws PgProtocolException in case the number of place-holders is not
     * the same of the provided values.
     */
    public static String bind(String preparedStatement, List<String> values) throws PgProtocolException {
        int pos = 1;
        for (String val : values) {
            preparedStatement = bind(preparedStatement, pos, val);
            if (preparedStatement == null) {
                throw new PgProtocolException("missing placeholder for parameter number " + pos);
            }
            pos++;
        }
        //this checks that there are no placeholders leftover
        if (bind(preparedStatement, pos, "") != null) {
            throw new PgProtocolException("missing parameter for placeholder number " + pos);
        }
        return preparedStatement;
    }

    /**
     * Substitutes the provided value in the specified position. Returns null in
     * case the placeholder is missing and takes care of quoted text.
     *
     * @param stm the statement.
     * @param pos the position counting from 1.
     * @param value the value to insert.
     * @return the statement with the value or null in case of error
     */
    private static String bind(String stm, int pos, String value) {
        String placeholder = "^\\$" + pos; // this is a regex that matches strings beginning in $n
        boolean quoted = false;
        for (int i = 0; i < stm.length(); i++) {
            char c = stm.charAt(i);
            if (c == '\'') {
                quoted = !quoted;
            }
            if (!quoted && c == '$') //Process char
            {
                String sub = stm.substring(i);
                if (sub.matches(placeholder + "(\\D.*|$)")) { //regex matches string beginning in $n and followed by a non digit or end of string
                    stm = stm.substring(0, i) + sub.replaceFirst(placeholder, value);
                    return stm;
                }
            }
        }
        return null;
    }
}
