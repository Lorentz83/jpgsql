package edu.purdue.jpgs.io;

import edu.purdue.jpgs.utils.Conversions;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Implements a reader specialized in reading the Postgres datatypes.
 *
 * @author Lorenzo Bossi [lbossi@purdue.edu]
 */
public class RawReader {

    /**
     * Represents a c string. The problem is that the postgres protocol uses
     * null terminated strings and counts their size in bytes. Unfortunately,
     * the equality str.length() == number of bytes does not hold for unicode
     * string. This class is a pair [String, length] where length is expressed
     * in bytes and counts also the null terminator.
     */
    public class CString {

        public final String str;
        public final int length;

        private CString(String str, int length) {
            this.str = str;
            this.length = length;
        }

        @Override
        public String toString() {
            return str;
        }
    }

    private final InputStream _in;

    /**
     * Creates a RawReader from the input stream.
     *
     * @param in the stream to read.
     */
    public RawReader(InputStream in) {
        _in = in;
    }

    /**
     * Reads an int32
     *
     * @return the value.
     * @throws IOException if an I/O error occurs or the end of stream is
     * reached.
     */
    public int readInt32() throws IOException {
        return (read() << 24) + (read() << 16) + (read() << 8) + read();
    }

    /**
     * Reads an int16
     *
     * @return the value.
     * @throws IOException if an I/O error occurs or the end of stream is
     * reached.
     */
    public short readInt16() throws IOException {
        return (short) ((read() << 8) + _in.read());
    }

    /**
     * Reads an int8
     *
     * @return the value.
     * @throws IOException if an I/O error occurs or the end of stream is
     * reached.
     */
    public byte readInt8() throws IOException {
        return (byte) read();
    }

    /**
     * Reads a byte.
     *
     * @return the value.
     * @throws IOException if an I/O error occurs or the end of stream is
     * reached.
     */
    public char readByte() throws IOException {
        return (char) read();
    }

    /**
     * Reads a null terminated string.
     *
     * @return the string without the trailing null byte.
     * @throws IOException if an I/O error occurs or the end of stream is
     * reached.
     */
    public CString readString() throws IOException {
        ArrayList<Byte> val = new ArrayList<>();
        int c;
        int len = 0;
        for (c = read(); c != 0; c = read()) {
            len++;
            val.add((byte) c);
        }
        return new CString(Conversions.toString(val), len + 1);
    }

    /**
     * Reads a list of null terminated strings.
     *
     * @param len how many strings are to be read.
     * @return the list of strings.
     * @throws IOException if an I/O error occurs or the end of stream is
     * reached.
     */
    public List<String> readStringList(int len) throws IOException {
        byte[] buf = new byte[len];
        if (_in.read(buf) != len) {
            throw new IOException("missing bytes from the stream");
        }
        if (buf[len - 1] != '\0') {
            throw new IOException("attempting to read a non null terminated string");
        }
        List<String> stringList = new ArrayList<>();
        List<Byte> cstr = new ArrayList<>();
        for (int n = 0; n < len; n++) {
            if (buf[n] == '\0') {
                stringList.add(Conversions.toString(cstr));
                cstr.clear();
            } else {
                cstr.add(buf[n]);
            }
        }
        return stringList;
    }

    /**
     * Returns the next byte from the stream or throws if the stream is empty.
     *
     * @return the next byte.
     * @throws IOException if an I/O error occurs or the end of stream is
     * reached.
     */
    private int read() throws IOException {
        int val = _in.read();
        if (val == -1) {
            throw new IOException("attempting to read from an empty stream");
        }
        return val;
    }

    /**
     * Discards some bytes from the stream.
     *
     * @param howMany the number of bytes to discard.
     * @throws IOException if an I/O error occurs or the end of stream is
     * reached.
     */
    void skip(long howMany) throws IOException {
        if (howMany != _in.skip(howMany)) {
            throw new IOException("end of stream");
        }
    }
}
