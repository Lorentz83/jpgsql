package edu.purdue.jpgs.io;

import edu.purdue.jpgs.type.Conversions;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Lorenzo Bossi [lbossi@purdue.edu]
 */
public class RawReader {

    /**
     * Represents a c string. The problem is that the postgres protocol uses
     * null terminated strings and count the size in bytes. The equality
     * str.length() == number of bytes does not hold for unicode string. This
     * class is a pair [String, length] where length is in bytes and counts also
     * the null terminator.
     */
    public class CString {

        public final String str;
        public final int length;

        public CString(String str, int length) {
            this.str = str;
            this.length = length;
        }

        @Override
        public String toString() {
            return str;
        }
    }

    private final InputStream _in;

    public RawReader(InputStream in) throws IOException {
        _in = in;
    }

    public int readInt32() throws IOException {
        return (read() << 24) + (read() << 16) + (read() << 8) + read();
    }

    public short readInt16() throws IOException {
        return (short) ((read() << 8) + _in.read());
    }

    public byte readInt8() throws IOException {
        return (byte) read();
    }

    public char readByte() throws IOException {
        return (char) read();
    }

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

    private int read() throws IOException {
        int val = _in.read();
        if (val == -1) {
            throw new IOException("attempting to read from an empty stream");
        }
        return val;
    }

    void skip(long howMany) throws IOException {
        if (howMany != _in.skip(howMany)) {
            throw new IOException("end of stream");
        }
    }
}
