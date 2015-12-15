package edu.purdue.jpgs.io;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 *
 * @author Lorenzo Bossi [lbossi@purdue.edu]
 */
public class RawReader {

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

    public String readString(int len) throws IOException {
        byte[] buf = new byte[len];
        if (_in.read(buf) != len) {
            throw new IOException("missing bytes from the stream");
        }
        if (buf[len - 1] != '\0') {
            throw new IOException("attempting to read a non null terminated string");
        }
        return new String(Arrays.copyOf(buf, len - 1));
    }

    public List<String> readStringList(int len) throws IOException {
        byte[] buf = new byte[len];
        if (_in.read(buf) != len) {
            throw new IOException("missing bytes from the stream");
        }
        if (buf[len - 1] != '\0') {
            throw new IOException("attempting to read a non null terminated string");
        }
        List<String> ret = new ArrayList<>();
        int start = 0;
        for (int n = 0; n < len; n++) {
            if (buf[n] == '\0') {
                ret.add(new String(Arrays.copyOfRange(buf, start, n)));
                start = n + 1;
            }
        }
        return ret;
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
