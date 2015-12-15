package edu.purdue.jpgs.io;

import edu.purdue.jpgs.PgProtocolException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;

/**
 *
 * @author Lorenzo Bossi
 */
public class PgWriter implements AutoCloseable {

    private final char _command;
    private final OutputStream _os;
    private ByteBuffer _buffer;

    public PgWriter(OutputStream os, char command) {
        _os = os;
        _command = command;
        _buffer = new ByteBuffer();
    }

    public void addString(String str) {
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        _buffer.append(bytes);
        _buffer.append((byte) 0);
    }

    public void addInt32(int i) {
        _buffer.append((byte) ((i >>> 24) & 0xFF));
        _buffer.append((byte) ((i >>> 16) & 0xFF));
        _buffer.append((byte) ((i >>> 8) & 0xFF));
        _buffer.append((byte) (i & 0xFF));
    }

    public void addInt16(short i) {
        _buffer.append((byte) (i >>> 8));
        _buffer.append((byte) (i & 0xFF));
    }

    public void addInt8(byte b) {
        _buffer.append(b);
    }

    public void addByte(char b) {
        _buffer.append((byte) b);
    }

    public void writeInt16(Collection<Short> formats) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public void flush() throws PgProtocolException {
        if (_buffer == null) {
            return;
        }
        try {
            int size = _buffer.len() + 4;
            _buffer.prepend((byte) (size & 0xFF));
            _buffer.prepend((byte) ((size >>> 8) & 0xFF));
            _buffer.prepend((byte) ((size >>> 16) & 0xFF));
            _buffer.prepend((byte) ((size >>> 24) & 0xFF));

            if (_command != '\0') {
                _buffer.prepend((byte) _command);
            }
            _os.write(_buffer.toArray());
            _buffer = null;
        } catch (IOException ex) {
            throw new PgProtocolException(ex);
        }
    }

    @Override
    public void close() throws PgProtocolException {
        flush();
    }
}

class ByteBuffer {

    private final ArrayList<Byte> _buffer = new ArrayList<Byte>();

    void append(byte[] bytes) {
        for (byte b : bytes) {
            _buffer.add(b);
        }
    }

    void append(byte b) {
        _buffer.add(b);
    }

    byte[] toArray() {
        byte[] ret = new byte[_buffer.size()];
        for (int i = 0; i < _buffer.size(); i++) {
            ret[i] = _buffer.get(i);
        }
        return ret;
    }

    int len() {
        return _buffer.size();
    }

    void prepend(byte b) {
        _buffer.add(0, b);
    }

}
