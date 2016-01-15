package edu.purdue.jpgs.io;

import edu.purdue.jpgs.PgProtocolException;
import edu.purdue.jpgs.type.Conversions;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;

/*
 TODO: right now we use one instance of this class for each command sent to the client.
 But commands can actually been packed to save network usage.
 It may be good to change BaseConnection to implement this.
 */
/**
 * Implements a writer specialized in writing the Postgres messages. Postgres
 * messages are triplets [command, size of the message, additional parameters].
 * This helper class keeps a buffer of all the parameters and automatically
 * counts the size of the message before flushing it to the network. This class
 * should be used once for every command and either created in a
 * try-with-resources or manually call {@link #flush() } to send the data to the
 * network.
 *
 * @author Lorenzo Bossi [lbossi@purdue.edu]
 */
public class PgWriter implements AutoCloseable {

    private final char _command;
    private final OutputStream _os;
    private ByteBuffer _buffer;

    /**
     * Initializes a PgWriter. Messages are not actually sent over the network
     * until {@link #close() } or {@link #flush()} is called.
     *
     * @param os the output stream to write.
     * @param command the Postgres command.
     */
    public PgWriter(OutputStream os, char command) {
        _os = os;
        _command = command;
        _buffer = new ByteBuffer();
    }

    public void addString(String str) {
        byte[] bytes = Conversions.getBytes(str);
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

    /**
     * Sends the message over the network. This method can be safely called more
     * than once, but only the first actually sends data. Once this method is
     * called, this writer is not valid anymore and any call of any add* method
     * will result in an exception.
     *
     * @throws PgProtocolException
     */
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

    /**
     * Sends the message over the network. It is a convenience method to call {@link #flush()
     * }.
     *
     * @throws PgProtocolException
     */
    @Override
    public void close() throws PgProtocolException {
        flush();
    }
}

/**
 * Implements a simple byte buffer.
 *
 * @author Lorenzo Bossi [lbossi@purdue.edu]
 */
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
