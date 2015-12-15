package edu.purdue.jpgs.io;

import edu.purdue.jpgs.PgProtocolException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Lorenzo Bossi [lbossi@purdue.edu]
 */
public class PgReader implements AutoCloseable {

    private final RawReader _in;
    private int _size = 0;

    public PgReader(RawReader in) {
        _in = in;
    }

    public char readCommand() throws PgProtocolException {
        try {
            char command = _in.readByte();
            _size = _in.readInt32() - 4;
            return command;
        } catch (IOException ex) {
            throw new PgProtocolException(ex);
        }
    }

    public int readInt32() throws PgProtocolException {
        decreaseSize(4);
        try {
            return _in.readInt32();
        } catch (IOException ex) {
            throw new PgProtocolException(ex);
        }
    }

    public short readInt16() throws PgProtocolException {
        decreaseSize(2);
        try {
            return _in.readInt16();
        } catch (IOException ex) {
            throw new PgProtocolException(ex);
        }
    }

    public byte readInt8() throws PgProtocolException {
        decreaseSize(1);
        try {
            return _in.readInt8();
        } catch (IOException ex) {
            throw new PgProtocolException(ex);
        }
    }

    public char readByte() throws PgProtocolException {
        try {
            decreaseSize(1);
            return _in.readByte();
        } catch (IOException ex) {
            throw new PgProtocolException(ex);
        }
    }

    public void discardCommand() throws PgProtocolException {
        try {
            _in.skip(_size);
            _size = 0;
        } catch (IOException ex) {
            throw new PgProtocolException(ex);
        }
    }

    public List<Short> readInt16Vector(int howMany) throws PgProtocolException {
        List<Short> ret = new ArrayList<>(howMany);
        for (int i = 0; i < howMany; i++) {
            ret.add(readInt16());
        }
        return ret;
    }

    public void checkSize() throws PgProtocolException {
        if (_size != 0) {
            throw new PgProtocolException("protocol connection out of sync " + _size + " bytes");
        }
    }

    @Override
    public void close() throws PgProtocolException {
        checkSize();
    }

    private void decreaseSize(int bytes) throws PgProtocolException {
        if (bytes < 0) {
            throw new PgProtocolException("reading a negative number of bytes");
        }
        _size -= bytes;
        if (_size < 0) {
            throw new PgProtocolException("protocol connection out of sync: no more data to read");
        }
    }

    public String readString() throws PgProtocolException {
        try {
            return _in.readString(_size);
        } catch (IOException ex) {
            throw new PgProtocolException(ex);
        }
    }

    public List<Byte> readByteVector() throws PgProtocolException {
        return readByteVector(_size);
    }

    public List<Byte> readByteVector(int size) throws PgProtocolException {
        List<Byte> ret = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            ret.add(readInt8());
        }
        return ret;
    }
}
