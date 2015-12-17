package edu.purdue.jpgs.io;

import edu.purdue.jpgs.PgProtocolException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Lorenzo Bossi [lbossi@purdue.edu]
 */
public class PgReader {

    private final RawReader _in;
    private int _size = 0;

    public PgReader(RawReader in) {
        _in = in;
    }

    public char readCommand() throws IOException {
        char command = _in.readByte();
        _size = _in.readInt32() - 4;
        return command;
    }

    public int readInt32() throws PgProtocolException, IOException {
        decreaseSize(4);
        return _in.readInt32();
    }

    public short readInt16() throws PgProtocolException, IOException {
        decreaseSize(2);
        return _in.readInt16();
    }

    public byte readInt8() throws PgProtocolException, IOException {
        decreaseSize(1);
        return _in.readInt8();
    }

    public char readByte() throws PgProtocolException, IOException {
        decreaseSize(1);
        return _in.readByte();
    }

    public void discardCommand() throws PgProtocolException, IOException {
        _in.skip(_size);
        _size = 0;
    }

    public List<Short> readInt16Vector(int howMany) throws PgProtocolException, IOException {
        List<Short> ret = new ArrayList<>(howMany);
        for (int i = 0; i < howMany; i++) {
            ret.add(readInt16());
        }
        return ret;
    }

    public void checkAndReset() throws PgProtocolException {
        if (_size != 0) {
            throw new PgProtocolException("protocol connection out of sync " + _size + " bytes");
        }
        _size = 0;
    }

    private void decreaseSize(int bytes) throws PgProtocolException, IOException {
        assert (bytes > 0);
        _size -= bytes;
        if (_size < 0) {
            throw new PgProtocolException("protocol connection out of sync: no more data to read");
        }
    }

    public String readString() throws PgProtocolException, IOException {
        String str = _in.readString();
        _size -= str.length() + 1; //TODO this is not true in Unicode
        return str;
    }

    public List<Byte> readByteVector() throws PgProtocolException, IOException {
        return readByteVector(_size);
    }

    public List<Byte> readByteVector(int size) throws PgProtocolException, IOException {
        List<Byte> ret = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            ret.add(readInt8());
        }
        return ret;
    }

}
