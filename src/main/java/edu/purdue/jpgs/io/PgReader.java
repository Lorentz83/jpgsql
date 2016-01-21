package edu.purdue.jpgs.io;

import edu.purdue.jpgs.PgProtocolException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Implements a reader specialized in reading the Postgres messages. Postgres
 * messages are triplets [command, size of the message, additional parameters].
 * This helper class wraps around {@link RawReader} to enforce that only the
 * right number of bytes, specified by the size of the message, are read for
 * every command.
 *
 * @author Lorenzo Bossi [lbossi@purdue.edu]
 */
public class PgReader {

    private final RawReader _in;
    private int _size = 0;

    /**
     * Creates a PgReader. Note, it is safe to reuse this class as soon as at
     * every iteration the first method called is {@link #readCommand() } and
     * the last is {@link #check()}.
     *
     * @param in the RawReader to read.
     */
    public PgReader(RawReader in) {
        _in = in;
    }

    /**
     * Reads the command and initializes the message size.
     *
     * @return the command code.
     * @throws IOException if an I/O error occurs.
     */
    public char readCommand() throws IOException {
        char command = _in.readByte();
        _size = _in.readInt32() - 4;
        return command;
    }

    /**
     * Reads an int32.
     *
     * @return the value
     * @throws PgProtocolException if the remaining message length cannot
     * contain the required data.
     * @throws IOException if an I/O error occurs.
     */
    public int readInt32() throws PgProtocolException, IOException {
        decreaseSize(4);
        return _in.readInt32();
    }

    /**
     * Reads an int16.
     *
     * @return the value
     * @throws PgProtocolException if the remaining message length cannot
     * contain the required data.
     * @throws IOException if an I/O error occurs.
     */
    public short readInt16() throws PgProtocolException, IOException {
        decreaseSize(2);
        return _in.readInt16();
    }

    /**
     * Reads an int8.
     *
     * @return the value
     * @throws PgProtocolException if the remaining message length cannot
     * contain the required data.
     * @throws IOException if an I/O error occurs.
     */
    public byte readInt8() throws PgProtocolException, IOException {
        decreaseSize(1);
        return _in.readInt8();
    }

    /**
     * Reads a byte.
     *
     * @return the value
     * @throws PgProtocolException if the remaining message length cannot
     * contain the required data.
     * @throws IOException if an I/O error occurs.
     */
    public char readByte() throws PgProtocolException, IOException {
        decreaseSize(1);
        return _in.readByte();
    }

    /**
     * Discards all the bytes left in this command.
     *
     * @throws IOException if an I/O error occurs.
     */
    public void discardCommand() throws IOException {
        _in.skip(_size);
        _size = 0;
    }

    /**
     * Reads a list of int16.
     *
     * @param howMany how many values read.
     * @return the list of int16.
     * @throws PgProtocolException if the remaining message length cannot
     * contain the required data.
     * @throws IOException if an I/O error occurs.
     */
    public List<Short> readInt16List(int howMany) throws PgProtocolException, IOException {
        List<Short> ret = new ArrayList<>(howMany);
        for (int i = 0; i < howMany; i++) {
            ret.add(readInt16());
        }
        return ret;
    }

    /**
     * Reads null terminated string.
     *
     * @return the value.
     * @throws PgProtocolException if the remaining message length cannot
     * contain the required data.
     * @throws IOException if an I/O error occurs.
     */
    public String readString() throws PgProtocolException, IOException {
        RawReader.CString str = _in.readString();
        _size -= str.length;
        return str.str;
    }

    /**
     * Reads all the remaining bytes of the command.
     *
     * @return the list of bytes.
     * @throws IOException if an I/O error occurs.
     */
    public List<Byte> readByteList() throws IOException {
        try {
            return PgReader.this.readByteList(_size);
        } catch (PgProtocolException ex) {
            throw new AssertionError("The buffer size has been checked, this error should not happen");
        }
    }

    /**
     * Reads a list of bytes.
     *
     * @param howMany the number of bytes to read.
     * @return the list of bytes.
     * @throws PgProtocolException if the remaining message length cannot
     * contain the required data.
     * @throws IOException if an I/O error occurs.
     */
    public List<Byte> readByteList(int howMany) throws PgProtocolException, IOException {
        List<Byte> ret = new ArrayList<>(howMany);
        for (int i = 0; i < howMany; i++) {
            ret.add(readInt8());
        }
        return ret;
    }

    /**
     * Checks that the command has been completely read. This method either
     * returns with success or throws and should be called at the end of every
     * command to be sure that the protocol is not out of sync.
     *
     * @throws PgProtocolException if the remaining message length cannot
     * contain the required data.
     */
    public void check() throws PgProtocolException {
        if (_size != 0) {
            throw new PgProtocolException("protocol connection out of sync " + _size + " bytes");
        }
    }

    /**
     * Decrease the counter of the bytes left to read for this command.
     *
     * @param bytes the number of bytes to decrease.
     * @throws PgProtocolException if the remaining message length cannot
     * contain the required data.
     */
    private void decreaseSize(int bytes) throws PgProtocolException {
        assert (bytes > 0);
        _size -= bytes;
        if (_size < 0) {
            throw new PgProtocolException("protocol connection out of sync: no more data to read");
        }
    }

}
