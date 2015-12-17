package edu.purdue.jpgs.io;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import edu.purdue.jpgs.testUtil.Pair;
import java.io.IOException;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import org.junit.Test;
import static edu.purdue.jpgs.testUtil.SimpleConversion.*;

/**
 *
 * @author Lorenzo Bossi [lbossi@purdue.edu]
 */
public class RawReaderTest {

    @Test
    public void readInt32() throws Exception {
        List<Pair<byte[], Integer>> values = new ArrayList<>();

        values.add(new Pair<>(getByteArray(0, 0, 0, 0), 0));
        values.add(new Pair<>(getByteArray(0, 0, 0, 47), 47));
        values.add(new Pair<>(getByteArray(0, 0, 0, 255), 255));
        values.add(new Pair<>(getByteArray(0, 0, 1, 0), 256));
        values.add(new Pair<>(getByteArray(127, 194, 239, 47), 2143481647));

        for (Pair<byte[], Integer> pair : values) {
            ByteArrayInputStream is = new ByteArrayInputStream(pair.first);
            RawReader reader = new RawReader(is);
            int res = reader.readInt32();

            assertThat(res, is(pair.second));
            assertThat(is.read(), is(-1));
        }
    }

    @Test
    public void readInt16() throws Exception {
        List<Pair<byte[], Integer>> values = new ArrayList<>();

        values.add(new Pair<>(getByteArray(0, 0), 0));
        values.add(new Pair<>(getByteArray(0, 254), 254));
        values.add(new Pair<>(getByteArray(1, 0), 256));
        values.add(new Pair<>(getByteArray(11, 3), 2819));

        for (Pair<byte[], Integer> pair : values) {
            ByteArrayInputStream is = new ByteArrayInputStream(pair.first);
            RawReader reader = new RawReader(is);
            short res = reader.readInt16();

            assertThat(res, is(pair.second.shortValue()));
            assertThat(is.read(), is(-1));
        }
    }

    @Test
    public void readInt8() throws Exception {
        List<Pair<byte[], Integer>> values = new ArrayList<>();

        values.add(new Pair<>(getByteArray(0), 0));
        values.add(new Pair<>(getByteArray(254), 254));
        values.add(new Pair<>(getByteArray(255), -1));
        values.add(new Pair<>(getByteArray(-1), -1));

        for (Pair<byte[], Integer> pair : values) {
            ByteArrayInputStream is = new ByteArrayInputStream(pair.first);
            RawReader reader = new RawReader(is);
            byte res = reader.readInt8();

            assertThat(res, is(pair.second.byteValue()));
            assertThat(is.read(), is(-1));
        }
    }

    @Test
    public void readByte() throws Exception {
        ByteArrayInputStream is = new ByteArrayInputStream(getByteArray('a', 'Z', '\0'));
        RawReader reader = new RawReader(is);

        assertThat(reader.readByte(), is('a'));
        assertThat(reader.readByte(), is('Z'));
        assertThat(reader.readByte(), is('\0'));

        assertThat(is.read(), is(-1));
    }

    @Test
    public void readString() throws Exception {
        ByteArrayInputStream is = new ByteArrayInputStream(getByteArray('q', 'w', 'e', 'r', '\0'));
        RawReader reader = new RawReader(is);
        RawReader.CString cstr = reader.readString();
        assertThat(cstr.str, is("qwer"));
        assertThat(cstr.length, is(5));
    }

    @Test
    public void readString_unicode() throws Exception {
        ByteArrayInputStream is = new ByteArrayInputStream(getByteArray('q', 'w', 0xC3, 0xA8, 'r', '\0'));
        RawReader reader = new RawReader(is);
        RawReader.CString cstr = reader.readString();
        assertThat(cstr.str, is("qwèr"));
        assertThat(cstr.length, is(6));
    }

    @Test
    public void readString_nonNullTerminated() throws Exception {
        ByteArrayInputStream is = new ByteArrayInputStream(getByteArray('q', 'w'));
        RawReader reader = new RawReader(is);
        try {
            reader.readString();
            fail("missing exception");
        } catch (IOException ex) {
        }
    }

    @Test
    public void readStringList() throws Exception {
        byte[] byteArray = getByteArray('q', 'w', '\0', 0xC3, 0xA8, 'r', '\0', 't', 'y', '\0');
        ByteArrayInputStream is = new ByteArrayInputStream(byteArray);
        RawReader reader = new RawReader(is);
        assertThat(reader.readStringList(byteArray.length), contains("qw", "èr", "ty"));
    }

    @Test
    public void readStringList_nonNullTerminated() throws Exception {
        ByteArrayInputStream is;
        RawReader reader;

        is = new ByteArrayInputStream(getByteArray('q', 'w'));
        reader = new RawReader(is);
        try {
            reader.readStringList(2);
            fail("missing exception");
        } catch (IOException ex) {
        }
    }

    @Test
    public void exceptionIfEmptyStream() throws Exception {
        ByteArrayInputStream is = new ByteArrayInputStream(new byte[0]);
        RawReader reader = new RawReader(is);

        try {
            reader.readByte();
            fail("missing exception");
        } catch (IOException ex) {
        }
        try {
            reader.readInt8();
            fail("missing exception");
        } catch (IOException ex) {
        }
        try {
            reader.readInt16();
            fail("missing exception");
        } catch (IOException ex) {
        }
        try {
            reader.readInt32();
            fail("missing exception");
        } catch (IOException ex) {
        }
        try {
            reader.readString();
            fail("missing exception");
        } catch (IOException ex) {
        }
        try {
            reader.readStringList(1);
            fail("missing exception");
        } catch (IOException ex) {
        }
    }

}
