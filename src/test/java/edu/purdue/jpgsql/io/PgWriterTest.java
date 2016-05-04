package edu.purdue.jpgsql.io;

import edu.purdue.jpgsql.io.PgWriter;
import java.io.ByteArrayOutputStream;
import org.junit.Test;
import static edu.purdue.jpgsql.testUtil.SimpleConversion.*;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 *
 * @author Lorenzo Bossi [lbossi@purdue.edu]
 */
public class PgWriterTest {

    @Test
    public void flush_simpleCommand() throws Exception {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        PgWriter w = new PgWriter(os, 'C');
        w.flush();
        assertThat(os.toByteArray(), is(getByteArray('C', 0, 0, 0, 4)));
    }

    @Test
    public void flush_noCommand() throws Exception {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        PgWriter w = new PgWriter(os, '\0');
        w.flush();
        assertThat(os.toByteArray(), is(getByteArray(0, 0, 0, 4)));
    }

    @Test
    public void flush_twiceNoError() throws Exception {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        PgWriter w = new PgWriter(os, '\0');
        w.flush();
        w.flush();
        assertThat(os.toByteArray(), is(getByteArray(0, 0, 0, 4)));
    }

    @Test
    public void flush_autoClose() throws Exception {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        try (PgWriter w = new PgWriter(os, '\0')) {
            assertThat(os.toByteArray(), is(getByteArray()));
        }
        assertThat(os.toByteArray(), is(getByteArray(0, 0, 0, 4)));
    }

    @Test
    public void addInt8() throws Exception {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        PgWriter w = new PgWriter(os, '\0');
        w.addInt8((byte) 2);
        w.flush();
        assertThat(os.toByteArray(), is(getByteArray(
                0, 0, 0, 5,
                2
        )));
    }

    @Test
    public void addInt16() throws Exception {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        PgWriter w = new PgWriter(os, '\0');
        w.addInt16((short) 2819);
        w.flush();
        assertThat(os.toByteArray(), is(getByteArray(
                0, 0, 0, 6,
                11, 3
        )));
    }

    @Test
    public void addString() throws Exception {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        PgWriter w = new PgWriter(os, '\0');
        w.addString("qwerty");
        w.flush();
        assertThat(os.toByteArray(), is(getByteArray(
                0, 0, 0, 11,
                'q', 'w', 'e', 'r', 't', 'y', '\0'
        )));
    }

    @Test
    public void addInt32() throws Exception {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        PgWriter w = new PgWriter(os, '\0');
        w.addInt32(2143481647);
        w.flush();
        assertThat(os.toByteArray(), is(getByteArray(
                0, 0, 0, 8,
                127, 194, 239, 47
        )));
    }

    @Test
    public void flush_all() throws Exception {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        PgWriter w = new PgWriter(os, 'a');
        w.addByte('b');
        w.addInt8((byte) 2);
        w.addInt16((short) 3);
        w.addInt32(25);
        w.addString("qw");
        w.flush();
        assertThat(os.toByteArray(), is(getByteArray(
                'a',
                0, 0, 0, 15, //len
                'b',
                2,
                0, 3,
                0, 0, 0, 25,
                'q', 'w', '\0'
        )));
    }
}
