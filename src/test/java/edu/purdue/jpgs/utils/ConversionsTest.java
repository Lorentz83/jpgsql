package edu.purdue.jpgs.utils;

import edu.purdue.jpgs.PgProtocolException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import org.junit.Test;

/**
 *
 * @author Lorenzo Bossi [lbossi@purdue.edu]
 */
public class ConversionsTest {

    @Test
    public void toInt_errorCase() {
        try {
            Conversions.toInt(null);
            fail("missing ex");
        } catch (NullPointerException ex) {
        }
        try {
            Conversions.toInt(new ArrayList<Byte>());
            fail("missing ex");
        } catch (IllegalArgumentException ex) {
        }
        try {
            List<Byte> val = createByteList(1, 2, 3, 4, 5);
            Conversions.toInt(val);
            fail("missing ex");
        } catch (IllegalArgumentException ex) {
        }
    }

    @Test
    public void toInt() {
        List<Byte> val;

        val = createByteList(25);
        assertThat(Conversions.toInt(val), is(25));

        val = createByteList(0x7F, 0xC2, 0xEF, 0x2F);
        assertThat(Conversions.toInt(val), is(0x7FC2EF2F));
    }

    @Test
    public void toString_successUTF8() {
        List<Byte> val;
        val = createByteList();
        assertThat(Conversions.toString(val), is(""));

        val = createByteList('q', 'w', 'e');
        assertThat(Conversions.toString(val), is("qwe"));

        val = createByteList('q', 'w', 0xC3, 0xA8);
        assertThat(Conversions.toString(val), is("qwè"));

        val = createByteList('1', 0, '2');
        assertThat(Conversions.toString(val), is("1\u00002"));
    }

    @Test
    public void bind_success() throws PgProtocolException {
        assertThat(Conversions.bind("any string here", new ArrayList<>()), is("any string here"));
        assertThat(Conversions.bind("select ($1, $2=$3)", stringList("a", "b", "c")), is("select (a, b=c)"));
        assertThat(Conversions.bind("select ($3, $2=$1)", stringList("a", "b", "c")), is("select (c, b=a)"));
        assertThat(Conversions.bind("select 'a $tring'' $0, $1, $2' $1", stringList("@@")), is("select 'a $tring'' $0, $1, $2' @@"));
    }

    @Test
    public void bind_moreThan10() throws PgProtocolException {
        List<String> vals = new ArrayList<>();

        StringBuilder stm = new StringBuilder("SELECT ");
        StringBuilder query = new StringBuilder("SELECT ");

        for (int i = 15; i >= 1; i--) {
            stm.append("$").append(i).append(" ");
            char v = (char) ('a' + i);
            vals.add("" + v);
        }
        for (int i = 1; i <= 15; i++) {
            char v = (char) ('a' + i);
            query.append(v).append(" ");
        }
        assertThat(Conversions.bind(stm.toString(), vals), is(query.toString()));
    }

    @Test
    public void bind_errors() throws PgProtocolException {
        try {
            Conversions.bind("select where a = $1", stringList("a", "b"));
            fail("missing exception");
        } catch (PgProtocolException ex) {
            assertThat(ex.getMessage(), is("missing placeholder for parameter number 2"));
        }

        try {
            Conversions.bind("select where a = $1 b = $2", new ArrayList<>());
            fail("missing exception");
        } catch (PgProtocolException ex) {
            assertThat(ex.getMessage(), is("missing parameter for placeholder number 1"));
        }
        try {
            Conversions.bind("select where a = $2", stringList("@"));
            fail("missing exception");
        } catch (PgProtocolException ex) {
            assertThat(ex.getMessage(), is("missing placeholder for parameter number 1"));
        }
    }

    public List<String> stringList(String... vals) {
        return Arrays.asList(vals);
    }

    public List<Byte> createByteList(int... vals) {
        List<Byte> ret = new ArrayList<>();
        for (int v : vals) {
            ret.add((byte) v);
        }
        return ret;
    }
}
