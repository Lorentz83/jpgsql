package edu.purdue.jpgs.type;

import java.util.ArrayList;
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

    public List<Byte> createByteList(int... vals) {
        List<Byte> ret = new ArrayList<>();
        for (int v : vals) {
            ret.add((byte) v);
        }
        return ret;
    }
}
