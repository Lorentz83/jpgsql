package edu.purdue.jpgs.type;

import edu.purdue.jpgs.utils.Conversions;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Represents a table cell.
 *
 * @author Lorenzo Bossi [lbossi@purdue.edu]
 */
public class DataCellMsg {

    /**
     * The content of this cell represented in a byte list.
     */
    public final Collection<Byte> data;

    /**
     * Initializes the DataCellMsg from the byte list. If data is null, a null
     * cell is created.
     *
     * @param data the data to store in this cell.
     */
    public DataCellMsg(List<Byte> data) {
        this.data = data;
    }

    /**
     * Initializes the DataCellMsg from the string. If the string is null, a
     * null cell is created.
     *
     * @param value the data to store in this cell.
     */
    public DataCellMsg(String value) {
        if (value == null) {
            data = null;
        } else {
            byte[] bytes = Conversions.getBytes(value);
            ArrayList<Byte> d = new ArrayList<Byte>(bytes.length);
            for (byte b : bytes) {
                d.add(b);
            }
            data = Collections.unmodifiableList(d);
        }
    }

    /**
     * Initializes a null cell.
     */
    public DataCellMsg() {
        data = null;
    }

    /**
     * Returns the size of the binary representation of this cell.
     *
     * @return the number of bytes.
     */
    public int size() {
        return data.size();
    }

    /**
     * Return true if this cell contains a null value.
     *
     * @return true if this cell contains a null value.
     */
    public boolean isNull() {
        return data == null;
    }

}
