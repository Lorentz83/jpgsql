package edu.purdue.jpgs.type;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Lorenzo Bossi [lbossi@purdue.edu]
 */
public class DataCellMsg {

    public final List<Byte> data;

    public DataCellMsg(List<Byte> data) {
        this.data = data;
    }

    public DataCellMsg(String value) {
        byte[] bytes = value.getBytes();
        data = new ArrayList<Byte>(bytes.length);
        for (byte b : bytes) {
            data.add(b);
        }
    }

    public DataCellMsg() {
        data = null;
    }

    public int size() {
        return data.size();
    }

    public boolean isNull() {
        return data == null;
    }

}
