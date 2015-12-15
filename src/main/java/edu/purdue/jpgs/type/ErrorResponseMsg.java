package edu.purdue.jpgs.type;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Lorenzo Bossi [lbossi@purdue.edu]
 */
public class ErrorResponseMsg {

    public final byte type;
    public final String message;

    ErrorResponseMsg(char type, String message) {
        this.type = (byte) type;
        this.message = message;
    }

    public static List<ErrorResponseMsg> makeError(String sqlState, String message) {
        List<ErrorResponseMsg> errors = new ArrayList<>();
        errors.add(new ErrorResponseMsg('S', "ERROR"));
        errors.add(new ErrorResponseMsg('C', sqlState));
        errors.add(new ErrorResponseMsg('M', message));
        return errors;
    }

    public static List<ErrorResponseMsg> makeError(String message) {
        return makeError("XX000", message);
    }
}
