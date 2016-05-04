package edu.purdue.jpgsql.type;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents an error message. In Postgres, to properly report an error, a set
 * of error messages is returned. This class provides static methods to simply
 * create the list of error messages for most of the common cases.
 *
 * @author Lorenzo Bossi [lbossi@purdue.edu]
 */
public class ErrorResponseMsg {

    public final byte type;
    public final String message;

    /**
     * Creates an error message. See
     * <a href="http://www.postgresql.org/docs/9.4/static/protocol-error-fields.html">valid
     * types</a>.
     *
     * @param type the error type.
     * @param message the error message.
     */
    public ErrorResponseMsg(char type, String message) {
        this.type = (byte) type;
        this.message = message;
    }

    /**
     * Creates a list of error messages. One to specify the error condition, one
     * to specify the SQL error state and one to specify the user friendly
     * error. See valid
     * <a href="http://www.postgresql.org/docs/9.4/static/errcodes-appendix.html">sql
     * states</a>.
     *
     * @param sqlState the error code.
     * @param message the error message.
     * @return the list of error messages.
     */
    public static List<ErrorResponseMsg> makeError(String sqlState, String message) {
        List<ErrorResponseMsg> errors = new ArrayList<>();
        errors.add(new ErrorResponseMsg('S', "ERROR"));
        errors.add(new ErrorResponseMsg('C', sqlState));
        errors.add(new ErrorResponseMsg('M', message));
        return errors;
    }

    /**
     * Creates a list of error messages. One to specify the error condition, one
     * to specify a generic SQL error state (XX000) and one to specify the user
     * friendly error.
     *
     * @param message the error message.
     * @return the list of error messages.
     */
    public static List<ErrorResponseMsg> makeError(String message) {
        return makeError("XX000", message);
    }
}
