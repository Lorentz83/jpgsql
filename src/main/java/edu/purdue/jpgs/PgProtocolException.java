package edu.purdue.jpgs;

import java.sql.SQLException;

/**
 * Represents an error in the Postgres binary protocol.
 *
 * @author Lorenzo Bossi
 */
public class PgProtocolException extends SQLException {

    public PgProtocolException() {
        super();
    }

    public PgProtocolException(Throwable cause) {
        super(cause);
    }

    public PgProtocolException(String what) {
        super(what);
    }

    public PgProtocolException(String what, Throwable cause) {
        super(what, cause);
    }
}
