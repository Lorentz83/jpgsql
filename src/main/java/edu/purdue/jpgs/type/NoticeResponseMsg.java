package edu.purdue.jpgs.type;

/**
 * Represent a notice message. A message that the server can send to the client
 * to notify non fatal warnings.
 *
 * @author Lorenzo Bossi [lbossi@purdue.edu]
 */
public class NoticeResponseMsg {

    public final byte type;
    public final String message;

    /**
     * Creates a notice message. See
     * <a href="http://www.postgresql.org/docs/9.4/static/protocol-error-fields.html">valid
     * types</a>.
     *
     * @param type the notice type.
     * @param message the notice message.
     */
    NoticeResponseMsg(char type, String message) {
        this.type = (byte) type;
        this.message = message;
    }
}
