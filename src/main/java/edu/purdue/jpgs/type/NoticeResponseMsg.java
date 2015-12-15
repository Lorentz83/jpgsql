package edu.purdue.jpgs.type;

/**
 *
 * @author Lorenzo Bossi [lbossi@purdue.edu]
 */
public class NoticeResponseMsg {

    public final byte type;
    public final String message;

    NoticeResponseMsg(char type, String message) {
        this.type = (byte) type;
        this.message = message;
    }
}
