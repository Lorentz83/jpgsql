package edu.purdue.jpgs.type;

/**
 *
 * @author Lorenzo Bossi [lbossi@purdue.edu]
 */
public class ColumnDescriptionMsg {

    public final String name;
    public final int tableID;
    public final short columnID;
    public final int typeID;
    public final short typeSize;
    public final int typeModifier;
    public final short formatCode;

    public ColumnDescriptionMsg(String name, int tableID, short columnID, int typeID, short typeSize, int typeModifier, short formatCode) {
        this.name = name;
        this.tableID = tableID;
        this.columnID = columnID;
        this.typeID = typeID;
        this.typeSize = typeSize;
        this.typeModifier = typeModifier;
        this.formatCode = formatCode;
    }

    /**
     * Creates a columnDescriptionMsg. Not linked to any table and in text
     * format.
     *
     * @param name the name of the column.
     * @param typeID the type id.
     * @param typeSize the type size or -1 if variable size.
     */
    public ColumnDescriptionMsg(String name, int typeID, short typeSize) {
        this(name, 0, (short) 0, typeID, typeSize, 0, (short) 0);
    }
}
