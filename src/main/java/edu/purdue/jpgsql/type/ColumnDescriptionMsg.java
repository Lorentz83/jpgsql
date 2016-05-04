package edu.purdue.jpgsql.type;

/**
 * Represents a column header. This is a column header as represented by the
 * messages used in the Postgres protocol.
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

    /**
     * Creates a column description. The column reports information of the table
     * it is part of.
     *
     * @param name the column header.
     * @param tableID the id of the table this column is part of.
     * @param columnID the id of this column.
     * @param typeID the type id.
     * @param typeSize the type size or -1 if variable size.
     * @param typeModifier the type modifier. The meaning of the modifier is
     * type-specific.
     * @param formatCode the format code being used for the field. Currently
     * will be zero (text) or one (binary).
     */
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
     * Creates a columnDescriptionMsg. Not linked to any table and in variable
     * size text format.
     *
     * @param name the column header.
     */
    public ColumnDescriptionMsg(String name) {
        this(name, 0, (short) 0, 1043, (short) -1, 0, (short) -1);
    }
}
