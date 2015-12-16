package edu.purdue.jpgs;

import edu.purdue.jpgs.type.DataCellMsg;
import java.util.List;

/**
 *
 * @author Lorenzo Bossi [lbossi@purdue.edu]
 */
public interface DataProvider {

    public boolean setUser(String user);

    public boolean setDatabase(String database);

    public boolean setPassword(String password);

    public QueryResult getResult(String query);

    public interface QueryResult {

        public enum Type {

            CREATE, DELETE, INSERT, UPDATE, SELECT, ERROR
        };

        int getRowCount();

        String getErrorMessage();

        Iterable<List<DataCellMsg>> getRows();

        Type getType();

        List<String> getHeader();
    }

}
