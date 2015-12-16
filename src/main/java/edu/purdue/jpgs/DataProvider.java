package edu.purdue.jpgs;

import edu.purdue.jpgs.type.DataCellMsg;
import java.util.List;

/**
 *
 * @author Lorenzo Bossi [lbossi@purdue.edu]
 */
public interface DataProvider {

    public boolean setUser(String _user);

    public void setDatabase(String database);

    public boolean setPassword(String password);

    public QueryResult getResult(String query);

    public static class QueryResult {

        String getRowCount() {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        String getErrorMessage() {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        Iterable<List<DataCellMsg>> getRows() {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        public enum Type {

            CREATE, DELETE, INSERT, UPDATE, SELECT, ERROR
        };

        Type getType() {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        public QueryResult() {
        }

        List<String> getHeader() {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
    }

}
