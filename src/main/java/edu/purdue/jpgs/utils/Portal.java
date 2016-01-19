package edu.purdue.jpgs.utils;

import edu.purdue.jpgs.DataProvider;

/**
 * Represents a portal. Contains the SQL and the result as returned by the
 * DataProvider.
 *
 * @author Lorenzo Bossi [lbossi@purdue.edu]
 */
public class Portal {

    private DataProvider.QueryResult _result;

    final String originalStatement;

    public final String sql;

    /**
     * Creates a portal.
     *
     * @param originalStatement the statement name that originated this portal.
     * @param realQuery the SQL query.
     */
    Portal(String originalStatement, String realQuery) {
        sql = realQuery;
        this.originalStatement = originalStatement;
    }

    /**
     * Returns the query result. If no previous result is stored,
     * {@link  DataProvider#getResult(java.lang.String)} is called providing the
     * stored SQL.
     *
     * @param provider the provider to query only the first time this method is
     * called.
     * @return the query result.
     */
    public DataProvider.QueryResult getAndStoreResult(DataProvider provider) {
        if (_result == null) {
            _result = provider.getResult(sql);
        }
        return _result;
    }
}
