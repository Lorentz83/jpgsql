package edu.purdue.jpgs;

import java.util.Iterator;
import java.util.List;

/**
 * Represents a data provider used by {@link SimpleConnection} to get the data
 * to be returned to the client. An instance is required for any connection. It
 * is guaranteed that the functions {@link #setUser(java.lang.String)
 * } and {@link #setPassword(java.lang.String)} are called only once and in this
 * order. The function {@link #setPassword(java.lang.String) } is called only if
 * setUser returned false. Once the setup phase is completed and the user is
 * authenticated, only {@link #getResult(java.lang.String) } can be called.
 *
 * @author Lorenzo Bossi [lbossi@purdue.edu]
 */
public interface DataProvider {

    /**
     * Called when the client authenticates providing a user name.
     *
     * @param user the user name.
     * @return true if no password is required for this user.
     */
    public boolean setUser(String user);

    /**
     * Called specifying the database the client wants to use.
     *
     * @param database the database name.
     * @return true if the database exists and the current user can connect to
     * it.
     */
    public boolean setDatabase(String database);

    /**
     * Called specifying the user password.
     *
     * @param password the user password.
     * @return true if the password is valid and therefore the user is
     * authenticated.
     */
    public boolean setPassword(String password);

    /**
     * Called to get the result of a query. Note, this function should not
     * throw. In case any error must be reported, use the return result.
     *
     * @param query the query provided by the user.
     * @return the result.
     */
    public QueryResult getResult(String query);

    /**
     * Represents the result of a query.
     *
     */
    public interface QueryResult {

        /**
         * Represents the different allowed types of QueryResult.
         */
        public enum Type {

            CREATE, DELETE, INSERT, UPDATE, SELECT, ERROR
        };

        /**
         * Returns the type of this QueryResult.
         *
         * @return a non null type.
         */
        Type getType();

        /**
         * Returns the rows touched by this query. Called only if TYPE is not
         * ERROR. Counts the rows selected, deleted, inserted or updated by this
         * query. Note, in case of SELECT, this method is called only after the
         * iterator returned by {@link #getRows() } has been used, therefore it
         * is legit to count how many times {@link Iterator#next() } has been
         * successfully called to build this result.
         *
         * @return the row touched by this query.
         */
        int getRowCount();

        /**
         * Returns the error message. Called only if type is ERROR.
         *
         * @return the error message that will be displayed to the user.
         */
        String getErrorMessage();

        /**
         * Returns the content of this result. Called only if type is SELECT.
         * Returns a possibly empty Iterator of string list. All the string
         * lists must have exactly the same size of the list returned by {@link #getHeader()
         * }. Single string elements may be null. Note: this method may be
         * called multiple times but the iterator must not reset its position.
         *
         * @return the content of the selected table.
         */
        Iterator<List<String>> getRows();

        /**
         * Returns the header of this result. Called only if type is SELECT.
         *
         * @return a non null list of string containing the header of the
         * selected table.
         */
        List<String> getHeader();
    }

}
