package edu.purdue.jpgs;

import edu.purdue.jpgs.type.ColumnDescriptionMsg;
import edu.purdue.jpgs.type.Conversions;
import edu.purdue.jpgs.type.DataCellMsg;
import static edu.purdue.jpgs.type.ErrorResponseMsg.makeError;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Abstracts most of the complicated messages defined by the Postgres protocol
 * giving an easier interface to interact. Note, to correctly handle all data
 * types set 'binaryTransfer' to false when connecting through JDBC. To use this
 * class just implement a {@link DataProvider}, pass it to the constructor and
 * execute {@link #run() } to start the protocol execution.
 *
 * @author Lorenzo Bossi [lbossi@purdue.edu]
 */
public class SimpleConnection extends BaseConnection {

    protected String _database;
    private final int _processId, _secretKey;
    private final DataProvider _provider;
    private final Map<String, String> _preparedStatements;
    private final Map<String, String> _portals;
    private final BiConsumer<Integer, Integer> _cancelCallback;

    /**
     * Creates a SimpleConnection.
     *
     * @param socket the socket the client is connected to.
     * @param provider the data provider.
     * @param pid the current process id. Sent to the client to make it able to
     * cancel the request killing the current request.
     * @param cancelCallback the function to be called when a cancel callback is
     * received. The parameters are the processId and the secretKey received.
     * @throws IOException
     * @throws NullPointerException if any of the parameters is null.
     */
    public SimpleConnection(Socket socket, DataProvider provider, int pid, BiConsumer<Integer, Integer> cancelCallback) throws IOException, NullPointerException {
        super(socket);
        _processId = pid;
        _secretKey = (int) (Math.random() * Integer.MAX_VALUE);
        _provider = provider;
        _preparedStatements = new HashMap<>();
        _portals = new HashMap<>();
        _cancelCallback = cancelCallback;
        if (provider == null) {
            throw new NullPointerException();
        }
        if (cancelCallback == null) {
            throw new NullPointerException();
        }
    }

    /**
     * Creates a SimpleConnection. This is a convenience method that provides a
     * default process id and a void callback that does nothing in case of a
     * cancel request.
     *
     * @param socket the socket the client is connected to.
     * @param provider the data provider.
     * @throws IOException
     * @throws NullPointerException if any of the parameters is null.
     */
    public SimpleConnection(Socket socket, DataProvider provider) throws IOException {
        this(socket, provider, -1, (Integer a, Integer b) -> {
        });
    }

    @Override
    protected void CancelRequest(int backendProcessId, int secretKey) throws PgProtocolException, IOException {
        _cancelCallback.accept(backendProcessId, secretKey);
    }

    @Override
    protected boolean StartupMessage(int protocolVersion, Map<String, String> parameters) throws PgProtocolException, IOException, IOException {
        String user = parameters.get("user");
        if (user == null) {
            throw new PgProtocolException("Missing username");
        }
        _database = parameters.get("database");
        if (_database == null) {
            _database = user;
        }
        if (_provider.setUser(user)) {
            if (!_provider.setDatabase(_database)) {
                ErrorResponse(makeError("3D000", String.format("database \"%s\" does not exist", _database)));
                return false;
            }
            AuthenticationOk();
            BackendKeyData(_processId, _secretKey);
        } else {
            AuthenticationCleartextPassword();
        }
        return true;
    }

    @Override
    protected void PasswordMessage(String password) throws PgProtocolException, IOException, IOException {
        if (_provider.setPassword(password)) {
            AuthenticationOk();
            if (!_provider.setDatabase(_database)) {
                ErrorResponse(makeError("3D000", String.format("database \"%s\" does not exist", _database)));
            }
            BackendKeyData(_processId, _secretKey);
        }
    }

    @Override
    protected void Query(String query) throws PgProtocolException, IOException {
        /**
         * @todo the query string may contain multiple sql statements
         */
        _preparedStatements.remove(""); //erase the unnamed statement and portal
        _portals.remove("");
        query = query.trim();
        if (!respondToEmptyQuery(query)) {
            DataProvider.QueryResult table = _provider.getResult(query);
            if (table.getType() == DataProvider.QueryResult.Type.SELECT) {
                RowDescription(getTableHeader(table.getHeader()));
            }
            sendQueryResult(table, 0);
        }
        ReadyForQuery('I');
    }

    @Override
    protected void Bind(String portalName, String preparedStatment, List<Short> parameterFormatCodes, List<List<Byte>> parameterValues, List<Short> resultFormatCodes) throws PgProtocolException, IOException {
        String realQuery = _preparedStatements.get(preparedStatment);
        if (realQuery == null) {
            ErrorResponse(makeError("26000", "unknown statement name"));
            return;
        }
        for (short res : resultFormatCodes) {
            if (res == 1) {
                ErrorResponse(makeError("0A000", "unsupported binary result format"));
                //!> @todo support binary result format
            }
        }

        for (int i = 0; i < parameterValues.size(); i++) {
            boolean binary = parameterFormatCodes.get(i) == 1;
            List<Byte> value = parameterValues.get(i);
            String placeholder = "$" + (i + 1);
            String textVal;
            if (binary) {
                if (value.size() > 4) {
                    /*
                     TODO, depending of the method invoked ( set*(..) )
                     this value can have different binary encodings.
                     There is no real way to understand it without the knowledge of the
                     table.
                     Therefore we assume it is an integer and return an error message if not.
                     */
                    ErrorResponse(makeError("42804", "Binary encoding not supported. Set 'binaryTransfer' to false when connecting through JDBC" + (i + 1)));
                    return;
                }
                textVal = Integer.toString(Conversions.toInt(value));
            } else {
                textVal = Conversions.toString(value);
                textVal = textVal.replace("'", "''");
            }
            textVal = "'" + textVal + "'";
            realQuery = realQuery.replace(placeholder, textVal);

            /**
             * @todo this is a bug: here we don't have a parser, so it is
             * difficult to distinguish when $n is a placeholder from when it is
             * a string literal. Currently all the occurrences are replaced,
             * potentially breaking the semantic of the query.
             */
        }

        if (portalName.equals("")) {
            _portals.remove(""); //destroy the unnamed portal.
        }
        if (!_portals.containsKey(portalName)) {
            _portals.put(portalName, realQuery);
            BindComplete();
        } else {
            ErrorResponse(makeError("42602", "portal name already used"));
        }
    }

    @Override
    protected void Close(byte what, String name) throws PgProtocolException, IOException {
        /*
         * The Close message closes an existing prepared statement or portal and
         * releases resources.
         * It is not an error to issue Close against a nonexistent statement or
         * portal name. The response is normally CloseComplete, but could be
         * ErrorResponse if some difficulty is encountered while releasing resources.
         * Note that closing a prepared statement implicitly closes any open _portals
         * that were constructed from that statement.
         */
        switch (what) {
            case 'S':
                _preparedStatements.remove(name); //!>@todo close the related _portals too
                break;
            case 'P':
                _portals.remove(name);
                break;
            default:
                throw new PgProtocolException("Unrecognized close command " + what);
        }
        CloseComplete();
    }

    @Override
    protected void Execute(String portalName, int manRows) throws PgProtocolException, IOException {
        //maxRows == 0 means fetch them all
        String query = _portals.get(portalName);
        if (query == null) {
            ErrorResponse(makeError("42602", "unknown portal name"));
        } else {
            if (!respondToEmptyQuery(query)) {
                DataProvider.QueryResult table = _provider.getResult(query);
                sendQueryResult(table, manRows);
            }
        }
        /**
         * @todo currently the PortalSuspended message is not sent, because once
         * the execute command is issued again another query is actually
         * executed. The best way to suspend a portal may be decided once the
         * dataProvider is implemented.
         */
    }

    @Override
    protected void FunctionCall(int objectId, List<Short> argumentFormats, List<DataCellMsg> arguments, Short resultFormat) throws PgProtocolException, IOException {
        ErrorResponse(makeError("Not supported"));
    }

    @Override
    protected void Parse(String preparedStatment, String query, List<Integer> parametersType) throws PgProtocolException, IOException {
        if (preparedStatment.isEmpty()) {
            _preparedStatements.remove(preparedStatment);
        }
        if (!_preparedStatements.containsKey(preparedStatment)) {
            _preparedStatements.put(preparedStatment, query);
            ParseComplete();
        } else {
            ErrorResponse(makeError("26000", "the statement already exists"));
        }
    }

    @Override
    protected void Sync() throws PgProtocolException, IOException {
        ReadyForQuery('I'); //we don't implement transactions
    }

    private boolean respondToEmptyQuery(String query) throws PgProtocolException, IOException {
        if (query.isEmpty() || query.equals(";")) {
            EmptyQueryResponse();
            return true;
        }
        return false;
    }

    private List<ColumnDescriptionMsg> getTableHeader(List<String> headerNames) {
        List<ColumnDescriptionMsg> header = new ArrayList<>(headerNames.size());
        for (String n : headerNames) {
            header.add(new ColumnDescriptionMsg(n, 1043, (short) -1));
        }
        return header;
    }

    private void sendQueryResult(DataProvider.QueryResult table, int maxRows) throws PgProtocolException, IOException {
        switch (table.getType()) {
            case CREATE:
                CommandComplete("SELECT " + table.getRowCount());
                break;
            case DELETE:
                CommandComplete("DELETE " + table.getRowCount());
                break;
            case INSERT:
                CommandComplete("INSERT 0 " + table.getRowCount());
                break;
            case UPDATE:
                CommandComplete("UPDATE " + table.getRowCount());
                break;
            case SELECT: {
                int rows = 0;
                for (List<DataCellMsg> row : table.getRows()) {
                    DataRow(row);
                    if (++rows == maxRows) {
                        //TODO enable the PortalSuspended() messages
                        ErrorResponse(makeError("0A000", "PortalSuspend is not supported"));
                        return;
                    }
                }
                CommandComplete("SELECT " + table.getRowCount());
            }
            break;
            case ERROR: {
                ErrorResponse(makeError("42601", table.getErrorMessage()));
            }
            break;
            default:
                throw new PgProtocolException("unknown query result ");
        }
    }

    @Override
    protected void Describe(char what, String name) throws PgProtocolException, IOException {
        switch (what) {
            case 'S': // prepared statement
                ErrorResponse(makeError("0A000", "unsupported feature: describe prepared statement"));
                break;
            case 'P': // portal
                String q = _portals.get(name);
                if (q == null) {
                    ErrorResponse(makeError("42602", "unknown portal name"));
                } else {
                    DataProvider.QueryResult table = _provider.getResult(q);
                    if (table.getType() == DataProvider.QueryResult.Type.SELECT) {
                        RowDescription(getTableHeader(table.getHeader()));
                    } else {
                        NoData();
                    }
                }
                break;
            default:
                ErrorResponse(makeError("unknown command"));
                throw new PgProtocolException("Unknown message 'Describe " + what + "'");
        }
    }

    @Override
    protected void CopyFail(String errorMessage) throws PgProtocolException, IOException {
        throw new PgProtocolException("Not supported");
    }

    @Override
    protected void CopyDataClientMsg(List<Byte> data) throws PgProtocolException, IOException {
        throw new PgProtocolException("Not supported");
    }

    @Override
    protected void CopyDoneClientMsg() throws PgProtocolException, IOException {
        throw new PgProtocolException("Not supported");
    }

    @Override
    protected void Flush() throws PgProtocolException, IOException {
        /*
         * The Flush message does not cause any specific output to be generated, but
         * forces the backend to deliver any data pending in its output buffers.
         * A Flush must be sent after any extended-query command except Sync, if the
         * frontend wishes to examine the results of that command before issuing
         * more commands. Without Flush, messages returned by the backend will be
         * combined into the minimum possible number of packets to minimize network
         * overhead.
         */
    }

    /**
     * Closes the current connection if the current process id and secretKey
     * matches the parameters.
     *
     * @param pid the provided process id.
     * @param secretKey the provided secret key.
     * @return true if the parameter match and the connection has been closed.
     */
    public boolean kill(int pid, int secretKey) {
        if (_processId == pid && _secretKey == secretKey) {
            kill();
            return true;
        }
        return false;
    }
}
