package edu.purdue.jpgs;

import edu.purdue.jpgs.type.ColumnDescriptionMsg;
import edu.purdue.jpgs.utils.Conversions;
import edu.purdue.jpgs.type.DataCellMsg;
import static edu.purdue.jpgs.type.ErrorResponseMsg.makeError;
import edu.purdue.jpgs.utils.Portal;
import edu.purdue.jpgs.utils.StatementAndPortal;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.logging.Level;
import java.util.stream.Collectors;

/**
 * Abstracts most of the complicated messages defined by the Postgres protocol
 * giving an easier interface to interact. Note, to correctly handle all data
 * types set 'binaryTransfer' to false when connecting through JDBC. To use this
 * class just implement a {@link DataProvider}, pass it to the constructor and
 * execute {@link #run() } to start the protocol execution. This class supports
 * only authentication with clear text password.
 * <br>
 * Note: <ul>
 * <li>Describe a prepared statement is not supported;</li>
 * <li>Multiple queries in the same simple statement are not supported;</li>
 * <li>Binary result format is not supported;</li>
 * </ul>
 *
 * @author Lorenzo Bossi [lbossi@purdue.edu]
 */
public class SimpleConnection extends BaseConnection {

    private static final java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(SimpleConnection.class.getName());

    protected String _database;
    private final int _processId, _secretKey;
    private final DataProvider _provider;
    private final BiConsumer<Integer, Integer> _cancelCallback;
    private final StatementAndPortal _stm;

    /**
     * Creates a SimpleConnection.
     *
     * @param socket the socket the client is connected to.
     * @param provider the data provider.
     * @param pid the current process id. Sent to the client to make it able to
     * cancel the request killing the current request.
     * @param cancelCallback the function to be called when a cancel callback is
     * received. The parameters are the processId and the secretKey received.
     * @throws IOException if an I/O error occurs.
     * @throws NullPointerException if any of the parameters is null.
     */
    public SimpleConnection(Socket socket, DataProvider provider, int pid, BiConsumer<Integer, Integer> cancelCallback) throws IOException, NullPointerException {
        super(socket);
        _processId = pid;
        _secretKey = (int) (Math.random() * Integer.MAX_VALUE);
        _provider = provider;
        _cancelCallback = cancelCallback;
        _stm = new StatementAndPortal();
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
     * @throws IOException if an I/O error occurs.
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
        _stm.removeStatementCascade(""); //erase the unnamed statement and portal
        _stm.removePortal("");
        query = query.trim();
        if (isEmptyQuery(query)) {
            EmptyQueryResponse();
        } else {
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
        String statement = _stm.getStatementSql(preparedStatment);
        if (statement == null) {
            ErrorResponse(makeError("26000", "unknown statement name"));
            return;
        }
        for (short res : resultFormatCodes) {
            if (res == 1) {
                ErrorResponse(makeError("0A000", "unsupported binary result format"));
                LOGGER.log(Level.SEVERE, "The client requested a bynary result");
                //!> @todo support binary result format
            }
        }

        List<String> vals = new ArrayList<>(parameterValues.size());
        for (int i = 0; i < parameterValues.size(); i++) {
            boolean binary = parameterFormatCodes.get(i) == 1;
            List<Byte> value = parameterValues.get(i);
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
                    LOGGER.log(Level.SEVERE, "The client provided data in a bynary format. Set 'binaryTransfer' to false when connecting through JDBC");
                    ErrorResponse(makeError("42804", "Binary encoding not supported. Set 'binaryTransfer' to false when connecting through JDBC"));
                    return;
                }
                LOGGER.log(Level.WARNING, "The client provided data in a bynary format. It has been assumed to be an integer.");
                textVal = Integer.toString(Conversions.toInt(value));
            } else {
                textVal = Conversions.toString(value);
                textVal = textVal.replace("'", "''");
            }
            textVal = "'" + textVal + "'";
            vals.add(textVal);
        }
        try {
            String realQuery = Conversions.bind(statement, vals);
            if (portalName.equals("")) {
                _stm.removePortal(""); //destroy the unnamed portal.
            }
            if (_stm.putPortal(preparedStatment, portalName, realQuery)) {
                BindComplete();
            } else {
                ErrorResponse(makeError("42602", "portal name already used"));
            }
        } catch (PgProtocolException ex) {
            ErrorResponse(makeError("03000", ex.getMessage()));
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
                _stm.removeStatementCascade(name);
                break;
            case 'P':
                _stm.removePortal(name);
                break;
            default:
                LOGGER.log(Level.SEVERE, "Unrecognized close command {0}", what);
                throw new PgProtocolException("Unrecognized close command " + what);
        }
        CloseComplete();
    }

    @Override
    protected void Execute(String portalName, int maxRows) throws PgProtocolException, IOException {
        Portal portal = _stm.getPortal(portalName);
        if (portal == null) {
            ErrorResponse(makeError("42602", "unknown portal name"));
        } else {
            if (isEmptyQuery(portal.sql)) {
                EmptyQueryResponse();
            } else {
                DataProvider.QueryResult res = portal.getAndStoreResult(_provider);
                sendQueryResult(res, maxRows);
            }
        }
    }

    @Override
    protected void FunctionCall(int objectId, List<Short> argumentFormats, List<DataCellMsg> arguments, Short resultFormat) throws PgProtocolException, IOException {
        LOGGER.log(Level.SEVERE, "The client asked for a deprecated FunctionCall");
        ErrorResponse(makeError("Not supported"));
    }

    @Override
    protected void Parse(String preparedStatment, String query, List<Integer> parametersType) throws PgProtocolException, IOException {
        //TODO, we should check the parameter type here to avoid the problem of guessing it in the bind.
        if (preparedStatment.isEmpty()) {
            _stm.removeStatementCascade(preparedStatment);
        }
        if (_stm.putStatement(preparedStatment, query)) {
            ParseComplete();
        } else {
            ErrorResponse(makeError("26000", "the statement already exists"));
        }
    }

    @Override
    protected void Sync() throws PgProtocolException, IOException {
        ReadyForQuery('I'); //we don't implement transactions
    }

    private boolean isEmptyQuery(String query) {
        return query.isEmpty() || query.equals(";");
    }

    private List<ColumnDescriptionMsg> getTableHeader(List<String> headerNames) {
        List<ColumnDescriptionMsg> header = new ArrayList<>(headerNames.size());
        for (String n : headerNames) {
            header.add(new ColumnDescriptionMsg(n));
        }
        return header;
    }

    private void sendQueryResult(DataProvider.QueryResult table, int maxRows) throws PgProtocolException, IOException {
        switch (table.getType()) {
            case ERROR:
                ErrorResponse(makeError("42601", table.getErrorMessage()));
                break;
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
            case SELECT:
                if (maxRows == 0) { //maxRows == 0 means fetch them all
                    maxRows = Integer.MAX_VALUE;
                }
                Iterator<List<String>> it = table.getRows();
                for (int rowNum = 0; rowNum < maxRows && it.hasNext(); rowNum++) {
                    List<DataCellMsg> rawRow = it.next().stream().map(str -> {
                        return (str == null) ? new DataCellMsg() : new DataCellMsg(str);
                    }).collect(Collectors.toList());
                    DataRow(rawRow);
                }
                if (it.hasNext()) {
                    PortalSuspended();
                } else {
                    CommandComplete("SELECT " + table.getRowCount());
                }
                break;
            default:
                LOGGER.log(Level.SEVERE, "unknown query result {0}", table.getType());
                throw new PgProtocolException("unknown query result " + table.getType());
        }
    }

    @Override
    protected void Describe(char what, String name) throws PgProtocolException, IOException {
        switch (what) {
            case 'S':
                /* TODO implement this.
                 * The Describe message (statement variant) specifies the name
                 * of an existing prepared statement (or an empty string for the
                 * unnamed prepared statement). The response is a
                 * ParameterDescription message describing the parameters needed
                 * by the statement, followed by a RowDescription message
                 * describing the rows that will be returned when the statement
                 * is eventually executed (or a NoData message if the statement
                 * will not return rows). ErrorResponse is issued if there is no
                 * such prepared statement. Note that since Bind has not yet
                 * been issued, the formats to be used for returned columns are
                 * not yet known to the backend; the format code fields in the
                 * RowDescription message will be zeroes in this case.
                 */
                LOGGER.log(Level.SEVERE, "The client requested the unsupported 'describe prepared statement' function");
                ErrorResponse(makeError("0A000", "unsupported feature: describe prepared statement"));
                break;
            case 'P': // portal
                /*
                 * The Describe message (portal variant) specifies the name of
                 * an existing portal (or an empty string for the unnamed
                 * portal). The response is a RowDescription message describing
                 * the rows that will be returned by executing the portal; or a
                 * NoData message if the portal does not contain a query that
                 * will return rows; or ErrorResponse if there is no such
                 * portal.
                 */
                Portal portal = _stm.getPortal(name);
                if (portal == null) {
                    ErrorResponse(makeError("42602", "unknown portal name"));
                } else {
                    if (isEmptyQuery(portal.sql)) {
                        NoData();
                    } else {
                        DataProvider.QueryResult res = portal.getAndStoreResult(_provider);
                        if (res.getType() == DataProvider.QueryResult.Type.SELECT) {
                            RowDescription(getTableHeader(res.getHeader()));
                        } else {
                            NoData();
                        }
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
        LOGGER.log(Level.SEVERE, "Copy operations are not implemented");
        throw new PgProtocolException("Not supported");
    }

    @Override
    protected void CopyDataClientMsg(List<Byte> data) throws PgProtocolException, IOException {
        LOGGER.log(Level.SEVERE, "Copy operations are not implemented");
        throw new PgProtocolException("Not supported");
    }

    @Override
    protected void CopyDoneClientMsg() throws PgProtocolException, IOException {
        LOGGER.log(Level.SEVERE, "Copy operations are not implemented");
        throw new PgProtocolException("Not supported");
    }

    @Override
    protected int getSecretKey() {
        return _secretKey;
    }
}
