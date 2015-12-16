package edu.purdue.jpgs;

import edu.purdue.jpgs.type.ColumnDescriptionMsg;
import edu.purdue.jpgs.type.Conversions;
import edu.purdue.jpgs.type.DataCellMsg;
import edu.purdue.jpgs.type.ErrorResponseMsg;
import static edu.purdue.jpgs.type.ErrorResponseMsg.makeError;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Lorenzo Bossi [lbossi@purdue.edu]
 */
public class SimpleConnection extends BaseConnection {

    protected String _database;
    private final int _processId, _secretKey;
    private DataProvider _provider;
    Map<String, String> _preparedStatements;
    Map<String, String> _portals;

    public SimpleConnection(Socket socket, DataProvider provider, int pid) throws IOException {
        super(socket);
        _processId = pid;
        _secretKey = (int) (Math.random() * Integer.MAX_VALUE);
    }

    @Override
    protected void StartupMessage(int protocolVersion, Map<String, String> parameters) throws PgProtocolException {
        String user = parameters.get("user");
        if (user == null) {
            throw new PgProtocolException("Missing username");
        }
        String database = parameters.get("database");
        if (database == null) {
            database = user;
            _provider.setDatabase(database);
        }

        if (_provider.setUser(user)) {
            AuthenticationOk();
            BackendKeyData(_processId, _secretKey);
        } else {
            AuthenticationCleartextPassword();
        }
    }

    @Override
    protected void PasswordMessage(String password) throws PgProtocolException {
        if (_provider.setPassword(password)) {
            AuthenticationOk();
            BackendKeyData(_processId, _secretKey);
        }
    }

    @Override
    protected void Query(String query) throws PgProtocolException {
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
    protected void Bind(String portalName, String preparedStatment, List<Short> parameterFormatCodes, List<List<Byte>> parameterValues, List<Short> resultFormatCodes) throws PgProtocolException {
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
                textVal = Integer.toString(Conversions.toInt(value));
            } else {
                textVal = Conversions.toString(value);
                textVal = textVal.replace("\"", "\"\"");
                textVal = "\"" + textVal + "\"";
            }
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
        if (_portals.containsKey(portalName)) {
            _portals.put(portalName, realQuery);
            BindComplete();
        } else {
            ErrorResponse(makeError("42602", "portal name already used"));
        }
    }

    @Override
    protected void CancelRequest(int backendProcessId, int secretKey) throws PgProtocolException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    protected void Close(byte what, String name) throws PgProtocolException {
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
    protected void Execute(String portalName, int manRows) throws PgProtocolException {
        //maxRows == 0 means fetch them all
        String query = _portals.get(portalName);
        if (!respondToEmptyQuery(query)) {
            DataProvider.QueryResult table = _provider.getResult(query);
            sendQueryResult(table, manRows);
        }
        /**
         * @todo currently the PortalSuspended message is not sent, because once
         * the execute command is issued again another query is actually
         * executed. The best way to suspend a portal may be decided once the
         * dataProvider is implemented.
         */
    }

    @Override
    protected void FunctionCall(int objectId, List<Short> argumentFormats, List<DataCellMsg> arguments, Short resultFormat) throws PgProtocolException {
        ErrorResponse(makeError("Not supported"));
    }

    @Override
    protected void Parse(String preparedStatment, String query, List<Integer> parametersType) throws PgProtocolException {
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
    protected void Sync() throws PgProtocolException {
        ReadyForQuery('I'); //we don't implement transactions
    }

    private boolean respondToEmptyQuery(String query) throws PgProtocolException {
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

    private void sendQueryResult(DataProvider.QueryResult table, int maxRows) throws PgProtocolException {
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
                List<ErrorResponseMsg> errors = new ArrayList<>(3);
                errors.add(new ErrorResponseMsg('S', "ERROR"));
                errors.add(new ErrorResponseMsg('C', "42601"));
                errors.add(new ErrorResponseMsg('M', table.getErrorMessage()));
                //        errors.push_back(errorResponseMsg('P', "1"));
                //        errors.push_back(errorResponseMsg('F', "scan.l"));
                //        errors.push_back(errorResponseMsg('L', "1053"));
                //        errors.push_back(errorResponseMsg('R', "scanner_yyerror"));
                ErrorResponse(errors);
            }
            break;
            default:
                throw new PgProtocolException("unknown query result ");
        }
    }

    @Override
    protected void Describe(byte what, String name) throws PgProtocolException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    protected void CopyFail(String errorMessage) throws PgProtocolException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    protected void CopyDataClientMsg(List<Byte> data) throws PgProtocolException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    protected void CopyDoneClientMsg() throws PgProtocolException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    protected void Flush() throws PgProtocolException {
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
}
