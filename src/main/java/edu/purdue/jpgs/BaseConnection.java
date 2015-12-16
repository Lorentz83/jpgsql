package edu.purdue.jpgs;

import edu.purdue.jpgs.type.DataCellMsg;
import edu.purdue.jpgs.type.ColumnDescriptionMsg;
import edu.purdue.jpgs.type.NoticeResponseMsg;
import edu.purdue.jpgs.type.ErrorResponseMsg;
import edu.purdue.jpgs.io.PgReader;
import edu.purdue.jpgs.io.PgWriter;
import edu.purdue.jpgs.io.RawReader;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Lorenzo Bossi [lbossi@purdue.edu]
 */
public abstract class BaseConnection {

    private static final Logger LOGGER = Logger.getLogger(BaseConnection.class.getName());
    boolean authenticated = false;
    private final Socket _socket;
    private final RawReader _rawReader;

    public BaseConnection(Socket socket) throws IOException {
        if (socket == null) {
            throw new NullPointerException("socket");
        }
        _socket = socket;
        _rawReader = new RawReader(_socket.getInputStream());
    }

    private void simpleCommand(char command) throws PgProtocolException {
        try {
            new PgWriter(_socket.getOutputStream(), command).flush();
        } catch (IOException ex) {
            throw new PgProtocolException(ex);
        }
    }

    private PgWriter getWriter(char command) throws PgProtocolException {
        try {
            return new PgWriter(_socket.getOutputStream(), command);
        } catch (IOException ex) {
            throw new PgProtocolException(ex);
        }
    }

    protected void protocolStartUp() throws PgProtocolException {
        try {
            int req = _rawReader.readInt32();
            int version = _rawReader.readInt32();
            if (req == 8 && version == 80877103) {
                LOGGER.log(Level.WARNING, "refusing ssl request");
                simpleCommand('N');
                return;
            }
            if (req == 16 && version == 80877102) {
                int processId = _rawReader.readInt32();
                int secretKey = _rawReader.readInt32();
                CancelRequest(processId, secretKey);
                return;
            }
            if (version != 196608) {
                ErrorResponse(ErrorResponseMsg.makeError("08P01", "Unrecognized protocol version"));
                throw new PgProtocolException("Unrecognized protocol version " + version);
            }

            List<String> parList = _rawReader.readStringList(req - 8);
            Map<String, String> parameters = new TreeMap<>();

            for (int n = 0; n < parList.size() / 2; n += 2) {
                parameters.put(parList.get(n), parList.get(n + 1));
            }

            if (StartupMessage(version, parameters) && !authenticated) {
                char passwordRequest = _rawReader.readByte();
                if (passwordRequest != 'p') {
                    throw new PgProtocolException("expected password");
                }
                int len = _rawReader.readInt32(); //TODO check size
                PasswordMessage(_rawReader.readString());
            }
        } catch (IOException ex) {
            throw new PgProtocolException(ex);
        }
    }

    /**
     * Sends an AuthenticationOk message. This is a valid message only during
     * start-up phase, it should be called only in
     * baseConnection::StartupMessage or baseConnection::PasswordMessage once
     * the user is successfully authenticated.
     *
     * @ingroup postgresSartup
     */
    protected void AuthenticationOk() throws PgProtocolException {
        try (PgWriter writer = getWriter('R')) {
            writer.addInt32(0);
        }
        authenticated = true;
    }

    /**
     * Sends a request for a clear text Password. This method must be called
     * only in baseConnection::StartupMessage to require a clear text password.
     * The password will be provided by baseConnection::PasswordMessage
     *
     * @ingroup postgresSartup
     */
    protected void AuthenticationCleartextPassword() throws PgProtocolException {
        try (PgWriter writer = getWriter('R')) {
            writer.addInt32(3);
        }
    }

    /**
     * Sends a request for an MD5 encrypted password. The encrypted password
     * will be provided by baseConnection::PasswordMessage
     *
     * @param salt the MD5 salt.
     * @ingroup postgresSartup
     */
    protected void AuthenticationMD5Password(char salt0, char salt1, char salt2, char salt3) throws PgProtocolException {
        try (PgWriter writer = getWriter('R')) {
            writer.addInt32(5);
            writer.addByte(salt0);
            writer.addByte(salt1);
            writer.addByte(salt2);
            writer.addByte(salt3);
        }
    }

    //void AuthenticationKerberosV5()
    //void AuthenticationSCMCredential()
    //void AuthenticationGSS();
    //void AuthenticationSSPI();
    //void AuthenticationGSSContinue();
    /**
     * Sends a ready for query message. Currently an idle ready message is sent
     * automatically in baseConnection::run(). Valid statuses are: 'I' if idle
     * (not in a transaction block); 'T' if in a transaction block; or 'E' if in
     * a failed transaction block (queries will be rejected until block is
     * ended)
     *
     * @param status the status of the backend:
     * @ingroup postgresSimpleQuery
     */
    protected void ReadyForQuery(char status) throws PgProtocolException {
        try (PgWriter writer = getWriter('Z')) {
            writer.addByte(status);
        }
    }

    /**
     * Sends a command complete message.
     *
     * @param message a String representing what command was completed and how
     * many rows were involved.
     * @ingroup postgresSimpleQuery
     */
    protected void CommandComplete(String message) throws PgProtocolException {
        try (PgWriter writer = getWriter('Z')) {
            writer.addString(message);
        }
    }

    /**
     *
     * @ingroup postgresSimpleQuery
     */
    protected void CopyInResponse(byte format, Collection<Short> formats) throws PgProtocolException {
        try (PgWriter writer = getWriter('G')) {
            writer.addInt8(format);
            writer.addInt16((short) formats.size());
            writer.writeInt16(formats);
        }
    }

    /**
     * @ingroup postgresSimpleQuery
     */
    protected void CopyOutResponse(byte format, List<Short> formats) throws PgProtocolException {
        try (PgWriter writer = getWriter('H')) {
            writer.addInt8(format);
            writer.addInt16((short) formats.size());
            for (Short f : formats) {
                writer.addInt16(f);
            }
        }
    }

    /**
     * Sends a row description message. This message must be sent before
     * baseConnection::DataRow is used, whenever a query returns some data.
     *
     * @param tableHeader a list of ColumnDescriptionMsg to describe the table
     * header.
     * @ingroup postgresSimpleQuery
     */
    protected void RowDescription(List<ColumnDescriptionMsg> tableHeader) throws PgProtocolException {
        try (PgWriter writer = getWriter('T')) {
            writer.addInt16((short) tableHeader.size());
            for (ColumnDescriptionMsg it : tableHeader) {
                writer.addString(it.name);
                writer.addInt32(it.tableID);
                writer.addInt16(it.columnID);
                writer.addInt32(it.typeID);
                writer.addInt16(it.typeSize);
                writer.addInt32(it.typeModifier);
                writer.addInt16(it.formatCode);
            }
        }
    }

    /**
     * Sends a data row message. It sends one row of the returned table. Must be
     * called after a baseConnection::RowDescription.
     *
     * @param data a list of items, one per each column of the table returned.
     * @ingroup postgresSimpleQuery
     */
    protected void DataRow(List<DataCellMsg> data) throws PgProtocolException {
        try (PgWriter writer = getWriter('D')) {
            writer.addInt16((short) data.size());
            for (DataCellMsg it : data) {
                if (it.isNull()) {
                    writer.addInt32(-1);
                } else {
                    writer.addInt32(it.size());
                    for (Byte d : it.data) {
                        writer.addInt8(d);
                    }
                }
            }
        }
    }

    /**
     * Must be sent when an empty query String is recognized.
     *
     * @ingroup postgresSimpleQuery
     */
    protected void EmptyQueryResponse() throws PgProtocolException {
        simpleCommand('I');
    }

    /**
     * Sends an error response message. Note that the protocol allows different
     * level of messages (user readable, not localized, debug...). Refer to the
     * postgres documentation for the mandatory messages.
     *
     * @param messages list of error messages.
     * @ingroup postgresSimpleQuery
     */
    protected void ErrorResponse(List<ErrorResponseMsg> messages) throws PgProtocolException {
        try (PgWriter writer = getWriter('E')) {
            for (ErrorResponseMsg it : messages) {
                writer.addInt8(it.type);
                writer.addString(it.message);
            }
            writer.addInt8((byte) 0);
        }
    }

    /**
     *
     *
     * @ingroup postgresSimpleQuery
     */
    protected void NotificationResponse(int processId, String channelName, String payload) throws PgProtocolException {
        try (PgWriter writer = getWriter('A')) {
            writer.addInt32(processId);
            writer.addString(channelName);
            writer.addString(payload);
        }
    }

    //backend messages extended Query
    protected void PortalSuspended() throws PgProtocolException {
        simpleCommand('s');
    }

    protected void ParseComplete() throws PgProtocolException {
        simpleCommand('1');
    }

    protected void FunctionCallResponse() throws PgProtocolException {
        try (PgWriter writer = getWriter('V')) {
            writer.addInt32(-1);
        }
    }

    protected void FunctionCallResponse(List<Byte> result) throws PgProtocolException {
        try (PgWriter writer = getWriter('V')) {
            writer.addInt32(result.size());
            for (Byte b : result) {
                writer.addInt8(b);
            }
        }
    }

    protected void NoData() throws PgProtocolException {
        simpleCommand('n');
    }

    protected void NoticeResponse(Collection<NoticeResponseMsg> rsp) throws PgProtocolException {
        try (PgWriter writer = getWriter('N')) {
            for (NoticeResponseMsg it : rsp) {
                writer.addInt8(it.type);
                writer.addString(it.message);
            }
            writer.addInt32(0);
        }
    }

    protected void ParameterDescription(List<Integer> parametersId) throws PgProtocolException {
        try (PgWriter writer = getWriter('t')) {
            writer.addInt32(parametersId.size());
            for (int id : parametersId) {
                writer.addInt32(id);
            }
        }
    }

    protected void ParameterStatus(String paramName, String value) throws PgProtocolException {
        try (PgWriter writer = getWriter('S')) {
            writer.addString(paramName);
            writer.addString(value);
        }
    }

    protected void CopyBothResponse(byte format, List<Short> formats) throws PgProtocolException {
        try (PgWriter writer = getWriter('W')) {
            writer.addInt8(format);
            writer.addInt16((short) formats.size());
            for (Short f : formats) {
                writer.addInt16(f);
            }
        }
    }

    protected void CloseComplete() throws PgProtocolException {
        simpleCommand('3');
    }

    protected void BindComplete() throws PgProtocolException {
        simpleCommand('2');
    }

    protected void BackendKeyData(int processID, int secretKey) throws PgProtocolException {
        try (PgWriter writer = getWriter('K')) {
            writer.addInt32(processID);
            writer.addInt32(secretKey);
        }
    }

    protected void CopyDataServerMsg(Collection<Byte> data) throws PgProtocolException {
        try (PgWriter writer = getWriter('d')) {
            writer.addInt32(data.size());
            for (Byte b : data) {
                writer.addInt8(b);
            }
        }
    }

    protected void CopyDoneServerMsg() throws PgProtocolException {
        simpleCommand('c');
    }

    //frontend messages
    /**
     * Invoked when a StartupMessage is received. A subclass must implement this
     * to handle the user login. This method must call an Authentication*
     * method. If AuthenticationOk is called, no authentication is required and
     * the protocol exits the Start-up phase.
     *
     * @return false if the server wants close the connection without checking
     * the password.
     * @param protocolVersion the client protocol version (supported only
     * 196608)
     * @param parameters a map containing the additional informations provided
     * by the client. "user" is required and contains the user name; "database"
     * is optional and defaults to the user name.
     */
    protected abstract boolean StartupMessage(int protocolVersion, Map<String, String> parameters) throws PgProtocolException;

    /**
     * Invoked when the client provides a password. This method mast call
     * AuthenticationOk if the authentication is successful.
     *
     * @param password the (possibly encrypted) password provided by the client.
     */
    protected abstract void PasswordMessage(String password) throws PgProtocolException;

    /**
     * Invoked when a query the client send a query. According to the protocol,
     * the query String can contains multiple sql commands. Check the postgres
     * documentation for a complete set of
     * <a href="http://www.postgresql.org/docs/9.4/static/protocol-flow.html#AEN102803">valid
     * responses</a>.
     *
     * @param query the sql command(s).
     */
    protected abstract void Query(String query) throws PgProtocolException;

    protected abstract void Bind(String portalName, String preparedStatment, List<Short> parameterFormatCodes, List<List<Byte>> parameterValues, List<Short> resultFormatCodes) throws PgProtocolException;

    protected abstract void CancelRequest(int backendProcessId, int secretKey) throws PgProtocolException;

    protected abstract void Close(byte what, String name) throws PgProtocolException;

    protected abstract void CopyFail(String errorMessage) throws PgProtocolException;

    protected abstract void Describe(char what, String name) throws PgProtocolException;

    protected abstract void Execute(String portalName, int manRows) throws PgProtocolException;

    protected abstract void Flush() throws PgProtocolException;

    protected abstract void FunctionCall(int objectId, List<Short> argumentFormats, List<DataCellMsg> arguments, Short resultFormat) throws PgProtocolException;

    protected abstract void Parse(String preparedStatment, String query, List<Integer> parametersType) throws PgProtocolException;

    protected abstract void CopyDataClientMsg(List<Byte> data) throws PgProtocolException;

    protected abstract void CopyDoneClientMsg() throws PgProtocolException;

    protected abstract void Sync() throws PgProtocolException;

    /**
     * Called when the client gracefully closes the connection. The default
     * implementation does nothing, can be reimplemented to handle the event.
     */
    protected void Terminate() throws PgProtocolException {
    }

    /**
     * Starts the protocol loop.
     */
    public boolean run() throws PgProtocolException {
        try {
            protocolStartUp();
            if (!authenticated) {
                LOGGER.log(Level.WARNING, "user not authenticated, closing connection");
                return false;
            }
            ReadyForQuery('I');
            for (PgReader reader = new PgReader(_rawReader);; reader.checkAndReset()) {
                char command = reader.readCommand();

                switch (command) {
                    case 'Q': {
                        Query(reader.readString());
                        break;
                    }
                    case 'X': {
                        LOGGER.log(Level.INFO, "graceful termination");
                        Terminate();
                        return true;
                    }
                    case 'B': {
                        String portal = reader.readString();
                        String preparedStatement = reader.readString();
                        Short parameterNum = reader.readInt16();
                        List<Short> parameterFormats = reader.readInt16Vector(parameterNum);
                        Short parameterValNum = reader.readInt16();
                        List<List<Byte>> parameters = new ArrayList<>();
                        for (int n = 0; n < parameterValNum; n++) {
                            parameters.add(reader.readByteVector(reader.readInt32()));
                        }
                        List<Short> resultFormatCodes = reader.readInt16Vector(reader.readInt16());
                        Bind(portal, preparedStatement, parameterFormats, parameters, resultFormatCodes);
                        break;
                    }
                    case 'C': {
                        byte what = reader.readInt8();
                        Close(what, reader.readString());
                        break;
                    }
                    case 'd': {
                        CopyDataClientMsg(reader.readByteVector());
                        break;
                    }
                    case 'c': {
                        CopyDoneClientMsg();
                        break;
                    }
                    case 'f': {
                        CopyFail(reader.readString());
                        break;
                    }
                    case 'D': {
                        char what = reader.readByte();
                        Describe(what, reader.readString());
                        break;
                    }
                    case 'E': {
                        String name = reader.readString();
                        Execute(name, reader.readInt32());
                        break;
                    }
                    case 'H': {
                        Flush();
                        break;
                    }
                    case 'F': {
                        int objId = reader.readInt32();
                        List<Short> argsFormat = reader.readInt16Vector(reader.readInt16());
                        Short argProvided = reader.readInt16();
                        List<DataCellMsg> arguments = new ArrayList<>();
                        for (int n = 0; n < argProvided; n++) {
                            int size = reader.readInt32();
                            if (size < 0) {
                                arguments.add(new DataCellMsg());
                            } else {
                                List<Byte> data = reader.readByteVector(size);
                                arguments.add(new DataCellMsg(data));
                            }
                        }
                        Short resultFormat = reader.readInt16();
                        FunctionCall(objId, argsFormat, arguments, resultFormat);
                        break;
                    }
                    case 'P': {
                        String destination = reader.readString();
                        String query = reader.readString();
                        Short num = reader.readInt16();
                        List<Integer> parameterType = new ArrayList<>();
                        for (int n = 0; n < num; n++) {
                            parameterType.add(reader.readInt32());
                        }
                        Parse(destination, query, parameterType);
                        break;
                    }
                    case 'S': {
                        Sync();
                        break;
                    }
                    default:
                        LOGGER.log(Level.SEVERE, "unknown command {0}", command);
                        throw new PgProtocolException("unknown command " + command);
                }

            }
        } catch (PgProtocolException ex) {
            try {
                _socket.close();
            } catch (IOException ignoreme) {
            }
            throw ex;
        }
    }

    /**
     * Closes the network stream. This has the desired side effects to kill the
     * connection which will thrown an exception. Useful to implement the cancel
     * request.
     */
    public void kill() {
        try {
            LOGGER.log(Level.WARNING, "Killing server");
            _socket.close();
        } catch (IOException ex) {
            LOGGER.log(Level.SEVERE, "Error while attempting to kill server ", ex);
        }
    }

}
