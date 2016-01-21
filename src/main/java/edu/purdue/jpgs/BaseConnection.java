package edu.purdue.jpgs;

import edu.purdue.jpgs.type.DataCellMsg;
import edu.purdue.jpgs.type.ColumnDescriptionMsg;
import edu.purdue.jpgs.type.NoticeResponseMsg;
import edu.purdue.jpgs.type.ErrorResponseMsg;
import edu.purdue.jpgs.io.PgReader;
import edu.purdue.jpgs.io.PgWriter;
import edu.purdue.jpgs.io.RawReader;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implements the base functions required by the Postgres protocol to handle one
 * connection. See
 * <a href="http://www.postgresql.org/docs/9.4/static/protocol-message-formats.html">message
 * formats</a> for details. Upper case functions are named after the message
 * defined by the Postgres communication protocol. The methods implemented are
 * the one used by the backend to sends reply, the abstracts methods are the one
 * to be implemented to handle the client requests. This implementation refuses
 * SSL connections.
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

    private void simpleCommand(char command) throws PgProtocolException, IOException {
        try {
            new PgWriter(_socket.getOutputStream(), command).flush();
        } catch (IOException ex) {
            throw new PgProtocolException(ex);
        }
    }

    private PgWriter getWriter(char command) throws IOException {
        return new PgWriter(_socket.getOutputStream(), command);
    }

    /**
     * Sends a BackendKeyData message. This message provides secret-key data
     * that the frontend must save if it wants to be able to issue cancel
     * requests later.
     *
     * @param processID the current process id.
     * @param secretKey the current secretKey.
     * @throws PgProtocolException in case of errors in the protocol.
     * @throws IOException if an I/O error occurs.
     */
    protected void BackendKeyData(int processID, int secretKey) throws PgProtocolException, IOException {
        try (PgWriter writer = getWriter('K')) {
            writer.addInt32(processID);
            writer.addInt32(secretKey);
        }
    }

    /**
     * Performs the protocol startup. In this phase the client provide user
     * name, password and database name. Note that ssl request is denied in this
     * phase.
     *
     * @throws PgProtocolException in case of errors in the protocol.
     * @throws IOException if an I/O error occurs.
     */
    protected void protocolStartUp() throws PgProtocolException, IOException {
        try {
            int req = _rawReader.readInt32();
            int version = _rawReader.readInt32();
            if (req == 8 && version == 80877103) {
                LOGGER.log(Level.WARNING, "refusing ssl request");
                OutputStream os = _socket.getOutputStream();
                os.write('N');
                os.flush();
                //To continue after N, send the usual StartupMessage and proceed without encryption.
                protocolStartUp();
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
                int len = _rawReader.readInt32();
                RawReader.CString password = _rawReader.readString();
                if (len - 4 != password.length) {
                    throw new PgProtocolException("protocol out of sync");
                }
                PasswordMessage(password.str);
            }
        } catch (IOException ex) {
            throw new PgProtocolException(ex);
        }
    }

    /**
     * Sends an AuthenticationOk message. This is a valid message only during
     * start-up phase, it should be called only in
     * {@link #StartupMessage(int, java.util.Map)} or
     * {@link #PasswordMessage(java.lang.String) } once the user is successfully
     * authenticated.
     *
     * Part of the protocol startup.
     *
     * @throws PgProtocolException in case of errors in the protocol.
     * @throws IOException if an I/O error occurs.
     */
    protected void AuthenticationOk() throws PgProtocolException, IOException {
        try (PgWriter writer = getWriter('R')) {
            writer.addInt32(0);
        }
        authenticated = true;
    }

    /**
     * Sends a request for a clear text Password. This method must be called
     * only in {@link  #StartupMessage} to require a clear text password. The
     * password will be provided by {@link #PasswordMessage}.
     *
     * Part of the protocol startup.
     *
     * @throws PgProtocolException in case of errors in the protocol.
     * @throws IOException if an I/O error occurs.
     */
    protected void AuthenticationCleartextPassword() throws PgProtocolException, IOException {
        try (PgWriter writer = getWriter('R')) {
            writer.addInt32(3);
        }
    }

    /**
     * Sends a request for an MD5 encrypted password. The encrypted password
     * will be provided by {@link #PasswordMessage}. Part of the protocol
     * startup.
     *
     * @param salt0 first byte to use as salt.
     * @param salt1 second byte to use as salt.
     * @param salt2 third byte to use as salt.
     * @param salt3 fourth byte to use as salt.
     * @throws PgProtocolException in case of errors in the protocol.
     * @throws IOException if an I/O error occurs.
     */
    protected void AuthenticationMD5Password(char salt0, char salt1, char salt2, char salt3) throws PgProtocolException, IOException {
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
     * automatically in {@link #run()}. Valid statuses are: 'I' if idle (not in
     * a transaction block); 'T' if in a transaction block; or 'E' if in a
     * failed transaction block (queries will be rejected until block is ended).
     *
     *
     * @param status the status of the backend.
     * @throws PgProtocolException in case of errors in the protocol.
     * @throws IOException if an I/O error occurs.
     */
    protected void ReadyForQuery(char status) throws PgProtocolException, IOException {
        try (PgWriter writer = getWriter('Z')) {
            writer.addByte(status);
        }
    }

    /**
     * Sends a command complete message.
     *
     * @param message a String representing what command was completed and how
     * many rows were involved.
     * @throws PgProtocolException in case of errors in the protocol.
     * @throws IOException if an I/O error occurs.
     */
    protected void CommandComplete(String message) throws PgProtocolException, IOException {
        try (PgWriter writer = getWriter('C')) {
            writer.addString(message);
        }
    }

    /**
     * Sends a Start Copy In response. The frontend must now send copy-in data
     * (if not prepared to do so, send a CopyFail message).
     *
     * @param format 0 indicates the overall COPY format is textual (rows
     * separated by newlines, columns separated by separator characters, etc). 1
     * indicates the overall copy format is binary.
     * @param formats The format codes to be used for each column. Each must
     * presently be zero (text) or one (binary).
     * @throws PgProtocolException in case of errors in the protocol.
     * @throws IOException if an I/O error occurs.
     */
    protected void CopyInResponse(byte format, Collection<Short> formats) throws PgProtocolException, IOException {
        try (PgWriter writer = getWriter('G')) {
            writer.addInt8(format);
            writer.addInt16((short) formats.size());
            for (Short f : formats) {
                writer.addInt16(f);
            }
        }
    }

    /**
     * Sends a Start Copy Out response. This message will be followed by
     * copy-out data.
     *
     * @param format 0 indicates the overall COPY format is textual (rows
     * separated by newlines, columns separated by separator characters, etc). 1
     * indicates the overall copy format is binary.
     * @param formats The format codes to be used for each column. Each must
     * presently be zero (text) or one (binary).
     * @throws PgProtocolException in case of errors in the protocol.
     * @throws IOException if an I/O error occurs.
     */
    protected void CopyOutResponse(byte format, Collection<Short> formats) throws PgProtocolException, IOException {
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
     * {@link #DataRow} is used, whenever a query returns some data.
     *
     * @param tableHeader a collection of ColumnDescriptionMsg to describe the
     * table header.
     * @throws PgProtocolException in case of errors in the protocol.
     * @throws IOException if an I/O error occurs.
     */
    protected void RowDescription(Collection<ColumnDescriptionMsg> tableHeader) throws PgProtocolException, IOException {
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
     * called after a {@link #RowDescription}.
     *
     * @param data a collection of items, one per each column of the table
     * returned.
     * @throws PgProtocolException in case of errors in the protocol.
     * @throws IOException if an I/O error occurs.
     */
    protected void DataRow(Collection<DataCellMsg> data) throws PgProtocolException, IOException {
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
     * @throws PgProtocolException in case of errors in the protocol.
     * @throws IOException if an I/O error occurs.
     */
    protected void EmptyQueryResponse() throws PgProtocolException, IOException {
        simpleCommand('I');
    }

    /**
     * Sends an error response message. Note that the protocol allows different
     * level of messages (user readable, not localized, debug...). Refer to the
     * <a href="http://www.postgresql.org/docs/9.4/static/protocol-error-fields.html">postgres
     * documentation</a> for the minimum set of mandatory messages.
     *
     * @param messages collection of error messages.
     * @throws PgProtocolException in case of errors in the protocol.
     * @throws IOException if an I/O error occurs.
     */
    protected void ErrorResponse(Collection<ErrorResponseMsg> messages) throws PgProtocolException, IOException {
        try (PgWriter writer = getWriter('E')) {
            for (ErrorResponseMsg it : messages) {
                writer.addInt8(it.type);
                writer.addString(it.message);
            }
            writer.addInt8((byte) 0);
        }
    }

    /**
     * Sends a notification response message. If a frontend issues a LISTEN
     * command, then the backend will send a NotificationResponse message (not
     * to be confused with NoticeResponse!) whenever a NOTIFY command is
     * executed for the same channel name. This is used for asynchronous
     * operations.
     *
     * @param processId the process ID of the notifying backend process.
     * @param channelName the name of the channel that the notify has been
     * raised on.
     * @param payload the "payload" string passed from the notifying process.
     * @throws PgProtocolException in case of errors in the protocol.
     * @throws IOException if an I/O error occurs.
     */
    protected void NotificationResponse(int processId, String channelName, String payload) throws PgProtocolException, IOException {
        try (PgWriter writer = getWriter('A')) {
            writer.addInt32(processId);
            writer.addString(channelName);
            writer.addString(payload);
        }
    }

    /**
     * Sends a PortalSuspended Message. This only appears if an Execute
     * message's row-count limit was reached.
     *
     * @throws PgProtocolException in case of errors in the protocol.
     * @throws IOException if an I/O error occurs.
     */
    protected void PortalSuspended() throws PgProtocolException, IOException {
        simpleCommand('s');
    }

    /**
     * Sends a ParseComplete message.
     *
     * @throws PgProtocolException in case of errors in the protocol.
     * @throws IOException if an I/O error occurs.
     */
    protected void ParseComplete() throws PgProtocolException, IOException {
        simpleCommand('1');
    }

    /**
     * Sends a FunctionCallResponse with a null result. Note that this part of
     * the protocol is now deprecated.
     *
     * @throws PgProtocolException in case of errors in the protocol.
     * @throws IOException if an I/O error occurs.
     */
    protected void FunctionCallResponse() throws PgProtocolException, IOException {
        try (PgWriter writer = getWriter('V')) {
            writer.addInt32(-1);
        }
    }

    /**
     * Sends a FunctionCallResponse. Note that this part of the protocol is now
     * deprecated.
     *
     * @param result the result of the function.
     * @throws PgProtocolException in case of errors in the protocol.
     * @throws IOException if an I/O error occurs.
     */
    protected void FunctionCallResponse(Collection<Byte> result) throws PgProtocolException, IOException {
        try (PgWriter writer = getWriter('V')) {
            writer.addInt32(result.size());
            for (Byte b : result) {
                writer.addInt8(b);
            }
        }
    }

    /**
     * Sends a NoData message. In response to a Describe when no data will be
     * returned.
     *
     * @throws PgProtocolException in case of errors in the protocol.
     * @throws IOException if an I/O error occurs.
     */
    protected void NoData() throws PgProtocolException, IOException {
        simpleCommand('n');
    }

    /**
     * Sends a NoticeResponse. A warning message has been issued. The frontend
     * should display the message but continue listening for ReadyForQuery or
     * ErrorResponse.
     *
     * @param messages a collection of notice messages.
     * @throws PgProtocolException in case of errors in the protocol.
     * @throws IOException if an I/O error occurs.
     */
    protected void NoticeResponse(Collection<NoticeResponseMsg> messages) throws PgProtocolException, IOException {
        try (PgWriter writer = getWriter('N')) {
            for (NoticeResponseMsg it : messages) {
                writer.addInt8(it.type);
                writer.addString(it.message);
            }
            writer.addInt32(0);
        }
    }

    /**
     * Sends a ParameterDescription message.
     *
     * @param parametersId a collection that specifies the object ID of the
     * parameter data type.
     * @throws PgProtocolException in case of errors in the protocol.
     * @throws IOException if an I/O error occurs.
     */
    protected void ParameterDescription(Collection<Integer> parametersId) throws PgProtocolException, IOException {
        try (PgWriter writer = getWriter('t')) {
            writer.addInt32(parametersId.size());
            for (int id : parametersId) {
                writer.addInt32(id);
            }
        }
    }

    /**
     * Sends a ParameterStatus message.
     *
     * @param paramName the name of the run-time parameter being reported.
     * @param value the current value of the parameter.
     * @throws PgProtocolException in case of errors in the protocol.
     * @throws IOException if an I/O error occurs.
     */
    protected void ParameterStatus(String paramName, String value) throws PgProtocolException, IOException {
        try (PgWriter writer = getWriter('S')) {
            writer.addString(paramName);
            writer.addString(value);
        }
    }

    /**
     * Sends a CopyBothResponse message.
     *
     * @param format 0 indicates the overall COPY format is textual (rows
     * separated by newlines, columns separated by separator characters, etc). 1
     * indicates the overall copy format is binary.
     * @param formats The format codes to be used for each column. Each must
     * presently be zero (text) or one (binary).
     * @throws PgProtocolException in case of errors in the protocol.
     * @throws IOException if an I/O error occurs.
     */
    protected void CopyBothResponse(byte format, Collection<Short> formats) throws PgProtocolException, IOException {
        try (PgWriter writer = getWriter('W')) {
            writer.addInt8(format);
            writer.addInt16((short) formats.size());
            for (Short f : formats) {
                writer.addInt16(f);
            }
        }
    }

    /**
     * Sends a CloseComplete message. In reply to a {@link #Close(byte, java.lang.String)
     * }.
     *
     * @throws PgProtocolException in case of errors in the protocol.
     * @throws IOException if an I/O error occurs.
     */
    protected void CloseComplete() throws PgProtocolException, IOException {
        simpleCommand('3');
    }

    /**
     * Sends a BindComplete message. In reply to a {@link #Bind(java.lang.String, java.lang.String, java.util.List, java.util.List, java.util.List)
     * }.
     *
     * @throws PgProtocolException in case of errors in the protocol.
     * @throws IOException if an I/O error occurs.
     */
    protected void BindComplete() throws PgProtocolException, IOException {
        simpleCommand('2');
    }

    /**
     * Sends a CopyData message.
     *
     * @param data data that forms part of a COPY data stream.
     * @throws PgProtocolException in case of errors in the protocol.
     * @throws IOException if an I/O error occurs.
     */
    protected void CopyDataServerMsg(Collection<Byte> data) throws PgProtocolException, IOException {
        try (PgWriter writer = getWriter('d')) {
            writer.addInt32(data.size());
            for (Byte b : data) {
                writer.addInt8(b);
            }
        }
    }

    /**
     * Sends a CopyDone message.
     *
     * @throws PgProtocolException in case of errors in the protocol.
     * @throws IOException if an I/O error occurs.
     */
    protected void CopyDoneServerMsg() throws PgProtocolException, IOException {
        simpleCommand('c');
    }

    //frontend messages
    /**
     * Invoked when a StartupMessage is received. A subclass must implement this
     * to handle the user login. This method must call an Authentication*
     * method. If {@link #AuthenticationOk() } is called, no authentication is
     * required and the protocol exits the Start-up phase.
     *
     * @return false if the server wants close the connection without checking
     * the password (i.e. the user is not allowed at all).
     * @param protocolVersion the client protocol version (supported only
     * 196608)
     * @param parameters a map containing the additional informations provided
     * by the client. "user" is required and contains the user name; "database"
     * is optional and defaults to the user name.
     * @throws PgProtocolException in case of errors in the protocol.
     * @throws IOException if an I/O error occurs.
     */
    protected abstract boolean StartupMessage(int protocolVersion, Map<String, String> parameters) throws PgProtocolException, IOException;

    /**
     * Invoked when the client provides a password. This method mast call
     * {@link #AuthenticationOk()} if the authentication is successful.
     *
     * @param password the (possibly encrypted) password provided by the client.
     * @throws PgProtocolException in case of errors in the protocol.
     * @throws IOException if an I/O error occurs.
     */
    protected abstract void PasswordMessage(String password) throws PgProtocolException, IOException;

    /**
     * Invoked when the client send a query. According to the protocol, the
     * query String can contains multiple sql commands. This is part of the
     * simple query protocol, used by psql command line tool but not by jdbc.
     *
     * @param query the sql command(s).
     * @throws PgProtocolException in case of errors in the protocol.
     * @throws IOException if an I/O error occurs.
     */
    protected abstract void Query(String query) throws PgProtocolException, IOException;

    /**
     * Invoked when a bind command is received. Note that (contrary to what is
     * specified in the Postgres message), it is guaranteed that
     * parameterFormatCodes and parameterValues have the same size.
     *
     * @param portalName the name of the destination portal (an empty string
     * selects the unnamed portal).
     * @param preparedStatment the name of the source prepared statement (an
     * empty string selects the unnamed prepared statement).
     * @param parameterFormatCodes the parameter format codes. Each must
     * presently be zero (text) or one (binary).
     * @param parameterValues the list of parameters.
     * @param resultFormatCodes a list containing the format code of the result.
     * @throws PgProtocolException in case of errors in the protocol.
     * @throws IOException if an I/O error occurs.
     */
    protected abstract void Bind(String portalName, String preparedStatment, List<Short> parameterFormatCodes, List<List<Byte>> parameterValues, List<Short> resultFormatCodes) throws PgProtocolException, IOException;

    /**
     * Invoked when a CancelRequest message is received. When the frontend opens
     * a new connection, instead of the StartupMessage can send a CancelRequest
     * to kill another running backend instance.
     *
     * @param backendProcessId the process id to cancel.
     * @param secretKey the process secret key to match.
     * @throws PgProtocolException in case of errors in the protocol.
     * @throws IOException if an I/O error occurs.
     */
    protected abstract void CancelRequest(int backendProcessId, int secretKey) throws PgProtocolException, IOException;

    /**
     * Invoked when a Close message is received.
     *
     * @param what 'S' to close a prepared statement; or 'P' to close a portal.
     * @param name the name of the prepared statement or portal to close (an
     * empty string selects the unnamed prepared statement or portal).
     * @throws PgProtocolException in case of errors in the protocol.
     * @throws IOException if an I/O error occurs.
     */
    protected abstract void Close(byte what, String name) throws PgProtocolException, IOException;

    /**
     * Invoked when a CopyFail message is received.
     *
     * @param errorMessage an error message to report as the cause of failure.
     * @throws PgProtocolException in case of errors in the protocol.
     * @throws IOException if an I/O error occurs.
     */
    protected abstract void CopyFail(String errorMessage) throws PgProtocolException, IOException;

    /**
     * Invoked when a Describe message is received.
     *
     * @param what 'S' to describe a prepared statement; or 'P' to describe a
     * portal.
     * @param name the name of the prepared statement or portal to describe (an
     * empty string selects the unnamed prepared statement or portal).
     * @throws PgProtocolException in case of errors in the protocol.
     * @throws IOException if an I/O error occurs.
     */
    protected abstract void Describe(char what, String name) throws PgProtocolException, IOException;

    /**
     * Invoked when an Execute message is received.
     *
     * @param portalName the name of the portal to execute (an empty string
     * selects the unnamed portal).
     * @param manRows the maximum number of rows to return, if portal contains a
     * query that returns rows (ignored otherwise). Zero denotes "no limit".
     * @throws PgProtocolException in case of errors in the protocol.
     * @throws IOException if an I/O error occurs.
     */
    protected abstract void Execute(String portalName, int manRows) throws PgProtocolException, IOException;

    /**
     * Invoked when a FunctionCall message is received. Note: The Function Call
     * sub-protocol is a legacy feature that is probably best avoided in new
     * code.
     *
     * @param objectId specifies the object ID of the function to call.
     * @param argumentFormats the argument format codes. Each must presently be
     * zero (text) or one (binary).
     * @param arguments a collection with the argument values.
     * @param resultFormat the format code for the function result. Must
     * presently be zero (text) or one (binary).
     * @throws PgProtocolException in case of errors in the protocol.
     * @throws IOException if an I/O error occurs.
     */
    protected abstract void FunctionCall(int objectId, List<Short> argumentFormats, List<DataCellMsg> arguments, Short resultFormat) throws PgProtocolException, IOException;

    /**
     * Invoked when a Parse message is received.
     *
     * @param preparedStatment the name of the destination prepared statement
     * (an empty string selects the unnamed prepared statement).
     * @param query the query string to be parsed.
     * @param parametersType a list that specifies the object ID of the
     * parameter data type. Placing a zero here is equivalent to leaving the
     * type unspecified.
     * @throws PgProtocolException in case of errors in the protocol.
     * @throws IOException if an I/O error occurs.
     */
    protected abstract void Parse(String preparedStatment, String query, List<Integer> parametersType) throws PgProtocolException, IOException;

    /**
     * Invoked when a CopyData message is received.
     *
     * @param data data that forms part of a COPY data stream.
     * @throws PgProtocolException in case of errors in the protocol.
     * @throws IOException if an I/O error occurs.
     */
    protected abstract void CopyDataClientMsg(List<Byte> data) throws PgProtocolException, IOException;

    /**
     * Invoked when a CopyDone message is received.
     *
     * @throws PgProtocolException in case of errors in the protocol.
     * @throws IOException if an I/O error occurs.
     */
    protected abstract void CopyDoneClientMsg() throws PgProtocolException, IOException;

    /**
     * Invoked when a Sync message is received.
     *
     * @throws PgProtocolException in case of errors in the protocol.
     * @throws IOException if an I/O error occurs.
     */
    protected abstract void Sync() throws PgProtocolException, IOException;

    /**
     * Returns the secret key generated by this connection. This is the value
     * sent to the client through {@link #BackendKeyData(int, int)} that can be
     * provided through a new connection to ask to terminate this one.
     *
     * @return the secret key.
     */
    protected abstract int getSecretKey();

    /**
     * Called when the client gracefully closes the connection. The default
     * implementation does nothing, can be reimplemented to handle the event.
     *
     * @throws PgProtocolException in case of errors in the protocol.
     * @throws IOException if an I/O error occurs.
     */
    protected void Terminate() throws PgProtocolException, IOException {
    }

    /**
     * Starts the protocol loop. This method silently suppress connection errors
     * because many clients just close the connection when they are done. Note
     * that this method does not throw any IOException because clients may close
     * the connection without notification.
     *
     * @return true if the client gracefully terminated the connection, false if
     * the client did not get authenticated or the connection has been closed
     * without any notification.
     * @throws PgProtocolException in case of errors in the protocol.
     */
    public boolean run() throws PgProtocolException {
        try {
            protocolStartUp();
            if (!authenticated) {
                LOGGER.log(Level.WARNING, "user not authenticated, closing connection");
                return false;
            }
            ReadyForQuery('I');
            for (PgReader reader = new PgReader(_rawReader);; reader.check()) {
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
                        List<Short> parameterFormats = reader.readInt16List(parameterNum);
                        Short parameterValNum = reader.readInt16();
                        List<List<Byte>> parameters = new ArrayList<>();
                        for (int n = 0; n < parameterValNum; n++) {
                            int parLen = reader.readInt32();
                            parameters.add(reader.readByteList(parLen));
                        }
                        List<Short> resultFormatCodes = reader.readInt16List(reader.readInt16());
                        if (parameterNum == 0) {
                            parameterFormats = Collections.nCopies(parameters.size(), (short) 0);
                        }
                        if (parameterNum == 1) {
                            short def = parameterFormats.get(0);
                            parameterFormats = Collections.nCopies(parameters.size(), def);
                        }
                        Bind(portal, preparedStatement, parameterFormats, parameters, resultFormatCodes);
                        break;
                    }
                    case 'C': {
                        byte what = reader.readInt8();
                        Close(what, reader.readString());
                        break;
                    }
                    case 'd': {
                        CopyDataClientMsg(reader.readByteList());
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
                        List<Short> argsFormat = reader.readInt16List(reader.readInt16());
                        Short argProvided = reader.readInt16();
                        List<DataCellMsg> arguments = new ArrayList<>();
                        for (int n = 0; n < argProvided; n++) {
                            int size = reader.readInt32();
                            if (size < 0) {
                                arguments.add(new DataCellMsg());
                            } else {
                                List<Byte> data = reader.readByteList(size);
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
                LOGGER.log(Level.SEVERE, "Error in the protocol, closing the connection", ex);
                _socket.close();
            } catch (IOException ignoreme) {
            }
            throw ex;
        } catch (IOException ex) {
            LOGGER.log(Level.WARNING, "Connection closed", ex);
            return false;
        }
    }

    /**
     * Invoked when a flush message is received. This message is handled by this
     * class, but subclasses may reimplmenent this method to get notified when a
     * flush happens.
     *
     * @throws PgProtocolException in case of errors in the protocol.
     * @throws IOException if an I/O error occurs.
     */
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

    @Override
    public String toString() {
        return _socket.getRemoteSocketAddress().toString();
    }
}
