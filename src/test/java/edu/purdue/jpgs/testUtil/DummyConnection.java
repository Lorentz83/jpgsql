package edu.purdue.jpgs.testUtil;

import edu.purdue.jpgs.BaseConnection;
import edu.purdue.jpgs.PgProtocolException;
import edu.purdue.jpgs.type.DataCellMsg;
import java.io.IOException;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import static org.junit.Assert.fail;

/**
 *
 * @author Lorenzo Bossi [lbossi@purdue.edu]
 */
public class DummyConnection extends BaseConnection {

    public DummyConnection(Socket socket) throws IOException {
        super(socket);
    }

    @Override
    protected void StartupMessage(int protocolVersion, Map<String, String> parameters) throws IOException, PgProtocolException {
        fail("StartupMessage should not be called");
    }

    @Override
    protected void PasswordMessage(String password) throws IOException, PgProtocolException {
        fail("PasswordMessage should not be called");
    }

    @Override
    protected void Query(String query) throws IOException, PgProtocolException {
        fail("Query should not be called");
    }

    @Override
    protected void CancelRequest(int backendProcessId, int secretKey) throws IOException, PgProtocolException {
        fail("CancelRequest should not be called");
    }

    @Override
    protected void Close(byte what, String name) throws IOException, PgProtocolException {
        fail("Close should not be called");
    }

    @Override
    protected void CopyFail(String errorMessage) throws IOException, PgProtocolException {
        fail("CopyFail should not be called");
    }

    @Override
    protected void Describe(byte what, String name) throws IOException, PgProtocolException {
        fail("Describe should not be called");
    }

    @Override
    protected void Execute(String portalName, int manRows) throws IOException, PgProtocolException {
        fail("Execute should not be called");
    }

    @Override
    protected void Flush() throws IOException, PgProtocolException {
        fail("Flush should not be called");
    }

    @Override
    protected void Parse(String preparedStatment, String query, List<Integer> parametersType) throws IOException, PgProtocolException {
        fail("Parse should not be called");
    }

    @Override
    protected void CopyDataClientMsg(List<Byte> data) throws IOException, PgProtocolException {
        fail("CopyDataClientMsg should not be called");
    }

    @Override
    protected void CopyDoneClientMsg() throws IOException, PgProtocolException {
        fail("CopyDoneClientMsg should not be called");
    }

    @Override
    protected void Sync() throws IOException, PgProtocolException {
        fail("Sync should not be called");
    }

    @Override
    protected void Bind(String portalName, String source, List<Short> parameterFormatCodes, List<List<Byte>> parameterValues, List<Short> resultFormatCodes) throws IOException, PgProtocolException {
        fail("Bind should not be called");
    }

    @Override
    protected void FunctionCall(int objectId, List<Short> argumentFormats, List<DataCellMsg> arguments, Short resultFormat) throws IOException, PgProtocolException {
        fail("FunctionCall should not be called");
    }

}
