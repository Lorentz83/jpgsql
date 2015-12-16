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
    protected boolean StartupMessage(int protocolVersion, Map<String, String> parameters) throws PgProtocolException {
        fail("StartupMessage should not be called");
        return false;
    }

    @Override
    protected void PasswordMessage(String password) throws PgProtocolException {
        fail("PasswordMessage should not be called");
    }

    @Override
    protected void Query(String query) throws PgProtocolException {
        fail("Query should not be called");
    }

    @Override
    protected void CancelRequest(int backendProcessId, int secretKey) throws PgProtocolException {
        fail("CancelRequest should not be called");
    }

    @Override
    protected void Close(byte what, String name) throws PgProtocolException {
        fail("Close should not be called");
    }

    @Override
    protected void CopyFail(String errorMessage) throws PgProtocolException {
        fail("CopyFail should not be called");
    }

    @Override
    protected void Describe(char what, String name) throws PgProtocolException {
        fail("Describe should not be called");
    }

    @Override
    protected void Execute(String portalName, int manRows) throws PgProtocolException {
        fail("Execute should not be called");
    }

    @Override
    protected void Flush() throws PgProtocolException {
        fail("Flush should not be called");
    }

    @Override
    protected void Parse(String preparedStatment, String query, List<Integer> parametersType) throws PgProtocolException {
        fail("Parse should not be called");
    }

    @Override
    protected void CopyDataClientMsg(List<Byte> data) throws PgProtocolException {
        fail("CopyDataClientMsg should not be called");
    }

    @Override
    protected void CopyDoneClientMsg() throws PgProtocolException {
        fail("CopyDoneClientMsg should not be called");
    }

    @Override
    protected void Sync() throws PgProtocolException {
        fail("Sync should not be called");
    }

    @Override
    protected void Bind(String portalName, String source, List<Short> parameterFormatCodes, List<List<Byte>> parameterValues, List<Short> resultFormatCodes) throws PgProtocolException {
        fail("Bind should not be called");
    }

    @Override
    protected void FunctionCall(int objectId, List<Short> argumentFormats, List<DataCellMsg> arguments, Short resultFormat) throws PgProtocolException {
        fail("FunctionCall should not be called");
    }

}
