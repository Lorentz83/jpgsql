package edu.purdue.jpgsql;

import edu.purdue.jpgsql.DataProvider;
import edu.purdue.jpgsql.PgProtocolException;
import edu.purdue.jpgsql.testUtil.ClientRunner;
import edu.purdue.jpgsql.testUtil.BaseConnectionRuner;
import edu.purdue.jpgsql.testUtil.DummyConnection;
import edu.purdue.jpgsql.testUtil.SimpleConnectionRunner;
import edu.purdue.jpgsql.testUtil.StrictMock;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Connection;
import java.util.Map;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.collection.IsMapContaining.*;
import org.junit.After;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.*;
import org.postgresql.util.PSQLException;

/**
 *
 * @author Lorenzo Bossi [lbossi@purdue.edu]
 */
public class SimpleQuery_StartupPhase_Test {

    private final int _portNumber = 8998;
    private final String _username = "fred";
    private final String _password = "secret";
    private final String _dbName = "test";
    private ServerSocket _listener;
    private DataProvider _provider;
    DataProvider.QueryResult _table;

    private final StrictMock _strictMock = new StrictMock();

    @Before
    public void init() throws IOException {
        _listener = new ServerSocket(_portNumber);
        _provider = mock(DataProvider.class, _strictMock);
        _table = mock(DataProvider.QueryResult.class, _strictMock);
    }

    @After
    public void closeListener() throws IOException {
        _listener.close();
    }

    @Test
    public void startupMessage() throws Throwable {

        ClientRunner client = new ClientRunner(_username, _password, _dbName, _portNumber, (Connection conn) -> {
        });

        BaseConnectionRuner server = new BaseConnectionRuner(_listener, (Socket s) -> {
            try {
                return new DummyConnection(s) {
                    @Override
                    protected boolean StartupMessage(int protocolVersion, Map<String, String> parameters) throws PgProtocolException, IOException {
                        assertThat(parameters, hasEntry("user", _username));
                        assertThat(parameters, hasEntry("database", _dbName));
                        AuthenticationOk();
                        return true;
                    }
                };
            } catch (IOException ex) {
                return null;
            }
        });
        server.start();
        client.start();

        server.assertCompleted();
        client.assertCompleted();
    }

    @Test
    public void simpleQuery_askPasswordAndReject() throws Throwable {
        when(_provider.setUser(_username)).thenReturn(false);
        when(_provider.setPassword(_password)).thenReturn(false);

        _strictMock.turnOn();

        ClientRunner client = new ClientRunner(_username, _password, _dbName, _portNumber, (Connection conn) -> {
        });

        SimpleConnectionRunner server = new SimpleConnectionRunner(_listener, _provider);

        server.start();
        client.start();

        server.assertCompleted();
        try {
            client.assertCompleted();
            fail("missing exception");
        } catch (PSQLException ex) {
            assertThat(ex.getMessage(), is("The connection attempt failed."));
        }
    }

    @Test
    public void simpleQuery_trustUserRejectDb() throws Throwable {

        when(_provider.setUser(_username)).thenReturn(true);
        when(_provider.setDatabase(_dbName)).thenReturn(false);

        _strictMock.turnOn();

        ClientRunner client = new ClientRunner(_username, _password, _dbName, _portNumber, (Connection conn) -> {
        });

        SimpleConnectionRunner server = new SimpleConnectionRunner(_listener, _provider);

        server.start();
        client.start();

        server.assertCompleted();
        try {
            client.assertCompleted();
            fail("missing exception");
        } catch (PSQLException ex) {
            assertThat(ex.getMessage(), is("ERROR: database \"test\" does not exist"));
        }
    }

    @Test
    public void simpleQuery_acceptUserRejectDb() throws Throwable {

        when(_provider.setUser(_username)).thenReturn(true);
        when(_provider.setPassword(_password)).thenReturn(true);
        when(_provider.setDatabase(_dbName)).thenReturn(false);

        _strictMock.turnOn();

        ClientRunner client = new ClientRunner(_username, _password, _dbName, _portNumber, (Connection conn) -> {
        });

        SimpleConnectionRunner server = new SimpleConnectionRunner(_listener, _provider);

        server.start();
        client.start();

        server.assertCompleted();
        try {
            client.assertCompleted();
            fail("missing exception");
        } catch (PSQLException ex) {
            assertThat(ex.getMessage(), is("ERROR: database \"test\" does not exist"));
        }
    }

}
