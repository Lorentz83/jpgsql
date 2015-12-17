package edu.purdue.jpgs;

import edu.purdue.jpgs.testUtil.ClientRunner;
import edu.purdue.jpgs.testUtil.BaseConnectionRuner;

import edu.purdue.jpgs.testUtil.DummyConnection;
import edu.purdue.jpgs.testUtil.SimpleConnectionRunner;
import edu.purdue.jpgs.testUtil.StrictMock;
import edu.purdue.jpgs.type.DataCellMsg;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.collection.IsMapContaining.*;
import org.junit.After;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import static org.mockito.Mockito.*;
import org.postgresql.util.PSQLException;

/**
 *
 * @author Lorenzo Bossi [lbossi@purdue.edu]
 */
public class IntegrationTest {

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

    @Test
    public void simpleQuery_select() throws Throwable {
        final String query = "select * from table";
        List<String> header = new ArrayList<>();
        header.add("col1");
        header.add("col2");
        List<DataCellMsg> row1 = new ArrayList<>();
        row1.add(new DataCellMsg("1 1"));
        row1.add(new DataCellMsg("1 2"));
        List<DataCellMsg> row2 = new ArrayList<>();
        row2.add(new DataCellMsg("2 1"));
        row2.add(new DataCellMsg("2 2"));
        List<List<DataCellMsg>> rows = new ArrayList<>();
        rows.add(row1);
        rows.add(row2);

        when(_provider.setUser(_username)).thenReturn(true);
        when(_provider.setPassword(_password)).thenReturn(true);
        when(_provider.setDatabase(_dbName)).thenReturn(true);

        when(_provider.getResult(query)).thenReturn(_table);
        when(_table.getHeader()).thenReturn(header);
        when(_table.getType()).thenReturn(DataProvider.QueryResult.Type.SELECT);

        when(_table.getRows()).thenReturn(rows);
        when(_table.getRowCount()).thenReturn(2);

        _strictMock.turnOn();

        ClientRunner client = new ClientRunner(_username, _password, _dbName, _portNumber, (Connection conn) -> {
            Statement stm = conn.createStatement();
            try (ResultSet rs = stm.executeQuery(query)) {
                assertThat(rs.getMetaData().getColumnName(1), is("col1"));
                assertThat(rs.getMetaData().getColumnName(2), is("col2"));
                assertThat(rs.getMetaData().getColumnCount(), is(2));
                for (int r = 1; r <= 2; r++) {
                    assertThat(rs.next(), is(true));
                    System.out.println(rs.getString("col1") + " " + rs.getString("col2"));
                    assertThat(rs.getString("col1"), is(String.format("%s %s", r, 1)));
                    assertThat(rs.getString("col2"), is(String.format("%s %s", r, 2)));
                }
                assertThat(rs.next(), is(false));
            }
        });

        SimpleConnectionRunner server = new SimpleConnectionRunner(_listener, _provider);

        server.start();
        client.start();

        server.assertCompleted();
        client.assertCompleted();
    }

    @Test
    public void simpleQuery_preparedStatementSelect() throws Throwable {
        List<String> header = new ArrayList<>();
        header.add("col1");
        header.add("col2");
        List<DataCellMsg> row1 = new ArrayList<>();
        row1.add(new DataCellMsg("1 1"));
        row1.add(new DataCellMsg("1 2"));
        List<DataCellMsg> row2 = new ArrayList<>();
        row2.add(new DataCellMsg("2 1"));
        row2.add(new DataCellMsg("2 2"));
        List<List<DataCellMsg>> rows = new ArrayList<>();
        rows.add(row1);
        rows.add(row2);

        when(_provider.setUser(_username)).thenReturn(true);
        when(_provider.setPassword(_password)).thenReturn(true);
        when(_provider.setDatabase(_dbName)).thenReturn(true);

        when(_provider.getResult("select * from tbl where f > \"65024\"")).thenReturn(_table);
        when(_provider.getResult("select * from tbl where f > \"25\"")).thenReturn(_table);
        when(_table.getHeader()).thenReturn(header);
        when(_table.getType()).thenReturn(DataProvider.QueryResult.Type.SELECT);

        when(_table.getRows()).thenReturn(rows);
        when(_table.getRowCount()).thenReturn(2);

        _strictMock.turnOn();

        ClientRunner client = new ClientRunner(_username, _password, _dbName, _portNumber, (Connection conn) -> {
            PreparedStatement stm = conn.prepareStatement("select * from tbl where f > ?");
            stm.setInt(1, 65024);
            try (ResultSet rs = stm.executeQuery()) {
                assertThat(rs.getMetaData().getColumnName(1), is("col1"));
                assertThat(rs.getMetaData().getColumnName(2), is("col2"));
                assertThat(rs.getMetaData().getColumnCount(), is(2));
                for (int r = 1; r <= 2; r++) {
                    assertThat(rs.next(), is(true));
                    System.out.println(rs.getString("col1") + " " + rs.getString("col2"));
                    assertThat(rs.getString("col1"), is(String.format("%s %s", r, 1)));
                    assertThat(rs.getString("col2"), is(String.format("%s %s", r, 2)));
                }
                assertThat(rs.next(), is(false));
            }
            stm.setInt(1, 25);
            try (ResultSet rs = stm.executeQuery()) {
                assertThat(rs.getMetaData().getColumnCount(), is(2));
            }
        });

        SimpleConnectionRunner server = new SimpleConnectionRunner(_listener, _provider);

        server.start();
        client.start();

        server.assertCompleted();
        client.assertCompleted();
    }

    @Ignore("TODO")
    @Test
    public void simpleQuery_preparedStatementTypes() throws Throwable {
        when(_provider.setUser(_username)).thenReturn(true);
        when(_provider.setPassword(_password)).thenReturn(true);
        when(_provider.setDatabase(_dbName)).thenReturn(true);

        when(_provider.getResult("delete from tbl where f1 = \"10\" AND f2 = \"a \"\" quoted \' string\" AND f3 = \"2.5\" AND f4 = \"2.4\" AND f5 = \"b\"")).thenReturn(_table);
        when(_table.getType()).thenReturn(DataProvider.QueryResult.Type.DELETE);
        when(_table.getRowCount()).thenReturn(3);

        _strictMock.turnOn();

        ClientRunner client = new ClientRunner(_username, _password, _dbName, _portNumber, (Connection conn) -> {
            PreparedStatement stm = conn.prepareStatement("delete from tbl where f1 = ? AND f2 = ? AND f3 = ? AND f4 = ? AND f5 = ?");
            stm.setInt(1, 10);  // 4
            stm.setString(2, "a \" quoted \' string"); //19
            stm.setDouble(3, 2.5); //8
            stm.setFloat(4, (float) 2.4); //4
            stm.setByte(5, (byte) 'b');  //2
            stm.execute();
        });

        SimpleConnectionRunner server = new SimpleConnectionRunner(_listener, _provider);

        server.start();
        client.start();

        server.assertCompleted();
        client.assertCompleted();

    }
}
