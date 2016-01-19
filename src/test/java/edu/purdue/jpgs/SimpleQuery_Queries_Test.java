package edu.purdue.jpgs;

import edu.purdue.jpgs.testUtil.ClientRunner;
import edu.purdue.jpgs.testUtil.SimpleConnectionRunner;
import static edu.purdue.jpgs.testUtil.SimpleConversion.row;
import static edu.purdue.jpgs.testUtil.SimpleConversion.table;
import edu.purdue.jpgs.testUtil.StrictMock;
import java.io.IOException;
import java.net.ServerSocket;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.emptyString;
import org.junit.After;
import static org.junit.Assert.assertThat;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.*;

/**
 *
 * @author Lorenzo Bossi [lbossi@purdue.edu]
 */
public class SimpleQuery_Queries_Test {

    private final int _portNumber = 8998;
    private final String _username = "fred";
    private final String _password = "secret";
    private final String _dbName = "test";
    private ServerSocket _listener;
    private DataProvider _provider;
    DataProvider.QueryResult _table;

    private final StrictMock _strictMock = new StrictMock();
    private ClientRunner client;
    private SimpleConnectionRunner server;

    @Before
    public void init() throws IOException {
        _listener = new ServerSocket(_portNumber);
        _provider = mock(DataProvider.class, _strictMock);
        _table = mock(DataProvider.QueryResult.class, _strictMock);

        when(_provider.setUser(_username)).thenReturn(true);
        when(_provider.setPassword(_password)).thenReturn(true);
        when(_provider.setDatabase(_dbName)).thenReturn(true);
    }

    @After
    public void check() throws IOException, Throwable {
        try {
            server = new SimpleConnectionRunner(_listener, _provider);

            server.start();
            client.start();

            server.assertCompleted();
            client.assertCompleted();
        } finally {
            _listener.close();
        }
    }

    private void setUpClient(final ClientRunner.SqlCommands c) {
        client = new ClientRunner(_username, _password, _dbName, _portNumber, c);
    }

    @Test
    public void statementSelect() throws Throwable {
        final String query = "select * from table";
        List<String> header = Arrays.asList("col1", "col2");
        Iterator<List<String>> rows = table(row("1 1", "1 2"), row("2 1", "2 2"), row("", null));

        when(_provider.getResult(query)).thenReturn(_table);
        when(_table.getHeader()).thenReturn(header);
        when(_table.getType()).thenReturn(DataProvider.QueryResult.Type.SELECT);

        when(_table.getRows()).thenReturn(rows);
        when(_table.getRowCount()).thenReturn(2);

        _strictMock.turnOn();

        setUpClient((Connection conn) -> {
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
                assertThat(rs.next(), is(true));
                assertThat(rs.getString("col1"), is(emptyString()));
                assertThat(rs.getString("col2"), is(nullValue()));
                assertThat(rs.next(), is(false));
            }
        });

    }

    @Test
    public void preparedStatementSelect() throws Throwable {
        List<String> header = Arrays.asList("col1", "col2");
        Iterator<List<String>> rows = table(row("1 1", "1 2"), row("2 1", "2 2"));

        when(_provider.getResult("select * from tbl where f > '65024'")).thenReturn(_table);
        when(_provider.getResult("select * from tbl where f > '25'")).thenReturn(_table);
        when(_table.getHeader()).thenReturn(header);
        when(_table.getType()).thenReturn(DataProvider.QueryResult.Type.SELECT);

        when(_table.getRows()).thenReturn(rows);
        when(_table.getRowCount()).thenReturn(2);

        _strictMock.turnOn();

        setUpClient((Connection conn) -> {
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

    }

    @Test
    public void preparedStatementTypes() throws Throwable {
        when(_provider.getResult("delete from tbl where f1 = '10' AND f2 = 'a \" quoted '' string'")).thenReturn(_table);
        when(_table.getType()).thenReturn(DataProvider.QueryResult.Type.DELETE);
        when(_table.getRowCount()).thenReturn(3);

        _strictMock.turnOn();

        setUpClient((Connection conn) -> {
            PreparedStatement stm = conn.prepareStatement("delete from tbl where f1 = ? AND f2 = ?");
            stm.setInt(1, 10);  // 4
            stm.setString(2, "a \" quoted ' string"); //19
            stm.execute();
        });

    }

    @Test
    public void preparedStatementAllTypes() throws Throwable {
        when(_provider.getResult("delete from tbl where f1 = '1' AND f2 = '2' AND f3 = '3.3' AND f4 = '4.4' AND f5 = 'str'")).thenReturn(_table);
        when(_table.getType()).thenReturn(DataProvider.QueryResult.Type.DELETE);
        when(_table.getRowCount()).thenReturn(3);

        _strictMock.turnOn();

        setUpClient((Connection conn) -> {
            PreparedStatement stm = conn.prepareStatement("delete from tbl where f1 = ? AND f2 = ? AND f3 = ? AND f4 = ? AND f5 = ?");
            stm.setInt(1, 1);  // 4
            stm.setLong(2, 2);
            stm.setDouble(3, 3.3);
            stm.setFloat(4, 4.4f);
            stm.setString(5, "str"); //19
            stm.execute();
        });
    }

    @Test
    public void preparedStatementQuotedPlaceHolders() throws Throwable {
        int deleted = 3;
        when(_provider.getResult("delete from tbl where f1 = 'seriou$1y?' and f2 = 'param'")).thenReturn(_table);
        when(_table.getType()).thenReturn(DataProvider.QueryResult.Type.DELETE);
        when(_table.getRowCount()).thenReturn(deleted);

        _strictMock.turnOn();

        setUpClient((Connection conn) -> {
            PreparedStatement stm = conn.prepareStatement("delete from tbl where f1 = 'seriou$1y?' and f2 = ?");
            stm.setString(1, "param");
            assertThat(stm.execute(), is(false));
            assertThat(stm.getUpdateCount(), is(deleted));
        });
    }

    @Test
    public void emptyQuery() throws Throwable {
        _strictMock.turnOn();
        setUpClient((Connection conn) -> {
            Statement stm;
            stm = conn.createStatement();
            assertThat(stm.execute(""), is(false));
            assertThat(stm.execute(" "), is(false));
            assertThat(stm.execute(";"), is(false));
            assertThat(stm.execute(" ; "), is(false));
        });
    }

    @Test
    public void preparedStatement_MaxRows() throws Throwable {
        final String query = "select * from table";
        List<String> header = Arrays.asList("col1");
        Iterator<List<String>> rows = table(row("1"), row("2"));

        when(_provider.getResult(query)).thenReturn(_table);
        when(_table.getHeader()).thenReturn(header);
        when(_table.getType()).thenReturn(DataProvider.QueryResult.Type.SELECT);

        when(_table.getRows()).thenReturn(rows);
        when(_table.getRowCount()).thenReturn(2);

        _strictMock.turnOn();

        setUpClient((Connection conn) -> {
            PreparedStatement stm = conn.prepareStatement(query);
            stm.setMaxRows(1);
            ResultSet rs = stm.executeQuery();
            assertThat(rs.next(), is(true));
            assertThat(rs.getString(1), is("1"));
            assertThat(rs.next(), is(false));

            rs = stm.executeQuery();
            assertThat(rs.next(), is(true));
            assertThat(rs.getString(1), is("2"));
            assertThat(rs.next(), is(false));
        });
    }

}
