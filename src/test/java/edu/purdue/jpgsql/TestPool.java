package edu.purdue.jpgsql;

import edu.purdue.jpgsql.ConnectionPool;
import edu.purdue.jpgsql.DataProvider;
import edu.purdue.jpgsql.SimpleConnection;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiConsumer;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 * @author Lorenzo Bossi [lbossi@purdue.edu]
 */
public class TestPool {

    /**
     * Tests the thread pool. Note, this test is not supposed to be
     * automatically executed. Run this test and try to connect using the
     * command line <code>psql postgresql://test:test@localhost:5444/mydb</code>
     * If it works correctly, multiple connections can be made, whenever a query
     * contains "slow" the result is returned with 5 seconds delay and can be
     * killed using a ctrl-c
     *
     * @throws IOException
     */
    @Ignore
    @Test
    public void pool() throws IOException {

        ActualProvider provider = new ActualProvider();

        try (ServerSocket serverSocket = new ServerSocket(5444)) {

            ConnectionPool pool = new ConnectionPool(serverSocket, (Socket socket, int pid, BiConsumer<Integer, Integer> cancelCallback) -> new SimpleConnection(socket, provider, pid, cancelCallback));

            for (int n = 0; n < 10; n++) {
                pool.accept();
            }
        }
    }

}

class ActualProvider implements DataProvider {

    @Override
    public boolean setUser(String user) {
        return true;
    }

    @Override
    public boolean setDatabase(String database) {
        return true;
    }

    @Override
    public boolean setPassword(String password) {
        return true;
    }

    @Override
    public DataProvider.QueryResult getResult(String query) {
        return new ActualResult(query);
    }
}

class ActualResult implements DataProvider.QueryResult {

    private final String _query;
    private final List<List<String>> _data;
    private final List<String> _header;

    ActualResult(String query) {
        _query = query;
        _data = new ArrayList<>();
        for (int n = 0; n < 5; n++) {
            ArrayList<String> row = new ArrayList<>();
            row.add("hello " + n);
            _data.add(row);
        }
        _header = new ArrayList<>();
        _header.add("msg");
    }

    @Override
    public Type getType() {
        return Type.SELECT;
    }

    @Override
    public int getRowCount() {
        return _data.size();
    }

    @Override
    public String getErrorMessage() {
        return "";
    }

    @Override
    public Iterator<List<String>> getRows() {
        if (_query.contains("slow")) {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException ex) {
            }
        }
        return _data.iterator();
    }

    @Override
    public List<String> getHeader() {
        return _header;
    }

}
