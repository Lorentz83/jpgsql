package edu.purdue.jpgsql.testUtil;

import edu.purdue.jpgsql.DataProvider;
import edu.purdue.jpgsql.SimpleConnection;
import java.net.ServerSocket;
import java.net.Socket;

/**
 *
 * @author Lorenzo Bossi [lbossi@purdue.edu]
 */
public class SimpleConnectionRunner extends BaseRunner {

    private final DataProvider _provider;
    private final ServerSocket _listener;

    public SimpleConnectionRunner(ServerSocket listener, DataProvider provider) {
        _provider = provider;
        _listener = listener;
    }

    @Override
    protected void testCode() throws Throwable {
        try (Socket socket = _listener.accept()) {
            SimpleConnection conn = new SimpleConnection(socket, _provider);
            conn.run();
        }
    }

}
