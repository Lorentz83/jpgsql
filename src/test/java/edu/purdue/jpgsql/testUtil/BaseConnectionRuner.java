package edu.purdue.jpgsql.testUtil;

import edu.purdue.jpgsql.BaseConnection;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.function.Function;

/**
 *
 * @author Lorenzo Bossi [lbossi@purdue.edu]
 */
public class BaseConnectionRuner extends BaseRunner {

    private final Function<Socket, BaseConnection> _func;
    private final ServerSocket _listener;

    public BaseConnectionRuner(ServerSocket listener, Function<Socket, BaseConnection> func) {
        _listener = listener;
        _func = func;
    }

    @Override
    protected void testCode() throws Throwable {
        try (Socket socket = _listener.accept()) {
            BaseConnection conn = _func.apply(socket);
            conn.run();
        }
    }
}
