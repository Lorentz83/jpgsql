package edu.purdue.jpgs.testUtil;

import edu.purdue.jpgs.BaseConnection;
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
        setName("ServerRunner");
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
