package edu.purdue.jpgs.testUtil;

import edu.purdue.jpgs.BaseConnection;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.function.Function;
import static org.junit.Assert.fail;

/**
 *
 * @author Lorenzo Bossi [lbossi@purdue.edu]
 */
public class ServerRunner extends Thread {

    private final Function<Socket, BaseConnection> _func;
    private final ServerSocket _listener;
    private Throwable _ex;
    private BaseConnection _conn;
    private boolean _completed;

    public ServerRunner(ServerSocket listener, Function<Socket, BaseConnection> func) {
        setName("ServerRunner");
        _listener = listener;
        _func = func;
        _conn = null;
        _ex = null;
        _completed = false;
    }

    @Override
    public void run() {
        try (Socket socket = _listener.accept()) {
            _conn = _func.apply(socket);
            _conn.run();
        } catch (Throwable ex) {
            _ex = ex;
        }
        _completed = true;
    }

    public void assertCompleted() throws Throwable {
        join(2000);
        if (!_completed) {
            _listener.close();
            _conn.kill();
            fail(getName() + ": timeout reached");
        }
        if (_ex != null) {
            throw _ex;
        }
    }
}
