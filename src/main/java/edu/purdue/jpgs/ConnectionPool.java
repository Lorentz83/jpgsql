package edu.purdue.jpgs;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Lorenzo Bossi [lbossi@purdue.edu]
 */
public class ConnectionPool {

    @FunctionalInterface
    public interface Instancer {

        public BaseConnection getInstance(Socket socket, int pid, BiConsumer<Integer, Integer> cancelCallback) throws IOException;
    }

    private static final Logger LOGGER = Logger.getLogger(SimpleConnection.class.getName());

    private final ServerSocket _listener;
    private int _nextPid;
    private final TreeMap<Integer, PgProcess> _runningProcess;
    private final Instancer _instancer;

    public ConnectionPool(ServerSocket socket, Instancer instancer) {
        _listener = socket;
        _nextPid = 1;
        _runningProcess = new TreeMap<>();
        _instancer = instancer;
    }

    public void accept() throws IOException {
        BaseConnection conn = null;
        Socket socket = _listener.accept();
        LOGGER.log(Level.FINE, "Accepted connection from {0}", socket.getRemoteSocketAddress().toString());
        PgProcess process = newProcess(socket);
        process.start();

    }

    void freeProcess(PgProcess proc) {
        synchronized (_runningProcess) {
            _runningProcess.remove(proc.getPid());
        }
    }

    PgProcess newProcess(Socket socket) {
        synchronized (_runningProcess) {
            int pid;
            do {
                pid = _nextPid++ % Integer.MAX_VALUE;
            } while (_runningProcess.containsKey(pid));
            PgProcess proc = new PgProcess(pid, socket, this);
            _runningProcess.put(pid, proc);
            return proc;
        }
    }

    private void kill(int pid, int secretKey) {
        LOGGER.log(Level.INFO, "Killing request for pid:{0} , secretKey:{1}", new Object[]{pid, secretKey});
        synchronized (_runningProcess) {
            PgProcess proc = _runningProcess.get(pid);
            if (proc != null) {
                if (proc.getSecretKey() == secretKey) {
                    LOGGER.log(Level.WARNING, "Killing connection {0}", proc);
                    proc.kill();
                } else {
                    LOGGER.log(Level.WARNING, "Not killing connection {0}, secret key does not match", proc);
                }
            } else {
                LOGGER.log(Level.WARNING, "Not killing process id {0}, the process does not exist", pid);
            }
        }
    }

    BaseConnection getInstance(Socket socket, int pid) throws IOException {
        return _instancer.getInstance(socket, pid, this::kill);
    }
}

class PgProcess extends Thread {

    private static final Logger LOGGER = Logger.getLogger(PgProcess.class.getName());

    private final Socket _socket;
    private final int _pid;
    private final ConnectionPool _pool;
    private BaseConnection _conn;

    PgProcess(int pid, Socket socket, ConnectionPool pool) {
        _pid = pid;
        _socket = socket;
        _pool = pool;
    }

    public int getSecretKey() {
        return _conn == null ? -1 : _conn.getSecretKey();
    }

    public void kill() {
        if (_conn != null) {
            _conn.kill();
            LOGGER.log(Level.INFO, "Killing server {0}", _conn);
        }
    }

    int getPid() {
        return _pid;
    }

    @Override
    public void run() {
        try {
            LOGGER.log(Level.FINE, "Creating server instance");
            _conn = _pool.getInstance(_socket, _pid);
            LOGGER.log(Level.INFO, "Starting server {0}", _conn);
            _conn.run();
        } catch (IOException ex) {
            LOGGER.log(Level.WARNING, "IOException", ex);
        } catch (PgProtocolException ex) {
            LOGGER.log(Level.SEVERE, "PgProtocolException", ex);
        } finally {
            _pool.freeProcess(this);
            try {
                _socket.close();
            } catch (IOException ex) {
            }
            LOGGER.log(Level.INFO, "Server closed {0}", _conn);
        }
    }
}
