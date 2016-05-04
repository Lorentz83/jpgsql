package edu.purdue.jpgsql;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The implementation of a multithreaded server. For every new incoming
 * connection, this class spawn a new thread and instances a new
 * {@link BaseConnection}. This class is partially aware of the postgres
 * protocol, therefore implements methods in support to kill a running process
 * when required by another one.
 *
 * @author Lorenzo Bossi [lbossi@purdue.edu]
 */
public class ConnectionPool {

    /**
     * Is the interface required to create an instance of a
     * {@link  BaseConnection}.
     */
    @FunctionalInterface
    public interface Instancer {

        /**
         * Creates an instance of a {@link BaseConnection}. Note, in case there
         * is no interest in supporting the CancelRequest message, just ignore
         * the pid and the cancelCallback parameters.
         *
         * @param socket the socket to communicate with the client.
         * @param pid the pid of the new instance.
         * @param cancelCallback the function to call when a
         * {@link BaseConnection#CancelRequest(int, int)} is received.
         * @return the new instance implementing the Postgres protocol.
         * @throws IOException in case there is any problem with the socket.
         */
        public BaseConnection getInstance(Socket socket, int pid, BiConsumer<Integer, Integer> cancelCallback) throws IOException;
    }

    private static final Logger LOGGER = Logger.getLogger(SimpleConnection.class.getName());

    private final ServerSocket _listener;
    private int _nextPid;
    private final TreeMap<Integer, PgProcess> _runningProcess;
    private final Instancer _instancer;

    /**
     * Creates a new connection pool.
     *
     * @param socket the server socket to listen.
     * @param instancer the lambda to create a new {@link BaseConnection}
     * instance when a new connection is incoming.
     */
    public ConnectionPool(ServerSocket socket, Instancer instancer) {
        if (socket == null || instancer == null) {
            throw new NullPointerException();
        }
        _listener = socket;
        _nextPid = 1;
        _runningProcess = new TreeMap<>();
        _instancer = instancer;
    }

    /**
     * Waits for a new connection and accepts it. This method actually spans a
     * new thread. It should be used inside an infinite loop.
     *
     * @throws IOException if an I/O error occurs.
     */
    public void accept() throws IOException {
        BaseConnection conn = null;
        Socket socket = _listener.accept();
        LOGGER.log(Level.FINE, "Accepted connection from {0}", socket.getRemoteSocketAddress().toString());
        PgProcess process = newProcess(socket);
        process.start();

    }

    /**
     * Removes the process from the list of active processes. Note that this
     * method does not actually kill the process, just loses the reference to
     * it. It should be called only when the process is already terminated.
     *
     * @param proc the process to free.
     */
    void freeProcess(PgProcess proc) {
        synchronized (_runningProcess) {
            _runningProcess.remove(proc.getPid());
        }
    }

    /**
     * Creates a new instance of the process, and put it in the list of active
     * processes.
     *
     * @param socket the socket to use.
     * @return the new process.
     */
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

    /**
     * Kills a process. If there is no process or the secretKey does not match
     * the selected process, this method fails silently.
     *
     * @param pid the process id.
     * @param secretKey the secret key to compare with the one returned by {@link BaseConnection#getSecretKey()
     * }
     */
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

    /**
     * A simple convenience method to create an instance of a
     * {@link BaseConnection}.
     *
     * @param socket the socket to use.
     * @param pid the pid of the new process.
     * @return the new instance.
     * @throws IOException in case of problem with the socket.
     */
    BaseConnection getInstance(Socket socket, int pid) throws IOException {
        return _instancer.getInstance(socket, pid, this::kill);
    }
}

/**
 * Represents a thread of a running Postgres server.
 *
 * @author Lorenzo Bossi [lbossi@purdue.edu]
 */
class PgProcess extends Thread {

    private static final Logger LOGGER = Logger.getLogger(PgProcess.class.getName());

    private final Socket _socket;
    private final int _pid;
    private final ConnectionPool _pool;
    private BaseConnection _conn;

    /**
     * Creates a new instance of PgProcess. Uses the connection pool to instance
     * the {@link BaseConnection} and to notify the end of the current thread.
     *
     * @param pid the current process id.
     * @param socket the socket to use.
     * @param pool the connection pool that created this instance.
     */
    PgProcess(int pid, Socket socket, ConnectionPool pool) {
        _pid = pid;
        _socket = socket;
        _pool = pool;
    }

    /**
     * Returns the secret key of this thread.
     *
     * @return the secret key {@link BaseConnection#getSecretKey() }.
     */
    public int getSecretKey() {
        return _conn == null ? -1 : _conn.getSecretKey();
    }

    /**
     * Kills the current thread calling {@link BaseConnection#kill() }.
     */
    public void kill() {
        if (_conn != null) {
            _conn.kill();
            LOGGER.log(Level.INFO, "Killing server {0}", _conn);
        }
    }

    /**
     * Returns the process id of the current thread.
     *
     * @return the process id.
     */
    int getPid() {
        return _pid;
    }

    /**
     * Execute the Postgres server. Actually creates the instance of the server
     * calling {@link ConnectionPool#getInstance(java.net.Socket, int) } and
     * executes it using {@link BaseConnection#run() }. When the execution is
     * done, this process is removed from the pool calling {@link ConnectionPool#freeProcess(edu.purdue.jpgs.PgProcess)
     * }.
     */
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
