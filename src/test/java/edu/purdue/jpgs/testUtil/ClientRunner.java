package edu.purdue.jpgs.testUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;
import java.util.function.Consumer;
import static org.junit.Assert.fail;

/**
 *
 * @author Lorenzo Bossi [lbossi@purdue.edu]
 */
public class ClientRunner extends Thread {

    private final String _url;
    private final Properties _properties;
    private final Consumer<Connection> _func;
    private Throwable _ex;
    private boolean _completed;

    public ClientRunner(String name, String password, String db, int portNumber, Consumer<Connection> func) {
        setName("ClientRunner");
        _url = "jdbc:postgresql://localhost:" + portNumber + "/" + db;
        _properties = new Properties();
        _properties.setProperty("user", "fred");
        _properties.setProperty("password", "secret");
        _func = func;
        _ex = null;
        _completed = false;
    }

    @Override
    public void run() {
        try (Connection conn = DriverManager.getConnection(_url, _properties)) {
            _func.accept(conn);
        } catch (Throwable ex) {
            _ex = ex;
        }
        _completed = true;
    }

    public void assertCompleted() throws Throwable {
        join(10000);
        if (!_completed) {
            interrupt();
            fail(getName() + ": timeout reached");
        }
        if (_ex != null) {
            throw _ex;
        }
    }
}
