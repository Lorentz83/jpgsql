package edu.purdue.jpgs.testUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 *
 * @author Lorenzo Bossi [lbossi@purdue.edu]
 */
public class ClientRunner extends BaseRunner {

    private final String _url;
    private final Properties _properties;
    private final SqlCommands _func;

    @FunctionalInterface
    public interface SqlCommands {

        void accept(Connection t) throws SQLException;
    }

    @Override
    protected void testCode() throws Throwable {
        try (Connection conn = DriverManager.getConnection(_url, _properties)) {
            _func.accept(conn);
        }
    }

    public ClientRunner(String name, String password, String db, int portNumber, SqlCommands func) {
        setName("ClientRunner");
        _url = "jdbc:postgresql://localhost:" + portNumber + "/" + db;
        _properties = new Properties();
        _properties.setProperty("user", "fred");
        _properties.setProperty("password", "secret");
        _func = func;
    }

}
