package edu.purdue.jpgs;

import edu.purdue.jpgs.testUtil.ClientRunner;
import edu.purdue.jpgs.testUtil.ServerRunner;

import edu.purdue.jpgs.testUtil.DummyConnection;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Connection;
import java.util.Map;
import static org.hamcrest.collection.IsMapContaining.*;
import static org.junit.Assert.assertThat;
import org.junit.Test;

/**
 *
 * @author Lorenzo Bossi [lbossi@purdue.edu]
 */
public class IntegrationTest {

    private final int portNumber = 8998;

    @Test
    public void login() throws Throwable {
        try (ServerSocket listener = new ServerSocket(portNumber)) {

            ClientRunner client = new ClientRunner("fred", "secret", "test", portNumber, (Connection conn) -> {

            });

            ServerRunner server = new ServerRunner(listener, (Socket s) -> {
                try {
                    return new DummyConnection(s) {
                        @Override
                        protected void StartupMessage(int protocolVersion, Map<String, String> parameters) throws PgProtocolException {
                            assertThat(parameters, hasEntry("user", "fred"));
                            assertThat(parameters, hasEntry("database", "test"));
                            AuthenticationOk();
                        }
                    };
                } catch (IOException ex) {
                    return null;
                }
            });
            server.start();
            client.start();

            server.assertCompleted();
            client.assertCompleted();
        }
    }
}
