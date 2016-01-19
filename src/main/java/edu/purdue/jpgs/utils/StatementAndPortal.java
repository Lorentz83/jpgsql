package edu.purdue.jpgs.utils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Is a container for portals and statements. Implements the logic to link
 * portals to the original statement in order to automatically delete the
 * portals when the original statement is deleted.
 *
 * @author Lorenzo Bossi [lbossi@purdue.edu]
 */
public class StatementAndPortal {

    Map<String, Statement> _statements = new HashMap<>();
    Map<String, Portal> _portals = new HashMap<>();

    /**
     * Removes the specified statement and all the related portals.
     *
     * @param statementName the name of the statement.
     * @return true if the statement has been removed, false if no such
     * statement existed.
     */
    public boolean removeStatementCascade(String statementName) {
        Statement stm = _statements.remove(statementName);
        if (stm != null) {
            for (String portal : stm.portals) {
                _portals.remove(portal);
            }
            return true;
        }
        return false;
    }

    /**
     * Removes the specified portal.
     *
     * @param portalName the name of the portal.
     * @return true if the portal has been removed, false if no such portal
     * existed.
     */
    public boolean removePortal(String portalName) {
        Portal portal = _portals.remove(portalName);
        if (portal != null) {
            _statements.get(portal.originalStatement).portals.remove(portalName);
            return true;
        }
        return false;
    }

    /**
     * Adds the statement if no other statements with the same name exists.
     *
     * @param statementName the statement name.
     * @param query the statement sql.
     * @return true if the statement has been added, false if another with the
     * same name already exist.
     */
    public boolean putStatement(String statementName, String query) {
        if (_statements.containsKey(statementName)) {
            return false;
        }
        _statements.put(statementName, new Statement(query));
        return true;
    }

    /**
     * Adds the portal if no other portals with the same name exist.
     *
     * @param statementName the statement name.
     * @param portalName the portal name.
     * @param realQuery the actual query.
     * @return true if the portal has been added, false if another portal with
     * the same name exists.
     * @throws IllegalStateException if the specified statement does not exist.
     */
    public boolean putPortal(String statementName, String portalName, String realQuery) throws IllegalStateException {
        if (_portals.containsKey(portalName)) {
            return false;
        }
        if (!_statements.containsKey(statementName)) {
            throw new IllegalStateException();
        }
        Portal p = new Portal(statementName, realQuery);
        _portals.put(portalName, p);
        return true;
    }

    /**
     * Returns the specified portal or null if it does not exist.
     *
     * @param portalName the name of the portal.
     * @return the portal or null.
     */
    public Portal getPortal(String portalName) {
        return _portals.get(portalName);
    }

    /**
     * Returns the SQL associated to the statement or null if the statement does
     * not exist.
     *
     * @param statementName the name of the statement.
     * @return the SQL or null.
     */
    public String getStatementSql(String statementName) {
        Statement stm = _statements.get(statementName);
        return stm == null ? null : stm.query;
    }

}

/**
 * A simple container to link the statement to the portals associated.
 *
 * @author Lorenzo Bossi [lbossi@purdue.edu]
 */
class Statement {

    public final String query;
    public final Set<String> portals;

    public Statement(String query) {
        this.query = query;
        this.portals = new HashSet<>();
    }
}
