package edu.purdue.jpgs.testUtil;

import static org.junit.Assert.fail;

/**
 *
 * @author Lorenzo Bossi [lbossi@purdue.edu]
 */
public abstract class BaseRunner extends Thread {

    private Throwable _ex;
    private boolean _completed;
    private final long TIMEOUT = 1000000;

    public BaseRunner() {
        _ex = null;
        _completed = false;
    }

    protected abstract void testCode() throws Throwable;

    @Override
    public void run() {
        try {
            testCode();
        } catch (Throwable ex) {
            _ex = ex;
        }
        _completed = true;
    }

    public void assertCompleted() throws Throwable {
        join(TIMEOUT);
        if (!_completed) {
            interrupt();
            fail(getName() + ": timeout reached");
        }
        if (_ex != null) {
            throw _ex;
        }
    }

}
