package edu.purdue.jpgs.testUtil;

import java.util.Arrays;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 *
 * @author Lorenzo Bossi [lbossi@purdue.edu]
 */
public class StrictMock implements Answer {

    private boolean verify;

    public StrictMock() {
        verify = false;
    }

    @Override
    public Object answer(InvocationOnMock invocation) throws Throwable {
        if (verify) {
            throw new IllegalArgumentException(String.format("%s(%s) not mocked", invocation.getMethod().getName(), Arrays.toString(invocation.getArguments())));
        }
        return null;
    }

    public void turnOn() {
        verify = true;
    }

}
