package org.apache.doris.common.util;

import java.util.concurrent.locks.ReentrantLock;

/*
 * This Lock is for exposing the getOwner() method,
 * which is a protected method of ReentrantLock
 */
public class QueryableReentrantLock extends ReentrantLock {
    private static final long serialVersionUID = 1L;

    public QueryableReentrantLock() {
        super();
    }

    public QueryableReentrantLock(boolean fair) {
        super(fair);
    }

    @Override
    public Thread getOwner() {
        return super.getOwner();
    }
}
