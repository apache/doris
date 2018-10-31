package org.apache.doris.transaction;

import org.apache.doris.common.UserException;

public class BeginTransactionException extends UserException {

    private static final long serialVersionUID = 1L;

    public BeginTransactionException(String msg) {
        super(msg);
    }

    public BeginTransactionException(String msg, Throwable e) {
        super(msg, e);
    }
}
