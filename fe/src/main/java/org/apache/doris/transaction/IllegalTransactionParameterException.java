package org.apache.doris.transaction;

import org.apache.doris.common.UserException;

public class IllegalTransactionParameterException extends UserException {

    private static final long serialVersionUID = 1L;

    public IllegalTransactionParameterException(String msg) {
        super(msg);
    }

    public IllegalTransactionParameterException(String msg, Throwable e) {
        super(msg, e);
    }
}