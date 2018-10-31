package com.baidu.palo.transaction;

import com.baidu.palo.common.UserException;

public class BeginTransactionException extends UserException {

    private static final long serialVersionUID = 1L;

    public BeginTransactionException(String msg) {
        super(msg);
    }

    public BeginTransactionException(String msg, Throwable e) {
        super(msg, e);
    }
}
