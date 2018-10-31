package com.baidu.palo.transaction;

import com.baidu.palo.common.UserException;

public class IllegalTransactionParameterException extends UserException {

    private static final long serialVersionUID = 1L;

    public IllegalTransactionParameterException(String msg) {
        super(msg);
    }

    public IllegalTransactionParameterException(String msg, Throwable e) {
        super(msg, e);
    }
}