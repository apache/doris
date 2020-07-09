package org.apache.doris.http.exception;

import org.apache.doris.common.DdlException;

public class UnauthorizedException extends DdlException {
    public UnauthorizedException(String msg) {
        super(msg);
    }
}
