package org.apache.doris.qe;

import com.google.common.base.Strings;

public class QueryStateException extends Exception {
    public QueryStateException(String msg) {
        super(Strings.nullToEmpty(msg));
    }
    public QueryStateException(String msg, Throwable cause) {
        super(Strings.nullToEmpty(msg), cause);
    }
}
