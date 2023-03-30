package org.apache.hadoop.hive.metastore;

public class HiveThriftException extends Exception {

    private static final long serialVersionUID = 1L;

    public HiveThriftException() {
        super();
    }

    public HiveThriftException(String message, Throwable cause) {
        super(message, cause);
    }

    public HiveThriftException(String message) {
        super(message);
    }

    public HiveThriftException(Throwable cause) {
        super(cause);
    }
}
