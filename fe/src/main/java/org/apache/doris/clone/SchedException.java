package org.apache.doris.clone;

public class SchedException extends Exception {
    private static final long serialVersionUID = 4233856721704062083L;

    public enum Status {
        SCHEDULE_FAILED, 
        UNRECOVERABLE
    }

    private Status status;

    public SchedException(Status status, String errorMsg) {
        super(errorMsg);
        this.status = status;
    }

    public Status getStatus() {
        return status;
    }
}
