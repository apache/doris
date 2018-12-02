package org.apache.doris.clone;

public class SchedException extends Exception {
    private static final long serialVersionUID = 4233856721704062083L;

    public enum Status {
        SCHEDULE_FAILED, // failed to schedule the tablet, this should only happen in scheduling pending tablets.
        RUNNING_FAILED, // failed to running the clone task, this should only happen in handling running tablets.
        UNRECOVERABLE, // unable to go on, the tablet should be removed from tablet scheduler.
        FINISHED // schedule is done, remove the tablet from tablet scheduler with status FINISHED
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
