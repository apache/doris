package org.apache.doris.scheduler.constants;

public enum JobCategory {
    COMMON(1, "common"),
    SQL(2, "sql"),
    ;

    private int code;

    private String description;

    JobCategory(int code, String description) {
        this.code = code;
        this.description = description;
    }
}
