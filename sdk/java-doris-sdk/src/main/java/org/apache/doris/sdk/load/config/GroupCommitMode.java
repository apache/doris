package org.apache.doris.sdk.load.config;

/**
 * Group Commit mode for stream load.
 * When SYNC or ASYNC is enabled, all label configurations are automatically ignored.
 */
public enum GroupCommitMode {
    /** Synchronous group commit: data visible immediately. */
    SYNC,
    /** Asynchronous group commit: highest throughput. */
    ASYNC,
    /** Disabled: traditional stream load mode. */
    OFF
}
