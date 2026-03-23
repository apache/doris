package org.apache.doris.sdk.load.exception;

/**
 * Thrown for retryable HTTP-level stream load failures (e.g. HTTP 5xx, connection errors).
 * Non-retryable business failures (bad data, auth error) are returned via LoadResponse.
 */
public class StreamLoadException extends RuntimeException {

    public StreamLoadException(String message) {
        super(message);
    }

    public StreamLoadException(String message, Throwable cause) {
        super(message, cause);
    }
}
