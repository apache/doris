package org.apache.doris.sdk.load.model;

/**
 * Result of a stream load operation.
 */
public class LoadResponse {

    public enum Status { SUCCESS, FAILURE }

    private final Status status;
    private final RespContent respContent;
    private final String errorMessage;

    private LoadResponse(Status status, RespContent respContent, String errorMessage) {
        this.status = status;
        this.respContent = respContent;
        this.errorMessage = errorMessage;
    }

    public static LoadResponse success(RespContent resp) {
        return new LoadResponse(Status.SUCCESS, resp, null);
    }

    public static LoadResponse failure(RespContent resp, String errorMessage) {
        return new LoadResponse(Status.FAILURE, resp, errorMessage);
    }

    public Status getStatus() { return status; }
    public RespContent getRespContent() { return respContent; }
    public String getErrorMessage() { return errorMessage; }

    @Override
    public String toString() {
        return "LoadResponse{status=" + status
                + (errorMessage != null ? ", error='" + errorMessage + "'" : "")
                + (respContent != null ? ", resp=" + respContent : "")
                + "}";
    }
}
