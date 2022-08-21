package org.apache.doris.scheduler.metadata;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.scheduler.Utils;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ChangeTaskRecord implements Writable {

    @SerializedName("taskId")
    private long taskId;

    @SerializedName("queryId")
    private String queryId;

    @SerializedName("finishTime")
    private long finishTime;

    @SerializedName("fromStatus")
    Utils.TaskState fromStatus;

    @SerializedName("toStatus")
    Utils.TaskState toStatus;

    @SerializedName("errorCode")
    private int errorCode;

    @SerializedName("errorMessage")
    private String errorMessage;


    public ChangeTaskRecord(long taskId, TaskRecord taskRecord, Utils.TaskState fromStatus, Utils.TaskState toStatus) {
        this.taskId = taskId;
        this.queryId = taskRecord.getQueryId();
        this.fromStatus = fromStatus;
        this.toStatus = toStatus;
        this.finishTime = taskRecord.getFinishTime();
        if (toStatus == Utils.TaskState.FAILED) {
            errorCode = taskRecord.getErrorCode();
            errorMessage = taskRecord.getErrorMessage();
        }
    }

    public long getTaskId() {
        return taskId;
    }

    public void setTaskId(long taskId) {
        this.taskId = taskId;
    }

    public String getQueryId() {
        return queryId;
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }

    public Utils.TaskState getFromStatus() {
        return fromStatus;
    }

    public void setFromStatus(Utils.TaskState fromStatus) {
        this.fromStatus = fromStatus;
    }

    public Utils.TaskState getToStatus() {
        return toStatus;
    }

    public void setToStatus(Utils.TaskState toStatus) {
        this.toStatus = toStatus;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(int errorCode) {
        this.errorCode = errorCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public long getFinishTime() {
        return finishTime;
    }

    public void setFinishTime(long finishTime) {
        this.finishTime = finishTime;
    }

    public static ChangeTaskRecord read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, ChangeTaskRecord.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

}
