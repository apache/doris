// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.mtmv.metadata;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.mtmv.MTMVUtils.TaskState;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AlterMTMVTask implements Writable {

    @SerializedName("jobId")
    private long jobId;

    @SerializedName("taskId")
    private String taskId;

    @SerializedName("finishTime")
    private long finishTime;

    @SerializedName("fromStatus")
    TaskState fromStatus;

    @SerializedName("toStatus")
    TaskState toStatus;

    @SerializedName("errorCode")
    private int errorCode = -1;

    @SerializedName("errorMessage")
    private String errorMessage = "";


    public AlterMTMVTask(long jobId, MTMVTask task, TaskState fromStatus, TaskState toStatus) {
        this.jobId = jobId;
        this.taskId = task.getTaskId();
        this.fromStatus = fromStatus;
        this.toStatus = toStatus;
        this.finishTime = task.getFinishTime();
        if (toStatus == TaskState.FAILED) {
            errorCode = task.getErrorCode();
            errorMessage = task.getErrorMessage();
        }
    }

    public long getJobId() {
        return jobId;
    }

    public void setJobId(long jobId) {
        this.jobId = jobId;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public TaskState getFromStatus() {
        return fromStatus;
    }

    public void setFromStatus(TaskState fromStatus) {
        this.fromStatus = fromStatus;
    }

    public TaskState getToStatus() {
        return toStatus;
    }

    public void setToStatus(TaskState toStatus) {
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

    public static AlterMTMVTask read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, AlterMTMVTask.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }
}
