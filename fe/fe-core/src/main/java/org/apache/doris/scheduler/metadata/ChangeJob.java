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

package org.apache.doris.scheduler.metadata;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.scheduler.Utils.JobState;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ChangeJob implements Writable {
    @SerializedName("jobId")
    private long jobId;

    @SerializedName("finishTime")
    private long finishTime;

    @SerializedName("fromStatus")
    JobState fromStatus;

    @SerializedName("toStatus")
    JobState toStatus;

    @SerializedName("errorCode")
    private int errorCode;

    @SerializedName("errorMessage")
    private String errorMessage;

    public ChangeJob(long jobId, JobState toStatus) {
        this.jobId = jobId;
        this.toStatus = toStatus;
        this.finishTime = System.currentTimeMillis();
    }

    public long getJobId() {
        return jobId;
    }

    public void setJobId(long jobId) {
        this.jobId = jobId;
    }

    public long getFinishTime() {
        return finishTime;
    }

    public void setFinishTime(long finishTime) {
        this.finishTime = finishTime;
    }

    public JobState getFromStatus() {
        return fromStatus;
    }

    public void setFromStatus(JobState fromStatus) {
        this.fromStatus = fromStatus;
    }

    public JobState getToStatus() {
        return toStatus;
    }

    public void setToStatus(JobState toStatus) {
        this.toStatus = toStatus;
    }

    public static ChangeJob read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, ChangeJob.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }
}
