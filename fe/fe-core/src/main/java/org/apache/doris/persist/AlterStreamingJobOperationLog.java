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

package org.apache.doris.persist;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.job.common.JobStatus;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

public class AlterStreamingJobOperationLog implements Writable {
    @SerializedName(value = "jid")
    private long jobId;
    @SerializedName(value = "js")
    private JobStatus status;
    @SerializedName(value = "jp")
    private Map<String, String> jobProperties;
    @SerializedName(value = "sql")
    String executeSql;
    @SerializedName(value = "c")
    private String comment;
    @SerializedName(value = "ei")
    private String extraInfo;

    public AlterStreamingJobOperationLog(long jobId, JobStatus status, Map<String, String> jobProperties,
            String executeSql, String comment, String extraInfo) {
        this.jobId = jobId;
        this.status = status;
        this.jobProperties = jobProperties;
        this.executeSql = executeSql;
        this.comment = comment;
        this.extraInfo = extraInfo;
    }

    public long getJobId() {
        return jobId;
    }

    public Map<String, String> getJobProperties() {
        return jobProperties;
    }

    public String getExecuteSql() {
        return executeSql;
    }

    public JobStatus getStatus() {
        return status;
    }

    public String getComment() {
        return comment;
    }

    public String getExtraInfo() {
        return extraInfo;
    }

    public static AlterStreamingJobOperationLog read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, AlterStreamingJobOperationLog.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    @Override
    public String toString() {
        return "AlterStreamingJobOperationLog{"
                + "jobId=" + jobId
                + ", status=" + status
                + ", jobProperties=" + jobProperties
                + ", executeSql='" + executeSql + '\''
                + ", comment='" + comment + '\''
                + ", extraInfo='" + extraInfo + '\''
                + '}';
    }
}
