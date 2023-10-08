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

package org.apache.doris.scheduler.job;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import lombok.Data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

@Data
public class JobTask implements Writable {

    @SerializedName("jobId")
    private Long jobId;
    @SerializedName("taskId")
    private Long taskId;
    @SerializedName("createTimeMs")
    private Long createTimeMs;
    @SerializedName("startTimeMs")
    private Long startTimeMs;
    @SerializedName("endTimeMs")
    private Long endTimeMs;
    @SerializedName("successful")
    private Boolean isSuccessful;

    @SerializedName("executeSql")
    private String executeSql;
    @SerializedName("executeResult")
    private String executeResult;
    @SerializedName("errorMsg")
    private String errorMsg;

    public JobTask(Long jobId, Long taskId, Long createTimeMs) {
        //it's enough to use nanoTime to identify a task
        this.taskId = taskId;
        this.jobId = jobId;
        this.createTimeMs = createTimeMs;
    }

    public List<String> getShowInfo() {
        List<String> row = Lists.newArrayList();
        row.add(String.valueOf(jobId));
        row.add(String.valueOf(taskId));
        if (null != createTimeMs) {
            row.add(TimeUtils.longToTimeString(createTimeMs));
        }
        row.add(TimeUtils.longToTimeString(startTimeMs));
        row.add(null == endTimeMs ? "null" : TimeUtils.longToTimeString(endTimeMs));
        if (endTimeMs == null) {
            row.add("RUNNING");
        } else {
            row.add(isSuccessful ? "SUCCESS" : "FAILED");
        }
        if (null == executeSql) {
            row.add("null");
        } else {
            row.add(executeSql);
        }
        if (null == executeResult) {
            row.add("null");
        } else {
            row.add(executeResult);
        }
        if (null == errorMsg) {
            row.add("null");
        } else {
            row.add(errorMsg);
        }
        return row;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String jobData = GsonUtils.GSON.toJson(this);
        Text.writeString(out, jobData);
    }

    public static JobTask readFields(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), JobTask.class);
    }
}
