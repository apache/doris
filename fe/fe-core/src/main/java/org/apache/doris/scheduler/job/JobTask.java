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
import org.apache.doris.scheduler.constants.TaskType;

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import lombok.Data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

@Data
public class JobTask<T> implements Writable {

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

    @SerializedName("taskType")
    private TaskType taskType = TaskType.SCHEDULER_JOB_TASK;

    /**
     * Some parameters specific to the current task that need to be used to execute the task
     * eg: sql task, sql it's: select * from table where id = 1 order by id desc limit ${limit} offset ${offset}
     * contextData is a map, key1 is limit, value is 10,key2 is offset, value is 1
     * when execute the task, we will replace the ${limit} to 10, ${offset} to 1
     * so to execute sql is: select * from table where id = 1 order by id desc limit 10 offset 1.
     */
    private T contextData;

    public JobTask(Long jobId, Long taskId, Long createTimeMs) {
        //it's enough to use nanoTime to identify a task
        this.taskId = taskId;
        this.jobId = jobId;
        this.createTimeMs = createTimeMs;
    }

    public JobTask(Long jobId, Long taskId, Long createTimeMs, T contextData) {
        this(jobId, taskId, createTimeMs);
        this.contextData = contextData;
    }

    public List<String> getShowInfo(String jobName) {
        List<String> row = Lists.newArrayList();
        row.add(String.valueOf(taskId));
        row.add(String.valueOf(jobId));
        row.add(jobName);
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
        row.add(taskType.name());
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
