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

package org.apache.doris.job.extensions.insert.streaming;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.io.Text;
import org.apache.doris.job.base.AbstractJob;
import org.apache.doris.job.base.JobExecutionConfiguration;
import org.apache.doris.job.common.JobStatus;
import org.apache.doris.job.common.JobType;
import org.apache.doris.job.common.PauseReason;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.Setter;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class StreamingInsertJob extends AbstractJob<StreamingJobSchedulerTask, Map<Object, Object>> {

    @SerializedName("did")
    private final long dbId;
    @Getter
    @SerializedName("st")
    protected JobStatus status;

    @Getter
    protected PauseReason pauseReason;

    @Getter
    @Setter
    protected long latestAutoResumeTimestamp;

    @Getter
    @Setter
    protected long autoResumeCount;

    @Getter
    @SerializedName("jp")
    private StreamingJobProperties jobProperties;

    public StreamingInsertJob(String jobName,
            JobStatus jobStatus,
            String dbName,
            String comment,
            UserIdentity createUser,
            JobExecutionConfiguration jobConfig,
            Long createTimeMs,
            String executeSql,
            StreamingJobProperties jobProperties) {
        super(getNextJobId(), jobName, jobStatus, dbName, comment, createUser,
                jobConfig, createTimeMs, executeSql);
        this.dbId = ConnectContext.get().getCurrentDbId();
        this.jobProperties = jobProperties;
    }


    @Override
    public void updateJobStatus(JobStatus status) throws JobException {
        super.updateJobStatus(status);
    }

    protected void createStreamingInsertTask() {
    }

    protected void fetchMeta() {
    }

    @Override
    public JobType getJobType() {
        return JobType.INSERT;
    }

    @Override
    protected void checkJobParamsInternal() {
    }

    @Override
    public boolean isReadyForScheduling(Map<Object, Object> taskContext) {
        return true;
    }

    @Override
    public java.util.List<StreamingJobSchedulerTask> createTasks(org.apache.doris.job.common.TaskType taskType,
                                                                 Map<Object, Object> taskContext) {
        return java.util.Collections.emptyList();
    }

    @Override
    public org.apache.doris.qe.ShowResultSetMetaData getTaskMetaData() {
        return org.apache.doris.qe.ShowResultSetMetaData.builder().build();
    }

    @Override
    public String formatMsgWhenExecuteQueueFull(Long taskId) {
        return commonFormatMsgWhenExecuteQueueFull(taskId, "streaming_task_queue_size",
                "job_streaming_task_consumer_thread_num");
    }

    @Override
    public List<StreamingJobSchedulerTask> queryTasks() {
        return new ArrayList<>();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }
}
