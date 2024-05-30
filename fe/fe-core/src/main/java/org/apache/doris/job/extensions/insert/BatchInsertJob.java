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

package org.apache.doris.job.extensions.insert;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.io.Text;
import org.apache.doris.job.base.JobExecuteType;
import org.apache.doris.job.base.JobExecutionConfiguration;
import org.apache.doris.job.common.JobStatus;
import org.apache.doris.job.common.JobType;
import org.apache.doris.job.common.TaskType;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.nereids.trees.plans.commands.info.SplitColumnInfo;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang3.Range;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Batch insert job,usually used for batch insert data
 */
public class BatchInsertJob extends AbstractInsertJob<BatchInsertTask> implements GsonPostProcessable {


    @SerializedName(value = "sc")
    private int shardCount;

    @SerializedName(value = "scio")
    private SplitColumnInfo splitColumnInfo;

    @SerializedName(value = "ll")
    private long lowerLimit;

    @SerializedName(value = "ul")
    private long upperLimit;

    public BatchInsertJob(Long jobId, String jobName, JobStatus jobStatus, String dbName,
                          String comment, UserIdentity createUser,
                          JobExecutionConfiguration jobConfig, String executeSql, SplitColumnInfo splitColumnInfo,
                          int shardCount, long lowerLimit, long upperLimit) {
        super(jobId, jobName, jobStatus, dbName, comment, createUser, jobConfig,
                System.currentTimeMillis(), executeSql);
        this.shardCount = shardCount;
        this.splitColumnInfo = splitColumnInfo;
        this.lowerLimit = lowerLimit;
        this.upperLimit = upperLimit;
    }


    @Override
    protected void checkJobParamsInternal() {
        if (shardCount <= 0) {
            throw new IllegalArgumentException("shard count should be greater than 0");
        }
        if (lowerLimit >= upperLimit) {
            throw new IllegalArgumentException("lower limit should be less than upper limit");
        }
        if (null == splitColumnInfo) {
            throw new IllegalArgumentException("shard key should not be empty");
        }

        super.checkJobParamsInternal();
        if (getJobConfig().getExecuteType().equals(JobExecuteType.INSTANT)) {
            return;
        }
        throw new IllegalArgumentException("BatchInsertJob only support INSTANT execute type");
    }

    @Override
    public JobType getJobType() {
        return JobType.BATCH_INSERT;
    }

    @Override
    public List<BatchInsertTask> queryTasks() {
        if (insertTaskQueue.isEmpty()) {
            return new ArrayList<>();
        }
        return new ArrayList<>(insertTaskQueue);
    }

    @Override
    public List<BatchInsertTask> createTasks(TaskType taskType, Map<Object, Object> taskContext, Long groupId) {
        String originalSQL = getExecuteSql();
        List<Range> splitRanges = SQLRangeGenerator.generateRanges(shardCount, lowerLimit, upperLimit);
        List<BatchInsertTask> tasks = new ArrayList<>();
        for (Range splitRange : splitRanges) {
            BatchInsertTask task = new BatchInsertTask(splitColumnInfo, splitRange,
                    getCurrentDbName(), originalSQL, getCreateUser());
            tasks.add(task);
        }
        initTasks(tasks, taskType, groupId);
        insertTaskQueue.addAll(tasks);
        recordTasks(tasks);
        return tasks;
    }

    @Override
    public ShowResultSetMetaData getTaskMetaData() {
        return null;
    }
    //split task


    @Override
    public void onTaskSuccess(BatchInsertTask task) throws JobException {
        //should trigger next task
        // we need to check if all task is finished
        super.onTaskSuccess(task);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }
}
