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
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
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
import org.apache.doris.thrift.TCell;
import org.apache.doris.thrift.TRow;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
    public static final ImmutableList<Column> SCHEMA = ImmutableList.<Column>builder()
            .add(new Column("Id", ScalarType.createStringType()))
            .add(new Column("Name", ScalarType.createStringType()))
            .add(new Column("Definer", ScalarType.createStringType()))
            .add(new Column("ExecuteType", ScalarType.createStringType()))
            .add(new Column("RecurringStrategy", ScalarType.createStringType()))
            .add(new Column("Status", ScalarType.createStringType()))
            .add(new Column("ExecuteSql", ScalarType.createStringType()))
            .add(new Column("CreateTime", ScalarType.createStringType()))
            .addAll(COMMON_SCHEMA)
            .add(new Column("Comment", ScalarType.createStringType()))
            .add(new Column("SplitColumn", ScalarType.createStringType()))
            .add(new Column("Limit", ScalarType.createStringType()))
            .add(new Column("Starts", ScalarType.createStringType()))
            .add(new Column("Ends", ScalarType.createStringType()))
            .build();
    public static final ImmutableMap<String, Integer> COLUMN_TO_INDEX;

    static {
        ImmutableMap.Builder<String, Integer> builder = new ImmutableMap.Builder<>();
        for (int i = 0; i < SCHEMA.size(); i++) {
            builder.put(SCHEMA.get(i).getName().toLowerCase(), i);
        }
        COLUMN_TO_INDEX = builder.build();
    }

    @SerializedName(value = "sc")
    private int shardCount;

    @SerializedName(value = "scio")
    private SplitColumnInfo splitColumnInfo;

    @SerializedName(value = "ll")
    private String lowerLimit;

    @SerializedName(value = "ul")
    private String upperLimit;

    public BatchInsertJob(Long jobId, String jobName, JobStatus jobStatus, String dbName,
                          String comment, UserIdentity createUser,
                          JobExecutionConfiguration jobConfig, String executeSql, SplitColumnInfo splitColumnInfo,
                          int shardCount, String lowerLimit, String upperLimit) {
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
        if (Long.parseLong(lowerLimit) >= Long.parseLong(upperLimit)) {
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
        List<Range> splitRanges = SQLRangeGenerator.generateRanges(shardCount, Long.parseLong(lowerLimit),
                Long.parseLong(upperLimit));
        List<BatchInsertTask> tasks = new ArrayList<>();
        for (Range splitRange : splitRanges) {
            BatchInsertTask task = new BatchInsertTask(splitColumnInfo, splitRange,
                    getCurrentDbName(), originalSQL, getCreateUser());
            tasks.add(task);
        }
        initTasks(tasks, taskType, groupId);
        recordTasks(tasks);
        return tasks;
    }

    @Override
    public ShowResultSetMetaData getTaskMetaData() {
        return TASK_META_DATA;
    }
    //split task


    @Override
    public void onTaskSuccess(BatchInsertTask task) throws JobException {
        //should trigger next task
        // we need to check if all task is finished
        super.onTaskSuccess(task);
    }

    private String convertSplitColumnToString() {
        return splitColumnInfo.getTableNameInfo().getDb() + "." + splitColumnInfo.getTableNameInfo().getTbl()
                + "." + splitColumnInfo.getColumnName();
    }

    @Override
    public TRow getTvfInfo() {
        TRow row = getCommonTvfInfo();
        row.addToColumnValue(new TCell().setStringVal(null == splitColumnInfo ? "" : convertSplitColumnToString()));
        row.addToColumnValue(new TCell().setStringVal(String.valueOf(shardCount)));
        row.addToColumnValue(new TCell().setStringVal(String.valueOf(lowerLimit)));
        row.addToColumnValue(new TCell().setStringVal(String.valueOf(upperLimit)));
        return row;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }
}
