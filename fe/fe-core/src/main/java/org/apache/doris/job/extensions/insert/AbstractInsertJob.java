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
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.job.base.AbstractJob;
import org.apache.doris.job.base.JobExecuteType;
import org.apache.doris.job.base.JobExecutionConfiguration;
import org.apache.doris.job.common.JobStatus;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.load.FailMsg;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.annotations.SerializedName;
import lombok.Data;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

@Data
@Log4j2
public abstract class AbstractInsertJob<T extends AbstractInsertTask> extends AbstractJob<T, Map<Object, Object>>
        implements GsonPostProcessable {
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
            .build();

    public static final ShowResultSetMetaData TASK_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("TaskId", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Label", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Status", ScalarType.createVarchar(20)))
                    .addColumn(new Column("EtlInfo", ScalarType.createVarchar(100)))
                    .addColumn(new Column("TaskInfo", ScalarType.createVarchar(100)))
                    .addColumn(new Column("ErrorMsg", ScalarType.createVarchar(100)))

                    .addColumn(new Column("CreateTimeMs", ScalarType.createVarchar(20)))
                    .addColumn(new Column("FinishTimeMs", ScalarType.createVarchar(20)))
                    .addColumn(new Column("TrackingUrl", ScalarType.createVarchar(200)))
                    .addColumn(new Column("LoadStatistic", ScalarType.createVarchar(200)))
                    .addColumn(new Column("User", ScalarType.createVarchar(50)))
                    .build();

    public static final ImmutableMap<String, Integer> COLUMN_TO_INDEX;

    static {
        ImmutableMap.Builder<String, Integer> builder = new ImmutableMap.Builder<>();
        for (int i = 0; i < SCHEMA.size(); i++) {
            builder.put(SCHEMA.get(i).getName().toLowerCase(), i);
        }
        COLUMN_TO_INDEX = builder.build();
    }


    @SerializedName("did")
    protected long dbId;
    @SerializedName("ln")
    protected String labelName;


    @SerializedName("fm")
    protected FailMsg failMsg;


    protected Set<Long> finishedTaskIds = new HashSet<>();


    protected ConnectContext ctx;
    protected StmtExecutor stmtExecutor;

    public AbstractInsertJob(long nextId, String labelName, JobStatus jobStatus, String currentDbName, String comment,
                             UserIdentity currentUserIdentity, JobExecutionConfiguration jobConfig) {
        super(nextId, labelName, jobStatus, currentDbName, comment, currentUserIdentity, jobConfig);
    }

    public AbstractInsertJob(long nextId, String jobName, JobStatus jobStatus, String dbName, String comment,
                             UserIdentity createUser, JobExecutionConfiguration jobConfig,
                             Long createTimeMs, String executeSql) {
        super(nextId, jobName, jobStatus, dbName, comment, createUser, jobConfig, createTimeMs, executeSql);
    }

    @Override
    public void gsonPostProcess() throws IOException {

        if (null == insertTaskQueue) {
            insertTaskQueue = new ConcurrentLinkedQueue<>();
        }

        if (null == finishedTaskIds) {
            finishedTaskIds = new HashSet<>();
        }

        if (null == getSucceedTaskCount()) {
            setSucceedTaskCount(new AtomicLong(0));
        }
        if (null == getFailedTaskCount()) {
            setFailedTaskCount(new AtomicLong(0));
        }
        if (null == getCanceledTaskCount()) {
            setCanceledTaskCount(new AtomicLong(0));
        }
    }


    @SerializedName("tas")
    protected ConcurrentLinkedQueue<T> insertTaskQueue = new ConcurrentLinkedQueue<>();


    public void recordTasks(List<T> tasks) {
        if (Config.max_persistence_task_count < 1) {
            return;
        }
        insertTaskQueue.addAll(tasks);

        while (insertTaskQueue.size() > Config.max_persistence_task_count) {
            insertTaskQueue.poll();
        }
        Env.getCurrentEnv().getEditLog().logUpdateJob(this);
    }

    @Override
    public void cancelTaskById(long taskId) throws JobException {
        super.cancelTaskById(taskId);
    }

    @Override
    public void cancelAllTasks() throws JobException {
        super.cancelAllTasks();
        this.failMsg = new FailMsg(FailMsg.CancelType.USER_CANCEL, "user cancel");
    }

    @Override
    public boolean isReadyForScheduling(Map<Object, Object> taskContext) {
        return CollectionUtils.isEmpty(getRunningTasks());
    }

    @Override
    protected void checkJobParamsInternal() {
        if (StringUtils.isBlank(getExecuteSql())) {
            throw new IllegalArgumentException("command or sql is null,must be set");
        }
    }

    @Override
    public void onReplayCreate() throws JobException {
        JobExecutionConfiguration jobConfig = new JobExecutionConfiguration();
        jobConfig.setExecuteType(JobExecuteType.INSTANT);
        setJobConfig(jobConfig);
        onRegister();
        checkJobParams();
        log.info(new LogBuilder(LogKey.LOAD_JOB, getJobId()).add("msg", "replay create load job").build());
    }

    @Override
    public void onReplayEnd(AbstractJob replayJob) throws JobException {
        if (!(replayJob instanceof InsertJob)) {
            return;
        }
        AbstractInsertJob insertJob = (AbstractInsertJob) replayJob;
        unprotectReadEndOperation(insertJob);
        log.info(new LogBuilder(LogKey.LOAD_JOB,
                insertJob.getJobId()).add("operation", insertJob).add("msg", "replay end load job").build());
    }

    public void unprotectReadEndOperation(AbstractInsertJob replayLog) {
        setJobStatus(replayLog.getJobStatus());
        setStartTimeMs(replayLog.getStartTimeMs());
        setFinishTimeMs(replayLog.getFinishTimeMs());
        failMsg = replayLog.failMsg;
    }

    public void onTaskSuccess(T task) throws JobException {
        super.onTaskSuccess(task);
    }
}
