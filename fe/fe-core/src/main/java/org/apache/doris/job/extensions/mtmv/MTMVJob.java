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

package org.apache.doris.job.extensions.mtmv;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.job.base.AbstractJob;
import org.apache.doris.job.common.JobType;
import org.apache.doris.job.common.TaskType;
import org.apache.doris.job.extensions.mtmv.MTMVTask.MTMVTaskTriggerMode;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.thrift.TCell;
import org.apache.doris.thrift.TRow;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MTMVJob extends AbstractJob<MTMVTask, MTMVTaskContext> {
    private static final Logger LOG = LogManager.getLogger(MTMVJob.class);
    private ReentrantReadWriteLock jobRwLock;

    private static final ShowResultSetMetaData JOB_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("JobId", ScalarType.createVarchar(20)))
                    .addColumn(new Column("JobName", ScalarType.createVarchar(20)))
                    .addColumn(new Column("ExecuteType", ScalarType.createVarchar(20)))
                    .addColumn(new Column("RecurringStrategy", ScalarType.createVarchar(20)))
                    .addColumn(new Column("JobStatus", ScalarType.createVarchar(20)))
                    .addColumn(new Column("CreateTime", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Comment", ScalarType.createVarchar(20)))
                    .build();

    public static final ImmutableList<Column> SCHEMA = ImmutableList.of(
            new Column("Id", ScalarType.createStringType()),
            new Column("Name", ScalarType.createStringType()),
            new Column("MvId", ScalarType.createStringType()),
            new Column("MvName", ScalarType.createStringType()),
            new Column("MvDatabaseId", ScalarType.createStringType()),
            new Column("MvDatabaseName", ScalarType.createStringType()),
            new Column("ExecuteType", ScalarType.createStringType()),
            new Column("RecurringStrategy", ScalarType.createStringType()),
            new Column("Status", ScalarType.createStringType()),
            new Column("CreateTime", ScalarType.createStringType()));

    public static final ImmutableMap<String, Integer> COLUMN_TO_INDEX;

    static {
        ImmutableMap.Builder<String, Integer> builder = new ImmutableMap.Builder();
        for (int i = 0; i < SCHEMA.size(); i++) {
            builder.put(SCHEMA.get(i).getName().toLowerCase(), i);
        }
        COLUMN_TO_INDEX = builder.build();
    }

    private static final ShowResultSetMetaData TASK_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("JobId", ScalarType.createVarchar(20)))
                    .addColumn(new Column("TaskId", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Status", ScalarType.createVarchar(20)))
                    .addColumn(new Column("CreateTime", ScalarType.createVarchar(20)))
                    .addColumn(new Column("StartTime", ScalarType.createVarchar(20)))
                    .addColumn(new Column("FinishTime", ScalarType.createVarchar(20)))
                    .addColumn(new Column("DurationMs", ScalarType.createVarchar(20)))
                    .addColumn(new Column("ExecuteSql", ScalarType.createVarchar(20)))
                    .build();

    @SerializedName(value = "di")
    private long dbId;
    @SerializedName(value = "mi")
    private long mtmvId;

    public MTMVJob() {
        jobRwLock = new ReentrantReadWriteLock(true);
    }

    public MTMVJob(long dbId, long mtmvId) {
        this.dbId = dbId;
        this.mtmvId = mtmvId;
        super.setCreateTimeMs(System.currentTimeMillis());
        jobRwLock = new ReentrantReadWriteLock(true);
    }

    @Override
    protected void checkJobParamsInternal() {

    }

    @Override
    public List<MTMVTask> createTasks(TaskType taskType, MTMVTaskContext taskContext) {
        LOG.info("begin create mtmv task, jobId: {}, taskContext: {}", super.getJobId(), taskContext);
        if (taskContext == null) {
            taskContext = new MTMVTaskContext(MTMVTaskTriggerMode.SYSTEM);
        }
        MTMVTask task = new MTMVTask(dbId, mtmvId, taskContext);
        task.setTaskType(taskType);
        ArrayList<MTMVTask> tasks = new ArrayList<>();
        tasks.add(task);
        super.initTasks(tasks, taskType);
        LOG.info("finish create mtmv task, task: {}", task);
        return tasks;
    }

    /**
     * if user trigger, return true
     * if system trigger, Check if there are any system triggered tasks, and if so, return false
     *
     * @param taskContext
     * @return
     */
    @Override
    public boolean isReadyForScheduling(MTMVTaskContext taskContext) {
        if (taskContext != null) {
            return true;
        }
        List<MTMVTask> runningTasks = getRunningTasks();
        for (MTMVTask task : runningTasks) {
            if (task.getTaskContext() == null || task.getTaskContext().getTriggerMode() == MTMVTaskTriggerMode.SYSTEM) {
                LOG.warn("isReadyForScheduling return false, because current taskContext is null, exist task: {}",
                        task);
                return false;
            }
        }
        return true;
    }

    @Override
    public ShowResultSetMetaData getJobMetaData() {
        return JOB_META_DATA;
    }

    @Override
    public ShowResultSetMetaData getTaskMetaData() {
        return TASK_META_DATA;
    }

    @Override
    public JobType getJobType() {
        return JobType.MV;
    }

    @Override
    public List<MTMVTask> queryTasks() {
        MTMV mtmv = null;
        try {
            mtmv = getMTMV();
        } catch (DdlException | MetaNotFoundException e) {
            LOG.warn("get mtmv failed", e);
            return Lists.newArrayList();
        }
        return mtmv.getJobInfo().getHistoryTasks();
    }

    @Override
    public List<String> getShowInfo() {
        List<String> data = Lists.newArrayList();
        data.add(String.valueOf(super.getJobId()));
        data.add(super.getJobName());
        data.add(super.getJobConfig().getExecuteType().name());
        data.add(super.getJobConfig().convertRecurringStrategyToString());
        data.add(super.getJobStatus().name());
        data.add(TimeUtils.longToTimeString(super.getCreateTimeMs()));
        data.add(super.getComment());
        return data;
    }

    @Override
    public TRow getTvfInfo() {
        TRow trow = new TRow();
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(super.getJobId())));
        trow.addToColumnValue(new TCell().setStringVal(super.getJobName()));
        String dbName = "";
        String mvName = "";
        try {
            MTMV mtmv = getMTMV();
            dbName = mtmv.getQualifiedDbName();
            mvName = mtmv.getName();
        } catch (UserException e) {
            LOG.warn("can not find mv", e);
        }
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(mtmvId)));
        trow.addToColumnValue(new TCell().setStringVal(mvName));
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(dbId)));
        trow.addToColumnValue(new TCell().setStringVal(dbName));
        trow.addToColumnValue(new TCell().setStringVal(super.getJobConfig().getExecuteType().name()));
        trow.addToColumnValue(new TCell().setStringVal(super.getJobConfig().convertRecurringStrategyToString()));
        trow.addToColumnValue(new TCell().setStringVal(super.getJobStatus().name()));
        trow.addToColumnValue(new TCell().setStringVal(TimeUtils.longToTimeString(super.getCreateTimeMs())));
        return trow;
    }

    public boolean hasPriv(UserIdentity userIdentity, PrivPredicate wanted) {
        MTMV mtmv;
        try {
            mtmv = getMTMV();
        } catch (UserException e) {
            LOG.warn("can not find mv", e);
            return false;
        }
        return Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(userIdentity, InternalCatalog.INTERNAL_CATALOG_NAME,
                        mtmv.getQualifiedDbName(), mtmv.getName(),
                        wanted);
    }

    private MTMV getMTMV() throws DdlException, MetaNotFoundException {
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(dbId);
        return (MTMV) db.getTableOrMetaException(mtmvId, TableType.MATERIALIZED_VIEW);
    }

    public void readLock() {
        this.jobRwLock.readLock().lock();
    }

    public void readUnlock() {
        this.jobRwLock.readLock().unlock();
    }

    public void writeLock() {
        this.jobRwLock.writeLock().lock();
    }

    public void writeUnlock() {
        this.jobRwLock.writeLock().unlock();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

}
