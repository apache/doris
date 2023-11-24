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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.job.base.AbstractJob;
import org.apache.doris.job.common.JobType;
import org.apache.doris.job.common.TaskType;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MTMVJob extends AbstractJob<MTMVTask> {
    private static final Logger LOG = LogManager.getLogger(MTMVJob.class);
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

    public MTMVJob(long dbId, long mtmvId) {
        this.dbId = dbId;
        this.mtmvId = mtmvId;
        super.setCreateTimeMs(System.currentTimeMillis());
    }

    @Override
    protected void checkJobParamsInternal() {

    }

    @Override
    public List<MTMVTask> createTasks(TaskType taskType) {
        MTMVTask task = new MTMVTask(dbId, mtmvId);
        task.setTaskType(taskType);
        ArrayList<MTMVTask> tasks = new ArrayList<>();
        tasks.add(task);
        super.initTasks(tasks);
        return tasks;
    }

    @Override
    public boolean isReadyForScheduling() {
        return CollectionUtils.isEmpty(getRunningTasks());
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
        return JobType.MTMV;
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

    private MTMV getMTMV() throws DdlException, MetaNotFoundException {
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(dbId);
        return (MTMV) db.getTableOrMetaException(mtmvId, TableType.MATERIALIZED_VIEW);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

}
