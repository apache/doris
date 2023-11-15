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
import org.apache.doris.job.base.AbstractJob;
import org.apache.doris.job.common.JobType;
import org.apache.doris.job.common.TaskType;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MTMVJob extends AbstractJob<MTMVTask> {
    private static final ShowResultSetMetaData JOB_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("JobId", ScalarType.createVarchar(20)))
                    .addColumn(new Column("JobName", ScalarType.createVarchar(20)))
                    .build();
    private static final ShowResultSetMetaData TASK_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("JobId", ScalarType.createVarchar(20)))
                    .addColumn(new Column("TaskId", ScalarType.createVarchar(20)))
                    .build();

    @SerializedName(value = "dn")
    private String dbName;
    @SerializedName(value = "mi")
    private long mtmvId;

    public MTMVJob(String dbName, long mtmvId) {
        this.dbName = dbName;
        this.mtmvId = mtmvId;
        super.setCreateTimeMs(System.currentTimeMillis());
    }

    @Override
    protected void checkJobParamsInternal() {

    }

    @Override
    public List<MTMVTask> createTasks(TaskType taskType) {
        MTMVTask task = new MTMVTask(dbName, mtmvId);
        task.setTaskType(taskType);
        ArrayList<MTMVTask> tasks = new ArrayList<>();
        tasks.add(task);
        super.initTasks(tasks);
        return tasks;
    }

    @Override
    public boolean isReadyForScheduling() {
        return getRunningTasks().size() == 0;
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
        return getMTMV().getJobInfo().getHistoryTasks();
    }

    @Override
    public List<String> getShowInfo() {
        List<String> data = Lists.newArrayList();
        data.add(super.getJobId() + "");
        data.add(super.getJobName());
        return data;
    }

    private MTMV getMTMV() {
        try {
            Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(dbName);
            return (MTMV) db.getTableOrMetaException(mtmvId, TableType.MATERIALIZED_VIEW);
        } catch (MetaNotFoundException | DdlException e) {
            e.printStackTrace();
            return null;
        }
    }


    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, JobType.MTMV.name());
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static MTMVJob readFields(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), MTMVJob.class);
    }
}
