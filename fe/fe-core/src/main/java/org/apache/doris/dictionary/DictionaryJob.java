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

package org.apache.doris.dictionary;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.Config;
import org.apache.doris.common.io.Text;
import org.apache.doris.job.base.AbstractJob;
import org.apache.doris.job.common.JobType;
import org.apache.doris.job.common.TaskType;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DictionaryJob extends AbstractJob<DictionaryTask, DictionaryTaskContext> {
    private static final Logger LOG = LogManager.getLogger(DictionaryJob.class);
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

    @SerializedName(value = "ds")
    private Set<Long> dictionaries = Sets.newConcurrentHashSet();

    public DictionaryJob() {
        super.setCreateTimeMs(System.currentTimeMillis());
        jobRwLock = new ReentrantReadWriteLock(true);
    }

    @Override
    protected void checkJobParamsInternal() {
        // Nothing to check for now
    }

    @Override
    public List<DictionaryTask> createTasks(TaskType taskType, DictionaryTaskContext taskContext) {
        LOG.info("begin create dictionary task, jobId: {}, taskContext: {}", super.getJobId(), taskContext);
        ArrayList<DictionaryTask> tasks = Lists.newArrayList();
        
        if (taskContext != null) { // for register job
            // Create tasks for all dictionaries
            DictionaryTask task = new DictionaryTask(taskContext.getDictionary().getId(), taskContext);
            task.setTaskType(taskType);
            tasks.add(task);
            super.initTasks(tasks, taskType);
        }

        LOG.info("finish create dictionary tasks, count: {}", tasks.size());
        return tasks;
    }

    @Override
    public boolean isReadyForScheduling(DictionaryTaskContext taskContext) {
        // Only allow one batch of tasks at a time
        return getRunningTasks().size() < Config.job_dictionary_task_consumer_thread_num;
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
        return JobType.DICTIONARY;
    }

    @Override
    public List<DictionaryTask> queryTasks() {
        // not support now
        return Lists.newArrayList();
    }

    @Override
    public String formatMsgWhenExecuteQueueFull(Long taskId) {
        return String.format("Dictionary task %d is not ready because queue is full", taskId);
    }

    public boolean submitDataLoad(Dictionary dictionary) {
        try {
            DictionaryTaskContext context = new DictionaryTaskContext(true, dictionary);
            Env.getCurrentEnv().getJobManager().triggerJob(getJobId(), context);
        } catch (JobException e) {
            LOG.warn("Failed to submit data load for dictionary: {}", dictionary.getId(), e);
            return false;
        }
        return true;
    }

    public boolean submitDataUnload(Dictionary dictionary) {
        try {
            DictionaryTaskContext context = new DictionaryTaskContext(false, dictionary);
            Env.getCurrentEnv().getJobManager().triggerJob(getJobId(), context);
        } catch (JobException e) {
            LOG.warn("Failed to submit data unload for dictionary: {}", dictionary.getId(), e);
            return false;
        }
        return true;
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

    public static Dictionary read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, Dictionary.class);
    }
}
