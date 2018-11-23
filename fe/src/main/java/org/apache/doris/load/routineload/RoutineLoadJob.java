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

package org.apache.doris.load.routineload;

import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.io.Writable;
import org.apache.doris.thrift.TResourceInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Routine load job is a function which stream load data from streaming medium to doris.
 * This function is suitable for streaming load job which loading data continuously
 * The properties include stream load properties and job properties.
 * The desireTaskConcurrentNum means that user expect the number of concurrent stream load
 * The routine load job support different streaming medium such as KAFKA
 */
public abstract class RoutineLoadJob implements Writable {

    private static final Logger LOG = LogManager.getLogger(RoutineLoadJob.class);

    private static final int DEFAULT_TASK_TIMEOUT_SECONDS = 10;

    public enum JobState {
        NEED_SCHEDULER,
        RUNNING,
        PAUSED,
        STOPPED,
        CANCELLED
    }

    public enum DataSourceType {
        KAFKA
    }

    protected String id;
    protected String name;
    protected String userName;
    protected long dbId;
    protected long tableId;
    protected String partitions;
    protected String columns;
    protected String where;
    protected String columnSeparator;
    protected int desireTaskConcurrentNum;
    protected JobState state;
    protected DataSourceType dataSourceType;
    // max number of error data in ten thousand data
    protected int maxErrorNum;
    protected String progress;

    // The tasks belong to this job
    protected List<RoutineLoadTaskInfo> routineLoadTaskInfoList;
    protected List<RoutineLoadTaskInfo> needSchedulerTaskInfoList;

    protected ReentrantReadWriteLock lock;
    // TODO(ml): error sample


    public RoutineLoadJob() {
    }

    public RoutineLoadJob(String id, String name, String userName, long dbId, long tableId,
                          String partitions, String columns, String where, String columnSeparator,
                          int desireTaskConcurrentNum, JobState state, DataSourceType dataSourceType,
                          int maxErrorNum, TResourceInfo resourceInfo) {
        this.id = id;
        this.name = name;
        this.userName = userName;
        this.dbId = dbId;
        this.tableId = tableId;
        this.partitions = partitions;
        this.columns = columns;
        this.where = where;
        this.columnSeparator = columnSeparator;
        this.desireTaskConcurrentNum = desireTaskConcurrentNum;
        this.state = state;
        this.dataSourceType = dataSourceType;
        this.maxErrorNum = maxErrorNum;
        this.resourceInfo = resourceInfo;
        this.progress = "";
        this.routineLoadTaskInfoList = new ArrayList<>();
        this.needSchedulerTaskInfoList = new ArrayList<>();
        lock = new ReentrantReadWriteLock(true);
    }

    public void readLock() {
        lock.readLock().lock();
    }

    public void readUnlock() {
        lock.readLock().unlock();
    }

    public void writeLock() {
        lock.writeLock().lock();
    }

    public void writeUnlock() {
        lock.writeLock().unlock();
    }

    // thrift object
    private TResourceInfo resourceInfo;

    public String getId() {
        return id;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public String getColumns() {
        return columns;
    }

    public String getWhere() {
        return where;
    }

    public String getColumnSeparator() {
        return columnSeparator;
    }

    public JobState getState() {
        return state;
    }

    public void setState(JobState state) {
        this.state = state;
    }

    public TResourceInfo getResourceInfo() {
        return resourceInfo;
    }

    public int getSizeOfRoutineLoadTaskInfoList() {
        readLock();
        try {
            return routineLoadTaskInfoList.size();
        } finally {
            readUnlock();
        }

    }

    public List<RoutineLoadTaskInfo> getNeedSchedulerTaskInfoList() {
        return needSchedulerTaskInfoList;
    }

    public void updateState(JobState jobState) {
        writeLock();
        try {
            state = jobState;
        } finally {
            writeUnlock();
        }
    }

    public void processTimeoutTasks() {
        writeLock();
        try {
            List<RoutineLoadTaskInfo> runningTasks = new ArrayList<>(routineLoadTaskInfoList);
            runningTasks.removeAll(needSchedulerTaskInfoList);

            for (RoutineLoadTaskInfo routineLoadTaskInfo : runningTasks) {
                if ((System.currentTimeMillis() - routineLoadTaskInfo.getLoadStartTimeMs())
                        > DEFAULT_TASK_TIMEOUT_SECONDS * 1000) {
                    String oldSignature = routineLoadTaskInfo.getId();
                    if (routineLoadTaskInfo instanceof KafkaTaskInfo) {
                        // remove old task
                        routineLoadTaskInfoList.remove(routineLoadTaskInfo);
                        // add new task
                        KafkaTaskInfo kafkaTaskInfo = new KafkaTaskInfo((KafkaTaskInfo) routineLoadTaskInfo);
                        routineLoadTaskInfoList.add(kafkaTaskInfo);
                        needSchedulerTaskInfoList.add(kafkaTaskInfo);
                    }
                    LOG.debug("Task {} was ran more then {} minutes. It was removed and rescheduled",
                            oldSignature, DEFAULT_TASK_TIMEOUT_SECONDS);
                }
            }
        } finally {
            writeUnlock();
        }
    }

    public void divideRoutineLoadJob(int currentConcurrentTaskNum) {
    }

    public int calculateCurrentConcurrentTaskNum() throws MetaNotFoundException {
        return 0;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // TODO(ml)
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // TODO(ml)
    }

    abstract RoutineLoadTask createTask(RoutineLoadTaskInfo routineLoadTaskInfo, long beId);

    private void checkStateTransform(RoutineLoadJob.JobState currentState, RoutineLoadJob.JobState desireState)
            throws LoadException {
        if (currentState == RoutineLoadJob.JobState.PAUSED && desireState == RoutineLoadJob.JobState.NEED_SCHEDULER) {
            throw new LoadException("could not transform " + currentState + " to " + desireState);
        } else if (currentState == RoutineLoadJob.JobState.CANCELLED ||
                currentState == RoutineLoadJob.JobState.STOPPED) {
            throw new LoadException("could not transform " + currentState + " to " + desireState);
        }
    }

}
