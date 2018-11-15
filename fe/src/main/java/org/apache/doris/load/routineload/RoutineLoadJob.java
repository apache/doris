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

import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.io.Writable;
import org.apache.doris.thrift.TResourceInfo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Routine load job is a function which stream load data from streaming medium to doris.
 * This function is suitable for streaming load job which loading data continuously
 * The properties include stream load properties and job properties.
 * The desireTaskConcurrentNum means that user expect the number of concurrent stream load
 * The routine load job support different streaming medium such as KAFKA
 */
public class RoutineLoadJob implements Writable {

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

    protected long id;
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
    protected ReentrantReadWriteLock lock;
    // TODO(ml): error sample


    public RoutineLoadJob() {
    }

    public RoutineLoadJob(long id, String name, String userName, long dbId, long tableId,
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

    public long getId() {
        return id;
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

    public List<RoutineLoadTask> divideRoutineLoadJob(int currentConcurrentTaskNum) {
        return null;
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
}
