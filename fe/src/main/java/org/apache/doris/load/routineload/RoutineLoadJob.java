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

import com.google.common.collect.Lists;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Writable;
import org.apache.doris.load.TxnStateChangeListener;
import org.apache.doris.service.ExecuteEnv;
import org.apache.doris.service.FrontendServiceImpl;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.thrift.TLoadTxnCommitRequest;
import org.apache.doris.thrift.TResourceInfo;
import org.apache.doris.thrift.TTaskType;
import org.apache.doris.transaction.AbortTransactionException;
import org.apache.doris.transaction.BeginTransactionException;
import org.apache.doris.transaction.LabelAlreadyExistsException;
import org.apache.doris.transaction.TransactionState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Routine load job is a function which stream load data from streaming medium to doris.
 * This function is suitable for streaming load job which loading data continuously
 * The properties include stream load properties and job properties.
 * The desireTaskConcurrentNum means that user expect the number of concurrent stream load
 * The routine load job support different streaming medium such as KAFKA
 */
public abstract class RoutineLoadJob implements Writable, TxnStateChangeListener {
    
    private static final Logger LOG = LogManager.getLogger(RoutineLoadJob.class);
    
    private static final int DEFAULT_TASK_TIMEOUT_SECONDS = 10;
    private static final int BASE_OF_ERROR_RATE = 10000;
    
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
    // maxErrorNum / BASE_OF_ERROR_RATE = max error rate of routine load job
    // if current error rate is more then max error rate, the job will be paused
    protected int maxErrorNum;
    protected RoutineLoadProgress progress;
    protected String pausedReason;
    
    // currentErrorNum and currentTotalNum will be update
    // when currentTotalNum is more then ten thousand or currentErrorNum is more then maxErrorNum
    protected int currentErrorNum;
    protected int currentTotalNum;
    
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
    
    public RoutineLoadProgress getProgress() {
        return progress;
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
    
    public List<RoutineLoadTaskInfo> processTimeoutTasks() {
        List<RoutineLoadTaskInfo> result = new ArrayList<>();
        writeLock();
        try {
            List<RoutineLoadTaskInfo> runningTasks = new ArrayList<>(routineLoadTaskInfoList);
            runningTasks.removeAll(needSchedulerTaskInfoList);
            
            for (RoutineLoadTaskInfo routineLoadTaskInfo : runningTasks) {
                if ((System.currentTimeMillis() - routineLoadTaskInfo.getLoadStartTimeMs())
                        > DEFAULT_TASK_TIMEOUT_SECONDS * 1000) {
                    String oldSignature = routineLoadTaskInfo.getId();
                    // abort txn if not committed
                    try {
                        Catalog.getCurrentGlobalTransactionMgr()
                                .abortTransaction(routineLoadTaskInfo.getTxnId(), "routine load task of txn was timeout");
                    } catch (UserException e) {
                        if (e.getMessage().contains("committed")) {
                            LOG.debug("txn of task {} has been committed, timeout task has been ignored", oldSignature);
                            continue;
                        }
                    }
                    
                    try {
                        result.add(reNewTask(routineLoadTaskInfo));
                        LOG.debug("Task {} was ran more then {} minutes. It was removed and rescheduled",
                                  oldSignature, DEFAULT_TASK_TIMEOUT_SECONDS);
                    } catch (UserException e) {
                        state = JobState.CANCELLED;
                        // TODO(ml): edit log
                        LOG.warn("failed to renew a routine load task in job {} with error message {}", id, e.getMessage());
                    }
                }
            }
        } finally {
            writeUnlock();
        }
        return result;
    }
    
    abstract List<RoutineLoadTaskInfo> divideRoutineLoadJob(int currentConcurrentTaskNum);
    
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
    
    
    public void removeNeedSchedulerTask(RoutineLoadTaskInfo routineLoadTaskInfo) {
        writeLock();
        try {
            needSchedulerTaskInfoList.remove(routineLoadTaskInfo);
        } finally {
            writeUnlock();
        }
    }
    
    abstract void updateProgress(RoutineLoadProgress progress);
    
    public boolean containsTask(String taskId) {
        readLock();
        try {
            return routineLoadTaskInfoList.parallelStream()
                    .anyMatch(entity -> entity.getId().equals(taskId));
        } finally {
            readUnlock();
        }
    }
    
    // All of private method could not be call without lock
    private void checkStateTransform(RoutineLoadJob.JobState currentState, RoutineLoadJob.JobState desireState)
            throws LoadException {
        if (currentState == RoutineLoadJob.JobState.PAUSED && desireState == RoutineLoadJob.JobState.NEED_SCHEDULER) {
            throw new LoadException("could not transform " + currentState + " to " + desireState);
        } else if (currentState == RoutineLoadJob.JobState.CANCELLED ||
                currentState == RoutineLoadJob.JobState.STOPPED) {
            throw new LoadException("could not transform " + currentState + " to " + desireState);
        }
    }
    
    private void loadTxnCommit(TLoadTxnCommitRequest request) throws TException {
        FrontendServiceImpl frontendService = new FrontendServiceImpl(ExecuteEnv.getInstance());
        frontendService.loadTxnCommit(request);
    }
    
    private void updateNumOfData(int numOfErrorData, int numOfTotalData) {
        currentErrorNum += numOfErrorData;
        currentTotalNum += numOfTotalData;
        if (currentTotalNum > BASE_OF_ERROR_RATE) {
            if (currentErrorNum > maxErrorNum) {
                LOG.info("current error num {} of job {} is more then max error num {}. begin to pause job",
                         currentErrorNum, id, maxErrorNum);
                // remove all of task in jobs and change job state to paused
                pausedJob("current error num of job is more then max error num");
            }
            
            // reset currentTotalNum and currentErrorNum
            currentErrorNum = 0;
            currentTotalNum = 0;
        } else if (currentErrorNum > maxErrorNum) {
            LOG.info("current error num {} of job {} is more then max error num {}. begin to pause job",
                     currentErrorNum, id, maxErrorNum);
            // remove all of task in jobs and change job state to paused
            pausedJob("current error num is more then max error num");
            // reset currentTotalNum and currentErrorNum
            currentErrorNum = 0;
            currentTotalNum = 0;
        }
    }
    
    abstract RoutineLoadTaskInfo reNewTask(RoutineLoadTaskInfo routineLoadTaskInfo) throws AnalysisException,
            LabelAlreadyExistsException, BeginTransactionException;
    
    @Override
    public void beforeAborted(TransactionState txnState, TransactionState.TxnStatusChangeReason txnStatusChangeReason)
            throws AbortTransactionException {
        readLock();
        try {
            if (txnStatusChangeReason != null) {
                switch (txnStatusChangeReason) {
                    case TIMEOUT:
                        String taskId = txnState.getLabel();
                        if (routineLoadTaskInfoList.parallelStream().anyMatch(entity -> entity.getId().equals(taskId))) {
                            throw new AbortTransactionException(
                                    "there are task " + taskId + " related to this txn, "
                                            + "txn could not be abort", txnState.getTransactionId());
                        }
                        break;
                }
            }
        } finally {
            readUnlock();
        }
    }
    
    @Override
    public void onCommitted(TransactionState txnState) {
        // step0: get progress from transaction state
        RLTaskTxnCommitAttachment rlTaskTxnCommitAttachment = (RLTaskTxnCommitAttachment) txnState.getTxnCommitAttachment();
        
        writeLock();
        try {
            // step1: find task in job
            Optional<RoutineLoadTaskInfo> routineLoadTaskInfoOptional =
                    routineLoadTaskInfoList.parallelStream()
                            .filter(entity -> entity.getId().equals(txnState.getLabel())).findFirst();
            RoutineLoadTaskInfo routineLoadTaskInfo = routineLoadTaskInfoOptional.get();
            
            // step2: update job progress
            updateProgress(rlTaskTxnCommitAttachment.getProgress());
            
            // step3: remove task in agentTaskQueue
            AgentTaskQueue.removeTask(rlTaskTxnCommitAttachment.getBackendId(), TTaskType.STREAM_LOAD,
                                      rlTaskTxnCommitAttachment.getTaskSignature());
            
            // step4: if rate of error data is more then max_filter_ratio, paused job
            updateNumOfData(rlTaskTxnCommitAttachment.getNumOfErrorData(), rlTaskTxnCommitAttachment.getNumOfTotalData());
            
            if (state == JobState.RUNNING) {
                // step5: create a new task for partitions
                RoutineLoadTaskInfo newRoutineLoadTaskInfo = reNewTask(routineLoadTaskInfo);
                Catalog.getCurrentCatalog().getRoutineLoadInstance()
                        .getNeedSchedulerTasksQueue().addAll(Lists.newArrayList(newRoutineLoadTaskInfo));
            }
        } catch (Throwable e) {
            LOG.error("failed to update offset in routine load task {} when transaction {} has been committed. "
                              + "change job to paused",
                      rlTaskTxnCommitAttachment.getTaskId(), txnState.getTransactionId());
            pausedJob("failed to update offset when transaction "
                              + txnState.getTransactionId() + " has been committed");
        } finally {
            writeUnlock();
        }
    }
    
    @Override
    public void onAborted(TransactionState txnState, TransactionState.TxnStatusChangeReason txnStatusChangeReason) {
        pausedJob(txnStatusChangeReason.name());
        LOG.debug("job {} need to be paused while txn {} abort with reason {}",
                  id, txnState.getTransactionId(), txnStatusChangeReason.name());
    }
    
    private void pausedJob(String reason) {
        // TODO(ml): edit log
        // remove all of task in jobs and change job state to paused
        pausedReason = reason;
        state = JobState.PAUSED;
        routineLoadTaskInfoList.clear();
        needSchedulerTaskInfoList.clear();
    }
}
