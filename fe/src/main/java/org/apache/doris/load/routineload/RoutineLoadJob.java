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

import org.apache.doris.analysis.CreateRoutineLoadStmt;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Writable;
import org.apache.doris.load.RoutineLoadDesc;
import org.apache.doris.load.TxnStateChangeListener;
import org.apache.doris.planner.StreamLoadPlanner;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.service.ExecuteEnv;
import org.apache.doris.service.FrontendServiceImpl;
import org.apache.doris.task.StreamLoadTask;
import org.apache.doris.thrift.TExecPlanFragmentParams;
import org.apache.doris.thrift.TLoadTxnCommitRequest;
import org.apache.doris.thrift.TResourceInfo;
import org.apache.doris.transaction.AbortTransactionException;
import org.apache.doris.transaction.BeginTransactionException;
import org.apache.doris.transaction.TransactionState;

import com.google.common.annotations.VisibleForTesting;

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
    private static final String STAR_STRING = "*";
    protected static final int DEFAULT_TASK_MAX_CONCURRENT_NUM = 3;

    /**
     *                  +-----------------+
     * fe schedule job |  NEED_SCHEDULE |  user resume job
     * +-----------     +                 | <---------+
     * |                |                 |           |
     * v                +-----------------+           ^
     * |
     * +------------+   user pause job        +-------+----+
     * |  RUNNING   |                         |  PAUSED    |
     * |            +-----------------------> |            |
     * +----+-------+                         +-------+----+
     * |                                              |
     * |                +---------------+             |
     * |                | STOPPED       |             |
     * +--------------> |               | <-----------+
     * user stop job    +---------------+    user stop| job
     * |                                              |
     * |                                              |
     * |                 +---------------+            |
     * |                 | CANCELLED     |            |
     * +---------------> |               | <----------+
     * system error      +---------------+    system error
     */
    public enum JobState {
        NEED_SCHEDULE,
        RUNNING,
        PAUSED,
        STOPPED,
        CANCELLED;

        public boolean isFinalState() {
            return this == STOPPED || this == CANCELLED;
        }

    }

    protected long id;
    protected String name;
    protected long dbId;
    protected long tableId;
    // this code is used to verify be task request
    protected long authCode;
    protected RoutineLoadDesc routineLoadDesc; // optional
    protected int desireTaskConcurrentNum; // optional
    protected JobState state;
    protected LoadDataSourceType dataSourceType;
    // max number of error data in ten thousand data
    // maxErrorNum / BASE_OF_ERROR_RATE = max error rate of routine load job
    // if current error rate is more then max error rate, the job will be paused
    protected int maxErrorNum; // optional
    // thrift object
    protected TResourceInfo resourceInfo;

    protected RoutineLoadProgress progress;
    protected String pausedReason;
    protected String cancelReason;

    // currentErrorNum and currentTotalNum will be update
    // when currentTotalNum is more then ten thousand or currentErrorNum is more then maxErrorNum
    protected int currentErrorNum;
    protected int currentTotalNum;

    // The tasks belong to this job
    protected List<RoutineLoadTaskInfo> routineLoadTaskInfoList;
    protected List<RoutineLoadTaskInfo> needScheduleTaskInfoList;

    // plan fragment which will be initialized during job scheduler
    protected TExecPlanFragmentParams tExecPlanFragmentParams;

    protected ReentrantReadWriteLock lock;
    // TODO(ml): error sample

    public RoutineLoadJob(String name, long dbId, long tableId, LoadDataSourceType dataSourceType) {
        this.id = Catalog.getInstance().getNextId();
        this.name = name;
        this.dbId = dbId;
        this.tableId = tableId;
        this.state = JobState.NEED_SCHEDULE;
        this.dataSourceType = dataSourceType;
        this.resourceInfo = ConnectContext.get().toResourceCtx();
        this.authCode = new StringBuilder().append(ConnectContext.get().getQualifiedUser())
                .append(ConnectContext.get().getRemoteIP())
                .append(id).append(System.currentTimeMillis()).toString().hashCode();
        this.routineLoadTaskInfoList = new ArrayList<>();
        this.needScheduleTaskInfoList = new ArrayList<>();
        lock = new ReentrantReadWriteLock(true);
    }

    // TODO(ml): I will change it after ut.
    @VisibleForTesting
    public RoutineLoadJob(long id, String name, long dbId, long tableId,
                          RoutineLoadDesc routineLoadDesc,
                          int desireTaskConcurrentNum, LoadDataSourceType dataSourceType,
                          int maxErrorNum) {
        this.id = id;
        this.name = name;
        this.dbId = dbId;
        this.tableId = tableId;
        this.routineLoadDesc = routineLoadDesc;
        this.desireTaskConcurrentNum = desireTaskConcurrentNum;
        this.state = JobState.NEED_SCHEDULE;
        this.dataSourceType = dataSourceType;
        this.maxErrorNum = maxErrorNum;
        this.resourceInfo = ConnectContext.get().toResourceCtx();
        this.routineLoadTaskInfoList = new ArrayList<>();
        this.needScheduleTaskInfoList = new ArrayList<>();
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

    public long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public long getDbId() {
        return dbId;
    }

    public String getDbFullName() throws MetaNotFoundException {
        Database database = Catalog.getCurrentCatalog().getDb(dbId);
        if (database == null) {
            throw new MetaNotFoundException("Database " + dbId + "has been deleted");
        }
        database.readLock();
        try {
            return database.getFullName();
        } finally {
            database.readUnlock();
        }
    }

    public long getTableId() {
        return tableId;
    }

    public String getTableName() throws MetaNotFoundException {
        Database database = Catalog.getCurrentCatalog().getDb(dbId);
        if (database == null) {
            throw new MetaNotFoundException("Database " + dbId + "has been deleted");
        }
        database.readLock();
        try {
            Table table = database.getTable(tableId);
            return table.getName();
        } finally {
            database.readUnlock();
        }
    }

    public JobState getState() {
        return state;
    }

    public long getAuthCode() {
        return authCode;
    }

    // this is a unprotected method which is called in the initialization function
    protected void setRoutineLoadDesc(RoutineLoadDesc routineLoadDesc) throws LoadException {
        if (this.routineLoadDesc != null) {
            throw new LoadException("Routine load desc has been initialized");
        }
        this.routineLoadDesc = routineLoadDesc;
    }

    public RoutineLoadDesc getRoutineLoadDesc() {
        return routineLoadDesc;
    }

    public TResourceInfo getResourceInfo() {
        return resourceInfo;
    }

    public RoutineLoadProgress getProgress() {
        return progress;
    }

    public String getPartitions() {
        if (routineLoadDesc == null
                || routineLoadDesc.getPartitionNames() == null
                || routineLoadDesc.getPartitionNames().size() == 0) {
            return STAR_STRING;
        } else {
            return String.join(",", routineLoadDesc.getPartitionNames());
        }
    }

    public String getClusterName() throws MetaNotFoundException {
        Database database = Catalog.getCurrentCatalog().getDb(id);
        if (database == null) {
            throw new MetaNotFoundException("Database " + dbId + "has been deleted");
        }
        database.readLock();
        try {
            return database.getClusterName();
        } finally {
            database.readUnlock();
        }
    }

    protected void setDesireTaskConcurrentNum(int desireTaskConcurrentNum) throws LoadException {
        writeLock();
        try {
            if (this.desireTaskConcurrentNum != 0) {
                throw new LoadException("Desired task concurrent num has been initialized");
            }
            this.desireTaskConcurrentNum = desireTaskConcurrentNum;
        } finally {
            writeUnlock();
        }
    }

    public String getDesiredConcurrentNumber() {
        if (desireTaskConcurrentNum == 0) {
            return "";
        } else {
            return String.valueOf(desireTaskConcurrentNum);
        }
    }

    protected void setMaxErrorNum(int maxErrorNum) throws LoadException {
        writeLock();
        try {
            if (this.maxErrorNum != 0) {
                throw new LoadException("Max error num has been initialized");
            }
            this.maxErrorNum = maxErrorNum;
        } finally {
            writeUnlock();
        }
    }

    public int getSizeOfRoutineLoadTaskInfoList() {
        readLock();
        try {
            return routineLoadTaskInfoList.size();
        } finally {
            readUnlock();
        }

    }

    public List<RoutineLoadTaskInfo> getNeedScheduleTaskInfoList() {
        return needScheduleTaskInfoList;
    }

    public TExecPlanFragmentParams gettExecPlanFragmentParams() {
        return tExecPlanFragmentParams;
    }

    public List<RoutineLoadTaskInfo> processTimeoutTasks() {
        List<RoutineLoadTaskInfo> result = new ArrayList<>();
        writeLock();
        try {
            List<RoutineLoadTaskInfo> runningTasks = new ArrayList<>(routineLoadTaskInfoList);
            runningTasks.removeAll(needScheduleTaskInfoList);

            for (RoutineLoadTaskInfo routineLoadTaskInfo : runningTasks) {
                if ((System.currentTimeMillis() - routineLoadTaskInfo.getLoadStartTimeMs())
                        > DEFAULT_TASK_TIMEOUT_SECONDS * 1000) {
                    String oldSignature = routineLoadTaskInfo.getId().toString();
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
                        result.add(unprotectRenewTask(routineLoadTaskInfo));
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


    public void removeNeedScheduleTask(RoutineLoadTaskInfo routineLoadTaskInfo) {
        writeLock();
        try {
            needScheduleTaskInfoList.remove(routineLoadTaskInfo);
        } finally {
            writeUnlock();
        }
    }

    // if rate of error data is more then max_filter_ratio, pause job
    protected void updateProgress(RLTaskTxnCommitAttachment attachment) {
        updateNumOfData(attachment.getFilteredRows(), attachment.getLoadedRows());
    }

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
    private void checkStateTransform(RoutineLoadJob.JobState desireState)
            throws UnsupportedOperationException {
        switch (state) {
            case PAUSED:
                if (desireState == JobState.PAUSED) {
                    throw new UnsupportedOperationException("Could not transform " + state + " to " + desireState);
                }
                break;
            case STOPPED:
            case CANCELLED:
                throw new UnsupportedOperationException("Could not transfrom " + state + " to " + desireState);
            default:
                break;
        }
    }

    private void loadTxnCommit(TLoadTxnCommitRequest request) throws TException {
        FrontendServiceImpl frontendService = new FrontendServiceImpl(ExecuteEnv.getInstance());
        frontendService.loadTxnCommit(request);
    }

    private void updateNumOfData(long numOfErrorData, long numOfTotalData) {
        currentErrorNum += numOfErrorData;
        currentTotalNum += numOfTotalData;
        if (currentTotalNum > BASE_OF_ERROR_RATE) {
            if (currentErrorNum > maxErrorNum) {
                LOG.info("current error num {} of job {} is more then max error num {}. begin to pause job",
                         currentErrorNum, id, maxErrorNum);
                // remove all of task in jobs and change job state to paused
                executePause("current error num of job is more then max error num");
            }

            // reset currentTotalNum and currentErrorNum
            currentErrorNum = 0;
            currentTotalNum = 0;
        } else if (currentErrorNum > maxErrorNum) {
            LOG.info("current error num {} of job {} is more then max error num {}. begin to pause job",
                     currentErrorNum, id, maxErrorNum);
            // remove all of task in jobs and change job state to paused
            executePause("current error num is more then max error num");
            // reset currentTotalNum and currentErrorNum
            currentErrorNum = 0;
            currentTotalNum = 0;
        }
    }

    abstract RoutineLoadTaskInfo unprotectRenewTask(RoutineLoadTaskInfo routineLoadTaskInfo) throws AnalysisException,
            LabelAlreadyUsedException, BeginTransactionException;

    public void plan() throws UserException {
        StreamLoadTask streamLoadTask = StreamLoadTask.fromRoutineLoadJob(this);
        Database database = Catalog.getCurrentCatalog().getDb(this.getDbId());

        database.readLock();
        try {
            StreamLoadPlanner planner = new StreamLoadPlanner(database,
                                                              (OlapTable) database.getTable(this.tableId),
                                                              streamLoadTask);
            tExecPlanFragmentParams = planner.plan();
        } finally {
            database.readUnlock();
        }
    }

    @Override
    public void beforeAborted(TransactionState txnState, TransactionState.TxnStatusChangeReason txnStatusChangeReason)
            throws AbortTransactionException {
        readLock();
        try {
            if (txnStatusChangeReason != null) {
                switch (txnStatusChangeReason) {
                    case TIMEOUT:
                    default:
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
            if (routineLoadTaskInfoOptional.isPresent()) {
                RoutineLoadTaskInfo routineLoadTaskInfo = routineLoadTaskInfoOptional.get();

                // step2: update job progress
                updateProgress(rlTaskTxnCommitAttachment);

                if (state == JobState.RUNNING) {
                    // step3: create a new task for partitions
                    RoutineLoadTaskInfo newRoutineLoadTaskInfo = unprotectRenewTask(routineLoadTaskInfo);
                    Catalog.getCurrentCatalog().getRoutineLoadManager()
                            .getNeedScheduleTasksQueue().add(newRoutineLoadTaskInfo);
                }
            } else {
                LOG.debug("There is no {} task in task info list. Maybe task has been renew or job state has changed. "
                                  + " Transaction {} will not be committed",
                          txnState.getLabel(), txnState.getTransactionId());
            }
        } catch (Throwable e) {
            LOG.error("failed to update offset in routine load task {} when transaction {} has been committed. "
                              + "change job to paused",
                      rlTaskTxnCommitAttachment.getTaskId(), txnState.getTransactionId(), e);
            updateState(JobState.PAUSED, "failed to update offset when transaction "
                                 + txnState.getTransactionId() + " has been committed");
        } finally {
            writeUnlock();
        }
    }

    @Override
    public void onAborted(TransactionState txnState, TransactionState.TxnStatusChangeReason txnStatusChangeReason) {
        updateState(JobState.PAUSED, txnStatusChangeReason.name());
        LOG.debug("job {} need to be pause while txn {} abort with reason {}",
                  id, txnState.getTransactionId(), txnStatusChangeReason.name());
    }

    protected static void unprotectCheckCreate(CreateRoutineLoadStmt stmt) throws AnalysisException {
        // check table belong to db, partitions belong to table
        if (stmt.getRoutineLoadDesc() == null) {
            checkDBSemantics(stmt.getDBTableName(), null);
        } else {
            checkDBSemantics(stmt.getDBTableName(), stmt.getRoutineLoadDesc().getPartitionNames());
        }
    }

    private static void checkDBSemantics(TableName dbTableName, List<String> partitionNames)
            throws AnalysisException {
        String tableName = dbTableName.getTbl();
        String dbName = dbTableName.getDb();

        // check table belong to database
        Database database = Catalog.getCurrentCatalog().getDb(dbName);
        Table table = database.getTable(tableName);
        if (table == null) {
            throw new AnalysisException("There is no table named " + tableName + " in " + dbName);
        }
        // check table type
        if (table.getType() != Table.TableType.OLAP) {
            throw new AnalysisException("Only doris table support routine load");
        }

        if (partitionNames == null || partitionNames.size() == 0) {
            return;
        }
        // check partitions belong to table
        Optional<String> partitionNotInTable = partitionNames.parallelStream()
                .filter(entity -> ((OlapTable) table).getPartition(entity) == null).findFirst();
        if (partitionNotInTable != null && partitionNotInTable.isPresent()) {
            throw new AnalysisException("Partition " + partitionNotInTable.get()
                                                + " does not belong to table " + tableName);
        }
    }

    public void updateState(JobState jobState) {
        updateState(jobState, null);
    }

    public void updateState(JobState jobState, String reason) {
        writeLock();
        try {
            checkStateTransform(jobState);
            switch (jobState) {
                case PAUSED:
                    executePause(reason);
                    break;
                case NEED_SCHEDULE:
                    executeNeedSchedule();
                    break;
                case STOPPED:
                    executeStop();
                    break;
                case CANCELLED:
                    executeCancel(reason);
                    break;
                default:
                    break;
            }
        } finally {
            writeUnlock();
        }
    }

    private void executePause(String reason) {
        // TODO(ml): edit log
        // remove all of task in jobs and change job state to paused
        pausedReason = reason;
        state = JobState.PAUSED;
        routineLoadTaskInfoList.clear();
        needScheduleTaskInfoList.clear();
    }

    private void executeNeedSchedule() {
        // TODO(ml): edit log
        state = JobState.NEED_SCHEDULE;
        routineLoadTaskInfoList.clear();
        needScheduleTaskInfoList.clear();
    }

    private void executeStop() {
        // TODO(ml): edit log
        state = JobState.STOPPED;
        routineLoadTaskInfoList.clear();
        needScheduleTaskInfoList.clear();
    }

    private void executeCancel(String reason) {
        cancelReason = reason;
        state = JobState.CANCELLED;
        routineLoadTaskInfoList.clear();
        needScheduleTaskInfoList.clear();
    }

    public void update() {
        // check if db and table exist
        Database database = Catalog.getCurrentCatalog().getDb(dbId);
        if (database == null) {
            LOG.info("The database {} has been deleted. Change {} job state to stopped", dbId, id);
            updateState(JobState.STOPPED);
        }
        database.readLock();
        try {
            Table table = database.getTable(tableId);
            // check table belong to database
            if (table == null) {
                LOG.info("The table {} has been deleted. Change {} job state to stopeed", tableId, id);
                updateState(JobState.STOPPED);
            }
        } finally {
            database.readUnlock();
        }

        // check if partition has been changed
        if (needReschedule()) {
            executeUpdate();
            updateState(JobState.NEED_SCHEDULE);
        }
    }

    protected void executeUpdate() {
    }

    protected boolean needReschedule() {
        return false;
    }
}
