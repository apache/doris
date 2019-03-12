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

import com.google.common.collect.Maps;
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
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
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
import org.apache.doris.transaction.TransactionException;
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
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
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
    private static final int DEFAULT_MAX_ERROR_NUM = (int) (BASE_OF_ERROR_RATE * 0.5);
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
    protected int maxErrorNum = DEFAULT_MAX_ERROR_NUM; // optional
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
        Database database = Catalog.getCurrentCatalog().getDb(dbId);
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

    public TExecPlanFragmentParams gettExecPlanFragmentParams() {
        return tExecPlanFragmentParams;
    }

    // only check loading task
    public List<RoutineLoadTaskInfo> processTimeoutTasks() {
        List<RoutineLoadTaskInfo> result = new ArrayList<>();
        writeLock();
        try {
            List<RoutineLoadTaskInfo> runningTasks = new ArrayList<>(routineLoadTaskInfoList);
            for (RoutineLoadTaskInfo routineLoadTaskInfo : runningTasks) {
                if ((routineLoadTaskInfo.getLoadStartTimeMs() != 0L)
                        && ((System.currentTimeMillis() - routineLoadTaskInfo.getLoadStartTimeMs())
                        > DEFAULT_TASK_TIMEOUT_SECONDS * 1000)) {
                    UUID oldTaskId = routineLoadTaskInfo.getId();
                    // abort txn if not committed
                    try {
                        Catalog.getCurrentGlobalTransactionMgr()
                                .abortTransaction(routineLoadTaskInfo.getTxnId(), "routine load task of txn was timeout");
                    } catch (UserException e) {
                        if (e.getMessage().contains("committed")) {
                            LOG.debug(new LogBuilder(LogKey.ROUINTE_LOAD_TASK, oldTaskId)
                                              .add("msg", "txn of task has been committed when checking timeout")
                                              .build());
                            continue;
                        }
                    }
                }
            }
        } finally {
            writeUnlock();
        }
        return result;
    }

    abstract void divideRoutineLoadJob(int currentConcurrentTaskNum);

    public int calculateCurrentConcurrentTaskNum() throws MetaNotFoundException {
        return 0;
    }

    public Map<Long, Integer> getBeIdToConcurrentTaskNum() {
        Map<Long, Integer> beIdConcurrentTasksNum = Maps.newHashMap();
        readLock();
        try {
            for (RoutineLoadTaskInfo routineLoadTaskInfo : routineLoadTaskInfoList) {
                if (routineLoadTaskInfo.getBeId() != -1L) {
                    long beId = routineLoadTaskInfo.getBeId();
                    if (beIdConcurrentTasksNum.containsKey(beId)) {
                        beIdConcurrentTasksNum.put(beId, beIdConcurrentTasksNum.get(beId) + 1);
                    } else {
                        beIdConcurrentTasksNum.put(beId, 1);
                    }
                }
            }
            return beIdConcurrentTasksNum;
        } finally {
            readUnlock();
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // TODO(ml)
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // TODO(ml)
    }

    // if rate of error data is more then max_filter_ratio, pause job
    protected void updateProgress(RLTaskTxnCommitAttachment attachment) {
        updateNumOfData(attachment.getFilteredRows(), attachment.getLoadedRows() + attachment.getFilteredRows());
    }

    public boolean containsTask(UUID taskId) {
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
                throw new UnsupportedOperationException("Could not transform " + state + " to " + desireState);
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
                LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                                 .add("current_total_num", currentTotalNum)
                                 .add("current_error_num", currentErrorNum)
                                 .add("max_error_num", maxErrorNum)
                                 .add("msg", "current error num is more then max error num, begin to pause job")
                                 .build());
                // remove all of task in jobs and change job state to paused
                updateState(JobState.PAUSED, "current error num of job is more then max error num");
            }

            LOG.debug(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                              .add("current_total_num", currentTotalNum)
                              .add("current_error_num", currentErrorNum)
                              .add("max_error_num", maxErrorNum)
                              .add("msg", "reset current total num and current error num when current total num is more then base")
                              .build());
            // reset currentTotalNum and currentErrorNum
            currentErrorNum = 0;
            currentTotalNum = 0;
        } else if (currentErrorNum > maxErrorNum) {
            LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                             .add("current_total_num", currentTotalNum)
                             .add("current_error_num", currentErrorNum)
                             .add("max_error_num", maxErrorNum)
                             .add("msg", "current error num is more then max error num, begin to pause job")
                             .build());
            // remove all of task in jobs and change job state to paused
            updateState(JobState.PAUSED, "current error num is more then max error num");
            // reset currentTotalNum and currentErrorNum
            currentErrorNum = 0;
            currentTotalNum = 0;
            LOG.debug(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                              .add("current_total_num", currentTotalNum)
                              .add("current_error_num", currentErrorNum)
                              .add("max_error_num", maxErrorNum)
                              .add("msg", "reset current total num and current error num when current total num is more then max error num")
                              .build());
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
    public void beforeAborted(TransactionState txnState, String txnStatusChangeReason)
            throws AbortTransactionException {
        readLock();
        try {
            String taskId = txnState.getLabel();
            if (routineLoadTaskInfoList.parallelStream().anyMatch(entity -> DebugUtil.printId(entity.getId()).equals(taskId))) {
                LOG.debug(new LogBuilder(LogKey.ROUINTE_LOAD_TASK, txnState.getLabel())
                                  .add("txn_id", txnState.getTransactionId())
                                  .add("msg", "task will be aborted")
                                  .build());
            }
        } finally {
            readUnlock();
        }
    }

    @Override
    public void beforeCommitted(TransactionState txnState) throws TransactionException {
        readLock();
        try {
            // check if task has been aborted
            Optional<RoutineLoadTaskInfo> routineLoadTaskInfoOptional =
                    routineLoadTaskInfoList.parallelStream()
                            .filter(entity -> DebugUtil.printId(entity.getId()).equals(txnState.getLabel())).findFirst();
            if (!routineLoadTaskInfoOptional.isPresent()) {
                throw new TransactionException("txn " + txnState.getTransactionId() + " could not be committed"
                                                       + " while task " + txnState.getLabel() + "has been aborted ");
            }
        } finally {
            readUnlock();
        }
    }

    // the task is committed when the correct number of rows is more then 0
    @Override
    public void onCommitted(TransactionState txnState) throws TransactionException {
        writeLock();
        try {
            // step0: find task in job
            Optional<RoutineLoadTaskInfo> routineLoadTaskInfoOptional =
                    routineLoadTaskInfoList.parallelStream()
                            .filter(entity -> DebugUtil.printId(entity.getId()).equals(txnState.getLabel())).findFirst();
            if (routineLoadTaskInfoOptional.isPresent()) {
                executeCommitTask(routineLoadTaskInfoOptional.get(), txnState);
            } else {
                LOG.debug(new LogBuilder(LogKey.ROUINTE_LOAD_TASK, txnState.getLabel())
                                  .add("txn_id", txnState.getTransactionId())
                                  .add("msg", "The task is not in task info list. "
                                          + "Maybe task has been renew or job state has changed. Transaction will not be committed.")
                                  .build());
                throw new TransactionException("txn " + txnState.getTransactionId() + " could not be committed"
                                                       + " while task " + txnState.getLabel() + "has been aborted ");
            }
        } catch (TransactionException e) {
            LOG.warn(e.getMessage(), e);
            throw e;
        } catch (Throwable e) {
            LOG.warn(e.getMessage(), e);
            updateState(JobState.PAUSED, "failed to update offset when transaction "
                    + txnState.getTransactionId() + " has been committed");
        } finally {
            writeUnlock();
        }
    }

    // the task is aborted when the correct number of rows is more then 0
    // be will abort txn when all of kafka data is wrong or total consume data is 0
    // txn will be aborted but progress will be update
    // progress will be update otherwise the progress will be hung
    @Override
    public void onAborted(TransactionState txnState, String txnStatusChangeReason) {
        if (txnStatusChangeReason != null) {
            LOG.debug(new LogBuilder(LogKey.ROUINTE_LOAD_TASK, txnState.getLabel())
                              .add("txn_id", txnState.getTransactionId())
                              .add("msg", "txn abort with reason " + txnStatusChangeReason)
                              .build());
        } else {
            LOG.debug(new LogBuilder(LogKey.ROUINTE_LOAD_TASK, txnState.getLabel())
                              .add("txn_id", txnState.getTransactionId())
                              .add("msg", "txn abort").build());
        }
        writeLock();
        try {
            // step0: find task in job
            Optional<RoutineLoadTaskInfo> routineLoadTaskInfoOptional =
                    routineLoadTaskInfoList.parallelStream()
                            .filter(entity -> DebugUtil.printId(entity.getId()).equals(txnState.getLabel())).findFirst();
            if (routineLoadTaskInfoOptional.isPresent()) {
                // todo(ml): use previous be id depend on change reason
                executeCommitTask(routineLoadTaskInfoOptional.get(), txnState);
            } else {
                LOG.debug(new LogBuilder(LogKey.ROUINTE_LOAD_TASK, txnState.getLabel())
                                  .add("txn_id", txnState.getTransactionId())
                                  .add("msg", "The task is not in task info list. "
                                          + "Maybe task has been renew or job state has changed. Transaction will not be aborted successfully.")
                                  .build());
            }
        } catch (Exception e) {
            updateState(JobState.PAUSED,
                        "failed to renew task when txn has been aborted with error " + e.getMessage());
            // TODO(ml): edit log
            LOG.warn("failed to renew a routine load task in job {} with error message {}", id, e.getMessage(), e);
        } finally {
            writeUnlock();
        }
    }

    // check task exists or not before call method
    private void executeCommitTask(RoutineLoadTaskInfo routineLoadTaskInfo, TransactionState txnState)
            throws LabelAlreadyUsedException, BeginTransactionException, AnalysisException {
        // step0: get progress from transaction state
        RLTaskTxnCommitAttachment rlTaskTxnCommitAttachment = (RLTaskTxnCommitAttachment) txnState.getTxnCommitAttachment();
        if (rlTaskTxnCommitAttachment == null) {
            LOG.debug(new LogBuilder(LogKey.ROUINTE_LOAD_TASK, routineLoadTaskInfo.getId())
                              .add("job_id", routineLoadTaskInfo.getJobId())
                              .add("txn_id", routineLoadTaskInfo.getTxnId())
                              .add("msg", "commit task will be ignore when attachment txn of task is null,"
                                      + " maybe task was committed by master when timeout")
                              .build());
        } else if (checkCommitInfo(rlTaskTxnCommitAttachment)) {
            // step2: update job progress
            updateProgress(rlTaskTxnCommitAttachment);
        }

        if (state == JobState.RUNNING) {
            // step2: create a new task for partitions
            RoutineLoadTaskInfo newRoutineLoadTaskInfo = unprotectRenewTask(routineLoadTaskInfo);
            Catalog.getCurrentCatalog().getRoutineLoadTaskScheduler().addTaskInQueue(newRoutineLoadTaskInfo);
        }
    }

    // check the correctness of commit info
    abstract boolean checkCommitInfo(RLTaskTxnCommitAttachment rlTaskTxnCommitAttachment);

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
            LOG.debug(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                              .add("current_job_state", getState())
                              .add("desire_job_state", jobState)
                              .add("msg", "job will be change to desire state")
                              .build());
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
            LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                             .add("current_job_state", getState())
                             .add("msg", "job state has been changed")
                             .build());
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
    }

    private void executeNeedSchedule() {
        // TODO(ml): edit log
        state = JobState.NEED_SCHEDULE;
        routineLoadTaskInfoList.clear();
    }

    private void executeStop() {
        // TODO(ml): edit log
        state = JobState.STOPPED;
        routineLoadTaskInfoList.clear();
    }

    private void executeCancel(String reason) {
        cancelReason = reason;
        state = JobState.CANCELLED;
        routineLoadTaskInfoList.clear();
    }

    public void update() {
        // check if db and table exist
        Database database = Catalog.getCurrentCatalog().getDb(dbId);
        if (database == null) {
            LOG.warn(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                             .add("db_id", dbId)
                             .add("msg", "The database has been deleted. Change job state to stopped").build());
            updateState(JobState.STOPPED);
        }
        database.readLock();
        try {
            Table table = database.getTable(tableId);
            // check table belong to database
            if (table == null) {
                LOG.warn(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id).add("db_id", dbId)
                                 .add("table_id", tableId)
                                 .add("msg", "The table has been deleted Change job state to stopped").build());
                updateState(JobState.STOPPED);
            }
        } finally {
            database.readUnlock();
        }

        // check if partition has been changed
        writeLock();
        try {
            if (unprotectNeedReschedule()) {
                LOG.debug(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                                  .add("msg", "Job need to be rescheduled")
                                  .build());
                unprotectUpdateProgress();
                executeNeedSchedule();
            }
        } finally {
            writeUnlock();
        }
    }

    protected void unprotectUpdateProgress() {
    }

    protected boolean unprotectNeedReschedule() {
        return false;
    }
}
