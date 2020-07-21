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

import org.apache.doris.analysis.ColumnSeparator;
import org.apache.doris.analysis.CreateRoutineLoadStmt;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ImportColumnDesc;
import org.apache.doris.analysis.ImportColumnsStmt;
import org.apache.doris.analysis.LoadStmt;
import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.SqlScanner;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.InternalErrorCode;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.load.RoutineLoadDesc;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.persist.RoutineLoadOperation;
import org.apache.doris.planner.StreamLoadPlanner;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.SqlModeHelper;
import org.apache.doris.task.StreamLoadTask;
import org.apache.doris.thrift.TExecPlanFragmentParams;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.AbstractTxnStateChangeCallback;
import org.apache.doris.transaction.TransactionException;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionStatus;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.EvictingQueue;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Routine load job is a function which stream load data from streaming medium to doris.
 * This function is suitable for streaming load job which loading data continuously
 * The properties include stream load properties and job properties.
 * The desireTaskConcurrentNum means that user expect the number of concurrent stream load
 * The routine load job support different streaming medium such as KAFKA
 */
public abstract class RoutineLoadJob extends AbstractTxnStateChangeCallback implements Writable {
    private static final Logger LOG = LogManager.getLogger(RoutineLoadJob.class);

    public static final long DEFAULT_MAX_ERROR_NUM = 0;

    public static final long DEFAULT_MAX_INTERVAL_SECOND = 10;
    public static final long DEFAULT_MAX_BATCH_ROWS = 200000;
    public static final long DEFAULT_MAX_BATCH_SIZE = 100 * 1024 * 1024; // 100MB
    public static final boolean DEFAULT_STRICT_MODE = false; // default is false

    protected static final String STAR_STRING = "*";
     /*
                      +-----------------+
     fe schedule job  |  NEED_SCHEDULE  |  user resume job
          +-----------+                 | <---------+
          |           |                 |           |
          v           +-----------------+           ^
          |                                         |
     +------------+   user(system)pause job +-------+----+
     |  RUNNING   |                         |  PAUSED    |
     |            +-----------------------> |            |
     +----+-------+                         +-------+----+
     |    |                                         |
     |    |           +---------------+             |
     |    |           | STOPPED       |             |
     |    +---------> |               | <-----------+
     |   user stop job+---------------+    user stop job
     |
     |
     |               +---------------+
     |               | CANCELLED     |
     +-------------> |               |
     system error    +---------------+
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
    protected String clusterName;
    protected long dbId;
    protected long tableId;
    // this code is used to verify be task request
    protected long authCode;
    //    protected RoutineLoadDesc routineLoadDesc; // optional
    protected PartitionNames partitions; // optional
    protected List<ImportColumnDesc> columnDescs; // optional
    protected Expr whereExpr; // optional
    protected ColumnSeparator columnSeparator; // optional
    protected int desireTaskConcurrentNum; // optional
    protected JobState state = JobState.NEED_SCHEDULE;
    protected LoadDataSourceType dataSourceType;
    // max number of error data in max batch rows * 10
    // maxErrorNum / (maxBatchRows * 10) = max error rate of routine load job
    // if current error rate is more then max error rate, the job will be paused
    protected long maxErrorNum = DEFAULT_MAX_ERROR_NUM; // optional
    // include strict mode
    protected Map<String, String> jobProperties = Maps.newHashMap();

    // sessionVariable's name -> sessionVariable's value
    // we persist these sessionVariables due to the session is not available when replaying the job.
    protected Map<String, String> sessionVariables = Maps.newHashMap();

    /*
     * The following 3 variables control the max execute time of a single task.
     * The default max batch interval time is 10 secs.
     * If a task can consume data from source at rate of 10MB/s, and 500B a row,
     * then we can process 100MB for 10 secs, which is 200000 rows
     */
    protected long maxBatchIntervalS = DEFAULT_MAX_INTERVAL_SECOND;
    protected long maxBatchRows = DEFAULT_MAX_BATCH_ROWS;
    protected long maxBatchSizeBytes = DEFAULT_MAX_BATCH_SIZE;

    /**
     * RoutineLoad support json data.
     * Require Params:
     *   1) format = "json"
     *   2) jsonPath = "$.XXX.xxx"
     */
    private static final String PROPS_FORMAT = "format";
    private static final String PROPS_STRIP_OUTER_ARRAY = "strip_outer_array";
    private static final String PROPS_JSONPATHS = "jsonpaths";

    protected int currentTaskConcurrentNum;
    protected RoutineLoadProgress progress;

    protected long firstResumeTimestamp; // the first resume time
    protected long autoResumeCount;
    protected boolean autoResumeLock = false; //it can't auto resume iff true
    // some other msg which need to show to user;
    protected String otherMsg = "";
    protected ErrorReason pauseReason;
    protected ErrorReason cancelReason;

    protected long createTimestamp = System.currentTimeMillis();
    protected long pauseTimestamp = -1;
    protected long endTimestamp = -1;

    /*
     * The following variables are for statistics
     * currentErrorRows/currentTotalRows: the row statistics of current sampling period
     * errorRows/totalRows/receivedBytes: cumulative measurement
     * totalTaskExcutorTimeMs: cumulative execution time of tasks
     */
    /*
     * Rows will be updated after txn state changed when txn state has been successfully changed.
     */
    protected long currentErrorRows = 0;
    protected long currentTotalRows = 0;
    protected long errorRows = 0;
    protected long totalRows = 0;
    protected long unselectedRows = 0;
    protected long receivedBytes = 0;
    protected long totalTaskExcutionTimeMs = 1; // init as 1 to avoid division by zero
    protected long committedTaskNum = 0;
    protected long abortedTaskNum = 0;

    // The tasks belong to this job
    protected List<RoutineLoadTaskInfo> routineLoadTaskInfoList = Lists.newArrayList();

    // stream load planer will be initialized during job schedule
    protected StreamLoadPlanner planner;

    // this is the origin stmt of CreateRoutineLoadStmt, we use it to persist the RoutineLoadJob,
    // because we can not serialize the Expressions contained in job.
    protected OriginStatement origStmt;

    protected ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
    // TODO(ml): error sample

    // save the latest 3 error log urls
    private Queue<String> errorLogUrls = EvictingQueue.create(3);

    protected boolean isTypeRead = false;

    public void setTypeRead(boolean isTypeRead) {
        this.isTypeRead = isTypeRead;
    }

    public RoutineLoadJob(long id, LoadDataSourceType type) {
        this.id = id;
        this.dataSourceType = type;
    }

    public RoutineLoadJob(Long id, String name, String clusterName,
            long dbId, long tableId, LoadDataSourceType dataSourceType) {
        this(id, dataSourceType);
        this.name = name;
        this.clusterName = clusterName;
        this.dbId = dbId;
        this.tableId = tableId;
        this.authCode = 0;

        if (ConnectContext.get() != null) {
            SessionVariable var = ConnectContext.get().getSessionVariable();
            sessionVariables.put(SessionVariable.SQL_MODE, Long.toString(var.getSqlMode()));
        } else {
            sessionVariables.put(SessionVariable.SQL_MODE, String.valueOf(SqlModeHelper.MODE_DEFAULT));
        }
    }

    protected void setOptional(CreateRoutineLoadStmt stmt) throws UserException {
        setRoutineLoadDesc(stmt.getRoutineLoadDesc());
        if (stmt.getDesiredConcurrentNum() != -1) {
            this.desireTaskConcurrentNum = stmt.getDesiredConcurrentNum();
        }
        if (stmt.getMaxErrorNum() != -1) {
            this.maxErrorNum = stmt.getMaxErrorNum();
        }
        if (stmt.getMaxBatchIntervalS() != -1) {
            this.maxBatchIntervalS = stmt.getMaxBatchIntervalS();
        }
        if (stmt.getMaxBatchRows() != -1) {
            this.maxBatchRows = stmt.getMaxBatchRows();
        }
        if (stmt.getMaxBatchSize() != -1) {
            this.maxBatchSizeBytes = stmt.getMaxBatchSize();
        }
        jobProperties.put(LoadStmt.STRICT_MODE, String.valueOf(stmt.isStrictMode()));
        if (Strings.isNullOrEmpty(stmt.getFormat()) || stmt.getFormat().equals("csv")) {
            jobProperties.put(PROPS_FORMAT, "csv");
            jobProperties.put(PROPS_STRIP_OUTER_ARRAY, "false");
            jobProperties.put(PROPS_JSONPATHS, "");
        } else if (stmt.getFormat().equals("json")) {
            jobProperties.put(PROPS_FORMAT, "json");
            if (!Strings.isNullOrEmpty(stmt.getJsonPaths())) {
                jobProperties.put(PROPS_JSONPATHS, stmt.getJsonPaths());
            } else {
                jobProperties.put(PROPS_JSONPATHS, "");
            }
            if (stmt.isStripOuterArray()) {
                jobProperties.put(PROPS_STRIP_OUTER_ARRAY, "true");
            } else {
                jobProperties.put(PROPS_STRIP_OUTER_ARRAY, "false");
            }
        } else {
            throw new UserException("Invalid format type.");
        }
    }

    private void setRoutineLoadDesc(RoutineLoadDesc routineLoadDesc) {
        if (routineLoadDesc != null) {
            if (routineLoadDesc.getColumnsInfo() != null) {
                ImportColumnsStmt columnsStmt = routineLoadDesc.getColumnsInfo();
                if (columnsStmt.getColumns() != null || columnsStmt.getColumns().size() != 0) {
                    columnDescs = Lists.newArrayList();
                    for (ImportColumnDesc columnDesc : columnsStmt.getColumns()) {
                        columnDescs.add(columnDesc);
                    }
                }
            }
            if (routineLoadDesc.getWherePredicate() != null) {
                whereExpr = routineLoadDesc.getWherePredicate().getExpr();
            }
            if (routineLoadDesc.getColumnSeparator() != null) {
                columnSeparator = routineLoadDesc.getColumnSeparator();
            }
            if (routineLoadDesc.getPartitionNames() != null) {
                partitions = routineLoadDesc.getPartitionNames();
            }
        }
    }

    @Override
    public long getId() {
        return id;
    }

    protected void readLock() {
        lock.readLock().lock();
    }

    protected void readUnlock() {
        lock.readLock().unlock();
    }

    protected void writeLock() {
        lock.writeLock().lock();
    }

    protected void writeUnlock() {
        lock.writeLock().unlock();
    }

    public String getName() {
        return name;
    }

    public long getDbId() {
        return dbId;
    }

    public void setOtherMsg(String otherMsg) {
        this.otherMsg = Strings.nullToEmpty(otherMsg);
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
            if (table == null) {
                throw new MetaNotFoundException("Failed to find table " + tableId + " in db " + dbId);
            }
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

    public long getEndTimestamp() {
        return endTimestamp;
    }

    public PartitionNames getPartitions() {
        return partitions;
    }

    public List<ImportColumnDesc> getColumnDescs() {
        return columnDescs;
    }

    public Expr getWhereExpr() {
        return whereExpr;
    }

    public ColumnSeparator getColumnSeparator() {
        return columnSeparator;
    }

    public boolean isStrictMode() {
        String value = jobProperties.get(LoadStmt.STRICT_MODE);
        if (value == null) {
            return DEFAULT_STRICT_MODE;
        }
        return Boolean.valueOf(value);
    }

    public String getTimezone() {
        String value = jobProperties.get(LoadStmt.TIMEZONE);
        if (value == null) {
            return TimeUtils.DEFAULT_TIME_ZONE;
        }
        return value;
    }

    public RoutineLoadProgress getProgress() {
        return progress;
    }

    public long getMaxBatchIntervalS() {
        return maxBatchIntervalS;
    }

    public long getMaxBatchRows() {
        return maxBatchRows;
    }

    public long getMaxBatchSizeBytes() {
        return maxBatchSizeBytes;
    }

    public String getFormat() {
        String value = jobProperties.get(PROPS_FORMAT);
        if (value == null) {
            return "csv";
        }
        return value;
    }

    public boolean isStripOuterArray() {
        return Boolean.valueOf(jobProperties.get(PROPS_STRIP_OUTER_ARRAY));
    }

    public String getJsonPaths() {
        String value = jobProperties.get(PROPS_JSONPATHS);
        if (value == null) {
            return "";
        }
        return value;
    }

    public int getSizeOfRoutineLoadTaskInfoList() {
        readLock();
        try {
            return routineLoadTaskInfoList.size();
        } finally {
            readUnlock();
        }
    }

    // RoutineLoadScheduler will run this method at fixed interval, and renew the timeout tasks
    public void processTimeoutTasks() {
        writeLock();
        try {
            List<RoutineLoadTaskInfo> runningTasks = new ArrayList<>(routineLoadTaskInfoList);
            for (RoutineLoadTaskInfo routineLoadTaskInfo : runningTasks) {
                if (routineLoadTaskInfo.isTimeout()) {
                    // here we simply discard the timeout task and create a new one.
                    // the corresponding txn will be aborted by txn manager.
                    // and after renew, the previous task is removed from routineLoadTaskInfoList,
                    // so task can no longer be committed successfully.
                    // the already committed task will not be handled here.
                    RoutineLoadTaskInfo newTask = unprotectRenewTask(routineLoadTaskInfo);
                    Catalog.getCurrentCatalog().getRoutineLoadTaskScheduler().addTaskInQueue(newTask);
                }
            }
        } finally {
            writeUnlock();
        }
    }

    abstract void divideRoutineLoadJob(int currentConcurrentTaskNum) throws UserException;

    public int calculateCurrentConcurrentTaskNum() throws MetaNotFoundException {
        return 0;
    }

    public Map<Long, Integer> getBeCurrentTasksNumMap() {
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

    public boolean containsTask(UUID taskId) {
        readLock();
        try {
            return routineLoadTaskInfoList.stream()
                    .anyMatch(entity -> entity.getId().equals(taskId));
        } finally {
            readUnlock();
        }
    }

    // All of private method could not be call without lock
    private void checkStateTransform(RoutineLoadJob.JobState desireState) throws UserException {
        switch (state) {
            case RUNNING:
                if (desireState == JobState.NEED_SCHEDULE) {
                    throw new DdlException("Could not transform " + state + " to " + desireState);
                }
                break;
            case PAUSED:
                if (desireState == JobState.PAUSED) {
                    throw new DdlException("Could not transform " + state + " to " + desireState);
                }
                break;
            case STOPPED:
            case CANCELLED:
                throw new DdlException("Could not transform " + state + " to " + desireState);
            default:
                break;
        }
    }

    // if rate of error data is more then max_filter_ratio, pause job
    protected void updateProgress(RLTaskTxnCommitAttachment attachment) throws UserException {
        updateNumOfData(attachment.getTotalRows(), attachment.getFilteredRows(), attachment.getUnselectedRows(),
                attachment.getReceivedBytes(), attachment.getTaskExecutionTimeMs(),
                false /* not replay */);
    }

    private void updateNumOfData(long numOfTotalRows, long numOfErrorRows, long unselectedRows, long receivedBytes,
            long taskExecutionTime, boolean isReplay) throws UserException {
        this.totalRows += numOfTotalRows;
        this.errorRows += numOfErrorRows;
        this.unselectedRows += unselectedRows;
        this.receivedBytes += receivedBytes;
        this.totalTaskExcutionTimeMs += taskExecutionTime;

        if (MetricRepo.isInit.get() && !isReplay) {
            MetricRepo.COUNTER_ROUTINE_LOAD_ROWS.increase(numOfTotalRows);
            MetricRepo.COUNTER_ROUTINE_LOAD_ERROR_ROWS.increase(numOfErrorRows);
            MetricRepo.COUNTER_ROUTINE_LOAD_RECEIVED_BYTES.increase(receivedBytes);
        }

        // check error rate
        currentErrorRows += numOfErrorRows;
        currentTotalRows += numOfTotalRows;
        if (currentTotalRows > maxBatchRows * 10) {
            if (currentErrorRows > maxErrorNum) {
                LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                                 .add("current_total_rows", currentTotalRows)
                                 .add("current_error_rows", currentErrorRows)
                                 .add("max_error_num", maxErrorNum)
                                 .add("msg", "current error rows is more then max error num, begin to pause job")
                                 .build());
                // if this is a replay thread, the update state should already be replayed by OP_CHANGE_ROUTINE_LOAD_JOB
                if (!isReplay) {
                    // remove all of task in jobs and change job state to paused
                    updateState(JobState.PAUSED,
                            new ErrorReason(InternalErrorCode.TOO_MANY_FAILURE_ROWS_ERR, "current error rows of job is more then max error num"),
                            isReplay);
                }
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                                  .add("current_total_rows", currentTotalRows)
                                  .add("current_error_rows", currentErrorRows)
                                  .add("max_error_num", maxErrorNum)
                                  .add("msg", "reset current total rows and current error rows "
                                          + "when current total rows is more then base")
                                  .build());
            }
            // reset currentTotalNum and currentErrorNum
            currentErrorRows = 0;
            currentTotalRows = 0;
        } else if (currentErrorRows > maxErrorNum) {
            LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                             .add("current_total_rows", currentTotalRows)
                             .add("current_error_rows", currentErrorRows)
                             .add("max_error_num", maxErrorNum)
                             .add("msg", "current error rows is more then max error rows, begin to pause job")
                             .build());
            if (!isReplay) {
                // remove all of task in jobs and change job state to paused
                updateState(JobState.PAUSED,
                        new ErrorReason(InternalErrorCode.TOO_MANY_FAILURE_ROWS_ERR, "current error rows is more then max error num"),
                        isReplay);
            }
            // reset currentTotalNum and currentErrorNum
            currentErrorRows = 0;
            currentTotalRows = 0;
        }
    }

    protected void replayUpdateProgress(RLTaskTxnCommitAttachment attachment) {
        try {
            updateNumOfData(attachment.getTotalRows(), attachment.getFilteredRows(), attachment.getUnselectedRows(),
                    attachment.getReceivedBytes(), attachment.getTaskExecutionTimeMs(), true /* is replay */);
        } catch (UserException e) {
            LOG.error("should not happen", e);
        }
    }

    abstract RoutineLoadTaskInfo unprotectRenewTask(RoutineLoadTaskInfo routineLoadTaskInfo);

    // call before first scheduling
    // derived class can override this.
    public void prepare() throws UserException {
        initPlanner();
    }

    private void initPlanner() throws UserException {
        StreamLoadTask streamLoadTask = StreamLoadTask.fromRoutineLoadJob(this);
        Database db = Catalog.getCurrentCatalog().getDb(dbId);
        if (db == null) {
            throw new MetaNotFoundException("db " + dbId + " does not exist");
        }
        planner = new StreamLoadPlanner(db, (OlapTable) db.getTable(this.tableId), streamLoadTask);
    }

    public TExecPlanFragmentParams plan(TUniqueId loadId, long txnId) throws UserException {
        Preconditions.checkNotNull(planner);
        Database db = Catalog.getCurrentCatalog().getDb(dbId);
        if (db == null) {
            throw new MetaNotFoundException("db " + dbId + " does not exist");
        }
        db.readLock();
        try {
            TExecPlanFragmentParams planParams = planner.plan(loadId);
            // add table indexes to transaction state
            TransactionState txnState = Catalog.getCurrentGlobalTransactionMgr().getTransactionState(db.getId(), txnId);
            if (txnState == null) {
                throw new MetaNotFoundException("txn does not exist: " + txnId);
            }
            txnState.addTableIndexes(planner.getDestTable());

            return planParams;
        } finally {
            db.readUnlock();
        }
    }

    // if task not exists, before aborted will reset the txn attachment to null, task will not be updated
    // if task pass the checker, task will be updated by attachment
    // *** Please do not call before individually. It must be combined use with after ***
    @Override
    public void beforeAborted(TransactionState txnState) throws TransactionException {
        if (LOG.isDebugEnabled()) {
            LOG.debug(new LogBuilder(LogKey.ROUTINE_LOAD_TASK, txnState.getLabel())
                              .add("txn_state", txnState)
                              .add("msg", "task before aborted")
                              .build());
        }
        executeBeforeCheck(txnState, TransactionStatus.ABORTED);
    }

    // if task not exists, before committed will throw exception, commit txn will failed
    // if task pass the checker, lock job will be locked
    // *** Please do not call before individually. It must be combined use with after ***
    @Override
    public void beforeCommitted(TransactionState txnState) throws TransactionException {
        if (LOG.isDebugEnabled()) {
            LOG.debug(new LogBuilder(LogKey.ROUTINE_LOAD_TASK, txnState.getLabel())
                              .add("txn_state", txnState)
                              .add("msg", "task before committed")
                              .build());
        }
        executeBeforeCheck(txnState, TransactionStatus.COMMITTED);
    }

    /*
     * try lock the write lock.
     * Make sure lock is released if any exception being thrown
     */
    private void executeBeforeCheck(TransactionState txnState, TransactionStatus transactionStatus)
            throws TransactionException {
        writeLock();

        // task already pass the checker
        boolean passCheck = false;
        try {
            // check if task has been aborted
            Optional<RoutineLoadTaskInfo> routineLoadTaskInfoOptional =
                    routineLoadTaskInfoList.stream()
                            .filter(entity -> entity.getTxnId() == txnState.getTransactionId()).findFirst();
            if (!routineLoadTaskInfoOptional.isPresent()) {
                switch (transactionStatus) {
                    case COMMITTED:
                        throw new TransactionException("txn " + txnState.getTransactionId()
                                                       + " could not be " + transactionStatus
                                                       + " while task " + txnState.getLabel() + " has been aborted.");
                    default:
                        break;
                }
            }
            passCheck = true;
        } finally {
            if (!passCheck) {
                writeUnlock();
                LOG.debug("unlock write lock of routine load job before check: {}", id);
            }
        }
    }

    // txn already committed before calling afterCommitted
    // the task will be committed
    // check currentErrorRows > maxErrorRows
    // paused job or renew task
    // *** Please do not call after individually. It must be combined use with before ***
    @Override
    public void afterCommitted(TransactionState txnState, boolean txnOperated) throws UserException {
        long taskBeId = -1L;
        try {
            if (txnOperated) {
                // find task in job
                Optional<RoutineLoadTaskInfo> routineLoadTaskInfoOptional = routineLoadTaskInfoList.stream().filter(
                        entity -> entity.getTxnId() == txnState.getTransactionId()).findFirst();
                RoutineLoadTaskInfo routineLoadTaskInfo = routineLoadTaskInfoOptional.get();
                taskBeId = routineLoadTaskInfo.getBeId();
                executeTaskOnTxnStatusChanged(routineLoadTaskInfo, txnState, TransactionStatus.COMMITTED);
                ++committedTaskNum;
            }
        } catch (Throwable e) {
            LOG.warn("after committed failed", e);
            String errmsg = "be " + taskBeId + " commit task failed " + txnState.getLabel()
                    + " with error " + e.getMessage()
                    + " while transaction " + txnState.getTransactionId() + " has been committed";
            updateState(JobState.PAUSED, new ErrorReason(InternalErrorCode.INTERNAL_ERR, errmsg), false /* not replay */);
        } finally {
            writeUnlock();
            LOG.debug("unlock write lock of routine load job after committed: {}", id);
        }
    }

    @Override
    public void replayOnCommitted(TransactionState txnState) {
        Preconditions.checkNotNull(txnState.getTxnCommitAttachment(), txnState);
        replayUpdateProgress((RLTaskTxnCommitAttachment) txnState.getTxnCommitAttachment());
        this.committedTaskNum++;
        LOG.debug("replay on committed: {}", txnState);
    }

    /*
     * the corresponding txn is visible, create a new task
     */
    @Override
    public void afterVisible(TransactionState txnState, boolean txnOperated) {
        if (!txnOperated) {
            String msg = String.format(
                    "should not happen, we find that txnOperated if false when handling afterVisble. job id: %d, txn id: %d",
                    id, txnState.getTransactionId());
            LOG.warn(msg);
            // print a log and return.
            // if this really happen, the job will be blocked, and this task can be seen by
            // "show routine load task" stmt, which is in COMMITTED state for a long time.
            // so we can find this error and step in.
            return;
        }
        
        writeLock();
        try {
            if (state != JobState.RUNNING) {
                // job is not running, nothing need to be done
                return;
            }

            Optional<RoutineLoadTaskInfo> routineLoadTaskInfoOptional = routineLoadTaskInfoList.stream().filter(
                    entity -> entity.getTxnId() == txnState.getTransactionId()).findFirst();
            if (!routineLoadTaskInfoOptional.isPresent()) {
                // not find task in routineLoadTaskInfoList. this may happen in following case:
                //      After the txn of the task is COMMITTED, but before it becomes VISIBLE,
                //      the routine load job has been paused and then start again.
                //      The routineLoadTaskInfoList will be cleared when job being paused.
                //      So the task can not be found here.
                // This is a normal case, we just print a log here to observe.
                LOG.info("Can not find task with transaction {} after visible, job: {}", txnState.getTransactionId(), id);
                return;
            }
            RoutineLoadTaskInfo routineLoadTaskInfo = routineLoadTaskInfoOptional.get();
            if (routineLoadTaskInfo.getTxnStatus() != TransactionStatus.COMMITTED) {
                // TODO(cmy): Normally, this should not happen. But for safe reason, just pause the job
                String msg = String.format(
                        "should not happen, we find that task %s is not COMMITTED when handling afterVisble. job id: %d, txn id: %d, txn status: %s",
                        DebugUtil.printId(routineLoadTaskInfo.getId()), id, txnState.getTransactionId(), routineLoadTaskInfo.getTxnStatus().name());
                LOG.warn(msg);
                try {
                    updateState(JobState.PAUSED, new ErrorReason(InternalErrorCode.IMPOSSIBLE_ERROR_ERR, msg), false /* not replay */);
                } catch (UserException e) {
                    // should not happen
                    LOG.warn("failed to pause the job {}. this should not happen", id, e);
                }
                return;
            }

            // create new task
            RoutineLoadTaskInfo newRoutineLoadTaskInfo = unprotectRenewTask(routineLoadTaskInfo);
            Catalog.getCurrentCatalog().getRoutineLoadTaskScheduler().addTaskInQueue(newRoutineLoadTaskInfo);
        } finally {
            writeUnlock();
        }
    }

    // the task is aborted when the correct number of rows is more then 0
    // be will abort txn when all of kafka data is wrong or total consume data is 0
    // txn will be aborted but progress will be update
    // progress will be update otherwise the progress will be hung
    // *** Please do not call after individually. It must be combined use with before ***
    @Override
    public void afterAborted(TransactionState txnState, boolean txnOperated, String txnStatusChangeReasonString)
            throws UserException {
        long taskBeId = -1L;
        try {
            if (txnOperated) {
                // step0: find task in job
                Optional<RoutineLoadTaskInfo> routineLoadTaskInfoOptional = routineLoadTaskInfoList.stream().filter(
                        entity -> entity.getTxnId() == txnState.getTransactionId()).findFirst();
                if (!routineLoadTaskInfoOptional.isPresent()) {
                    // task will not be update when task has been aborted by fe
                    return;
                }
                RoutineLoadTaskInfo routineLoadTaskInfo = routineLoadTaskInfoOptional.get();
                taskBeId = routineLoadTaskInfo.getBeId();
                // step1: job state will be changed depending on txnStatusChangeReasonString
                if (LOG.isDebugEnabled()) {
                    LOG.debug(new LogBuilder(LogKey.ROUTINE_LOAD_TASK, txnState.getLabel())
                                      .add("txn_id", txnState.getTransactionId())
                                      .add("msg", "txn abort with reason " + txnStatusChangeReasonString)
                                      .build());
                }
                ++abortedTaskNum;
                if (txnStatusChangeReasonString != null) {
                    TransactionState.TxnStatusChangeReason txnStatusChangeReason =
                            TransactionState.TxnStatusChangeReason.fromString(txnStatusChangeReasonString);
                    if (txnStatusChangeReason != null) {
                        switch (txnStatusChangeReason) {
                            case OFFSET_OUT_OF_RANGE:
                            case PAUSE:
                                String msg = "be " + taskBeId + " abort task "
                                    + "with reason: " + txnStatusChangeReasonString;
                                updateState(JobState.PAUSED,
                                        new ErrorReason(InternalErrorCode.TASKS_ABORT_ERR, msg),
                                        false /* not replay */);
                                return;
                            default:
                                break;
                        }
                    }
                    // TODO(ml): use previous be id depend on change reason
                }
                // step2: commit task , update progress, maybe create a new task
                executeTaskOnTxnStatusChanged(routineLoadTaskInfo, txnState, TransactionStatus.ABORTED);
            }
        } catch (Exception e) {
            String msg = "be " + taskBeId + " abort task " + txnState.getLabel() + " failed with error " + e.getMessage();
            updateState(JobState.PAUSED, new ErrorReason(InternalErrorCode.TASKS_ABORT_ERR, msg),
                        false /* not replay */);
            LOG.warn(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                             .add("task_id", txnState.getLabel())
                             .add("error_msg", "change job state to paused when task has been aborted with error " + e.getMessage())
                             .build(), e);
        } finally {
            writeUnlock();
            LOG.debug("unlock write lock of routine load job after aborted: {}", id);
        }
    }

    @Override
    public void replayOnAborted(TransactionState txnState) {
        // attachment may be null if this task is aborted by FE
        if (txnState.getTxnCommitAttachment() != null) {
            replayUpdateProgress((RLTaskTxnCommitAttachment) txnState.getTxnCommitAttachment());
        }
        this.abortedTaskNum++;
        LOG.debug("replay on aborted: {}, has attachment: {}", txnState, txnState.getTxnCommitAttachment() == null);
    }

    // check task exists or not before call method
    private void executeTaskOnTxnStatusChanged(RoutineLoadTaskInfo routineLoadTaskInfo, TransactionState txnState,
            TransactionStatus txnStatus) throws UserException {
        // step0: get progress from transaction state
        RLTaskTxnCommitAttachment rlTaskTxnCommitAttachment = (RLTaskTxnCommitAttachment) txnState.getTxnCommitAttachment();
        if (rlTaskTxnCommitAttachment == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(new LogBuilder(LogKey.ROUTINE_LOAD_TASK, routineLoadTaskInfo.getId())
                                  .add("job_id", routineLoadTaskInfo.getJobId())
                                  .add("txn_id", routineLoadTaskInfo.getTxnId())
                                  .add("msg", "commit task will be ignore when attachment txn of task is null,"
                                          + " maybe task was aborted by master when timeout")
                                  .build());
            }
        } else if (checkCommitInfo(rlTaskTxnCommitAttachment, txnState.getTransactionStatus())) {
            // step2: update job progress
            updateProgress(rlTaskTxnCommitAttachment);
        }

        if (rlTaskTxnCommitAttachment != null && !Strings.isNullOrEmpty(rlTaskTxnCommitAttachment.getErrorLogUrl())) {
            errorLogUrls.add(rlTaskTxnCommitAttachment.getErrorLogUrl());
        }

        routineLoadTaskInfo.setTxnStatus(txnStatus);

        if (state == JobState.RUNNING) {
            if (txnStatus == TransactionStatus.ABORTED) {
                RoutineLoadTaskInfo newRoutineLoadTaskInfo = unprotectRenewTask(routineLoadTaskInfo);
                Catalog.getCurrentCatalog().getRoutineLoadTaskScheduler().addTaskInQueue(newRoutineLoadTaskInfo);
            } else if (txnStatus == TransactionStatus.COMMITTED) {
                // this txn is just COMMITTED, create new task when the this txn is VISIBLE
                // or if publish version task has some error, there will be lots of COMMITTED txns in GlobalTransactionMgr
            }
        }
    }

    protected static void unprotectedCheckMeta(Database db, String tblName, RoutineLoadDesc routineLoadDesc)
            throws UserException {
        Table table = db.getTable(tblName);
        if (table == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, tblName);
        }

        if (table.getType() != Table.TableType.OLAP) {
            throw new AnalysisException("Only olap table support routine load");
        }
        
        if (routineLoadDesc == null) {
            return;
        }
        
        PartitionNames partitionNames = routineLoadDesc.getPartitionNames();
        if (partitionNames == null) {
            return;
        }
        
        // check partitions
        OlapTable olapTable = (OlapTable) table;
        for (String partName : partitionNames.getPartitionNames()) {
            if (olapTable.getPartition(partName, partitionNames.isTemp()) == null) {
                throw new DdlException("Partition " + partName + " does not exist");
            }
        }

        // columns will be checked when planing
    }

    public void updateState(JobState jobState, ErrorReason reason, boolean isReplay) throws UserException {
        writeLock();
        try {
            unprotectUpdateState(jobState, reason, isReplay);
        } finally {
            writeUnlock();
        }
    }

    protected void unprotectUpdateState(JobState jobState, ErrorReason reason, boolean isReplay) throws UserException {
        LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                .add("current_job_state", getState())
                .add("desire_job_state", jobState)
                .add("msg", reason)
                .build());
      
        checkStateTransform(jobState);
        switch (jobState) {
            case RUNNING:
                executeRunning();
                break;
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

        if (state.isFinalState()) {
            Catalog.getCurrentGlobalTransactionMgr().getCallbackFactory().removeCallback(id);
        }

        if (!isReplay && jobState != JobState.RUNNING) {
            Catalog.getCurrentCatalog().getEditLog().logOpRoutineLoadJob(new RoutineLoadOperation(id, jobState));
        }
        LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                         .add("current_job_state", getState())
                         .add("msg", "job state has been changed")
                         .add("is replay", String.valueOf(isReplay))
                         .build());
    }

    private void executeRunning() {
        state = JobState.RUNNING;
    }

    private void executePause(ErrorReason reason) {
        // remove all of task in jobs and change job state to paused
        pauseReason = reason;
        state = JobState.PAUSED;
        pauseTimestamp = System.currentTimeMillis();
        routineLoadTaskInfoList.clear();
    }

    private void executeNeedSchedule() {
        state = JobState.NEED_SCHEDULE;
        pauseTimestamp = -1;
        routineLoadTaskInfoList.clear();
    }

    private void executeStop() {
        state = JobState.STOPPED;
        routineLoadTaskInfoList.clear();
        endTimestamp = System.currentTimeMillis();
    }

    private void executeCancel(ErrorReason reason) {
        cancelReason = reason;
        state = JobState.CANCELLED;
        routineLoadTaskInfoList.clear();
        endTimestamp = System.currentTimeMillis();
    }

    public void update() throws UserException {
        // check if db and table exist
        Database database = Catalog.getCurrentCatalog().getDb(dbId);
        if (database == null) {
            LOG.warn(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                             .add("db_id", dbId)
                             .add("msg", "The database has been deleted. Change job state to cancelled").build());
            writeLock();
            try {
                if (!state.isFinalState()) {
                    unprotectUpdateState(JobState.CANCELLED,
                            new ErrorReason(InternalErrorCode.DB_ERR, "db " + dbId + "not exist"),
                            false /* not replay */);
                }
                return;
            } finally {
                writeUnlock();
            }
        }

        // check table belong to database
        database.readLock();
        Table table;
        try {
            table = database.getTable(tableId);
        } finally {
            database.readUnlock();
        }
        if (table == null) {
            LOG.warn(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id).add("db_id", dbId)
                             .add("table_id", tableId)
                             .add("msg", "The table has been deleted change job state to cancelled").build());
            writeLock();
            try {
                if (!state.isFinalState()) {
                    unprotectUpdateState(JobState.CANCELLED,
                            new ErrorReason(InternalErrorCode.TABLE_ERR, "table not exist"), false /* not replay */);
                }
                return;
            } finally {
                writeUnlock();
            }
        }

        // check if partition has been changed
        writeLock();
        try {
            if (unprotectNeedReschedule()) {
                LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
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

    protected boolean unprotectNeedReschedule() throws UserException {
        return false;
    }

    public void setOrigStmt(OriginStatement origStmt) {
        this.origStmt = origStmt;
    }

    // check the correctness of commit info
    protected abstract boolean checkCommitInfo(RLTaskTxnCommitAttachment rlTaskTxnCommitAttachment,
            TransactionStatus txnStatus);

    protected abstract String getStatistic();

    public List<String> getShowInfo() {
        Database db = Catalog.getCurrentCatalog().getDb(dbId);
        Table tbl = null;
        if (db != null) {
            db.readLock();
            try {
                tbl = db.getTable(tableId);
            } finally {
                db.readUnlock();
            }
        }

        readLock();
        try {
            List<String> row = Lists.newArrayList();
            row.add(String.valueOf(id));
            row.add(name);
            row.add(TimeUtils.longToTimeString(createTimestamp));
            row.add(TimeUtils.longToTimeString(pauseTimestamp));
            row.add(TimeUtils.longToTimeString(endTimestamp));
            row.add(db == null ? String.valueOf(dbId) : db.getFullName());
            row.add(tbl == null ? String.valueOf(tableId) : tbl.getName());
            row.add(getState().name());
            row.add(dataSourceType.name());
            row.add(String.valueOf(getSizeOfRoutineLoadTaskInfoList()));
            row.add(jobPropertiesToJsonString());
            row.add(dataSourcePropertiesJsonToString());
            row.add(customPropertiesJsonToString());
            row.add(getStatistic());
            row.add(getProgress().toJsonString());
            switch (state) {
                case PAUSED:
                    row.add(pauseReason == null ? "" : pauseReason.toString());
                    break;
                case CANCELLED:
                    row.add(cancelReason == null? "" : cancelReason.toString());
                    break;
                default:
                    row.add("");
            }
            row.add(Joiner.on(", ").join(errorLogUrls));
            row.add(otherMsg);
            return row;
        } finally {
            readUnlock();
        }
    }

    public List<List<String>> getTasksShowInfo() {
        List<List<String>> rows = Lists.newArrayList();
        routineLoadTaskInfoList.stream().forEach(entity -> rows.add(entity.getTaskShowInfo()));
        return rows;
    }

    public List<String> getShowStatistic() {
        Database db = Catalog.getCurrentCatalog().getDb(dbId);

        List<String> row = Lists.newArrayList();
        row.add(name);
        row.add(String.valueOf(id));
        row.add(db == null ? String.valueOf(dbId) : db.getFullName());
        row.add(getStatistic());
        row.add(getTaskStatistic());
        return row;
    }

    private String getTaskStatistic() {
        Map<String, String> result = Maps.newHashMap();
        result.put("running_task",
                   String.valueOf(routineLoadTaskInfoList.stream().filter(entity -> entity.isRunning()).count()));
        result.put("waiting_task",
                   String.valueOf(routineLoadTaskInfoList.stream().filter(entity -> !entity.isRunning()).count()));
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(result);
    }

    private String jobPropertiesToJsonString() {
        Map<String, String> jobProperties = Maps.newHashMap();
        jobProperties.put("partitions", partitions == null ? STAR_STRING : Joiner.on(",").join(partitions.getPartitionNames()));
        jobProperties.put("columnToColumnExpr", columnDescs == null ? STAR_STRING : Joiner.on(",").join(columnDescs));
        jobProperties.put("whereExpr", whereExpr == null ? STAR_STRING : whereExpr.toSql());
        if (getFormat().equalsIgnoreCase("json")) {
            jobProperties.put("dataFormat", "json");
        } else {
            jobProperties.put("columnSeparator", columnSeparator == null ? "\t" : columnSeparator.toString());
        }
        jobProperties.put("maxErrorNum", String.valueOf(maxErrorNum));
        jobProperties.put("maxBatchIntervalS", String.valueOf(maxBatchIntervalS));
        jobProperties.put("maxBatchRows", String.valueOf(maxBatchRows));
        jobProperties.put("maxBatchSizeBytes", String.valueOf(maxBatchSizeBytes));
        jobProperties.put("currentTaskConcurrentNum", String.valueOf(currentTaskConcurrentNum));
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(jobProperties);
    }

    abstract String dataSourcePropertiesJsonToString();

    abstract String customPropertiesJsonToString();

    public boolean needRemove() {
        if (!isFinal()) {
            return false;
        }
        Preconditions.checkState(endTimestamp != -1, endTimestamp);
        if ((System.currentTimeMillis() - endTimestamp) > Config.label_keep_max_second * 1000) {
            return true;
        }
        return false;
    }

    public boolean isFinal() {
        return state.isFinalState();
    }

    public static RoutineLoadJob read(DataInput in) throws IOException {
        RoutineLoadJob job = null;
        LoadDataSourceType type = LoadDataSourceType.valueOf(Text.readString(in));
        if (type == LoadDataSourceType.KAFKA) {
            job = new KafkaRoutineLoadJob();
        } else {
            throw new IOException("Unknown load data source type: " + type.name());
        }

        job.setTypeRead(true);
        job.readFields(in);
        return job;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // ATTN: must write type first
        Text.writeString(out, dataSourceType.name());

        out.writeLong(id);
        Text.writeString(out, name);
        Text.writeString(out, clusterName);
        out.writeLong(dbId);
        out.writeLong(tableId);
        out.writeInt(desireTaskConcurrentNum);
        Text.writeString(out, state.name());
        out.writeLong(maxErrorNum);
        out.writeLong(maxBatchIntervalS);
        out.writeLong(maxBatchRows);
        out.writeLong(maxBatchSizeBytes);
        progress.write(out);

        out.writeLong(createTimestamp);
        out.writeLong(pauseTimestamp);
        out.writeLong(endTimestamp);

        out.writeLong(currentErrorRows);
        out.writeLong(currentTotalRows);
        out.writeLong(errorRows);
        out.writeLong(totalRows);
        out.writeLong(unselectedRows);
        out.writeLong(receivedBytes);
        out.writeLong(totalTaskExcutionTimeMs);
        out.writeLong(committedTaskNum);
        out.writeLong(abortedTaskNum);

        origStmt.write(out);
        out.writeInt(jobProperties.size());
        for (Map.Entry<String, String> entry : jobProperties.entrySet()) {
            Text.writeString(out, entry.getKey());
            Text.writeString(out, entry.getValue());
        }

        out.writeInt(sessionVariables.size());
        for (Map.Entry<String, String> entry : sessionVariables.entrySet()) {
            Text.writeString(out, entry.getKey());
            Text.writeString(out, entry.getValue());
        }
    }

    public void readFields(DataInput in) throws IOException {
        if (!isTypeRead) {
            dataSourceType = LoadDataSourceType.valueOf(Text.readString(in));
            isTypeRead = true;
        }

        id = in.readLong();
        name = Text.readString(in);
        clusterName = Text.readString(in);
        dbId = in.readLong();
        tableId = in.readLong();
        desireTaskConcurrentNum = in.readInt();
        state = JobState.valueOf(Text.readString(in));
        maxErrorNum = in.readLong();
        maxBatchIntervalS = in.readLong();
        maxBatchRows = in.readLong();
        maxBatchSizeBytes = in.readLong();

        switch (dataSourceType) {
            case KAFKA: {
                progress = new KafkaProgress();
                progress.readFields(in);
                break;
            }
            default:
                throw new IOException("unknown data source type: " + dataSourceType);
        }

        createTimestamp = in.readLong();
        pauseTimestamp = in.readLong();
        endTimestamp = in.readLong();

        currentErrorRows = in.readLong();
        currentTotalRows = in.readLong();
        errorRows = in.readLong();
        totalRows = in.readLong();
        unselectedRows = in.readLong();
        receivedBytes = in.readLong();
        totalTaskExcutionTimeMs = in.readLong();
        committedTaskNum = in.readLong();
        abortedTaskNum = in.readLong();

        if (Catalog.getCurrentCatalogJournalVersion() < FeMetaVersion.VERSION_76) {
            String stmt = Text.readString(in);
            origStmt = new OriginStatement(stmt, 0);
        } else {
            origStmt = OriginStatement.read(in);
        }

        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_59) {
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                String key = Text.readString(in);
                String value = Text.readString(in);
                jobProperties.put(key, value);
            }
        } else {
            // The behaviors of old broker load could not be changed
            jobProperties.put(LoadStmt.STRICT_MODE, Boolean.toString(false));
        }

        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_66) {
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                String key = Text.readString(in);
                String value = Text.readString(in);
                sessionVariables.put(key, value);
            }
        } else {
            // old version of load does not have sqlmode, set it to default
            sessionVariables.put(SessionVariable.SQL_MODE, String.valueOf(SqlModeHelper.MODE_DEFAULT));
        }

        // parse the origin stmt to get routine load desc
        SqlParser parser = new SqlParser(new SqlScanner(new StringReader(origStmt.originStmt),
                Long.valueOf(sessionVariables.get(SessionVariable.SQL_MODE))));
        CreateRoutineLoadStmt stmt = null;
        try {
            stmt = (CreateRoutineLoadStmt) SqlParserUtils.getStmt(parser, origStmt.idx);
            stmt.checkLoadProperties();
            setRoutineLoadDesc(stmt.getRoutineLoadDesc());
        } catch (Exception e) {
            throw new IOException("error happens when parsing create routine load stmt: " + origStmt, e);
        }
    }
}
