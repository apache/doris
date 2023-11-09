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

import org.apache.doris.analysis.AlterRoutineLoadStmt;
import org.apache.doris.analysis.CreateRoutineLoadStmt;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ImportColumnsStmt;
import org.apache.doris.analysis.LoadStmt;
import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.analysis.Separator;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.SqlScanner;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.InternalErrorCode;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.load.RoutineLoadDesc;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.load.routineload.kafka.KafkaConfiguration;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.persist.AlterRoutineLoadJobOperationLog;
import org.apache.doris.persist.RoutineLoadOperation;
import org.apache.doris.planner.StreamLoadPlanner;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.SqlModeHelper;
import org.apache.doris.task.LoadTaskInfo;
import org.apache.doris.thrift.TExecPlanFragmentParams;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TPipelineFragmentParams;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.AbstractTxnStateChangeCallback;
import org.apache.doris.transaction.DatabaseTransactionMgr;
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
import lombok.Getter;
import lombok.Setter;
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
public abstract class RoutineLoadJob extends AbstractTxnStateChangeCallback implements Writable, LoadTaskInfo {
    private static final Logger LOG = LogManager.getLogger(RoutineLoadJob.class);

    public static final long DEFAULT_MAX_ERROR_NUM = 0;
    public static final double DEFAULT_MAX_FILTER_RATIO = 1.0;

    public static final long DEFAULT_MAX_INTERVAL_SECOND = 10;
    public static final long DEFAULT_MAX_BATCH_ROWS = 200000;
    public static final long DEFAULT_MAX_BATCH_SIZE = 100 * 1024 * 1024; // 100MB
    public static final long DEFAULT_EXEC_MEM_LIMIT = 2 * 1024 * 1024 * 1024L;
    public static final boolean DEFAULT_STRICT_MODE = false; // default is false
    public static final int DEFAULT_SEND_BATCH_PARALLELISM = 1;
    public static final boolean DEFAULT_LOAD_TO_SINGLE_TABLET = false;

    protected static final String STAR_STRING = "*";

    @Getter
    @Setter
    private boolean isMultiTable = false;

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
    protected ImportColumnDescs columnDescs; // optional
    protected Expr precedingFilter; // optional
    protected Expr whereExpr; // optional
    protected Separator columnSeparator; // optional
    protected Separator lineDelimiter;
    protected int desireTaskConcurrentNum; // optional
    protected JobState state = JobState.NEED_SCHEDULE;
    @Getter
    protected LoadDataSourceType dataSourceType;
    // max number of error data in max batch rows * 10
    // maxErrorNum / (maxBatchRows * 10) = max error rate of routine load job
    // if current error rate is more than max error rate, the job will be paused
    protected long maxErrorNum = DEFAULT_MAX_ERROR_NUM; // optional
    protected double maxFilterRatio = DEFAULT_MAX_FILTER_RATIO;
    protected long execMemLimit = DEFAULT_EXEC_MEM_LIMIT;
    protected int sendBatchParallelism = DEFAULT_SEND_BATCH_PARALLELISM;
    protected boolean loadToSingleTablet = DEFAULT_LOAD_TO_SINGLE_TABLET;
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

    protected boolean isPartialUpdate = false;

    protected String sequenceCol;

    /**
     * RoutineLoad support json data.
     * Require Params:
     * 1) format = "json"
     * 2) jsonPath = "$.XXX.xxx"
     */
    private static final String PROPS_FORMAT = "format";
    private static final String PROPS_STRIP_OUTER_ARRAY = "strip_outer_array";
    private static final String PROPS_NUM_AS_STRING = "num_as_string";
    private static final String PROPS_JSONPATHS = "jsonpaths";
    private static final String PROPS_JSONROOT = "json_root";
    private static final String PROPS_FUZZY_PARSE = "fuzzy_parse";


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

    protected RoutineLoadStatistic jobStatistic = new RoutineLoadStatistic();

    // The tasks belong to this job
    protected List<RoutineLoadTaskInfo> routineLoadTaskInfoList = Lists.newArrayList();

    // stream load planer will be initialized during job schedule
    protected StreamLoadPlanner planner;

    // this is the origin stmt of CreateRoutineLoadStmt, we use it to persist the RoutineLoadJob,
    // because we can not serialize the Expressions contained in job.
    protected OriginStatement origStmt;
    // User who submit this job. Maybe null for the old version job(before v1.1)
    protected UserIdentity userIdentity;

    protected String comment = "";

    protected ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
    protected LoadTask.MergeType mergeType = LoadTask.MergeType.APPEND; // default is all data is load no delete
    protected Expr deleteCondition;
    // TODO(ml): error sample

    // save the latest 3 error log urls
    private Queue<String> errorLogUrls = EvictingQueue.create(3);

    protected boolean isTypeRead = false;

    protected byte enclose = 0;

    protected byte escape = 0;

    public void setTypeRead(boolean isTypeRead) {
        this.isTypeRead = isTypeRead;
    }

    public RoutineLoadJob(long id, LoadDataSourceType type) {
        this.id = id;
        this.dataSourceType = type;
    }

    public RoutineLoadJob(Long id, String name, String clusterName,
                          long dbId, long tableId, LoadDataSourceType dataSourceType,
                          UserIdentity userIdentity) {
        this(id, dataSourceType);
        this.name = name;
        this.clusterName = clusterName;
        this.dbId = dbId;
        this.tableId = tableId;
        this.authCode = 0;
        this.userIdentity = userIdentity;

        if (ConnectContext.get() != null) {
            SessionVariable var = ConnectContext.get().getSessionVariable();
            sessionVariables.put(SessionVariable.SQL_MODE, Long.toString(var.getSqlMode()));
        } else {
            sessionVariables.put(SessionVariable.SQL_MODE, String.valueOf(SqlModeHelper.MODE_DEFAULT));
        }
    }

    /**
     * MultiLoadJob will use this constructor
     */
    public RoutineLoadJob(Long id, String name, String clusterName,
                          long dbId, LoadDataSourceType dataSourceType,
                          UserIdentity userIdentity) {
        this(id, dataSourceType);
        this.name = name;
        this.clusterName = clusterName;
        this.dbId = dbId;
        this.authCode = 0;
        this.userIdentity = userIdentity;
        this.isMultiTable = true;

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
        if (stmt.getMaxFilterRatio() != -1) {
            this.maxFilterRatio = stmt.getMaxFilterRatio();
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
        if (stmt.getExecMemLimit() != -1) {
            this.execMemLimit = stmt.getExecMemLimit();
        }
        if (stmt.getSendBatchParallelism() > 0) {
            this.sendBatchParallelism = stmt.getSendBatchParallelism();
        }
        if (stmt.isLoadToSingleTablet()) {
            this.loadToSingleTablet = stmt.isLoadToSingleTablet();
        }
        jobProperties.put(LoadStmt.TIMEZONE, stmt.getTimezone());
        jobProperties.put(LoadStmt.STRICT_MODE, String.valueOf(stmt.isStrictMode()));
        jobProperties.put(LoadStmt.SEND_BATCH_PARALLELISM, String.valueOf(this.sendBatchParallelism));
        jobProperties.put(LoadStmt.LOAD_TO_SINGLE_TABLET, String.valueOf(this.loadToSingleTablet));
        jobProperties.put(CreateRoutineLoadStmt.PARTIAL_COLUMNS, stmt.isPartialUpdate() ? "true" : "false");
        if (stmt.isPartialUpdate()) {
            this.isPartialUpdate = true;
        }
        jobProperties.put(CreateRoutineLoadStmt.MAX_FILTER_RATIO_PROPERTY, String.valueOf(maxFilterRatio));

        if (Strings.isNullOrEmpty(stmt.getFormat()) || stmt.getFormat().equals("csv")) {
            jobProperties.put(PROPS_FORMAT, "csv");
        } else if (stmt.getFormat().equals("json")) {
            jobProperties.put(PROPS_FORMAT, "json");
        } else {
            throw new UserException("Invalid format type.");
        }

        if (!Strings.isNullOrEmpty(stmt.getJsonPaths())) {
            jobProperties.put(PROPS_JSONPATHS, stmt.getJsonPaths());
        } else {
            jobProperties.put(PROPS_JSONPATHS, "");
        }
        if (!Strings.isNullOrEmpty(stmt.getJsonRoot())) {
            jobProperties.put(PROPS_JSONROOT, stmt.getJsonRoot());
        } else {
            jobProperties.put(PROPS_JSONROOT, "");
        }
        if (stmt.isStripOuterArray()) {
            jobProperties.put(PROPS_STRIP_OUTER_ARRAY, "true");
        } else {
            jobProperties.put(PROPS_STRIP_OUTER_ARRAY, "false");
        }
        if (stmt.isNumAsString()) {
            jobProperties.put(PROPS_NUM_AS_STRING, "true");
        } else {
            jobProperties.put(PROPS_NUM_AS_STRING, "false");
        }
        if (stmt.isFuzzyParse()) {
            jobProperties.put(PROPS_FUZZY_PARSE, "true");
        } else {
            jobProperties.put(PROPS_FUZZY_PARSE, "false");
        }
    }

    private void setRoutineLoadDesc(RoutineLoadDesc routineLoadDesc) {
        if (routineLoadDesc != null) {
            columnDescs = new ImportColumnDescs();
            if (routineLoadDesc.getColumnsInfo() != null) {
                ImportColumnsStmt columnsStmt = routineLoadDesc.getColumnsInfo();
                if (columnsStmt.getColumns() != null) {
                    columnDescs.descs.addAll(columnsStmt.getColumns());
                }
            }
            if (routineLoadDesc.getPrecedingFilter() != null) {
                precedingFilter = routineLoadDesc.getPrecedingFilter().getExpr();
            }
            if (routineLoadDesc.getWherePredicate() != null) {
                whereExpr = routineLoadDesc.getWherePredicate().getExpr();
            }
            if (routineLoadDesc.getColumnSeparator() != null) {
                columnSeparator = routineLoadDesc.getColumnSeparator();
            }
            if (routineLoadDesc.getLineDelimiter() != null) {
                lineDelimiter = routineLoadDesc.getLineDelimiter();
            }
            if (routineLoadDesc.getPartitionNames() != null) {
                partitions = routineLoadDesc.getPartitionNames();
            }
            if (routineLoadDesc.getDeleteCondition() != null) {
                deleteCondition = routineLoadDesc.getDeleteCondition();
            }
            mergeType = routineLoadDesc.getMergeType();
            if (routineLoadDesc.hasSequenceCol()) {
                sequenceCol = routineLoadDesc.getSequenceColName();
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
        this.otherMsg = TimeUtils.getCurrentFormatTime() + ":" + Strings.nullToEmpty(otherMsg);
    }

    public String getDbFullName() throws MetaNotFoundException {
        return Env.getCurrentInternalCatalog().getDbOrMetaException(dbId).getFullName();
    }

    public long getTableId() {
        return tableId;
    }

    public String getTableName() throws MetaNotFoundException {
        Database database = Env.getCurrentInternalCatalog().getDbOrMetaException(dbId);
        if (isMultiTable) {
            return null;
        }
        return database.getTableOrMetaException(tableId).getName();
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

    public UserIdentity getUserIdentity() {
        return userIdentity;
    }

    @Override
    public LoadTask.MergeType getMergeType() {
        return mergeType;
    }

    @Override
    public Expr getDeleteCondition() {
        return deleteCondition;
    }

    @Override
    public TFileType getFileType() {
        return TFileType.FILE_STREAM;
    }

    @Override
    public TFileFormatType getFormatType() {
        TFileFormatType fileFormatType = TFileFormatType.FORMAT_CSV_PLAIN;
        if (getFormat().equals("json")) {
            fileFormatType = TFileFormatType.FORMAT_JSON;
        }
        return fileFormatType;
    }

    @Override
    public Expr getPrecedingFilter() {
        return precedingFilter;
    }

    @Override
    public Expr getWhereExpr() {
        return whereExpr;
    }

    public Separator getColumnSeparator() {
        return columnSeparator;
    }

    public Separator getLineDelimiter() {
        return lineDelimiter;
    }

    public byte getEnclose() {
        return enclose;
    }

    public byte getEscape() {
        return escape;
    }

    public boolean isStrictMode() {
        String value = jobProperties.get(LoadStmt.STRICT_MODE);
        if (value == null) {
            return DEFAULT_STRICT_MODE;
        }
        return Boolean.parseBoolean(value);
    }

    @Override
    public boolean getNegative() {
        return false;
    }

    @Override
    public long getTxnId() {
        return -1L;
    }

    @Override
    public int getTimeout() {
        return (int) getMaxBatchIntervalS();
    }

    @Override
    public long getMemLimit() {
        return execMemLimit;
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

    @Override
    public boolean isStripOuterArray() {
        return Boolean.parseBoolean(jobProperties.get(PROPS_STRIP_OUTER_ARRAY));
    }

    @Override
    public boolean isNumAsString() {
        return Boolean.parseBoolean(jobProperties.get(PROPS_NUM_AS_STRING));
    }

    @Override
    public boolean isFuzzyParse() {
        return Boolean.parseBoolean(jobProperties.get(PROPS_FUZZY_PARSE));
    }

    @Override
    public int getSendBatchParallelism() {
        return sendBatchParallelism;
    }

    @Override
    public boolean isLoadToSingleTablet() {
        return loadToSingleTablet;
    }

    @Override
    public boolean isReadJsonByLine() {
        return false;
    }

    @Override
    public String getPath() {
        return null;
    }

    //implement method for compatibility
    @Override
    public String getHeaderType() {
        return "";
    }

    @Override
    public List<String> getHiddenColumns() {
        return null;
    }

    @Override
    public boolean isPartialUpdate() {
        return isPartialUpdate;
    }

    @Override
    public ImportColumnDescs getColumnExprDescs() {
        if (columnDescs == null) {
            return new ImportColumnDescs();
        }
        return columnDescs;
    }

    public String getJsonPaths() {
        String value = jobProperties.get(PROPS_JSONPATHS);
        if (value == null) {
            return "";
        }
        return value;
    }

    public String getJsonRoot() {
        String value = jobProperties.get(PROPS_JSONROOT);
        if (value == null) {
            return "";
        }
        return value;
    }

    @Override
    public String getSequenceCol() {
        return sequenceCol;
    }

    public boolean hasSequenceCol() {
        return !Strings.isNullOrEmpty(sequenceCol);
    }

    public void setComment(String comment) {
        this.comment = comment;
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
                    Env.getCurrentEnv().getRoutineLoadTaskScheduler().addTaskInQueue(newTask);
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
                    beIdConcurrentTasksNum.put(beId, beIdConcurrentTasksNum.getOrDefault(beId, 0) + 1);
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

    // if rate of error data is more than max_filter_ratio, pause job
    protected void updateProgress(RLTaskTxnCommitAttachment attachment) throws UserException {
        updateNumOfData(attachment.getTotalRows(), attachment.getFilteredRows(), attachment.getUnselectedRows(),
                attachment.getReceivedBytes(), attachment.getTaskExecutionTimeMs(),
                false /* not replay */);
    }

    private void updateNumOfData(long numOfTotalRows, long numOfErrorRows, long unselectedRows, long receivedBytes,
                                 long taskExecutionTime, boolean isReplay) throws UserException {
        this.jobStatistic.totalRows += numOfTotalRows;
        this.jobStatistic.errorRows += numOfErrorRows;
        this.jobStatistic.unselectedRows += unselectedRows;
        this.jobStatistic.receivedBytes += receivedBytes;
        this.jobStatistic.totalTaskExcutionTimeMs += taskExecutionTime;

        if (MetricRepo.isInit && !isReplay) {
            MetricRepo.COUNTER_ROUTINE_LOAD_ROWS.increase(numOfTotalRows);
            MetricRepo.COUNTER_ROUTINE_LOAD_ERROR_ROWS.increase(numOfErrorRows);
            MetricRepo.COUNTER_ROUTINE_LOAD_RECEIVED_BYTES.increase(receivedBytes);
        }

        // check error rate
        this.jobStatistic.currentErrorRows += numOfErrorRows;
        this.jobStatistic.currentTotalRows += numOfTotalRows;
        this.jobStatistic.errorRowsAfterResumed = this.jobStatistic.currentErrorRows;
        if (this.jobStatistic.currentTotalRows > maxBatchRows * 10) {
            if (this.jobStatistic.currentErrorRows > maxErrorNum
                    || ((double) this.jobStatistic.currentErrorRows
                            / this.jobStatistic.currentTotalRows) > maxFilterRatio) {
                LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                        .add("current_total_rows", this.jobStatistic.currentTotalRows)
                        .add("current_error_rows", this.jobStatistic.currentErrorRows)
                        .add("max_error_num", maxErrorNum)
                        .add("max_filter_ratio", maxFilterRatio)
                        .add("msg", "current error rows is more than max error rows "
                            + "or the filter ratio is more than the max, begin to pause job")
                        .build());
                // if this is a replay thread, the update state should already be replayed by OP_CHANGE_ROUTINE_LOAD_JOB
                if (!isReplay) {
                    // remove all of task in jobs and change job state to paused
                    updateState(JobState.PAUSED, new ErrorReason(InternalErrorCode.TOO_MANY_FAILURE_ROWS_ERR,
                            "current error rows is more than max error num "
                            + "or the filter ratio is more than the max"), isReplay);
                }
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                        .add("current_total_rows", this.jobStatistic.currentTotalRows)
                        .add("current_error_rows", this.jobStatistic.currentErrorRows)
                        .add("max_error_num", maxErrorNum)
                        .add("max_filter_ratio", maxFilterRatio)
                        .add("msg", "reset current total rows and current error rows "
                                + "when current total rows is more than base or the filter ratio is more than the max")
                        .build());
            }
            // reset currentTotalNum and currentErrorNum
            this.jobStatistic.currentErrorRows = 0;
            this.jobStatistic.currentTotalRows = 0;
        } else if (this.jobStatistic.currentErrorRows > maxErrorNum
                || (this.jobStatistic.currentTotalRows > 0
                    && ((double) this.jobStatistic.currentErrorRows
                            / this.jobStatistic.currentTotalRows) > maxFilterRatio)) {
            LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                    .add("current_total_rows", this.jobStatistic.currentTotalRows)
                    .add("current_error_rows", this.jobStatistic.currentErrorRows)
                    .add("max_error_num", maxErrorNum)
                    .add("max_filter_ratio", maxFilterRatio)
                    .add("msg", "current error rows is more than max error rows "
                            + "or the filter ratio is more than the max, begin to pause job")
                    .build());
            if (!isReplay) {
                // remove all of task in jobs and change job state to paused
                updateState(JobState.PAUSED, new ErrorReason(InternalErrorCode.TOO_MANY_FAILURE_ROWS_ERR,
                        "current error rows is more than max error num "
                            + "or the filter ratio is more than the max"), isReplay);
            }
            // reset currentTotalNum and currentErrorNum
            this.jobStatistic.currentErrorRows = 0;
            this.jobStatistic.currentTotalRows = 0;
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
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(dbId);
        // for multi table load job, the table name is dynamic,we will set table when task scheduling.
        if (isMultiTable) {
            return;
        }
        planner = new StreamLoadPlanner(db,
                (OlapTable) db.getTableOrMetaException(this.tableId, Table.TableType.OLAP), this);
    }

    public TExecPlanFragmentParams plan(TUniqueId loadId, long txnId) throws UserException {
        Preconditions.checkNotNull(planner);
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(dbId);
        Table table = db.getTableOrMetaException(tableId, Table.TableType.OLAP);
        table.readLock();
        try {
            TExecPlanFragmentParams planParams = planner.plan(loadId);
            // add table indexes to transaction state
            TransactionState txnState = Env.getCurrentGlobalTransactionMgr().getTransactionState(db.getId(), txnId);
            if (txnState == null) {
                throw new MetaNotFoundException("txn does not exist: " + txnId);
            }
            txnState.addTableIndexes(planner.getDestTable());
            if (isPartialUpdate) {
                txnState.setSchemaForPartialUpdate((OlapTable) table);
            }

            return planParams;
        } finally {
            table.readUnlock();
        }
    }

    public TPipelineFragmentParams planForPipeline(TUniqueId loadId, long txnId) throws UserException {
        Preconditions.checkNotNull(planner);
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(dbId);
        Table table = db.getTableOrMetaException(tableId, Table.TableType.OLAP);
        table.readLock();
        try {
            TPipelineFragmentParams planParams = planner.planForPipeline(loadId);
            // add table indexes to transaction state
            TransactionState txnState = Env.getCurrentGlobalTransactionMgr().getTransactionState(db.getId(), txnId);
            if (txnState == null) {
                throw new MetaNotFoundException("txn does not exist: " + txnId);
            }
            txnState.addTableIndexes(planner.getDestTable());
            if (isPartialUpdate) {
                txnState.setSchemaForPartialUpdate((OlapTable) table);
            }

            return planParams;
        } finally {
            table.readUnlock();
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
                if (transactionStatus == TransactionStatus.COMMITTED) {
                    throw new TransactionException("txn " + txnState.getTransactionId()
                            + " could not be " + transactionStatus
                            + " while task " + txnState.getLabel() + " has been aborted.");
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
                executeTaskOnTxnStatusChanged(routineLoadTaskInfo, txnState, TransactionStatus.COMMITTED, null);
                ++this.jobStatistic.committedTaskNum;
                LOG.debug("routine load task committed. task id: {}, job id: {}", txnState.getLabel(), id);
            }
        } catch (Throwable e) {
            LOG.warn("after committed failed", e);
            String errmsg = "be " + taskBeId + " commit task failed " + txnState.getLabel()
                    + " with error " + e.getMessage()
                    + " while transaction " + txnState.getTransactionId() + " has been committed";
            updateState(JobState.PAUSED,
                    new ErrorReason(InternalErrorCode.INTERNAL_ERR, errmsg), false /* not replay */);
        } finally {
            writeUnlock();
            LOG.debug("unlock write lock of routine load job after committed: {}", id);
        }
    }

    @Override
    public void replayOnCommitted(TransactionState txnState) {
        Preconditions.checkNotNull(txnState.getTxnCommitAttachment(), txnState);
        replayUpdateProgress((RLTaskTxnCommitAttachment) txnState.getTxnCommitAttachment());
        this.jobStatistic.committedTaskNum++;
        LOG.debug("replay on committed: {}", txnState);
    }

    /*
     * the corresponding txn is visible, create a new task
     */
    @Override
    public void afterVisible(TransactionState txnState, boolean txnOperated) {
        if (!txnOperated) {
            String msg = String.format(
                    "should not happen, we find that txnOperated if false when handling afterVisble."
                            + " job id: %d, txn id: %d", id, txnState.getTransactionId());
            LOG.warn(msg);
            // print a log and return.
            // if this really happen, the job will be blocked, and this task can be seen by
            // "show routine load task" stmt, which is in COMMITTED state for a long time.
            // so we can find this error and step in.
            return;
        }

        writeLock();
        try {
            this.jobStatistic.runningTxnIds.remove(txnState.getTransactionId());
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
                LOG.info("Can not find task with transaction {} after visible, job: {}",
                        txnState.getTransactionId(), id);
                return;
            }
            RoutineLoadTaskInfo routineLoadTaskInfo = routineLoadTaskInfoOptional.get();
            if (routineLoadTaskInfo.getTxnStatus() != TransactionStatus.COMMITTED) {
                // TODO(cmy): Normally, this should not happen. But for safe reason, just pause the job
                String msg = String.format(
                        "should not happen, we find that task %s is not COMMITTED when handling afterVisble."
                                + " job id: %d, txn id: %d, txn status: %s",
                        DebugUtil.printId(routineLoadTaskInfo.getId()), id, txnState.getTransactionId(),
                        routineLoadTaskInfo.getTxnStatus().name());
                LOG.warn(msg);
                try {
                    updateState(JobState.PAUSED,
                            new ErrorReason(InternalErrorCode.IMPOSSIBLE_ERROR_ERR, msg), false /* not replay */);
                } catch (UserException e) {
                    // should not happen
                    LOG.warn("failed to pause the job {}. this should not happen", id, e);
                }
                return;
            }

            // create new task
            RoutineLoadTaskInfo newRoutineLoadTaskInfo = unprotectRenewTask(routineLoadTaskInfo);
            Env.getCurrentEnv().getRoutineLoadTaskScheduler().addTaskInQueue(newRoutineLoadTaskInfo);
        } finally {
            writeUnlock();
        }
    }

    // the task is aborted when the correct number of rows is more than 0
    // be will abort txn when all of kafka data is wrong or total consume data is 0
    // txn will be aborted but progress will be update
    // progress will be update otherwise the progress will be hung
    // *** Please do not call after individually. It must be combined use with before ***
    @Override
    public void afterAborted(TransactionState txnState, boolean txnOperated, String txnStatusChangeReasonString)
            throws UserException {
        long taskBeId = -1L;
        try {
            this.jobStatistic.runningTxnIds.remove(txnState.getTransactionId());
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
                ++this.jobStatistic.abortedTaskNum;
                TransactionState.TxnStatusChangeReason txnStatusChangeReason = null;
                if (txnStatusChangeReasonString != null) {
                    txnStatusChangeReason =
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
                executeTaskOnTxnStatusChanged(routineLoadTaskInfo, txnState,
                        TransactionStatus.ABORTED, txnStatusChangeReason);
            }
        } catch (Exception e) {
            String msg = "be " + taskBeId + " abort task " + txnState.getLabel()
                    + " failed with error " + e.getMessage();
            updateState(JobState.PAUSED, new ErrorReason(InternalErrorCode.TASKS_ABORT_ERR, msg),
                    false /* not replay */);
            LOG.warn(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                    .add("task_id", txnState.getLabel())
                    .add("error_msg", "change job state to paused"
                            + " when task has been aborted with error " + e.getMessage())
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
        this.jobStatistic.abortedTaskNum++;
        LOG.debug("replay on aborted: {}, has attachment: {}", txnState, txnState.getTxnCommitAttachment() == null);
    }

    // check task exists or not before call method
    private void executeTaskOnTxnStatusChanged(RoutineLoadTaskInfo routineLoadTaskInfo, TransactionState txnState,
                                               TransactionStatus txnStatus,
                                               TransactionState.TxnStatusChangeReason txnStatusChangeReason)
            throws UserException {
        // step0: get progress from transaction state
        RLTaskTxnCommitAttachment rlTaskTxnCommitAttachment
                = (RLTaskTxnCommitAttachment) txnState.getTxnCommitAttachment();
        if (rlTaskTxnCommitAttachment == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(new LogBuilder(LogKey.ROUTINE_LOAD_TASK, routineLoadTaskInfo.getId())
                        .add("job_id", routineLoadTaskInfo.getJobId())
                        .add("txn_id", routineLoadTaskInfo.getTxnId())
                        .add("msg", "commit task will be ignore when attachment txn of task is null,"
                                + " maybe task was aborted by master when timeout")
                        .build());
            }
        } else if (checkCommitInfo(rlTaskTxnCommitAttachment, txnState, txnStatusChangeReason)) {
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
                Env.getCurrentEnv().getRoutineLoadTaskScheduler().addTaskInQueue(newRoutineLoadTaskInfo);
            } else if (txnStatus == TransactionStatus.COMMITTED) {
                // this txn is just COMMITTED, create new task when the this txn is VISIBLE
                // or if publish version task has some error,
                // there will be lots of COMMITTED txns in GlobalTransactionMgr
            }
        }
    }

    protected static void checkMeta(OlapTable olapTable, RoutineLoadDesc routineLoadDesc) throws UserException {
        if (routineLoadDesc == null) {
            return;
        }

        PartitionNames partitionNames = routineLoadDesc.getPartitionNames();
        if (partitionNames == null) {
            return;
        }

        // check partitions
        olapTable.readLock();
        try {
            for (String partName : partitionNames.getPartitionNames()) {
                if (olapTable.getPartition(partName, partitionNames.isTemp()) == null) {
                    throw new DdlException("Partition " + partName + " does not exist");
                }
            }
        } finally {
            olapTable.readUnlock();
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
            Env.getCurrentGlobalTransactionMgr().getCallbackFactory().removeCallback(id);
        }

        if (!isReplay && jobState != JobState.RUNNING) {
            Env.getCurrentEnv().getEditLog().logOpRoutineLoadJob(new RoutineLoadOperation(id, jobState));
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
        Database database = Env.getCurrentInternalCatalog().getDbNullable(dbId);
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
        Table table = database.getTableNullable(tableId);
        if (table == null && !isMultiTable) {
            LOG.warn(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id).add("db_id", dbId)
                    .add("table_id", tableId)
                    .add("msg", "The table has been deleted change job state to cancelled").build());
            writeLock();
            try {
                if (!state.isFinalState()) {
                    unprotectUpdateState(JobState.CANCELLED, new ErrorReason(InternalErrorCode.TABLE_ERR,
                            "table does not exist"), false /* not replay */);
                }
                return;
            } finally {
                writeUnlock();
            }
        }

        preCheckNeedSchedule();

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

    // Call this before calling unprotectUpdateProgress().
    // Because unprotectUpdateProgress() is protected by writelock.
    // So if there are time-consuming operations, they should be done in this method.
    // (Such as getAllKafkaPartitions() in KafkaRoutineLoad)
    protected void preCheckNeedSchedule() throws UserException {

    }

    protected void unprotectUpdateProgress() throws UserException {
    }

    protected boolean unprotectNeedReschedule() throws UserException {
        return false;
    }

    public void setOrigStmt(OriginStatement origStmt) {
        this.origStmt = origStmt;
    }

    // check the correctness of commit info
    protected abstract boolean checkCommitInfo(RLTaskTxnCommitAttachment rlTaskTxnCommitAttachment,
                                               TransactionState txnState,
                                               TransactionState.TxnStatusChangeReason txnStatusChangeReason);

    protected abstract String getStatistic();

    protected abstract String getLag();

    public List<String> getShowInfo() {
        Optional<Database> database = Env.getCurrentInternalCatalog().getDb(dbId);
        Optional<Table> table = database.flatMap(db -> db.getTable(tableId));

        readLock();
        try {
            List<String> row = Lists.newArrayList();
            row.add(String.valueOf(id));
            row.add(name);
            row.add(TimeUtils.longToTimeString(createTimestamp));
            row.add(TimeUtils.longToTimeString(pauseTimestamp));
            row.add(TimeUtils.longToTimeString(endTimestamp));
            row.add(database.map(Database::getFullName).orElse(String.valueOf(dbId)));
            if (isMultiTable) {
                row.add("");
            } else {
                row.add(table.map(Table::getName).orElse(String.valueOf(tableId)));
            }
            row.add(Boolean.toString(isMultiTable));
            row.add(getState().name());
            row.add(dataSourceType.name());
            row.add(String.valueOf(getSizeOfRoutineLoadTaskInfoList()));
            row.add(jobPropertiesToJsonString());
            row.add(dataSourcePropertiesJsonToString());
            row.add(customPropertiesJsonToString());
            row.add(getStatistic());
            row.add(getProgress().toJsonString());
            row.add(getLag());
            switch (state) {
                case PAUSED:
                    row.add(pauseReason == null ? "" : pauseReason.toString());
                    break;
                case CANCELLED:
                    row.add(cancelReason == null ? "" : cancelReason.toString());
                    break;
                default:
                    row.add("");
            }
            row.add(Joiner.on(", ").join(errorLogUrls));
            row.add(otherMsg);
            row.add(userIdentity.getQualifiedUser());
            row.add(comment);
            return row;
        } finally {
            readUnlock();
        }
    }

    public List<List<String>> getTasksShowInfo() throws AnalysisException {
        List<List<String>> rows = Lists.newArrayList();
        if (null == routineLoadTaskInfoList || routineLoadTaskInfoList.isEmpty()) {
            return rows;
        }
        DatabaseTransactionMgr databaseTransactionMgr = Env.getCurrentEnv().getGlobalTransactionMgr()
                .getDatabaseTransactionMgr(dbId);

        routineLoadTaskInfoList.forEach(entity -> {
            long txnId = entity.getTxnId();
            if (RoutineLoadTaskInfo.INIT_TXN_ID == txnId) {
                rows.add(entity.getTaskShowInfo());
                return;
            }
            TransactionState transactionState = databaseTransactionMgr.getTransactionState(entity.getTxnId());
            if (null != transactionState && null != transactionState.getTransactionStatus()) {
                entity.setTxnStatus(transactionState.getTransactionStatus());
            }
            rows.add(entity.getTaskShowInfo());
        });
        return rows;
    }

    public String getShowCreateInfo() {
        Optional<Database> database = Env.getCurrentInternalCatalog().getDb(dbId);
        Optional<Table> table = database.flatMap(db -> db.getTable(tableId));
        StringBuilder sb = new StringBuilder();
        // 1.job_name
        sb.append("CREATE ROUTINE LOAD ").append(name);
        // 2.tbl_name
        if (!isMultiTable) {
            sb.append(" ON ").append(table.map(Table::getName).orElse(String.valueOf(tableId))).append("\n");
        }
        // 3.merge_type
        sb.append("WITH ").append(mergeType.name()).append("\n");
        // 4.load_properties
        // 4.1.column_separator
        if (columnSeparator != null) {
            sb.append("COLUMNS TERMINATED BY \"").append(columnSeparator.getOriSeparator()).append("\",\n");
        }
        // 4.2.columns_mapping
        if (columnDescs != null && !columnDescs.descs.isEmpty()) {
            sb.append("COLUMNS(").append(Joiner.on(",").join(columnDescs.descs)).append("),\n");
        }
        // 4.3.where_predicates
        if (whereExpr != null) {
            sb.append("WHERE ").append(whereExpr.toSql()).append(",\n");
        }
        // 4.4.partitions
        if (partitions != null) {
            sb.append("PARTITION(").append(Joiner.on(",").join(partitions.getPartitionNames())).append("),\n");
        }
        // 4.5.delete_on_predicates
        if (deleteCondition != null) {
            sb.append("DELETE ON ").append(deleteCondition.toSql()).append(",\n");
        }
        // 4.6.source_sequence
        if (sequenceCol != null) {
            sb.append("ORDER BY ").append(sequenceCol).append(",\n");
        }
        // 4.7.preceding_predicates
        if (precedingFilter != null) {
            sb.append("PRECEDING FILTER ").append(precedingFilter.toSql()).append(",\n");
        }
        // remove the last ,
        if (sb.charAt(sb.length() - 2) == ',') {
            sb.replace(sb.length() - 2, sb.length() - 1, "");
        }
        // 5.job_properties. See PROPERTIES_SET of CreateRoutineLoadStmt
        sb.append("PROPERTIES\n(\n");
        appendProperties(sb, CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY, desireTaskConcurrentNum, false);
        appendProperties(sb, CreateRoutineLoadStmt.MAX_ERROR_NUMBER_PROPERTY, maxErrorNum, false);
        appendProperties(sb, CreateRoutineLoadStmt.MAX_FILTER_RATIO_PROPERTY, maxFilterRatio, false);
        appendProperties(sb, CreateRoutineLoadStmt.MAX_BATCH_INTERVAL_SEC_PROPERTY, maxBatchIntervalS, false);
        appendProperties(sb, CreateRoutineLoadStmt.MAX_BATCH_ROWS_PROPERTY, maxBatchRows, false);
        appendProperties(sb, CreateRoutineLoadStmt.MAX_BATCH_SIZE_PROPERTY, maxBatchSizeBytes, false);
        appendProperties(sb, PROPS_FORMAT, getFormat(), false);
        if (isPartialUpdate) {
            appendProperties(sb, CreateRoutineLoadStmt.PARTIAL_COLUMNS, isPartialUpdate, false);
        }
        appendProperties(sb, PROPS_JSONPATHS, getJsonPaths(), false);
        appendProperties(sb, PROPS_STRIP_OUTER_ARRAY, isStripOuterArray(), false);
        appendProperties(sb, PROPS_NUM_AS_STRING, isNumAsString(), false);
        appendProperties(sb, PROPS_FUZZY_PARSE, isFuzzyParse(), false);
        appendProperties(sb, PROPS_JSONROOT, getJsonRoot(), false);
        appendProperties(sb, LoadStmt.STRICT_MODE, isStrictMode(), false);
        appendProperties(sb, LoadStmt.TIMEZONE, getTimezone(), false);
        appendProperties(sb, LoadStmt.EXEC_MEM_LIMIT, getMemLimit(), true);
        sb.append(")\n");
        // 6. data_source
        sb.append("FROM ").append(dataSourceType).append("\n");
        // 7. data_source_properties
        sb.append("(\n");
        getDataSourceProperties().forEach((k, v) -> appendProperties(sb, k, v, false));
        getCustomProperties().forEach((k, v) -> appendProperties(sb, k, v, false));
        if (progress instanceof KafkaProgress) {
            // append partitions and offsets.
            // the offsets are the next offset to be consumed.
            List<Pair<Integer, String>> pairs = ((KafkaProgress) progress).getPartitionOffsetPairs(false);
            appendProperties(sb, KafkaConfiguration.KAFKA_PARTITIONS.getName(),
                    Joiner.on(", ").join(pairs.stream().map(p -> p.first).toArray()), false);
            appendProperties(sb, KafkaConfiguration.KAFKA_OFFSETS.getName(),
                    Joiner.on(", ").join(pairs.stream().map(p -> p.second).toArray()), false);
        }
        // remove the last ","
        sb.replace(sb.length() - 2, sb.length() - 1, "");
        sb.append(");");
        return sb.toString();
    }

    private static void appendProperties(StringBuilder sb, String key, Object value, boolean end) {
        if (value == null || Strings.isNullOrEmpty(value.toString())) {
            return;
        }
        sb.append("\"").append(key).append("\"").append(" = ").append("\"").append(value).append("\"");
        if (!end) {
            sb.append(",\n");
        } else {
            sb.append("\n");
        }
    }

    public List<String> getShowStatistic() {
        Optional<Database> database = Env.getCurrentInternalCatalog().getDb(dbId);

        List<String> row = Lists.newArrayList();
        row.add(name);
        row.add(String.valueOf(id));
        row.add(database.map(Database::getFullName).orElse(String.valueOf(dbId)));
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
        jobProperties.put("partitions", partitions == null
                ? STAR_STRING : Joiner.on(",").join(partitions.getPartitionNames()));
        jobProperties.put("columnToColumnExpr", columnDescs == null
                ? STAR_STRING : Joiner.on(",").join(columnDescs.descs));
        jobProperties.put("precedingFilter", precedingFilter == null ? STAR_STRING : precedingFilter.toSql());
        jobProperties.put("whereExpr", whereExpr == null ? STAR_STRING : whereExpr.toSql());
        if (getFormat().equalsIgnoreCase("json")) {
            jobProperties.put(PROPS_FORMAT, "json");
        } else {
            jobProperties.put(LoadStmt.KEY_IN_PARAM_COLUMN_SEPARATOR,
                    columnSeparator == null ? "\t" : columnSeparator.toString());
            jobProperties.put(LoadStmt.KEY_IN_PARAM_LINE_DELIMITER,
                    lineDelimiter == null ? "\n" : lineDelimiter.toString());
        }
        jobProperties.put(CreateRoutineLoadStmt.PARTIAL_COLUMNS, String.valueOf(isPartialUpdate));
        jobProperties.put(CreateRoutineLoadStmt.MAX_ERROR_NUMBER_PROPERTY, String.valueOf(maxErrorNum));
        jobProperties.put(CreateRoutineLoadStmt.MAX_BATCH_INTERVAL_SEC_PROPERTY, String.valueOf(maxBatchIntervalS));
        jobProperties.put(CreateRoutineLoadStmt.MAX_BATCH_ROWS_PROPERTY, String.valueOf(maxBatchRows));
        jobProperties.put(CreateRoutineLoadStmt.MAX_BATCH_SIZE_PROPERTY, String.valueOf(maxBatchSizeBytes));
        jobProperties.put(CreateRoutineLoadStmt.CURRENT_CONCURRENT_NUMBER_PROPERTY,
                String.valueOf(currentTaskConcurrentNum));
        jobProperties.put(CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY,
                String.valueOf(desireTaskConcurrentNum));
        jobProperties.put(LoadStmt.EXEC_MEM_LIMIT, String.valueOf(execMemLimit));
        jobProperties.put(LoadStmt.KEY_IN_PARAM_MERGE_TYPE, mergeType.toString());
        jobProperties.put(LoadStmt.KEY_IN_PARAM_DELETE_CONDITION,
                deleteCondition == null ? STAR_STRING : deleteCondition.toSql());
        jobProperties.putAll(this.jobProperties);
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(jobProperties);
    }

    abstract String dataSourcePropertiesJsonToString();

    abstract String customPropertiesJsonToString();

    abstract Map<String, String> getDataSourceProperties();

    abstract Map<String, String> getCustomProperties();

    public boolean needRemove() {
        if (!isFinal()) {
            return false;
        }
        Preconditions.checkState(endTimestamp != -1, endTimestamp);
        return (System.currentTimeMillis() - endTimestamp) > Config.label_keep_max_second * 1000;
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

        this.jobStatistic.write(out);

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

        if (userIdentity == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            userIdentity.write(out);
        }
        Text.writeString(out, comment);
    }

    protected void readFields(DataInput in) throws IOException {
        if (!isTypeRead) {
            dataSourceType = LoadDataSourceType.valueOf(Text.readString(in));
            isTypeRead = true;
        }

        id = in.readLong();
        name = Text.readString(in);
        clusterName = Text.readString(in);
        dbId = in.readLong();
        tableId = in.readLong();
        if (tableId == 0) {
            isMultiTable = true;
        }
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

        if (Env.getCurrentEnvJournalVersion() < FeMetaVersion.VERSION_101) {
            this.jobStatistic.currentErrorRows = in.readLong();
            this.jobStatistic.currentTotalRows = in.readLong();
            this.jobStatistic.errorRows = in.readLong();
            this.jobStatistic.totalRows = in.readLong();
            this.jobStatistic.errorRowsAfterResumed = 0;
            this.jobStatistic.unselectedRows = in.readLong();
            this.jobStatistic.receivedBytes = in.readLong();
            this.jobStatistic.totalTaskExcutionTimeMs = in.readLong();
            this.jobStatistic.committedTaskNum = in.readLong();
            this.jobStatistic.abortedTaskNum = in.readLong();
        } else {
            this.jobStatistic = RoutineLoadStatistic.read(in);
        }
        origStmt = OriginStatement.read(in);

        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String key = Text.readString(in);
            String value = Text.readString(in);
            jobProperties.put(key, value);
            if (key.equals(CreateRoutineLoadStmt.PARTIAL_COLUMNS)) {
                isPartialUpdate = Boolean.parseBoolean(value);
            }
        }

        size = in.readInt();
        for (int i = 0; i < size; i++) {
            String key = Text.readString(in);
            String value = Text.readString(in);
            sessionVariables.put(key, value);
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
            throw new IOException("error happens when parsing create routine load stmt: " + origStmt.originStmt, e);
        }

        if (Env.getCurrentEnvJournalVersion() >= FeMetaVersion.VERSION_110) {
            if (in.readBoolean()) {
                userIdentity = UserIdentity.read(in);
                userIdentity.setIsAnalyzed();
            } else {
                userIdentity = UserIdentity.UNKNOWN;
            }
        }
        if (Env.getCurrentEnvJournalVersion() >= FeMetaVersion.VERSION_117) {
            comment = Text.readString(in);
        } else {
            comment = "";
        }
    }

    public abstract void modifyProperties(AlterRoutineLoadStmt stmt) throws UserException;

    public abstract void replayModifyProperties(AlterRoutineLoadJobOperationLog log);

    // for ALTER ROUTINE LOAD
    protected void modifyCommonJobProperties(Map<String, String> jobProperties) {
        if (jobProperties.containsKey(CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY)) {
            this.desireTaskConcurrentNum = Integer.parseInt(
                    jobProperties.remove(CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY));
        }

        if (jobProperties.containsKey(CreateRoutineLoadStmt.MAX_ERROR_NUMBER_PROPERTY)) {
            this.maxErrorNum = Long.parseLong(
                    jobProperties.remove(CreateRoutineLoadStmt.MAX_ERROR_NUMBER_PROPERTY));
        }

        if (jobProperties.containsKey(CreateRoutineLoadStmt.MAX_FILTER_RATIO_PROPERTY)) {
            this.maxFilterRatio = Double.parseDouble(
                    jobProperties.remove(CreateRoutineLoadStmt.MAX_FILTER_RATIO_PROPERTY));
        }

        if (jobProperties.containsKey(CreateRoutineLoadStmt.MAX_BATCH_INTERVAL_SEC_PROPERTY)) {
            this.maxBatchIntervalS = Long.parseLong(
                    jobProperties.remove(CreateRoutineLoadStmt.MAX_BATCH_INTERVAL_SEC_PROPERTY));
        }

        if (jobProperties.containsKey(CreateRoutineLoadStmt.MAX_BATCH_ROWS_PROPERTY)) {
            this.maxBatchRows = Long.parseLong(
                    jobProperties.remove(CreateRoutineLoadStmt.MAX_BATCH_ROWS_PROPERTY));
        }

        if (jobProperties.containsKey(CreateRoutineLoadStmt.MAX_BATCH_SIZE_PROPERTY)) {
            this.maxBatchSizeBytes = Long.parseLong(
                    jobProperties.remove(CreateRoutineLoadStmt.MAX_BATCH_SIZE_PROPERTY));
        }
    }
}
