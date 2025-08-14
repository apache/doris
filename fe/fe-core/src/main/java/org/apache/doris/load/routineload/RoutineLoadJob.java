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
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ImportColumnsStmt;
import org.apache.doris.analysis.LoadStmt;
import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.analysis.Separator;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.cloud.qe.ComputeGroupException;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.InternalErrorCode;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.property.fileformat.CsvFileFormatProperties;
import org.apache.doris.datasource.property.fileformat.FileFormatProperties;
import org.apache.doris.datasource.property.fileformat.JsonFileFormatProperties;
import org.apache.doris.load.RoutineLoadDesc;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.load.routineload.kafka.KafkaConfiguration;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.load.NereidsRoutineLoadTaskInfo;
import org.apache.doris.nereids.load.NereidsStreamLoadPlanner;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.AlterRoutineLoadCommand;
import org.apache.doris.nereids.trees.plans.commands.info.CreateRoutineLoadInfo;
import org.apache.doris.nereids.trees.plans.commands.load.CreateRoutineLoadCommand;
import org.apache.doris.persist.AlterRoutineLoadJobOperationLog;
import org.apache.doris.persist.RoutineLoadOperation;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.SqlModeHelper;
import org.apache.doris.resource.computegroup.ComputeGroup;
import org.apache.doris.task.LoadTaskInfo;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TPipelineFragmentParams;
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
import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
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
public abstract class RoutineLoadJob
        extends AbstractTxnStateChangeCallback
        implements Writable, LoadTaskInfo, GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(RoutineLoadJob.class);

    public static final long DEFAULT_MAX_ERROR_NUM = 0;
    public static final double DEFAULT_MAX_FILTER_RATIO = 1.0;

    public static final long DEFAULT_MAX_INTERVAL_SECOND = 60;
    public static final long DEFAULT_MAX_BATCH_ROWS = 20000000;
    public static final long DEFAULT_MAX_BATCH_SIZE = 1024 * 1024 * 1024; // 1GB
    public static final long DEFAULT_EXEC_MEM_LIMIT = 2 * 1024 * 1024 * 1024L;
    public static final boolean DEFAULT_STRICT_MODE = false; // default is false
    public static final int DEFAULT_SEND_BATCH_PARALLELISM = 1;
    public static final boolean DEFAULT_LOAD_TO_SINGLE_TABLET = false;

    protected static final String STAR_STRING = "*";

    public static final String WORKLOAD_GROUP = "workload_group";

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

    @SerializedName("id")
    protected long id;
    @SerializedName("n")
    protected String name;
    @SerializedName("dbid")
    protected long dbId;
    @SerializedName("tbid")
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
    @SerializedName("dtcn")
    protected int desireTaskConcurrentNum; // optional
    @SerializedName("st")
    protected JobState state = JobState.NEED_SCHEDULE;
    @Getter
    @SerializedName("dsrc")
    protected LoadDataSourceType dataSourceType;
    // max number of error data in max batch rows * 10
    // maxErrorNum / (maxBatchRows * 10) = max error rate of routine load job
    // if current error rate is more than max error rate, the job will be paused
    @SerializedName("men")
    protected long maxErrorNum = DEFAULT_MAX_ERROR_NUM; // optional
    protected double maxFilterRatio = DEFAULT_MAX_FILTER_RATIO;
    protected long execMemLimit = DEFAULT_EXEC_MEM_LIMIT;
    protected int sendBatchParallelism = DEFAULT_SEND_BATCH_PARALLELISM;
    protected boolean loadToSingleTablet = DEFAULT_LOAD_TO_SINGLE_TABLET;
    // include strict mode
    @SerializedName("jp")
    protected Map<String, String> jobProperties = Maps.newHashMap();

    // sessionVariable's name -> sessionVariable's value
    // we persist these sessionVariables due to the session is not available when replaying the job.
    @SerializedName("sv")
    protected Map<String, String> sessionVariables = Maps.newHashMap();

    /*
     * The following 3 variables control the max execute time of a single task.
     * The default max batch interval time is 10 secs.
     * If a task can consume data from source at rate of 10MB/s, and 500B a row,
     * then we can process 100MB for 10 secs, which is 200000 rows
     */
    @SerializedName("mbis")
    protected long maxBatchIntervalS = DEFAULT_MAX_INTERVAL_SECOND;
    @SerializedName("mbr")
    protected long maxBatchRows = DEFAULT_MAX_BATCH_ROWS;
    @SerializedName("mbsb")
    protected long maxBatchSizeBytes = DEFAULT_MAX_BATCH_SIZE;

    protected boolean isPartialUpdate = false;

    protected String sequenceCol;

    protected boolean memtableOnSinkNode = false;

    protected int currentTaskConcurrentNum;
    @SerializedName("pg")
    protected RoutineLoadProgress progress;

    protected long latestResumeTimestamp; // the latest resume time
    protected long autoResumeCount;
    // some other msg which need to show to user;
    protected String otherMsg = "";
    protected ErrorReason pauseReason;
    protected ErrorReason cancelReason;

    @SerializedName("cts")
    protected long createTimestamp = System.currentTimeMillis();
    @SerializedName("pts")
    protected long pauseTimestamp = -1;
    @SerializedName("ets")
    protected long endTimestamp = -1;

    @SerializedName("js")
    protected RoutineLoadStatistic jobStatistic = new RoutineLoadStatistic();

    // The tasks belong to this job
    protected List<RoutineLoadTaskInfo> routineLoadTaskInfoList = Lists.newArrayList();

    // this is the origin stmt of CreateRoutineLoadStmt, we use it to persist the RoutineLoadJob,
    // because we can not serialize the Expressions contained in job.
    @SerializedName("ostmt")
    protected OriginStatement origStmt;
    // User who submit this job. Maybe null for the old version job(before v1.1)
    @SerializedName("ui")
    protected UserIdentity userIdentity;

    @SerializedName("cm")
    protected String comment = "";

    protected ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
    protected LoadTask.MergeType mergeType = LoadTask.MergeType.APPEND; // default is all data is load no delete
    protected Expr deleteCondition;
    // TODO(ml): error sample

    // save the latest 3 error log urls
    private Queue<String> errorLogUrls = EvictingQueue.create(3);

    @SerializedName("ccid")
    private String cloudClusterId;

    protected byte enclose = 0;

    protected byte escape = 0;

    // use for cloud cluster mode
    @SerializedName("ccn")
    protected String cloudCluster;

    public RoutineLoadJob(long id, LoadDataSourceType type) {
        this.id = id;
        this.dataSourceType = type;
        if (ConnectContext.get() != null) {
            this.memtableOnSinkNode = ConnectContext.get().getSessionVariable().enableMemtableOnSinkNode;
            if (Config.isCloudMode()) {
                try {
                    this.cloudCluster = ConnectContext.get().getCloudCluster();
                } catch (ComputeGroupException e) {
                    LOG.warn("failed to get cloud cluster", e);
                }
            }
        }
    }

    public RoutineLoadJob(Long id, String name,
                          long dbId, long tableId, LoadDataSourceType dataSourceType,
                          UserIdentity userIdentity) {
        this(id, dataSourceType);
        this.name = name;
        this.dbId = dbId;
        this.tableId = tableId;
        this.authCode = 0;
        this.userIdentity = userIdentity;

        if (ConnectContext.get() != null) {
            SessionVariable var = ConnectContext.get().getSessionVariable();
            sessionVariables.put(SessionVariable.SQL_MODE, Long.toString(var.getSqlMode()));
            this.memtableOnSinkNode = ConnectContext.get().getSessionVariable().enableMemtableOnSinkNode;
            if (Config.isCloudMode()) {
                try {
                    this.cloudCluster = ConnectContext.get().getCloudCluster();
                } catch (ComputeGroupException e) {
                    LOG.warn("failed to get cloud cluster", e);
                }
            }
        } else {
            sessionVariables.put(SessionVariable.SQL_MODE, String.valueOf(SqlModeHelper.MODE_DEFAULT));
        }
    }

    /**
     * MultiLoadJob will use this constructor
     */
    public RoutineLoadJob(Long id, String name,
                          long dbId, LoadDataSourceType dataSourceType,
                          UserIdentity userIdentity) {
        this(id, dataSourceType);
        this.name = name;
        this.dbId = dbId;
        this.authCode = 0;
        this.userIdentity = userIdentity;
        this.isMultiTable = true;

        if (ConnectContext.get() != null) {
            SessionVariable var = ConnectContext.get().getSessionVariable();
            sessionVariables.put(SessionVariable.SQL_MODE, Long.toString(var.getSqlMode()));
            this.memtableOnSinkNode = ConnectContext.get().getSessionVariable().enableMemtableOnSinkNode;
            try {
                this.cloudCluster = ConnectContext.get().getCloudCluster();
            } catch (ComputeGroupException e) {
                LOG.warn("failed to get cloud cluster", e);
            }
        } else {
            sessionVariables.put(SessionVariable.SQL_MODE, String.valueOf(SqlModeHelper.MODE_DEFAULT));
        }
    }

    protected void setOptional(CreateRoutineLoadInfo info) throws UserException {
        setRoutineLoadDesc(info.getRoutineLoadDesc());
        if (info.getDesiredConcurrentNum() != -1) {
            this.desireTaskConcurrentNum = info.getDesiredConcurrentNum();
        }
        if (info.getMaxErrorNum() != -1) {
            this.maxErrorNum = info.getMaxErrorNum();
        }
        if (info.getMaxFilterRatio() != -1) {
            this.maxFilterRatio = info.getMaxFilterRatio();
        }
        if (info.getMaxBatchIntervalS() != -1) {
            this.maxBatchIntervalS = info.getMaxBatchIntervalS();
        }
        if (info.getMaxBatchRows() != -1) {
            this.maxBatchRows = info.getMaxBatchRows();
        }
        if (info.getMaxBatchSizeBytes() != -1) {
            this.maxBatchSizeBytes = info.getMaxBatchSizeBytes();
        }
        if (info.getExecMemLimit() != -1) {
            this.execMemLimit = info.getExecMemLimit();
        }
        if (info.getSendBatchParallelism() > 0) {
            this.sendBatchParallelism = info.getSendBatchParallelism();
        }
        if (info.isLoadToSingleTablet()) {
            this.loadToSingleTablet = info.isLoadToSingleTablet();
        }
        jobProperties.put(info.TIMEZONE, info.getTimezone());
        jobProperties.put(info.STRICT_MODE, String.valueOf(info.isStrictMode()));
        jobProperties.put(info.SEND_BATCH_PARALLELISM, String.valueOf(this.sendBatchParallelism));
        jobProperties.put(info.LOAD_TO_SINGLE_TABLET, String.valueOf(this.loadToSingleTablet));
        jobProperties.put(info.PARTIAL_COLUMNS, info.isPartialUpdate() ? "true" : "false");
        if (info.isPartialUpdate()) {
            this.isPartialUpdate = true;
        }
        jobProperties.put(info.MAX_FILTER_RATIO_PROPERTY, String.valueOf(maxFilterRatio));

        FileFormatProperties fileFormatProperties = info.getFileFormatProperties();
        if (fileFormatProperties instanceof CsvFileFormatProperties) {
            CsvFileFormatProperties csvFileFormatProperties = (CsvFileFormatProperties) fileFormatProperties;
            jobProperties.put(FileFormatProperties.PROP_FORMAT, "csv");
            jobProperties.put(CsvFileFormatProperties.PROP_ENCLOSE,
                    new String(new byte[]{csvFileFormatProperties.getEnclose()}));
            jobProperties.put(CsvFileFormatProperties.PROP_ESCAPE,
                    new String(new byte[]{csvFileFormatProperties.getEscape()}));
            this.enclose = csvFileFormatProperties.getEnclose();
            this.escape = csvFileFormatProperties.getEscape();
        } else if (fileFormatProperties instanceof JsonFileFormatProperties) {
            JsonFileFormatProperties jsonFileFormatProperties = (JsonFileFormatProperties) fileFormatProperties;
            jobProperties.put(FileFormatProperties.PROP_FORMAT, "json");
            jobProperties.put(JsonFileFormatProperties.PROP_JSON_PATHS, jsonFileFormatProperties.getJsonPaths());
            jobProperties.put(JsonFileFormatProperties.PROP_JSON_ROOT, jsonFileFormatProperties.getJsonRoot());
            jobProperties.put(JsonFileFormatProperties.PROP_STRIP_OUTER_ARRAY,
                    String.valueOf(jsonFileFormatProperties.isStripOuterArray()));
            jobProperties.put(JsonFileFormatProperties.PROP_NUM_AS_STRING,
                    String.valueOf(jsonFileFormatProperties.isNumAsString()));
            jobProperties.put(JsonFileFormatProperties.PROP_FUZZY_PARSE,
                    String.valueOf(jsonFileFormatProperties.isFuzzyParse()));
        } else {
            throw new UserException("Invalid format type.");
        }

        if (!StringUtils.isEmpty(info.getWorkloadGroupName())) {
            jobProperties.put(WORKLOAD_GROUP, info.getWorkloadGroupName());
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

        FileFormatProperties fileFormatProperties = stmt.getFileFormatProperties();
        if (fileFormatProperties instanceof CsvFileFormatProperties) {
            CsvFileFormatProperties csvFileFormatProperties = (CsvFileFormatProperties) fileFormatProperties;
            jobProperties.put(FileFormatProperties.PROP_FORMAT, "csv");
            jobProperties.put(LoadStmt.KEY_ENCLOSE, new String(new byte[]{csvFileFormatProperties.getEnclose()}));
            jobProperties.put(LoadStmt.KEY_ESCAPE, new String(new byte[]{csvFileFormatProperties.getEscape()}));
            this.enclose = csvFileFormatProperties.getEnclose();
            this.escape = csvFileFormatProperties.getEscape();
        } else if (fileFormatProperties instanceof JsonFileFormatProperties) {
            JsonFileFormatProperties jsonFileFormatProperties = (JsonFileFormatProperties) fileFormatProperties;
            jobProperties.put(FileFormatProperties.PROP_FORMAT, "json");
            jobProperties.put(JsonFileFormatProperties.PROP_JSON_PATHS, jsonFileFormatProperties.getJsonPaths());
            jobProperties.put(JsonFileFormatProperties.PROP_JSON_ROOT, jsonFileFormatProperties.getJsonRoot());
            jobProperties.put(JsonFileFormatProperties.PROP_STRIP_OUTER_ARRAY,
                    String.valueOf(jsonFileFormatProperties.isStripOuterArray()));
            jobProperties.put(JsonFileFormatProperties.PROP_NUM_AS_STRING,
                    String.valueOf(jsonFileFormatProperties.isNumAsString()));
            jobProperties.put(JsonFileFormatProperties.PROP_FUZZY_PARSE,
                    String.valueOf(jsonFileFormatProperties.isFuzzyParse()));
        } else {
            throw new UserException("Invalid format type.");
        }

        if (!StringUtils.isEmpty(stmt.getWorkloadGroupName())) {
            jobProperties.put(WORKLOAD_GROUP, stmt.getWorkloadGroupName());
        }
    }

    protected void setRoutineLoadDesc(RoutineLoadDesc routineLoadDesc) {
        if (routineLoadDesc != null) {
            if (routineLoadDesc.getColumnsInfo() != null) {
                ImportColumnsStmt columnsStmt = routineLoadDesc.getColumnsInfo();
                if (columnsStmt.getColumns() != null) {
                    columnDescs = new ImportColumnDescs();
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

    public void writeUnlock() {
        lock.writeLock().unlock();
    }

    public String getName() {
        return name;
    }

    public long getDbId() {
        return dbId;
    }

    public String getCreateTimestampString() {
        return TimeUtils.longToTimeString(createTimestamp);
    }

    public String getPauseTimestampString() {
        return TimeUtils.longToTimeString(pauseTimestamp);
    }

    public String getEndTimestampString() {
        return TimeUtils.longToTimeString(endTimestamp);
    }

    public void setOtherMsg(String otherMsg) {
        writeLock();
        try {
            this.otherMsg = TimeUtils.getCurrentFormatTime() + ":" + Strings.nullToEmpty(otherMsg);
        } finally {
            writeUnlock();
        }
    }

    public ErrorReason getPauseReason() {
        return pauseReason;
    }

    public RoutineLoadStatistic getRoutineLoadStatistic() {
        return jobStatistic;
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

    public String getWorkloadGroup() {
        return jobProperties.get(WORKLOAD_GROUP);
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

    public RoutineLoadStatistic getJobStatistic() {
        return jobStatistic;
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
        int timeoutSec = (int) getMaxBatchIntervalS() * Config.routine_load_task_timeout_multiplier;
        int realTimeoutSec = timeoutSec < Config.routine_load_task_min_timeout_sec
                    ? Config.routine_load_task_min_timeout_sec : timeoutSec;
        return realTimeoutSec;
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
        String value = jobProperties.get(FileFormatProperties.PROP_FORMAT);
        if (value == null) {
            return "csv";
        }
        return value;
    }

    @Override
    public boolean isStripOuterArray() {
        return Boolean.parseBoolean(jobProperties.get(JsonFileFormatProperties.PROP_STRIP_OUTER_ARRAY));
    }

    @Override
    public boolean isNumAsString() {
        return Boolean.parseBoolean(jobProperties.get(JsonFileFormatProperties.PROP_NUM_AS_STRING));
    }

    @Override
    public boolean isFuzzyParse() {
        return Boolean.parseBoolean(jobProperties.get(JsonFileFormatProperties.PROP_FUZZY_PARSE));
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
    public boolean isFixedPartialUpdate() {
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
        String value = jobProperties.get(JsonFileFormatProperties.PROP_JSON_PATHS);
        if (value == null) {
            return "";
        }
        return value;
    }

    public String getJsonRoot() {
        String value = jobProperties.get(JsonFileFormatProperties.PROP_JSON_ROOT);
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

    @Override
    public boolean isMemtableOnSinkNode() {
        return memtableOnSinkNode;
    }

    public void setMemtableOnSinkNode(boolean memtableOnSinkNode) {
        this.memtableOnSinkNode = memtableOnSinkNode;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String getCloudCluster() {
        return cloudCluster;
    }

    public int getSizeOfRoutineLoadTaskInfoList() {
        readLock();
        try {
            return routineLoadTaskInfoList.size();
        } finally {
            readUnlock();
        }
    }

    public Queue<String> getErrorLogUrls() {
        return errorLogUrls;
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

    abstract void updateCloudProgress() throws UserException;

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

    public boolean isAbnormalPause() {
        return this.state == JobState.PAUSED && this.pauseReason != null
                    && this.pauseReason.getCode() != InternalErrorCode.MANUAL_PAUSE_ERR;
    }

    // All of private method could not be call without lock
    private void checkStateTransform(RoutineLoadJob.JobState desireState) throws UserException {
        switch (state) {
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
                attachment.getReceivedBytes(), attachment.getTaskExecutionTimeMs(), false /* not replay */);
    }

    protected void updateCloudProgress(RLTaskTxnCommitAttachment attachment) {
        // In the cloud mode, the reason for needing to overwrite jobStatistic is that
        // pulling the progress of meta service is equivalent to a replay operation of edit log,
        // but this method will be called whenever scheduled by RoutineLoadScheduler,
        // and accumulation will result in incorrect jobStatistic information.
        this.jobStatistic.totalRows = attachment.getTotalRows();
        this.jobStatistic.errorRows = attachment.getFilteredRows();
        this.jobStatistic.unselectedRows = attachment.getUnselectedRows();
        this.jobStatistic.receivedBytes = attachment.getReceivedBytes();
        this.jobStatistic.totalTaskExcutionTimeMs = System.currentTimeMillis() - createTimestamp;
    }

    private void updateNumOfData(long numOfTotalRows, long numOfErrorRows, long unselectedRows, long receivedBytes,
                                 long taskExecutionTime, boolean isReplay) throws UserException {
        this.jobStatistic.totalRows += numOfTotalRows;
        this.jobStatistic.errorRows += numOfErrorRows;
        this.jobStatistic.unselectedRows += unselectedRows;
        this.jobStatistic.receivedBytes += receivedBytes;
        this.jobStatistic.totalTaskExcutionTimeMs = System.currentTimeMillis() - createTimestamp;

        if (MetricRepo.isInit && !isReplay) {
            MetricRepo.COUNTER_ROUTINE_LOAD_ROWS.increase(numOfTotalRows);
            MetricRepo.COUNTER_ROUTINE_LOAD_ERROR_ROWS.increase(numOfErrorRows);
            MetricRepo.COUNTER_ROUTINE_LOAD_RECEIVED_BYTES.increase(receivedBytes);
            MetricRepo.COUNTER_ROUTINE_LOAD_TASK_EXECUTE_TIME.increase(taskExecutionTime);
            MetricRepo.COUNTER_ROUTINE_LOAD_TASK_EXECUTE_TIME.increase(1L);
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
                        .add("msg", "current error rows is more than max_error_number "
                            + "or the max_filter_ratio is more than value set, begin to pause job")
                        .build());
                // if this is a replay thread, the update state should already be replayed by OP_CHANGE_ROUTINE_LOAD_JOB
                if (!isReplay) {
                    // remove all of task in jobs and change job state to paused
                    updateState(JobState.PAUSED, new ErrorReason(InternalErrorCode.TOO_MANY_FAILURE_ROWS_ERR,
                            "current error rows is more than max_error_number "
                            + "or the max_filter_ratio is more than the value set"), isReplay);
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
            // reset currentTotalNum, currentErrorNum and otherMsg
            this.jobStatistic.currentErrorRows = 0;
            this.jobStatistic.currentTotalRows = 0;
            this.otherMsg = "";
            this.jobStatistic.currentAbortedTaskNum = 0;
        } else if (this.jobStatistic.currentErrorRows > maxErrorNum
                || (this.jobStatistic.currentTotalRows > 0
                    && ((double) this.jobStatistic.currentErrorRows
                            / this.jobStatistic.currentTotalRows) > maxFilterRatio)) {
            LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                    .add("current_total_rows", this.jobStatistic.currentTotalRows)
                    .add("current_error_rows", this.jobStatistic.currentErrorRows)
                    .add("max_error_num", maxErrorNum)
                    .add("max_filter_ratio", maxFilterRatio)
                    .add("msg", "current error rows is more than max_error_number "
                            + "or the max_filter_ratio is more than the max, begin to pause job")
                    .build());
            if (!isReplay) {
                // remove all of task in jobs and change job state to paused
                updateState(JobState.PAUSED, new ErrorReason(InternalErrorCode.TOO_MANY_FAILURE_ROWS_ERR,
                        "current error rows is more than max_error_number "
                            + "or the max_filter_ratio is more than the value set"), isReplay);
            }
            // reset currentTotalNum, currentErrorNum and otherMsg
            this.jobStatistic.currentErrorRows = 0;
            this.jobStatistic.currentTotalRows = 0;
            this.otherMsg = "";
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

    public Long totalProgress() {
        return 0L;
    }

    public Long totalLag() {
        return 0L;
    }

    abstract RoutineLoadTaskInfo unprotectRenewTask(RoutineLoadTaskInfo routineLoadTaskInfo);

    // call before first scheduling
    // derived class can override this.
    public abstract void prepare() throws UserException;

    // make this public here just for UT.
    public void setComputeGroup() {
        ComputeGroup computeGroup = null;
        try {
            if (ConnectContext.get() == null) {
                ConnectContext ctx = new ConnectContext();
                ctx.setThreadLocalInfo();
            }
            String currentUser = ConnectContext.get().getQualifiedUser();
            if (StringUtils.isEmpty(currentUser)) {
                currentUser = getUserIdentity().getQualifiedUser();
            }
            if (StringUtils.isEmpty(currentUser)) {
                LOG.warn("can not find user in routine load");
                computeGroup = Env.getCurrentEnv().getComputeGroupMgr().getAllBackendComputeGroup();
            } else {
                computeGroup = Env.getCurrentEnv().getAuth().getComputeGroup(currentUser);
            }
            if (ComputeGroup.INVALID_COMPUTE_GROUP.equals(computeGroup)) {
                LOG.warn("get an invalid compute group in routine load");
                computeGroup = Env.getCurrentEnv().getComputeGroupMgr().getAllBackendComputeGroup();
            }
        } catch (Throwable t) {
            LOG.warn("error happens when set compute group for routine load", t);
            computeGroup = Env.getCurrentEnv().getComputeGroupMgr().getAllBackendComputeGroup();
        }
        ConnectContext.get().setComputeGroup(computeGroup);
    }

    public TPipelineFragmentParams plan(NereidsStreamLoadPlanner planner, TUniqueId loadId, long txnId)
            throws UserException {
        Preconditions.checkNotNull(planner);
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(dbId);
        Table table = db.getTableOrMetaException(tableId, Table.TableType.OLAP);
        boolean needCleanCtx = false;
        table.readLock();
        try {
            if (Config.isCloudMode()) {
                if (ConnectContext.get() == null) {
                    ConnectContext ctx = new ConnectContext();
                    ctx.setThreadLocalInfo();
                    ctx.setCloudCluster(cloudCluster);
                    needCleanCtx = true;
                } else {
                    ConnectContext.get().setCloudCluster(cloudCluster);
                }
                ConnectContext.get().setCurrentUserIdentity(this.getUserIdentity());
            } else {
                setComputeGroup();
            }
            if (ConnectContext.get().getEnv() == null) {
                ConnectContext.get().setEnv(Env.getCurrentEnv());
            }

            TPipelineFragmentParams planParams = planner.plan(loadId);
            // add table indexes to transaction state
            TransactionState txnState = Env.getCurrentGlobalTransactionMgr().getTransactionState(db.getId(), txnId);
            if (txnState == null) {
                throw new UserException("txn does not exist: " + txnId);
            }
            txnState.addTableIndexes(planner.getDestTable());
            if (isPartialUpdate) {
                txnState.setSchemaForPartialUpdate((OlapTable) table);
            }

            return planParams;
        } finally {
            if (needCleanCtx) {
                ConnectContext.remove();
            }
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
                if (LOG.isDebugEnabled()) {
                    LOG.debug("unlock write lock of routine load job before check: {}", id);
                }
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
                if (!routineLoadTaskInfoOptional.isPresent()) {
                    // not find task in routineLoadTaskInfoList. this may happen in following case:
                    //      the routine load job has been paused and before transaction committed.
                    //      The routineLoadTaskInfoList will be cleared when job being paused.
                    //      So the task can not be found here.
                    // This is a normal case, we just print a log here to observe.
                    LOG.info("Can not find task with transaction {} after committed, job: {}",
                            txnState.getTransactionId(), id);
                    return;
                }
                RoutineLoadTaskInfo routineLoadTaskInfo = routineLoadTaskInfoOptional.get();
                taskBeId = routineLoadTaskInfo.getBeId();
                executeTaskOnTxnStatusChanged(routineLoadTaskInfo, txnState, TransactionStatus.COMMITTED, null);
                ++this.jobStatistic.committedTaskNum;
                if (LOG.isDebugEnabled()) {
                    LOG.debug("routine load task committed. task id: {}, job id: {}", txnState.getLabel(), id);
                }
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
            if (LOG.isDebugEnabled()) {
                LOG.debug("unlock write lock of routine load job after committed: {}", id);
            }
        }
    }

    @Override
    public void replayOnCommitted(TransactionState txnState) {
        Preconditions.checkNotNull(txnState.getTxnCommitAttachment(), txnState);
        replayUpdateProgress((RLTaskTxnCommitAttachment) txnState.getTxnCommitAttachment());
        this.jobStatistic.committedTaskNum++;
        if (LOG.isDebugEnabled()) {
            LOG.debug("replay on committed: {}", txnState);
        }
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
            if (routineLoadTaskInfo.getTxnStatus() != TransactionStatus.COMMITTED
                        && routineLoadTaskInfo.getTxnStatus() != TransactionStatus.VISIBLE) {
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
            setOtherMsg(txnStatusChangeReasonString);
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
                ++this.jobStatistic.currentAbortedTaskNum;
                TransactionState.TxnStatusChangeReason txnStatusChangeReason = null;
                if (txnStatusChangeReasonString != null) {
                    txnStatusChangeReason =
                            TransactionState.TxnStatusChangeReason.fromString(txnStatusChangeReasonString);
                    String msg;
                    if (txnStatusChangeReason != null) {
                        switch (txnStatusChangeReason) {
                            case INVALID_JSON_PATH:
                                msg = "be " + taskBeId + " abort task,"
                                        + " task id: " + routineLoadTaskInfo.getId()
                                        + " job id: " + routineLoadTaskInfo.getJobId()
                                        + " with reason: " + txnStatusChangeReasonString
                                        + " please check the jsonpaths";
                                updateState(JobState.PAUSED,
                                        new ErrorReason(InternalErrorCode.CANNOT_RESUME_ERR, msg),
                                        false /* not replay */);
                                return;
                            case OFFSET_OUT_OF_RANGE:
                                msg = "be " + taskBeId + " abort task,"
                                        + " task id: " + routineLoadTaskInfo.getId()
                                        + " job id: " + routineLoadTaskInfo.getJobId()
                                        + " with reason: " + txnStatusChangeReasonString
                                        + " the offset used by job does not exist in kafka,"
                                        + " please check the offset,"
                                        + " using the Alter ROUTINE LOAD command to modify it,"
                                        + " and resume the job";
                                updateState(JobState.PAUSED,
                                        new ErrorReason(InternalErrorCode.CANNOT_RESUME_ERR, msg),
                                        false /* not replay */);
                                return;
                            case PAUSE:
                                msg = "be " + taskBeId + " abort task "
                                        + "with reason: " + txnStatusChangeReasonString;
                                updateState(JobState.PAUSED,
                                        new ErrorReason(InternalErrorCode.CANNOT_RESUME_ERR, msg),
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
            if (LOG.isDebugEnabled()) {
                LOG.debug("unlock write lock of routine load job after aborted: {}", id);
            }
        }
    }

    @Override
    public void replayOnAborted(TransactionState txnState) {
        // attachment may be null if this task is aborted by FE
        // it need check commit info before update progress
        // for follower FE node progress may exceed correct progress
        // the data will lost if FE leader change at this moment
        if (txnState.getTxnCommitAttachment() != null
                && checkCommitInfo((RLTaskTxnCommitAttachment) txnState.getTxnCommitAttachment(),
                        txnState,
                        TransactionState.TxnStatusChangeReason.fromString(txnState.getReason()))) {
            replayUpdateProgress((RLTaskTxnCommitAttachment) txnState.getTxnCommitAttachment());
        }
        this.jobStatistic.abortedTaskNum++;
        if (LOG.isDebugEnabled()) {
            LOG.debug("replay on aborted: {}, has attachment: {}", txnState, txnState.getTxnCommitAttachment() == null);
        }
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
            routineLoadTaskInfo.handleTaskByTxnCommitAttachment(rlTaskTxnCommitAttachment);
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

        if (olapTable.isTemporary()) {
            throw new DdlException("Cannot create routine load for temporary table "
                + olapTable.getDisplayName());
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
        LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                .add("current_job_state", getState())
                .add("desire_job_state", jobState)
                .add("msg", reason)
                .build());

        writeLock();
        try {
            unprotectUpdateState(jobState, reason, isReplay);
        } finally {
            writeUnlock();
        }

        LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                .add("current_job_state", getState())
                .add("msg", "job state has been changed")
                .add("is replay", String.valueOf(isReplay))
                .build());
    }

    protected void unprotectUpdateState(JobState jobState, ErrorReason reason, boolean isReplay) throws UserException {
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
            if (jobState == JobState.PAUSED) {
                Env.getCurrentEnv().getEditLog().logOpRoutineLoadJob(new RoutineLoadOperation(id, jobState, reason));
            } else {
                Env.getCurrentEnv().getEditLog().logOpRoutineLoadJob(new RoutineLoadOperation(id, jobState));
            }
        }
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
                    ErrorReason reason = new ErrorReason(InternalErrorCode.DB_ERR, "db " + dbId + "not exist");
                    LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                            .add("current_job_state", getState())
                            .add("desire_job_state", JobState.CANCELLED)
                            .add("msg", reason)
                            .build());
                    unprotectUpdateState(JobState.CANCELLED, reason, false /* not replay */);
                    LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                            .add("current_job_state", getState())
                            .add("msg", "job state has been changed")
                            .add("is replay", "false")
                            .build());
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
                    ErrorReason reason = new ErrorReason(InternalErrorCode.TABLE_ERR, "table does not exist");
                    LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                            .add("current_job_state", getState())
                            .add("desire_job_state", JobState.CANCELLED)
                            .add("msg", reason)
                            .build());
                    unprotectUpdateState(JobState.CANCELLED, reason, false /* not replay */);
                    LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                            .add("current_job_state", getState())
                            .add("msg", "job state has been changed")
                            .add("is replay", "false")
                            .build());
                }
                return;
            } finally {
                writeUnlock();
            }
        }

        boolean needAutoResume = needAutoResume();

        if (!refreshKafkaPartitions(needAutoResume)) {
            return;
        }

        writeLock();
        try {
            if (unprotectNeedReschedule() || needAutoResume) {
                LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                        .add("msg", "Job need to be rescheduled")
                        .build());
                this.otherMsg = pauseReason == null ? "" : pauseReason.getMsg();
                unprotectUpdateProgress();
                unprotectUpdateState(JobState.NEED_SCHEDULE, null, false);
            }
        } finally {
            writeUnlock();
        }
    }

    // Call this before calling unprotectUpdateProgress().
    // Because unprotectUpdateProgress() is protected by writelock.
    // So if there are time-consuming operations, they should be done in this method.
    // (Such as getAllKafkaPartitions() in KafkaRoutineLoad)
    protected boolean refreshKafkaPartitions(boolean needAutoResume) throws UserException {
        return false;
    }

    protected void unprotectUpdateProgress() throws UserException {
    }

    protected boolean unprotectNeedReschedule() throws UserException {
        return false;
    }

    protected boolean needAutoResume() {
        return false;
    }

    public void setOrigStmt(OriginStatement origStmt) {
        this.origStmt = origStmt;
    }

    public void setCloudCluster() {
        if (Strings.isNullOrEmpty(cloudCluster)) {
            this.cloudCluster = ((CloudSystemInfoService) Env.getCurrentSystemInfo())
                    .getClusterNameByClusterId(cloudClusterId);
        }
    }

    public void setCloudCluster(String cloudCluster) {
        this.cloudCluster = cloudCluster;
    }

    // check the correctness of commit info
    protected abstract boolean checkCommitInfo(RLTaskTxnCommitAttachment rlTaskTxnCommitAttachment,
                                               TransactionState txnState,
                                               TransactionState.TxnStatusChangeReason txnStatusChangeReason);

    public abstract String getStatistic();

    public abstract String getLag();

    public String getStateReason() {
        switch (state) {
            case PAUSED:
                return pauseReason == null ? "" : pauseReason.toString();
            case CANCELLED:
                return cancelReason == null ? "" : cancelReason.toString();
            default:
                return "";
        }
    }

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
            row.add(getStateReason());
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
        readLock();
        try {
            if (null == routineLoadTaskInfoList || routineLoadTaskInfoList.isEmpty()) {
                return rows;
            }
            routineLoadTaskInfoList.forEach(entity -> {
                long txnId = entity.getTxnId();
                if (RoutineLoadTaskInfo.INIT_TXN_ID == txnId) {
                    rows.add(entity.getTaskShowInfo());
                    return;
                }
                TransactionState transactionState = Env.getCurrentGlobalTransactionMgr()
                        .getTransactionState(dbId, entity.getTxnId());
                if (null != transactionState && null != transactionState.getTransactionStatus()) {
                    entity.setTxnStatus(transactionState.getTransactionStatus());
                }
                rows.add(entity.getTaskShowInfo());
            });
            return rows;
        } finally {
            readUnlock();
        }
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
            sb.append("WHERE ").append(whereExpr.toSqlWithoutTbl()).append(",\n");
        }
        // 4.4.partitions
        if (partitions != null) {
            sb.append("PARTITION(").append(Joiner.on(",").join(partitions.getPartitionNames())).append("),\n");
        }
        // 4.5.delete_on_predicates
        if (deleteCondition != null) {
            sb.append("DELETE ON ").append(deleteCondition.toSqlWithoutTbl()).append(",\n");
        }
        // 4.6.source_sequence
        if (sequenceCol != null) {
            sb.append("ORDER BY ").append(sequenceCol).append(",\n");
        }
        // 4.7.preceding_predicates
        if (precedingFilter != null) {
            sb.append("PRECEDING FILTER ").append(precedingFilter.toSqlWithoutTbl()).append(",\n");
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
        appendProperties(sb, FileFormatProperties.PROP_FORMAT, getFormat(), false);
        if (isPartialUpdate) {
            appendProperties(sb, CreateRoutineLoadStmt.PARTIAL_COLUMNS, isPartialUpdate, false);
        }
        appendProperties(sb, JsonFileFormatProperties.PROP_JSON_PATHS, getJsonPaths(), false);
        appendProperties(sb, JsonFileFormatProperties.PROP_STRIP_OUTER_ARRAY, isStripOuterArray(), false);
        appendProperties(sb, JsonFileFormatProperties.PROP_NUM_AS_STRING, isNumAsString(), false);
        appendProperties(sb, JsonFileFormatProperties.PROP_FUZZY_PARSE, isFuzzyParse(), false);
        appendProperties(sb, JsonFileFormatProperties.PROP_JSON_ROOT, getJsonRoot(), false);
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
        readLock();
        try {
            result.put("running_task",
                    String.valueOf(routineLoadTaskInfoList.stream().filter(entity -> entity.isRunning()).count()));
            result.put("waiting_task",
                    String.valueOf(routineLoadTaskInfoList.stream().filter(entity -> !entity.isRunning()).count()));
            Gson gson = new GsonBuilder().disableHtmlEscaping().create();
            return gson.toJson(result);
        } finally {
            readUnlock();
        }
    }

    // jobPropertiesJsonString contains both load properties and job properties defined in CreateRoutineLoadStmt
    public String jobPropertiesToJsonString() {
        Map<String, String> jobProperties = Maps.newHashMap();
        // load properties defined in CreateRoutineLoadStmt
        jobProperties.put("partitions", partitions == null
                ? STAR_STRING : Joiner.on(",").join(partitions.getPartitionNames()));
        jobProperties.put("columnToColumnExpr", columnDescs == null
                ? STAR_STRING : Joiner.on(",").join(columnDescs.descs));
        jobProperties.put("precedingFilter", precedingFilter == null ? STAR_STRING : precedingFilter.toSqlWithoutTbl());
        jobProperties.put("whereExpr", whereExpr == null ? STAR_STRING : whereExpr.toSqlWithoutTbl());
        if (getFormat().equalsIgnoreCase("json")) {
            jobProperties.put(FileFormatProperties.PROP_FORMAT, "json");
        } else {
            jobProperties.put(LoadStmt.KEY_IN_PARAM_COLUMN_SEPARATOR,
                    columnSeparator == null ? "\t" : columnSeparator.toString());
            jobProperties.put(LoadStmt.KEY_IN_PARAM_LINE_DELIMITER,
                    lineDelimiter == null ? "\n" : lineDelimiter.toString());
        }
        jobProperties.put(LoadStmt.KEY_IN_PARAM_DELETE_CONDITION,
                deleteCondition == null ? STAR_STRING : deleteCondition.toSqlWithoutTbl());
        jobProperties.put(LoadStmt.KEY_IN_PARAM_SEQUENCE_COL,
                sequenceCol == null ? STAR_STRING : sequenceCol);

        // job properties defined in CreateRoutineLoadStmt
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
        jobProperties.putAll(this.jobProperties);
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(jobProperties);
    }

    public abstract String dataSourcePropertiesJsonToString();

    public abstract String customPropertiesJsonToString();

    public abstract Map<String, String> getDataSourceProperties();

    public abstract Map<String, String> getCustomProperties();

    public boolean isExpired() {
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
        return GsonUtils.GSON.fromJson(Text.readString(in), RoutineLoadJob.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    @Override
    public void gsonPostProcess() throws IOException {
        if (tableId == 0) {
            isMultiTable = true;
        }
        jobProperties.forEach((k, v) -> {
            if (k.equals(CreateRoutineLoadStmt.PARTIAL_COLUMNS)) {
                isPartialUpdate = Boolean.parseBoolean(v);
            }
        });
        try {
            ConnectContext ctx = new ConnectContext();
            ctx.setDatabase(Env.getCurrentEnv().getInternalCatalog().getDb(dbId).get().getName());
            StatementContext statementContext = new StatementContext();
            statementContext.setConnectContext(ctx);
            ctx.setStatementContext(statementContext);
            ctx.setEnv(Env.getCurrentEnv());
            ctx.setCurrentUserIdentity(UserIdentity.ADMIN);
            ctx.getState().reset();
            try {
                ctx.setThreadLocalInfo();
                NereidsParser nereidsParser = new NereidsParser();
                CreateRoutineLoadCommand command = (CreateRoutineLoadCommand) nereidsParser.parseSingle(
                        origStmt.originStmt);
                CreateRoutineLoadInfo createRoutineLoadInfo = command.getCreateRoutineLoadInfo();
                createRoutineLoadInfo.validate(ctx);
                setRoutineLoadDesc(createRoutineLoadInfo.getRoutineLoadDesc());
            } finally {
                ctx.cleanup();
            }
        } catch (Exception e) {
            this.state = JobState.CANCELLED;
            LOG.warn("error happens when parsing create routine load stmt: " + origStmt.originStmt, e);
        }
        if (userIdentity != null) {
            userIdentity.setIsAnalyzed();
        }
    }

    public abstract void modifyProperties(AlterRoutineLoadCommand command) throws UserException;

    public abstract void replayModifyProperties(AlterRoutineLoadJobOperationLog log);

    public abstract NereidsRoutineLoadTaskInfo toNereidsRoutineLoadTaskInfo() throws UserException;

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
            this.jobProperties.put(CreateRoutineLoadStmt.MAX_FILTER_RATIO_PROPERTY, String.valueOf(maxFilterRatio));
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
