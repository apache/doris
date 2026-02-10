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

package org.apache.doris.job.extensions.insert.streaming;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.InternalErrorCode;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.httpv2.rest.StreamingJobAction.CommitOffsetRequest;
import org.apache.doris.job.base.AbstractJob;
import org.apache.doris.job.base.JobExecutionConfiguration;
import org.apache.doris.job.base.TimerDefinition;
import org.apache.doris.job.cdc.DataSourceConfigKeys;
import org.apache.doris.job.common.DataSourceType;
import org.apache.doris.job.common.FailureReason;
import org.apache.doris.job.common.IntervalUnit;
import org.apache.doris.job.common.JobStatus;
import org.apache.doris.job.common.JobType;
import org.apache.doris.job.common.TaskStatus;
import org.apache.doris.job.common.TaskType;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.extensions.insert.InsertJob;
import org.apache.doris.job.extensions.insert.InsertTask;
import org.apache.doris.job.offset.Offset;
import org.apache.doris.job.offset.SourceOffsetProvider;
import org.apache.doris.job.offset.SourceOffsetProviderFactory;
import org.apache.doris.job.offset.jdbc.JdbcSourceOffsetProvider;
import org.apache.doris.job.offset.kafka.KafkaPartitionOffset;
import org.apache.doris.job.offset.kafka.KafkaSourceOffsetProvider;
import org.apache.doris.job.util.StreamingJobUtils;
import org.apache.doris.load.loadv2.LoadJob;
import org.apache.doris.load.loadv2.LoadStatistic;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundTVFRelation;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.AlterJobCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateTableCommand;
import org.apache.doris.nereids.trees.plans.commands.info.BaseViewInfo;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertUtils;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.thrift.TCell;
import org.apache.doris.thrift.TRow;
import org.apache.doris.transaction.TransactionException;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TxnStateChangeCallback;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Log4j2
public class StreamingInsertJob extends AbstractJob<StreamingJobSchedulerTask, Map<Object, Object>> implements
        TxnStateChangeCallback, GsonPostProcessable {
    private long dbId;
    // Streaming job statistics, all persisted in txn attachment
    private StreamingJobStatistic jobStatistic = new StreamingJobStatistic();
    // Non-txn persisted statistics, used for streaming multi task
    @Getter
    @Setter
    @SerializedName("ntjs")
    private StreamingJobStatistic nonTxnJobStatistic = new StreamingJobStatistic();
    @Getter
    @Setter
    @SerializedName("fr")
    protected volatile FailureReason failureReason;
    @Getter
    @Setter
    protected long latestAutoResumeTimestamp;
    @Getter
    @Setter
    protected long autoResumeCount;
    @Getter
    @SerializedName("props")
    private Map<String, String> properties;
    private StreamingJobProperties jobProperties;
    @Getter
    @SerializedName("tvf")
    private String tvfType;
    private Map<String, String> originTvfProps;
    @Getter
    AbstractStreamingTask runningStreamTask;
    
    /**
     * Running Kafka partition tasks (partitionId -> task).
     * Used for Kafka streaming jobs where each partition has its own task.
     */
    private Map<Integer, AbstractStreamingTask> runningKafkaPartitionTasks = new ConcurrentHashMap<>();
    
    /**
     * Active partitions that have running pipelines.
     * A partition is active when it has a task running or pending.
     * A partition becomes idle when it has no more data to consume.
     */
    private java.util.Set<Integer> activePartitions = ConcurrentHashMap.newKeySet();
    
    SourceOffsetProvider offsetProvider;
    @Getter
    @Setter
    @SerializedName("opp")
    // The value to be persisted in offsetProvider
    private String offsetProviderPersist;
    @Setter
    @Getter
    private long lastScheduleTaskTimestamp = -1L;
    private InsertIntoTableCommand baseCommand;
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
    private ConcurrentLinkedQueue<AbstractStreamingTask> streamInsertTaskQueue = new ConcurrentLinkedQueue<>();
    @Setter
    @Getter
    private String jobRuntimeMsg = "";

    @Getter
    @SerializedName("tdb")
    private String targetDb;
    @Getter
    @SerializedName("ds")
    private DataSourceType dataSourceType;
    @Getter
    @SerializedName("sprops")
    private Map<String, String> sourceProperties;
    @Getter
    @SerializedName("tprops")
    private Map<String, String> targetProperties;

    public StreamingInsertJob(String jobName,
            JobStatus jobStatus,
            String dbName,
            String comment,
            UserIdentity createUser,
            JobExecutionConfiguration jobConfig,
            Long createTimeMs,
            String executeSql,
            Map<String, String> properties) {
        super(Env.getCurrentEnv().getNextId(), jobName, jobStatus, dbName, comment, createUser,
                jobConfig, createTimeMs, executeSql);
        this.properties = properties;
        initInsertJob();
    }

    // for streaming job from...to database
    public StreamingInsertJob(String jobName,
            JobStatus jobStatus,
            String dbName,
            String comment,
            UserIdentity createUser,
            JobExecutionConfiguration jobConfig,
            Long createTimeMs,
            String executeSql,
            Map<String, String> properties,
            String targetDb,
            DataSourceType dataSourceType,
            Map<String, String> sourceProperties,
            Map<String, String> targetProperties) {
        super(Env.getCurrentEnv().getNextId(), jobName, jobStatus, dbName, comment, createUser,
                jobConfig, createTimeMs, executeSql);
        this.properties = properties;
        this.targetDb = targetDb;
        this.dataSourceType = dataSourceType;
        this.sourceProperties = sourceProperties;
        this.targetProperties = targetProperties;
        initSourceJob();
    }

    /**
     * Initialize job from source to database, like multi table mysql to doris.
     * 1. get mysql connection info from sourceProperties
     * 2. fetch table list from mysql
     * 3. create doris table if not exists
     * 4. check whether need full data sync
     * 5. need => fetch split and write to system table
     */
    private void initSourceJob() {
        try {
            init();
            checkRequiredSourceProperties();
            List<String> createTbls = createTableIfNotExists();
            if (sourceProperties.get(DataSourceConfigKeys.INCLUDE_TABLES) == null) {
                // cdc need the final includeTables
                String includeTables = String.join(",", createTbls);
                sourceProperties.put(DataSourceConfigKeys.INCLUDE_TABLES, includeTables);
            }
            this.offsetProvider = new JdbcSourceOffsetProvider(getJobId(), dataSourceType, sourceProperties);
            JdbcSourceOffsetProvider rdsOffsetProvider = (JdbcSourceOffsetProvider) this.offsetProvider;
            rdsOffsetProvider.splitChunks(createTbls);
        } catch (Exception ex) {
            log.warn("init streaming job for {} failed", dataSourceType, ex);
            throw new RuntimeException(ex.getMessage());
        }
    }

    private void checkRequiredSourceProperties() {
        Preconditions.checkArgument(sourceProperties.get(DataSourceConfigKeys.JDBC_URL) != null,
                "jdbc_url is required property");
        Preconditions.checkArgument(sourceProperties.get(DataSourceConfigKeys.DRIVER_URL) != null,
                "driver_url is required property");
        Preconditions.checkArgument(sourceProperties.get(DataSourceConfigKeys.DRIVER_CLASS) != null,
                "driver_class is required property");
        Preconditions.checkArgument(sourceProperties.get(DataSourceConfigKeys.USER) != null,
                "user is required property");
        Preconditions.checkArgument(sourceProperties.get(DataSourceConfigKeys.PASSWORD) != null,
                "password is required property");
        Preconditions.checkArgument(sourceProperties.get(DataSourceConfigKeys.DATABASE) != null,
                "database is required property");
        if (!sourceProperties.containsKey(DataSourceConfigKeys.OFFSET)) {
            sourceProperties.put(DataSourceConfigKeys.OFFSET, DataSourceConfigKeys.OFFSET_LATEST);
        }
    }

    private List<String> createTableIfNotExists() throws Exception {
        List<String> syncTbls = new ArrayList<>();
        List<CreateTableCommand> createTblCmds = StreamingJobUtils.generateCreateTableCmds(targetDb,
                dataSourceType, sourceProperties, targetProperties);
        Database db = Env.getCurrentEnv().getInternalCatalog().getDbNullable(targetDb);
        Preconditions.checkNotNull(db, "target database %s does not exist", targetDb);
        for (CreateTableCommand createTblCmd : createTblCmds) {
            if (!db.isTableExist(createTblCmd.getCreateTableInfo().getTableName())) {
                createTblCmd.run(ConnectContext.get(), null);
            }
            syncTbls.add(createTblCmd.getCreateTableInfo().getTableName());
        }
        return syncTbls;
    }

    private void init() {
        try {
            this.jobProperties = new StreamingJobProperties(properties);
            jobProperties.validate();
            // build time definition
            JobExecutionConfiguration execConfig = getJobConfig();
            TimerDefinition timerDefinition = new TimerDefinition();
            timerDefinition.setInterval(jobProperties.getMaxIntervalSecond());
            timerDefinition.setIntervalUnit(IntervalUnit.SECOND);
            timerDefinition.setStartTimeMs(execConfig.getTimerDefinition().getStartTimeMs());
            execConfig.setTimerDefinition(timerDefinition);
        } catch (AnalysisException ae) {
            log.warn("parse streaming insert job failed, props: {}", properties, ae);
            throw new RuntimeException(ae.getMessage());
        }
    }

    private void initInsertJob() {
        try {
            init();
            UnboundTVFRelation currentTvf = getCurrentTvf();
            this.tvfType = currentTvf.getFunctionName();
            this.originTvfProps = currentTvf.getProperties().getMap();
            this.offsetProvider = SourceOffsetProviderFactory.createSourceOffsetProvider(currentTvf.getFunctionName());

            // For Kafka jobs, initialize the KafkaSourceOffsetProvider with TVF properties
            if (offsetProvider instanceof KafkaSourceOffsetProvider) {
                KafkaSourceOffsetProvider kafkaProvider = (KafkaSourceOffsetProvider) offsetProvider;
                kafkaProvider.setJobId(getJobId());
                kafkaProvider.initFromTvfProperties(originTvfProps);
            }
            
            // validate offset props
            if (jobProperties.getOffsetProperty() != null) {
                Offset offset = validateOffset(jobProperties.getOffsetProperty());
                this.offsetProvider.updateOffset(offset);
            }
        } catch (Exception ex) {
            log.warn("init streaming insert job failed, sql: {}", getExecuteSql(), ex);
            throw new RuntimeException(ex.getMessage());
        }
    }

    private void readLock() {
        lock.readLock().lock();
    }

    private void readUnlock() {
        lock.readLock().unlock();
    }

    private void writeLock() {
        lock.writeLock().lock();
    }

    private void writeUnlock() {
        // if (lock.writeLock().isHeldByCurrentThread()) {
        lock.writeLock().unlock();
        //}
    }

    private UnboundTVFRelation getCurrentTvf() {
        initLogicalPlan(false);
        List<UnboundTVFRelation> allTVFRelation = baseCommand.getAllTVFRelation();
        Preconditions.checkArgument(allTVFRelation.size() == 1, "Only support one source in insert streaming job");
        UnboundTVFRelation unboundTVFRelation = allTVFRelation.get(0);
        return unboundTVFRelation;
    }

    private void makeConnectContext() {
        ConnectContext ctx = InsertTask.makeConnectContext(getCreateUser(), getCurrentDbName());
        StatementContext statementContext = new StatementContext();
        ctx.setStatementContext(statementContext);
    }

    public void initLogicalPlan(boolean regen) {
        if (regen || baseCommand == null) {
            this.baseCommand = null;
            makeConnectContext();
            LogicalPlan logicalPlan = new NereidsParser().parseSingle(getExecuteSql());
            this.baseCommand = (InsertIntoTableCommand) logicalPlan;
        }
    }

    /**
     * Check whether Offset can be serialized into the corresponding data source
     * */
    public Offset validateOffset(String offsetStr) throws AnalysisException {
        Offset offset;
        try {
            offset = offsetProvider.deserializeOffsetProperty(offsetStr);
        } catch (Exception ex) {
            log.info("initialize offset failed, offset: {}", offsetStr, ex);
            throw new AnalysisException("Failed to initialize offset, " + ex.getMessage());
        }
        if (offset == null || !offset.isValidOffset()) {
            throw new AnalysisException("Invalid format for offset: " + offsetStr);
        }
        return offset;
    }

    public void alterJob(AlterJobCommand alterJobCommand) throws AnalysisException, JobException {
        List<String> logParts = new ArrayList<>();
        // update sql
        if (StringUtils.isNotEmpty(alterJobCommand.getSql())) {
            setExecuteSql(alterJobCommand.getSql());
            initLogicalPlan(true);
            String encryptedSql = generateEncryptedSql();
            logParts.add("sql: " + encryptedSql);
        }

        // update properties
        if (!alterJobCommand.getProperties().isEmpty()) {
            modifyPropertiesInternal(alterJobCommand.getProperties());
            logParts.add("properties: " + alterJobCommand.getProperties());
        }

        // update source properties
        if (!alterJobCommand.getSourceProperties().isEmpty()) {
            this.sourceProperties.putAll(alterJobCommand.getSourceProperties());
            logParts.add("source properties: " + alterJobCommand.getSourceProperties());
        }

        // update target properties
        if (!alterJobCommand.getTargetProperties().isEmpty()) {
            this.sourceProperties.putAll(alterJobCommand.getTargetProperties());
            logParts.add("target properties: " + alterJobCommand.getTargetProperties());
        }
        log.info("Alter streaming job {}, {}", getJobId(), String.join(", ", logParts));
    }

    @Override
    public void updateJobStatus(JobStatus status) throws JobException {
        lock.writeLock().lock();
        try {
            super.updateJobStatus(status);
            if (JobStatus.PAUSED.equals(getJobStatus())) {
                clearRunningStreamTask(status);
            }
            if (isFinalStatus()) {
                Env.getCurrentGlobalTransactionMgr().getCallbackFactory().removeCallback(getJobId());
            }
            log.info("Streaming insert job {} update status to {}", getJobId(), getJobStatus());
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void resetFailureInfo(FailureReason reason) {
        this.setFailureReason(reason);
        // Currently, only delayMsg is present here, which needs to be cleared when the status changes.
        this.setJobRuntimeMsg("");
    }

    @Override
    public void cancelAllTasks(boolean needWaitCancelComplete) throws JobException {
        lock.writeLock().lock();
        try {
            // Cancel single running task
            if (runningStreamTask != null) {
                runningStreamTask.cancel(needWaitCancelComplete);
                canceledTaskCount.incrementAndGet();
            }
            
            // Cancel all Kafka partition tasks
            if (!runningKafkaPartitionTasks.isEmpty()) {
                for (AbstractStreamingTask task : runningKafkaPartitionTasks.values()) {
                    task.cancel(needWaitCancelComplete);
                    canceledTaskCount.incrementAndGet();
                }
                runningKafkaPartitionTasks.clear();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public JobType getJobType() {
        return JobType.INSERT;
    }

    @Override
    protected void checkJobParamsInternal() {
    }

    @Override
    public boolean isReadyForScheduling(Map<Object, Object> taskContext) {
        return CollectionUtils.isEmpty(getRunningTasks()) && !isFinalStatus();
    }

    @Override
    public boolean isJobRunning() {
        return !isFinalStatus();
    }

    private boolean isFinalStatus() {
        return getJobStatus().equals(JobStatus.STOPPED) || getJobStatus().equals(JobStatus.FINISHED);
    }

    @Override
    public List<StreamingJobSchedulerTask> createTasks(TaskType taskType, Map<Object, Object> taskContext) {
        List<StreamingJobSchedulerTask> newTasks = new ArrayList<>();
        StreamingJobSchedulerTask streamingJobSchedulerTask = new StreamingJobSchedulerTask(this);
        newTasks.add(streamingJobSchedulerTask);
        super.initTasks(newTasks, taskType);
        return newTasks;
    }

    protected AbstractStreamingTask createStreamingTask() {
        // For Kafka streaming jobs, create multiple partition tasks
        if (isKafkaStreamingJob()) {
            List<AbstractStreamingTask> kafkaTasks = createKafkaPartitionTasks();
            if (!kafkaTasks.isEmpty()) {
                // Return the first task for backward compatibility
                // All tasks are registered and scheduled
                this.runningStreamTask = kafkaTasks.get(0);
                return this.runningStreamTask;
            }
            return null;
        }

        // For other sources (S3, JDBC, etc.), create a single task
        if (tvfType != null) {
            this.runningStreamTask = createStreamingInsertTask();
        } else {
            this.runningStreamTask = createStreamingMultiTblTask();
        }
        Env.getCurrentEnv().getJobManager().getStreamingTaskManager().registerTask(runningStreamTask);
        this.runningStreamTask.setStatus(TaskStatus.PENDING);
        log.info("create new streaming insert task for job {}, task {} ",
                getJobId(), runningStreamTask.getTaskId());
        recordTasks(runningStreamTask);
        return runningStreamTask;
    }

    /**
     * for From MySQL TO Database
     * @return
     */
    private AbstractStreamingTask createStreamingMultiTblTask() {
        return new StreamingMultiTblTask(getJobId(), Env.getCurrentEnv().getNextId(), dataSourceType,
                offsetProvider, sourceProperties, targetDb, targetProperties, jobProperties, getCreateUser());
    }

    protected AbstractStreamingTask createStreamingInsertTask() {
        if (originTvfProps == null) {
            this.originTvfProps = getCurrentTvf().getProperties().getMap();
        }
        return new StreamingInsertTask(getJobId(), Env.getCurrentEnv().getNextId(),
                getExecuteSql(),
                offsetProvider, getCurrentDbName(), jobProperties, originTvfProps, getCreateUser());
    }
    
    /**
     * Create multiple Kafka partition tasks for parallel execution (initial creation).
     * Each task handles exactly one partition for exactly-once semantics.
     * Called during PENDING state to start all partition pipelines.
     * 
     * @return list of created tasks, or empty list if no data to consume
     */
    protected List<AbstractStreamingTask> createKafkaPartitionTasks() {
        List<AbstractStreamingTask> tasks = new ArrayList<>();
        
        if (!(offsetProvider instanceof KafkaSourceOffsetProvider)) {
            log.warn("createKafkaPartitionTasks called but offsetProvider is not KafkaSourceOffsetProvider");
            return tasks;
        }
        
        KafkaSourceOffsetProvider kafkaProvider = (KafkaSourceOffsetProvider) offsetProvider;
        List<KafkaPartitionOffset> partitionOffsets = kafkaProvider.getNextPartitionOffsets(jobProperties);
        
        if (partitionOffsets.isEmpty()) {
            log.info("No Kafka partitions to consume for job {}", getJobId());
            return tasks;
        }
        
        if (originTvfProps == null) {
            this.originTvfProps = getCurrentTvf().getProperties().getMap();
        }

        // Clear previous state
        runningKafkaPartitionTasks.clear();
        activePartitions.clear();
        
        for (KafkaPartitionOffset partitionOffset : partitionOffsets) {
            StreamingInsertTask task = createTaskForPartition(partitionOffset);
            if (task != null) {
                tasks.add(task);
            }
        }

        log.info("Created {} Kafka partition tasks for job {}, active partitions: {}",
                tasks.size(), getJobId(), activePartitions);

        return tasks;
    }

    /**
     * Create a task for a specific partition offset.
     *
     * @param partitionOffset the partition offset range
     * @return the created task, or null if failed
     */
    private StreamingInsertTask createTaskForPartition(KafkaPartitionOffset partitionOffset) {
        int partitionId = partitionOffset.getPartitionId();

        StreamingInsertTask task = new StreamingInsertTask(
                getJobId(),
                Env.getCurrentEnv().getNextId(),
                getExecuteSql(),
                offsetProvider,
                getCurrentDbName(),
                jobProperties,
                originTvfProps,
                getCreateUser(),
                partitionOffset
        );

        Env.getCurrentEnv().getJobManager().getStreamingTaskManager().registerTask(task);
        task.setStatus(TaskStatus.PENDING);
        runningKafkaPartitionTasks.put(partitionId, task);
        activePartitions.add(partitionId);
        recordTasks(task);

        log.info("Created Kafka partition task {} for job {}, partition {}, offset range [{}, {})",
                task.getTaskId(), getJobId(), partitionId,
                partitionOffset.getStartOffset(), partitionOffset.getEndOffset());

        return task;
    }

    /**
     * Create the next task for a specific partition after the previous task completes.
     * This is the core of the independent pipeline model - each partition creates
     * its own next task without waiting for other partitions.
     *
     * @param partitionId the Kafka partition ID
     * @return the created task, or null if no more data for this partition
     */
    protected StreamingInsertTask createNextTaskForPartition(int partitionId) {
        if (!(offsetProvider instanceof KafkaSourceOffsetProvider)) {
            log.warn("createNextTaskForPartition called but offsetProvider is not KafkaSourceOffsetProvider");
            return null;
        }

        KafkaSourceOffsetProvider kafkaProvider = (KafkaSourceOffsetProvider) offsetProvider;

        // Get the next offset range for this partition
        KafkaPartitionOffset nextOffset = kafkaProvider.getNextPartitionOffset(partitionId, jobProperties);

        if (nextOffset == null || nextOffset.isEmpty()) {
            // No more data for this partition, mark it as idle
            activePartitions.remove(partitionId);
            log.info("Partition {} has no more data, stopping its pipeline. Active partitions: {}",
                    partitionId, activePartitions);
            return null;
        }

        if (originTvfProps == null) {
            this.originTvfProps = getCurrentTvf().getProperties().getMap();
        }

        return createTaskForPartition(nextOffset);
    }
    
    /**
     * Callback when a Kafka partition task completes successfully.
     * Updates the partition's offset and immediately creates the next task for this partition.
     *
     * This implements the independent pipeline model where each partition runs its own
     * continuous pipeline without waiting for other partitions.
     * 
     * @param task the completed task
     * @param partitionId the Kafka partition ID
     * @param consumedRows number of rows consumed by this task
     */
    public void onKafkaPartitionTaskSuccess(StreamingInsertTask task, int partitionId, long consumedRows) {
        writeLock();
        try {
            resetFailureInfo(null);
            succeedTaskCount.incrementAndGet();
            
            // Remove from running tasks
            Env.getCurrentEnv().getJobManager().getStreamingTaskManager().removeRunningTask(task);
            runningKafkaPartitionTasks.remove(partitionId);
            
            // Update partition offset
            if (offsetProvider instanceof KafkaSourceOffsetProvider) {
                KafkaSourceOffsetProvider kafkaProvider = (KafkaSourceOffsetProvider) offsetProvider;
                KafkaPartitionOffset partitionOffset = task.getKafkaPartitionOffset();
                long newOffset = partitionOffset.getStartOffset() + consumedRows;
                kafkaProvider.updatePartitionOffset(partitionId, newOffset);
                
                log.info("Kafka partition {} task {} success, consumed {} rows, new offset: {}",
                        partitionId, task.getTaskId(), consumedRows, newOffset);
            }

            // Immediately create the next task for this partition (independent pipeline)
            StreamingInsertTask nextTask = createNextTaskForPartition(partitionId);
            if (nextTask != null) {
                log.info("Created next task {} for partition {} (independent pipeline)",
                        nextTask.getTaskId(), partitionId);
            } else {
                log.info("Partition {} pipeline stopped (no more data), active partitions: {}",
                        partitionId, activePartitions);
            }
        } finally {
            writeUnlock();
        }
    }

    /**
     * Restart idle Kafka partitions that have new data available.
     * Called by StreamingJobSchedulerTask during RUNNING state to check for
     * partitions that stopped due to no data but now have new messages.
     */
    public void restartIdleKafkaPartitions() {
        if (!(offsetProvider instanceof KafkaSourceOffsetProvider)) {
            return;
        }

        KafkaSourceOffsetProvider kafkaProvider = (KafkaSourceOffsetProvider) offsetProvider;
        java.util.Set<Integer> allPartitions = kafkaProvider.getAllPartitionIds();

        int restartedCount = 0;
        for (Integer partitionId : allPartitions) {
            // Skip partitions that are already active
            if (activePartitions.contains(partitionId)) {
                continue;
            }

            // Check if this idle partition now has new data
            if (kafkaProvider.hasMoreDataForPartition(partitionId)) {
                writeLock();
                try {
                    // Double-check after acquiring lock
                    if (!activePartitions.contains(partitionId)) {
                        StreamingInsertTask task = createNextTaskForPartition(partitionId);
                        if (task != null) {
                            restartedCount++;
                            log.info("Restarted idle partition {} with new task {}",
                                    partitionId, task.getTaskId());
                        }
                    }
                } finally {
                    writeUnlock();
                }
            }
        }

        if (restartedCount > 0) {
            log.info("Restarted {} idle Kafka partitions for job {}", restartedCount, getJobId());
        }
    }

    /**
     * Get the set of currently active partitions.
     */
    public java.util.Set<Integer> getActivePartitions() {
        return java.util.Collections.unmodifiableSet(activePartitions);
    }
    
    /**
     * Check if this is a Kafka streaming job.
     */
    public boolean isKafkaStreamingJob() {
        return "kafka".equalsIgnoreCase(tvfType);
    }

    public void recordTasks(AbstractStreamingTask task) {
        if (Config.max_streaming_task_show_count < 1) {
            return;
        }
        streamInsertTaskQueue.add(task);

        while (streamInsertTaskQueue.size() > Config.max_streaming_task_show_count) {
            streamInsertTaskQueue.poll();
        }
    }

    /**
     * for show command to display all streaming insert tasks of this job.
     */
    public List<AbstractStreamingTask> queryAllStreamTasks() {
        if (CollectionUtils.isEmpty(streamInsertTaskQueue)) {
            return new ArrayList<>();
        }
        List<AbstractStreamingTask> tasks = new ArrayList<>(streamInsertTaskQueue);
        Comparator<AbstractStreamingTask> taskComparator =
                Comparator.comparingLong(AbstractStreamingTask::getCreateTimeMs).reversed();
        tasks.sort(taskComparator);
        return tasks;
    }

    protected void fetchMeta() throws JobException {
        try {
            if (tvfType != null) {
                if (originTvfProps == null) {
                    this.originTvfProps = getCurrentTvf().getProperties().getMap();
                }
                if (offsetProvider instanceof KafkaSourceOffsetProvider && originTvfProps != null) {
                    KafkaSourceOffsetProvider kafkaProvider = (KafkaSourceOffsetProvider) offsetProvider;
                    if (!kafkaProvider.isInitialized()) {
                        kafkaProvider.setJobId(getJobId());
                        kafkaProvider.initFromTvfProperties(originTvfProps);
                    }
                }
                offsetProvider.fetchRemoteMeta(originTvfProps);
            } else {
                offsetProvider.fetchRemoteMeta(new HashMap<>());
            }
        } catch (Exception ex) {
            log.warn("fetch remote meta failed, job id: {}", getJobId(), ex);
            if (this.getFailureReason() == null
                    || !InternalErrorCode.MANUAL_PAUSE_ERR.equals(this.getFailureReason().getCode())) {
                // When a job is manually paused, it does not need to be set again,
                // otherwise, it may be woken up by auto resume.
                this.setFailureReason(
                        new FailureReason(InternalErrorCode.GET_REMOTE_DATA_ERROR,
                                "Failed to fetch meta, " + ex.getMessage()));
                // If fetching meta fails, the job is paused
                // and auto resume will automatically wake it up.
                this.updateJobStatus(JobStatus.PAUSED);
            }
        }
    }

    public boolean needScheduleTask() {
        readLock();
        try {
            return (getJobStatus().equals(JobStatus.RUNNING)
                    || getJobStatus().equals(JobStatus.PENDING));
        } finally {
            readUnlock();
        }
    }

    public void clearRunningStreamTask(JobStatus newJobStatus) {
        boolean needWait = !JobStatus.STOPPED.equals(newJobStatus);
        
        // Clear single running task
        if (runningStreamTask != null) {
            log.info("clear running streaming insert task for job {}, task {}, status {} ",
                    getJobId(), runningStreamTask.getTaskId(), runningStreamTask.getStatus());
            runningStreamTask.cancel(needWait);
            runningStreamTask.closeOrReleaseResources();
        }
        
        // Clear all Kafka partition tasks
        if (!runningKafkaPartitionTasks.isEmpty()) {
            for (AbstractStreamingTask task : runningKafkaPartitionTasks.values()) {
                log.info("clear running Kafka partition task for job {}, task {}, status {} ",
                        getJobId(), task.getTaskId(), task.getStatus());
                task.cancel(needWait);
                task.closeOrReleaseResources();
            }
            runningKafkaPartitionTasks.clear();
            activePartitions.clear();
        }
    }

    public boolean hasMoreDataToConsume() {
        return offsetProvider.hasMoreDataToConsume();
    }

    @Override
    public void onTaskFail(StreamingJobSchedulerTask task) throws JobException {
        if (task.getErrMsg() != null) {
            this.failureReason = new FailureReason(task.getErrMsg());
        }
        // Here is the failure of StreamingJobSchedulerTask, no processing is required
        getRunningTasks().remove(task);
    }

    @Override
    public void onTaskSuccess(StreamingJobSchedulerTask task) throws JobException {
        // Here is the success of StreamingJobSchedulerTask, no processing is required
        getRunningTasks().remove(task);
    }

    public void onStreamTaskFail(AbstractStreamingTask task) throws JobException {
        writeLock();
        try {
            failedTaskCount.incrementAndGet();
            Env.getCurrentEnv().getJobManager().getStreamingTaskManager().removeRunningTask(task);
            this.failureReason = new FailureReason(task.getErrMsg());
        } finally {
            writeUnlock();
        }
        updateJobStatus(JobStatus.PAUSED);
    }

    public void onStreamTaskSuccess(AbstractStreamingTask task) {
        writeLock();
        try {
            resetFailureInfo(null);
            succeedTaskCount.incrementAndGet();
            Env.getCurrentEnv().getJobManager().getStreamingTaskManager().removeRunningTask(task);
            AbstractStreamingTask nextTask = createStreamingTask();
            this.runningStreamTask = nextTask;
            log.info("Streaming insert job {} create next streaming insert task {} after task {} success",
                    getJobId(), nextTask.getTaskId(), task.getTaskId());
        } finally {
            writeUnlock();
        }
    }

    private void updateJobStatisticAndOffset(StreamingTaskTxnCommitAttachment attachment) {
        if (this.jobStatistic == null) {
            this.jobStatistic = new StreamingJobStatistic();
        }
        this.jobStatistic.setScannedRows(this.jobStatistic.getScannedRows() + attachment.getScannedRows());
        this.jobStatistic.setLoadBytes(this.jobStatistic.getLoadBytes() + attachment.getLoadBytes());
        this.jobStatistic.setFileNumber(this.jobStatistic.getFileNumber() + attachment.getNumFiles());
        this.jobStatistic.setFileSize(this.jobStatistic.getFileSize() + attachment.getFileBytes());
        log.info("[KAFKA_OFFSET_DEBUG] updateJobStatisticAndOffset: jobId={}, about to deserialize offset from attachment",
                getJobId());
        Offset deserializedOffset = offsetProvider.deserializeOffset(attachment.getOffset());
        log.info("[KAFKA_OFFSET_DEBUG] updateJobStatisticAndOffset: jobId={}, deserialized offset type={}, calling updateOffset",
                getJobId(), deserializedOffset != null ? deserializedOffset.getClass().getSimpleName() : "null");
        offsetProvider.updateOffset(deserializedOffset);
    }

    private void updateCloudJobStatisticAndOffset(StreamingTaskTxnCommitAttachment attachment) {
        if (this.jobStatistic == null) {
            this.jobStatistic = new StreamingJobStatistic();
        }
        this.jobStatistic.setScannedRows(attachment.getScannedRows());
        this.jobStatistic.setLoadBytes(attachment.getLoadBytes());
        this.jobStatistic.setFileNumber(attachment.getNumFiles());
        this.jobStatistic.setFileSize(attachment.getFileBytes());
        offsetProvider.updateOffset(offsetProvider.deserializeOffset(attachment.getOffset()));
    }

    private void updateJobStatisticAndOffset(CommitOffsetRequest offsetRequest) {
        if (this.jobStatistic == null) {
            this.jobStatistic = new StreamingJobStatistic();
        }
        this.jobStatistic.setScannedRows(this.jobStatistic.getScannedRows() + offsetRequest.getScannedRows());
        this.jobStatistic.setLoadBytes(this.jobStatistic.getLoadBytes() + offsetRequest.getScannedBytes());
        offsetProvider.updateOffset(offsetProvider.deserializeOffset(offsetRequest.getOffset()));
    }

    private void updateNoTxnJobStatisticAndOffset(CommitOffsetRequest offsetRequest) {
        if (this.nonTxnJobStatistic == null) {
            this.nonTxnJobStatistic = new StreamingJobStatistic();
        }
        this.nonTxnJobStatistic
                .setScannedRows(this.nonTxnJobStatistic.getScannedRows() + offsetRequest.getScannedRows());
        this.nonTxnJobStatistic.setLoadBytes(this.nonTxnJobStatistic.getLoadBytes() + offsetRequest.getScannedBytes());
        offsetProvider.updateOffset(offsetProvider.deserializeOffset(offsetRequest.getOffset()));
    }

    @Override
    public void onRegister() throws JobException {
        Env.getCurrentGlobalTransactionMgr().getCallbackFactory().addCallback(this);
    }

    @Override
    public void onReplayCreate() throws JobException {
        onRegister();
        super.onReplayCreate();
    }


    /**
     * Because the offset statistics of the streamingInsertJob are all stored in txn,
     * only some fields are replayed here.
     * @param replayJob
     */
    public void replayOnUpdated(StreamingInsertJob replayJob) {
        log.info("[KAFKA_OFFSET_DEBUG] replayOnUpdated started: jobId={}, replayJob.offsetProviderPersist={}",
                getJobId(),
                replayJob.getOffsetProviderPersist() != null ? "has value (length=" + replayJob.getOffsetProviderPersist().length() + ")" : "null");
        if (!JobStatus.RUNNING.equals(replayJob.getJobStatus())) {
            // No need to restore in the running state, as scheduling relies on pending states.
            // insert TVF does not persist the running state.
            // streaming multi task persists the running state when commitOffset() is called.
            setJobStatus(replayJob.getJobStatus());
        }
        try {
            modifyPropertiesInternal(replayJob.getProperties());
            // When the pause state is restarted, it also needs to be updated
            if (Config.isCloudMode()) {
                replayOnCloudMode();
            }
        } catch (Exception e) {
            // should not happen
            log.error("replay modify streaming insert job properties failed, job id: {}", getJobId(), e);
        }
        if (replayJob.getOffsetProviderPersist() != null) {
            log.info("[KAFKA_OFFSET_DEBUG] replayOnUpdated: jobId={}, setting offsetProviderPersist from replayJob",
                    getJobId());
            setOffsetProviderPersist(replayJob.getOffsetProviderPersist());
        } else {
            log.info("[KAFKA_OFFSET_DEBUG] replayOnUpdated: jobId={}, replayJob.offsetProviderPersist is null, NOT setting",
                    getJobId());
        }
        if (replayJob.getNonTxnJobStatistic() != null) {
            setNonTxnJobStatistic(replayJob.getNonTxnJobStatistic());
        }
        setExecuteSql(replayJob.getExecuteSql());
        setSucceedTaskCount(replayJob.getSucceedTaskCount());
        setFailedTaskCount(replayJob.getFailedTaskCount());
        setCanceledTaskCount(replayJob.getCanceledTaskCount());
        log.info("[KAFKA_OFFSET_DEBUG] replayOnUpdated completed: jobId={}", getJobId());
    }

    /**
     * When updating offset, you need to reset the currentOffset
     */
    private void modifyPropertiesInternal(Map<String, String> inputProperties) throws AnalysisException, JobException {
        StreamingJobProperties inputStreamProps = new StreamingJobProperties(inputProperties);
        if (StringUtils.isNotEmpty(inputStreamProps.getOffsetProperty()) && this.tvfType != null) {
            Offset offset = validateOffset(inputStreamProps.getOffsetProperty());
            this.offsetProvider.updateOffset(offset);
            if (Config.isCloudMode()) {
                resetCloudProgress(offset);
            }
        }
        this.properties.putAll(inputProperties);
        this.jobProperties = new StreamingJobProperties(this.properties);
    }

    private void resetCloudProgress(Offset offset) throws JobException {
        Cloud.ResetStreamingJobOffsetRequest.Builder builder = Cloud.ResetStreamingJobOffsetRequest.newBuilder()
                .setRequestIp(FrontendOptions.getLocalHostAddressCached());
        builder.setCloudUniqueId(Config.cloud_unique_id);
        builder.setDbId(getDbId());
        builder.setJobId(getJobId());
        builder.setOffset(offset.toSerializedJson());

        Cloud.ResetStreamingJobOffsetResponse response;
        try {
            response = MetaServiceProxy.getInstance().resetStreamingJobOffset(builder.build());
            if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                log.warn("failed to reset streaming job cloud offset, response: {}", response);
                if (response.getStatus().getCode() == Cloud.MetaServiceCode.ROUTINE_LOAD_PROGRESS_NOT_FOUND) {
                    log.warn("not found streaming job offset, response: {}", response);
                    return;
                } else {
                    throw new JobException(response.getStatus().getMsg());
                }
            }
        } catch (RpcException e) {
            log.info("failed to reset cloud progress, ", e);
            throw new JobException(e.getMessage());
        }
    }

    @Override
    public ShowResultSetMetaData getTaskMetaData() {
        return InsertJob.TASK_META_DATA;
    }

    @Override
    public List<String> getShowInfo() {
        return getCommonShowInfo();
    }

    @Override
    public TRow getTvfInfo() {
        TRow trow = new TRow();
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(getJobId())));
        trow.addToColumnValue(new TCell().setStringVal(getJobName()));
        trow.addToColumnValue(new TCell().setStringVal(getCreateUser().getQualifiedUser()));
        trow.addToColumnValue(new TCell().setStringVal(getJobConfig().getExecuteType().name()));
        trow.addToColumnValue(new TCell().setStringVal(""));
        trow.addToColumnValue(new TCell().setStringVal(getJobStatus().name()));
        trow.addToColumnValue(new TCell().setStringVal(getShowSQL()));
        trow.addToColumnValue(new TCell().setStringVal(TimeUtils.longToTimeString(getCreateTimeMs())));
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(getSucceedTaskCount().get())));
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(getFailedTaskCount().get())));
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(getCanceledTaskCount().get())));
        trow.addToColumnValue(new TCell().setStringVal(getComment()));
        trow.addToColumnValue(new TCell().setStringVal(properties != null && !properties.isEmpty()
                ? GsonUtils.GSON.toJson(properties) : ""));

        if (offsetProvider != null && StringUtils.isNotEmpty(offsetProvider.getShowCurrentOffset())) {
            trow.addToColumnValue(new TCell().setStringVal(offsetProvider.getShowCurrentOffset()));
        } else {
            trow.addToColumnValue(new TCell().setStringVal(""));
        }

        if (offsetProvider != null && StringUtils.isNotEmpty(offsetProvider.getShowMaxOffset())) {
            trow.addToColumnValue(new TCell().setStringVal(offsetProvider.getShowMaxOffset()));
        } else {
            trow.addToColumnValue(new TCell().setStringVal(""));
        }
        if (tvfType != null) {
            trow.addToColumnValue(new TCell().setStringVal(
                    jobStatistic == null ? "" : jobStatistic.toJson()));
        } else {
            trow.addToColumnValue(new TCell().setStringVal(
                    nonTxnJobStatistic == null ? "" : nonTxnJobStatistic.toJson()));
        }
        trow.addToColumnValue(new TCell().setStringVal(failureReason == null
                ? "" : failureReason.getMsg()));
        trow.addToColumnValue(new TCell().setStringVal(jobRuntimeMsg == null
                ? "" : jobRuntimeMsg));
        return trow;
    }

    private String getShowSQL() {
        if (StringUtils.isNotEmpty(getExecuteSql())) {
            return StringUtils.isNotEmpty(getEncryptedSql())
                    ? getEncryptedSql() : generateEncryptedSql();
        } else {
            StringBuilder sb = new StringBuilder();
            sb.append("FROM ").append(dataSourceType.name());
            sb.append("(");
            for (Map.Entry<String, String> entry : sourceProperties.entrySet()) {
                sb.append("'").append(entry.getKey())
                        .append("'='").append(entry.getValue()).append("',");
            }
            sb.deleteCharAt(sb.length() - 1);
            sb.append(" ) TO DATABSE ").append(targetDb);
            if (!targetProperties.isEmpty()) {
                sb.append(" (");
                for (Map.Entry<String, String> entry : targetProperties.entrySet()) {
                    sb.append("'").append(entry.getKey())
                            .append("'='").append(entry.getValue()).append("',");
                }
                sb.deleteCharAt(sb.length() - 1);
                sb.append(")");
            }
            return sb.toString();
        }
    }

    private static boolean checkPrivilege(ConnectContext ctx, String  sql) throws AnalysisException {
        LogicalPlan logicalPlan = new NereidsParser().parseSingle(sql);
        if (!(logicalPlan instanceof InsertIntoTableCommand)) {
            throw new AnalysisException("Only support insert command");
        }
        LogicalPlan logicalQuery = ((InsertIntoTableCommand) logicalPlan).getLogicalQuery();
        List<String> targetTable = InsertUtils.getTargetTableQualified(logicalQuery, ctx);
        Preconditions.checkArgument(targetTable.size() == 3, "target table name is invalid");
        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(ctx,
                InternalCatalog.INTERNAL_CATALOG_NAME,
                targetTable.get(1),
                targetTable.get(2),
                PrivPredicate.LOAD)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "LOAD",
                    ctx.getQualifiedUser(),
                    ctx.getRemoteIP(),
                    targetTable.get(1),
                    targetTable.get(2));
        }
        return true;
    }

    public static boolean checkPrivilege(ConnectContext ctx, String sql, String targetDb) throws AnalysisException {
        if (StringUtils.isNotEmpty(sql)) {
            return checkPrivilege(ctx, sql);
        } else if (StringUtils.isNotEmpty(targetDb)) {
            return checkHasSourceJobPriv(ctx, targetDb);
        } else {
            log.info("insert sql and target db are both empty");
            return false;
        }
    }

    public boolean checkPrivilege(ConnectContext ctx) throws AnalysisException {
        if (StringUtils.isNotEmpty(getExecuteSql())) {
            return checkPrivilege(ctx, getExecuteSql());
        } else if (StringUtils.isNotEmpty(getTargetDb())) {
            return checkHasSourceJobPriv(ctx, targetDb);
        } else {
            log.info("insert sql and target db are both empty");
            return false;
        }
    }

    public boolean hasPrivilege(UserIdentity userIdentity) {
        ConnectContext ctx = InsertTask.makeConnectContext(userIdentity, getCurrentDbName());
        try {
            if (StringUtils.isNotEmpty(getExecuteSql())) {
                LogicalPlan logicalPlan = new NereidsParser().parseSingle(getExecuteSql());
                LogicalPlan logicalQuery = ((InsertIntoTableCommand) logicalPlan).getLogicalQuery();
                List<String> targetTable = InsertUtils.getTargetTableQualified(logicalQuery, ctx);
                Preconditions.checkArgument(targetTable.size() == 3, "target table name is invalid");
                return Env.getCurrentEnv().getAccessManager().checkTblPriv(userIdentity,
                        InternalCatalog.INTERNAL_CATALOG_NAME,
                        targetTable.get(1),
                        targetTable.get(2),
                        PrivPredicate.LOAD);
            } else if (StringUtils.isNotEmpty(getTargetDb())) {
                return Env.getCurrentEnv().getAccessManager().checkDbPriv(ctx,
                        InternalCatalog.INTERNAL_CATALOG_NAME,
                        targetDb,
                        PrivPredicate.LOAD);
            } else {
                log.info("insert sql and target db are both empty");
                return false;
            }
        } finally {
            ctx.cleanup();
        }
    }

    private static boolean checkHasSourceJobPriv(ConnectContext ctx, String targetDb) throws AnalysisException {
        if (!Env.getCurrentEnv().getAccessManager().checkDbPriv(ctx,
                InternalCatalog.INTERNAL_CATALOG_NAME,
                targetDb,
                PrivPredicate.LOAD)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_DB_ACCESS_DENIED_ERROR, "LOAD",
                    PrivPredicate.LOAD.getPrivs().toString(),
                    targetDb);
        }
        return true;
    }

    private String generateEncryptedSql() {
        makeConnectContext();
        TreeMap<Pair<Integer, Integer>, String> indexInSqlToString = new TreeMap<>(new Pair.PairComparator<>());
        new NereidsParser().parseForEncryption(getExecuteSql(), indexInSqlToString);
        BaseViewInfo.rewriteSql(indexInSqlToString, getExecuteSql());
        String encryptSql = BaseViewInfo.rewriteSql(indexInSqlToString, getExecuteSql());
        setEncryptedSql(encryptSql);
        return encryptSql;
    }

    @Override
    public String formatMsgWhenExecuteQueueFull(Long taskId) {
        return commonFormatMsgWhenExecuteQueueFull(taskId, "streaming_task_queue_size",
                "job_streaming_task_consumer_thread_num");
    }

    @Override
    public List<StreamingJobSchedulerTask> queryTasks() {
        if (!getRunningTasks().isEmpty()) {
            return getRunningTasks();
        } else {
            return Arrays.asList(new StreamingJobSchedulerTask(this));
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    @Override
    public long getId() {
        return getJobId();
    }

    @Override
    public void beforeCommitted(TransactionState txnState) throws TransactionException {
        // boolean shouldReleaseLock = false;
        boolean shouldReleaseLock = true;
        writeLock();
        try {
            // For Kafka streaming jobs with multiple partition tasks,
            // offset is managed in onKafkaPartitionTaskSuccess() instead.
            // Here we need to find the correct task from runningKafkaPartitionTasks.
            AbstractStreamingTask currentTask = runningStreamTask;
            // for kafka job, runningStreamTask is meaning less, find task in runningKafkaPartitionTasks
            if (isKafkaStreamingJob() && !runningKafkaPartitionTasks.isEmpty()) {
                // Try to find a running task from Kafka partition tasks
                // This handles the case where runningStreamTask is not set
                for (AbstractStreamingTask task : runningKafkaPartitionTasks.values()) {
                    if (task != null && !task.getIsCanceled().get()
                            && task.getLabelName().equals(txnState.getLabel())) {
                        currentTask = task;
                        break;
                    }
                }
            }
            
            if (currentTask == null) {
                log.warn("No running task found for job {} in beforeCommitted, skipping", getJobId());
                return;
            }
            
            if (currentTask.getIsCanceled().get()) {
                log.info("streaming insert job {} task {} is canceled, skip beforeCommitted",
                        getJobId(), currentTask.getTaskId());
                return;
            }

            ArrayList<Long> taskIds = new ArrayList<>();
            taskIds.add(currentTask.getTaskId());
            // todo: Check whether the taskid of runningtask is consistent with the taskid associated with txn

            List<LoadJob> loadJobs = Env.getCurrentEnv().getLoadManager().queryLoadJobsByJobIds(taskIds);
            if (loadJobs.size() == 0) {
                throw new TransactionException("load job not found, insert job id is " + currentTask.getTaskId());
            }
            LoadJob loadJob = loadJobs.get(0);
            LoadStatistic loadStatistic = loadJob.getLoadStatistic();

            // CRITICAL FIX: For KafkaPartitionOffset, we need to update consumedRows with actual scanned rows
            // Otherwise consumedRows stays at 0 (initialized in constructor) and offset won't advance
            Offset runningOffset = currentTask.getRunningOffset();
            String offsetJson;

            if (runningOffset instanceof KafkaPartitionOffset) {
                KafkaPartitionOffset kafkaOffset = (KafkaPartitionOffset) runningOffset;
                // Create a new KafkaPartitionOffset with the actual consumed rows
                KafkaPartitionOffset updatedOffset = new KafkaPartitionOffset(
                        kafkaOffset.getPartitionId(),
                        kafkaOffset.getStartOffset(),
                        kafkaOffset.getEndOffset());
                updatedOffset.setConsumedRows(loadStatistic.getScannedRows());
                offsetJson = updatedOffset.toSerializedJson();

                log.info("[KAFKA_OFFSET_DEBUG] <<<CRITICAL>>> beforeCommitted: jobId={}, taskId={}, partition={}, startOffset={}, endOffset={}, scannedRows={}, UPDATED consumedRows={} <<<PARTITION_0>>>",
                        getJobId(), currentTask.getTaskId(),
                        kafkaOffset.getPartitionId(),
                        kafkaOffset.getStartOffset(),
                        kafkaOffset.getEndOffset(),
                        loadStatistic.getScannedRows(),
                        updatedOffset.getConsumedRows());
            } else {
                offsetJson = runningOffset.toSerializedJson();
                log.info("[KAFKA_OFFSET_DEBUG] beforeCommitted: jobId={}, taskId={}, offsetType={}, offsetJson='{}'",
                        getJobId(), currentTask.getTaskId(),
                        runningOffset.getClass().getSimpleName(),
                        offsetJson);
            }

            txnState.setTxnCommitAttachment(new StreamingTaskTxnCommitAttachment(
                        getJobId(),
                        currentTask.getTaskId(),
                        loadStatistic.getScannedRows(),
                        loadStatistic.getLoadBytes(),
                        loadStatistic.getFileNumber(),
                        loadStatistic.getTotalFileSizeB(),
                        offsetJson));
        } finally {
            if (shouldReleaseLock) {
                writeUnlock();
            }
        }
    }

    @Override
    public void beforeAborted(TransactionState txnState) throws TransactionException {
    }

    @Override
    public void afterCommitted(TransactionState txnState, boolean txnOperated) throws UserException {
        Preconditions.checkNotNull(txnState.getTxnCommitAttachment(), txnState);
        StreamingTaskTxnCommitAttachment attachment =
                (StreamingTaskTxnCommitAttachment) txnState.getTxnCommitAttachment();
        log.info("[KAFKA_OFFSET_DEBUG] afterCommitted: jobId={}, taskId={}, attachmentOffset='{}'",
                getJobId(), attachment.getTaskId(), attachment.getOffset());
        updateJobStatisticAndOffset(attachment);
        // Persist offset to ensure recovery after FE restart
        persistOffsetProviderIfNeed();
        log.info("[KAFKA_OFFSET_DEBUG] afterCommitted completed, persistOffsetProviderIfNeed called");
    }

    @Override
    public void replayOnCommitted(TransactionState txnState) {
        Preconditions.checkNotNull(txnState.getTxnCommitAttachment(), txnState);
        StreamingTaskTxnCommitAttachment attachment =
                (StreamingTaskTxnCommitAttachment) txnState.getTxnCommitAttachment();
        updateJobStatisticAndOffset(attachment);
        succeedTaskCount.incrementAndGet();
        // Persist offset during replay to keep offsetProviderPersist in sync
        persistOffsetProviderIfNeed();
    }

    public long getDbId() {
        if (dbId <= 0) {
            try {
                this.dbId = Env.getCurrentInternalCatalog().getDbOrAnalysisException(getCurrentDbName()).getId();
            } catch (AnalysisException e) {
                log.warn("failed to get db id for streaming insert job {}, db name: {}, msg: {}",
                        getJobId(), getCurrentDbName(), e.getMessage());
                failureReason = new FailureReason(InternalErrorCode.DB_ERR, "Failed to get db id, " + e.getMessage());
            }
        }
        return dbId;
    }

    public void replayOnCloudMode() throws JobException {
        Cloud.GetStreamingTaskCommitAttachRequest.Builder builder =
                Cloud.GetStreamingTaskCommitAttachRequest.newBuilder()
                        .setRequestIp(FrontendOptions.getLocalHostAddressCached());
        builder.setCloudUniqueId(Config.cloud_unique_id);
        builder.setDbId(getDbId());
        builder.setJobId(getJobId());

        Cloud.GetStreamingTaskCommitAttachResponse response = null;
        try {
            response = MetaServiceProxy.getInstance().getStreamingTaskCommitAttach(builder.build());
            if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                log.warn("failed to get streaming task commit attach, response: {}", response);
                if (response.getStatus().getCode() == Cloud.MetaServiceCode.STREAMING_JOB_PROGRESS_NOT_FOUND) {
                    log.warn("not found streaming job progress, response: {}", response);
                    return;
                } else {
                    throw new JobException(response.getStatus().getMsg());
                }
            }
        } catch (RpcException e) {
            log.info("failed to get streaming task commit attach {}", response, e);
            throw new JobException(e.getMessage());
        }

        StreamingTaskTxnCommitAttachment commitAttach =
                new StreamingTaskTxnCommitAttachment(response.getCommitAttach());
        updateCloudJobStatisticAndOffset(commitAttach);
    }

    @Override
    public void afterAborted(TransactionState txnState, boolean txnOperated, String txnStatusChangeReason)
            throws UserException {
    }

    @Override
    public void replayOnAborted(TransactionState txnState) {
    }

    @Override
    public void afterVisible(TransactionState txnState, boolean txnOperated) {
    }

    @Override
    public void replayOnVisible(TransactionState txnState) {
    }

    @Override
    public void gsonPostProcess() throws IOException {
        log.info("[KAFKA_OFFSET_DEBUG] gsonPostProcess started: jobId={}, tvfType={}, offsetProviderPersist={}",
                getJobId(), tvfType,
                offsetProviderPersist != null ? "has value (length=" + offsetProviderPersist.length() + ")" : "null");
        if (offsetProvider == null) {
            if (tvfType != null) {
                offsetProvider = SourceOffsetProviderFactory.createSourceOffsetProvider(tvfType);
                log.info("[KAFKA_OFFSET_DEBUG] gsonPostProcess: created offsetProvider of type {}",
                        offsetProvider.getClass().getSimpleName());
            } else {
                offsetProvider = new JdbcSourceOffsetProvider(getJobId(), dataSourceType, sourceProperties);
            }
        }

        if (jobProperties == null && properties != null) {
            jobProperties = new StreamingJobProperties(properties);
        }

        if (null == getSucceedTaskCount()) {
            setSucceedTaskCount(new AtomicLong(0));
        }
        if (null == getFailedTaskCount()) {
            setFailedTaskCount(new AtomicLong(0));
        }
        if (null == getCanceledTaskCount()) {
            setCanceledTaskCount(new AtomicLong(0));
        }

        if (null == streamInsertTaskQueue) {
            streamInsertTaskQueue = new ConcurrentLinkedQueue<>();
        }

        if (null == lock) {
            this.lock = new ReentrantReadWriteLock(true);
        }

        // Initialize Kafka partition task map
        if (null == runningKafkaPartitionTasks) {
            runningKafkaPartitionTasks = new ConcurrentHashMap<>();
        }

        // Initialize active partitions set
        if (null == activePartitions) {
            activePartitions = ConcurrentHashMap.newKeySet();
        }
        log.info("[KAFKA_OFFSET_DEBUG] gsonPostProcess completed: jobId={}", getJobId());
    }

    /**
     * The current streamingTask times out; create a new streamingTask.
     * Only applies to StreamingMultiTask.
     */
    public void processTimeoutTasks() throws JobException {
        if (!(runningStreamTask instanceof StreamingMultiTblTask)) {
            return;
        }
        writeLock();
        try {
            StreamingMultiTblTask runningMultiTask = (StreamingMultiTblTask) this.runningStreamTask;
            if (TaskStatus.RUNNING.equals(runningMultiTask.getStatus())
                    && runningMultiTask.isTimeout()) {
                String timeoutReason = runningMultiTask.getTimeoutReason();
                if (StringUtils.isEmpty(timeoutReason)) {
                    timeoutReason = "task failed cause timeout";
                }
                runningMultiTask.onFail(timeoutReason);
                // renew streaming task by auto resume
            }
        } finally {
            writeUnlock();
        }
    }

    public void commitOffset(CommitOffsetRequest offsetRequest) throws JobException {
        if (!(offsetProvider instanceof JdbcSourceOffsetProvider)) {
            throw new JobException("Unsupported commit offset for offset provider type: "
                    + offsetProvider.getClass().getSimpleName());
        }
        writeLock();
        try {
            if (this.runningStreamTask != null
                    && this.runningStreamTask instanceof StreamingMultiTblTask) {
                if (this.runningStreamTask.getTaskId() != offsetRequest.getTaskId()) {
                    throw new JobException("Task id mismatch when commit offset. expected: "
                            + this.runningStreamTask.getTaskId() + ", actual: " + offsetRequest.getTaskId());
                }
                updateNoTxnJobStatisticAndOffset(offsetRequest);
                if (offsetRequest.getScannedRows() == 0 && offsetRequest.getScannedBytes() == 0) {
                    JdbcSourceOffsetProvider op = (JdbcSourceOffsetProvider) offsetProvider;
                    op.setHasMoreData(false);
                }

                persistOffsetProviderIfNeed();
                ((StreamingMultiTblTask) this.runningStreamTask).successCallback(offsetRequest);
            }

        } finally {
            writeUnlock();
        }
    }

    private void persistOffsetProviderIfNeed() {
        log.info("[KAFKA_OFFSET_DEBUG] persistOffsetProviderIfNeed called: jobId={}", getJobId());
        // Persist offset for Kafka and JDBC sources
        this.offsetProviderPersist = offsetProvider.getPersistInfo();
        log.info("[KAFKA_OFFSET_DEBUG] persistOffsetProviderIfNeed: jobId={}, offsetProviderPersist={}",
                getJobId(), offsetProviderPersist != null ? "has value (length=" + offsetProviderPersist.length() + ")" : "null");
        if (this.offsetProviderPersist != null) {
            log.info("[KAFKA_OFFSET_DEBUG] persistOffsetProviderIfNeed: jobId={}, calling logUpdateOperation to persist",
                    getJobId());
            logUpdateOperation();
        } else {
            log.info("[KAFKA_OFFSET_DEBUG] persistOffsetProviderIfNeed: jobId={}, offsetProviderPersist is null, NOT persisting",
                    getJobId());
        }
    }

    public void replayOffsetProviderIfNeed() throws JobException {
        if (offsetProvider != null) {
            offsetProvider.replayIfNeed(this);
        }
    }

    /**
     * 1. Clean offset info in ms (s3 tvf)
     * 2. Clean chunk info in meta table (jdbc)
     */
    public void cleanup() throws JobException {
        log.info("cleanup streaming job {}", getJobId());
        // s3 tvf clean offset
        if (tvfType != null && Config.isCloudMode()) {
            Cloud.DeleteStreamingJobResponse resp = null;
            try {
                Cloud.DeleteStreamingJobRequest req = Cloud.DeleteStreamingJobRequest.newBuilder()
                        .setCloudUniqueId(Config.cloud_unique_id)
                        .setDbId(getDbId())
                        .setJobId(getJobId())
                        .build();
                resp = MetaServiceProxy.getInstance().deleteStreamingJob(req);
                if (resp.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                    log.warn("failed to delete streaming job, response: {}", resp);
                    throw new JobException("deleteJobKey failed for jobId=%s, dbId=%s, status=%s",
                            getJobId(), getJobId(), resp.getStatus());
                }
            } catch (RpcException e) {
                log.warn("failed to delete streaming job {}", resp, e);
            }
        }

        if (this.offsetProvider instanceof JdbcSourceOffsetProvider) {
            // jdbc clean chunk meta table
            ((JdbcSourceOffsetProvider) this.offsetProvider).cleanMeta(getJobId());
        }
    }
}
