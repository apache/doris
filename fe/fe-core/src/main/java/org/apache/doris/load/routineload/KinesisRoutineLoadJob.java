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

import org.apache.doris.analysis.ImportColumnDesc;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.InternalErrorCode;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.load.routineload.kinesis.KinesisConfiguration;
import org.apache.doris.load.routineload.kinesis.KinesisDataSourceProperties;
import org.apache.doris.nereids.load.NereidsImportColumnDesc;
import org.apache.doris.nereids.load.NereidsLoadTaskInfo;
import org.apache.doris.nereids.load.NereidsLoadUtils;
import org.apache.doris.nereids.load.NereidsRoutineLoadTaskInfo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.commands.AlterRoutineLoadCommand;
import org.apache.doris.nereids.trees.plans.commands.info.CreateRoutineLoadInfo;
import org.apache.doris.persist.AlterRoutineLoadJobOperationLog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.thrift.TPartialUpdateNewRowPolicy;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionStatus;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * KinesisRoutineLoadJob is a RoutineLoadJob that fetches data from AWS Kinesis streams.
 * 
 * Key concepts:
 * - Stream: Named collection of data records (similar to Kafka topic)
 * - Shard: Sequence of data records in a stream (similar to Kafka partition)
 * - Sequence Number: Unique identifier for each record within a shard (similar to Kafka offset)
 * - Consumer: Application that reads from a stream
 * 
 * The progress tracks sequence numbers for each shard, represented as:
 * {"shardId-000000000000": "49590338271490256608559692538361571095921575989136588802", ...}
 */
public class KinesisRoutineLoadJob extends RoutineLoadJob {
    private static final Logger LOG = LogManager.getLogger(KinesisRoutineLoadJob.class);

    public static final String KINESIS_FILE_CATALOG = "kinesis";

    /**
     * AWS region where the Kinesis stream is located.
     */
    @SerializedName("rg")
    private String region;

    /**
     * Name of the Kinesis stream to consume from.
     */
    @SerializedName("stm")
    private String stream;

    /**
     * Optional custom endpoint URL.
     */
    @SerializedName("ep")
    private String endpoint;

    /**
     * User-specified shards to consume from.
     * If empty, all shards will be discovered and consumed.
     */
    @SerializedName("csks")
    private List<String> customKinesisShards = Lists.newArrayList();

    /**
     * Current shards being consumed.
     * Updated periodically as shards may split or merge.
     */
    private List<String> currentKinesisShards = Lists.newArrayList();

    /**
     * Default starting position for new shards.
     * Values: TRIM_HORIZON, LATEST, or a timestamp string.
     */
    private String kinesisDefaultPosition = "";

    /**
     * Custom Kinesis properties including AWS credentials and client settings.
     */
    @SerializedName("prop")
    private Map<String, String> customProperties = Maps.newHashMap();
    private Map<String, String> convertedCustomProperties = Maps.newHashMap();

    /**
     * Cache of shard lag information (milliseconds behind latest).
     */
    private Map<String, Long> cachedShardWithMillsBehindLatest = Maps.newConcurrentMap();

    /**
     * Newly discovered shards from Kinesis.
     */
    private List<String> newCurrentKinesisShards = Lists.newArrayList();

    public KinesisRoutineLoadJob() {
        // For serialization
        super(-1, LoadDataSourceType.KINESIS);
    }

    public KinesisRoutineLoadJob(Long id, String name, long dbId, long tableId,
                                 String region, String stream, UserIdentity userIdentity) {
        super(id, name, dbId, tableId, LoadDataSourceType.KINESIS, userIdentity);
        this.region = region;
        this.stream = stream;
        this.progress = new KinesisProgress();
    }

    public KinesisRoutineLoadJob(Long id, String name, long dbId,
                                 String region, String stream,
                                 UserIdentity userIdentity, boolean isMultiTable) {
        super(id, name, dbId, LoadDataSourceType.KINESIS, userIdentity);
        this.region = region;
        this.stream = stream;
        this.progress = new KinesisProgress();
        setMultiTable(isMultiTable);
    }

    public String getRegion() {
        return region;
    }

    public String getStream() {
        return stream;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public Map<String, String> getConvertedCustomProperties() {
        return convertedCustomProperties;
    }

    @Override
    public void prepare() throws UserException {
        convertCustomProperties(true);
    }

    private void convertCustomProperties(boolean rebuild) throws DdlException {
        if (customProperties.isEmpty()) {
            return;
        }

        if (!rebuild && !convertedCustomProperties.isEmpty()) {
            return;
        }

        if (rebuild) {
            convertedCustomProperties.clear();
        }

        for (Map.Entry<String, String> entry : customProperties.entrySet()) {
            convertedCustomProperties.put(entry.getKey(), entry.getValue());
        }

        // Handle default position
        if (convertedCustomProperties.containsKey(KinesisConfiguration.KINESIS_DEFAULT_POSITION.getName())) {
            kinesisDefaultPosition = convertedCustomProperties.remove(
                    KinesisConfiguration.KINESIS_DEFAULT_POSITION.getName());
        }
    }

    private String convertedDefaultPosition() {
        if (this.kinesisDefaultPosition.isEmpty()) {
            return KinesisProgress.POSITION_LATEST;
        }
        return this.kinesisDefaultPosition;
    }

    @Override
    public void divideRoutineLoadJob(int currentConcurrentTaskNum) throws UserException {
        List<RoutineLoadTaskInfo> result = new ArrayList<>();
        writeLock();
        try {
            if (state == JobState.NEED_SCHEDULE) {
                // Divide shards into tasks
                for (int i = 0; i < currentConcurrentTaskNum; i++) {
                    Map<String, String> taskKinesisProgress = Maps.newHashMap();
                    for (int j = i; j < currentKinesisShards.size(); j = j + currentConcurrentTaskNum) {
                        String shardId = currentKinesisShards.get(j);
                        taskKinesisProgress.put(shardId,
                                ((KinesisProgress) progress).getSequenceNumberByShard(shardId));
                    }
                    KinesisTaskInfo kinesisTaskInfo = new KinesisTaskInfo(UUID.randomUUID(), id,
                            getTimeout() * 1000, taskKinesisProgress, isMultiTable(), -1, false);
                    routineLoadTaskInfoList.add(kinesisTaskInfo);
                    result.add(kinesisTaskInfo);
                }
                // Change job state to running
                if (!result.isEmpty()) {
                    unprotectUpdateState(JobState.RUNNING, null, false);
                }
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Ignore to divide routine load job while job state {}", state);
                }
            }
            // Save task into queue of needScheduleTasks
            Env.getCurrentEnv().getRoutineLoadTaskScheduler().addTasksInQueue(result);
        } finally {
            writeUnlock();
        }
    }

    @Override
    public int calculateCurrentConcurrentTaskNum() {
        int shardNum = currentKinesisShards.size();
        if (desireTaskConcurrentNum == 0) {
            desireTaskConcurrentNum = Config.max_routine_load_task_concurrent_num;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("current concurrent task number is min(shard num: {}, desire task concurrent num: {}, config: {})",
                    shardNum, desireTaskConcurrentNum, Config.max_routine_load_task_concurrent_num);
        }
        currentTaskConcurrentNum = Math.min(shardNum, Math.min(desireTaskConcurrentNum,
                Config.max_routine_load_task_concurrent_num));
        return currentTaskConcurrentNum;
    }

    @Override
    protected boolean checkCommitInfo(RLTaskTxnCommitAttachment rlTaskTxnCommitAttachment,
                                      TransactionState txnState,
                                      TransactionState.TxnStatusChangeReason txnStatusChangeReason) {
        if (txnState.getTransactionStatus() == TransactionStatus.COMMITTED
                || txnState.getTransactionStatus() == TransactionStatus.VISIBLE) {
            return true;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("no need to update the progress of kinesis routine load. txn status: {}, "
                            + "txnStatusChangeReason: {}, task: {}, job: {}",
                    txnState.getTransactionStatus(), txnStatusChangeReason,
                    DebugUtil.printId(rlTaskTxnCommitAttachment.getTaskId()), id);
        }
        return false;
    }

    private void updateProgressAndCache(RLTaskTxnCommitAttachment attachment) {
        ((KinesisProgress) attachment.getProgress()).getShardIdToSequenceNumber().forEach((shardId, seqNum) -> {
            // Update cached shard info if needed
        });
        this.progress.update(attachment);
    }

    @Override
    protected void updateProgress(RLTaskTxnCommitAttachment attachment) throws UserException {
        updateProgressAndCache(attachment);
        super.updateProgress(attachment);
    }

    @Override
    protected void replayUpdateProgress(RLTaskTxnCommitAttachment attachment) {
        super.replayUpdateProgress(attachment);
        updateProgressAndCache(attachment);
    }

    @Override
    protected RoutineLoadTaskInfo unprotectRenewTask(RoutineLoadTaskInfo routineLoadTaskInfo, boolean delaySchedule) {
        KinesisTaskInfo oldKinesisTaskInfo = (KinesisTaskInfo) routineLoadTaskInfo;
        // Add new task
        KinesisTaskInfo kinesisTaskInfo = new KinesisTaskInfo(oldKinesisTaskInfo,
                ((KinesisProgress) progress).getShardIdToSequenceNumber(oldKinesisTaskInfo.getShards()), isMultiTable());
        kinesisTaskInfo.setDelaySchedule(delaySchedule);
        // Remove old task
        routineLoadTaskInfoList.remove(routineLoadTaskInfo);
        // Add new task
        routineLoadTaskInfoList.add(kinesisTaskInfo);
        return kinesisTaskInfo;
    }

    @Override
    protected void unprotectUpdateProgress() throws UserException {
        updateNewShardProgress();
    }

    @Override
    protected boolean refreshKafkaPartitions(boolean needAutoResume) throws UserException {
        // For Kinesis, we refresh shards instead of Kafka partitions
        if (this.state == JobState.RUNNING || this.state == JobState.NEED_SCHEDULE || needAutoResume) {
            if (customKinesisShards != null && !customKinesisShards.isEmpty()) {
                return true;
            }
            return updateKinesisShards();
        }
        return true;
    }

    private boolean updateKinesisShards() throws UserException {
        try {
            this.newCurrentKinesisShards = getAllKinesisShards();
        } catch (Exception e) {
            String msg = e.getMessage()
                    + " may be Kinesis properties set in job is error"
                    + " or no shard in this stream that should check Kinesis";
            LOG.warn(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                    .add("error_msg", msg)
                    .build(), e);
            if (this.state == JobState.NEED_SCHEDULE) {
                unprotectUpdateState(JobState.PAUSED,
                        new ErrorReason(InternalErrorCode.PARTITIONS_ERR, msg),
                        false /* not replay */);
            }
            return false;
        }
        return true;
    }

    @Override
    protected boolean unprotectNeedReschedule() throws UserException {
        if (this.state == JobState.RUNNING || this.state == JobState.NEED_SCHEDULE) {
            return isKinesisShardsChanged();
        }
        return false;
    }

    private boolean isKinesisShardsChanged() throws UserException {
        if (CollectionUtils.isNotEmpty(customKinesisShards)) {
            currentKinesisShards = customKinesisShards;
            return false;
        }
        
        Preconditions.checkNotNull(this.newCurrentKinesisShards);
        if (new HashSet<>(currentKinesisShards).containsAll(this.newCurrentKinesisShards)) {
            if (currentKinesisShards.size() > this.newCurrentKinesisShards.size()) {
                currentKinesisShards = this.newCurrentKinesisShards;
                if (LOG.isDebugEnabled()) {
                    LOG.debug(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                            .add("current_kinesis_shards", Joiner.on(",").join(currentKinesisShards))
                            .add("msg", "current kinesis shards has been changed")
                            .build());
                }
                return true;
            } else {
                // Check if progress is consistent
                for (String shardId : currentKinesisShards) {
                    if (!((KinesisProgress) progress).containsShard(shardId)) {
                        return true;
                    }
                }
                return false;
            }
        } else {
            currentKinesisShards = this.newCurrentKinesisShards;
            if (LOG.isDebugEnabled()) {
                LOG.debug(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                        .add("current_kinesis_shards", Joiner.on(",").join(currentKinesisShards))
                        .add("msg", "current kinesis shards has been changed")
                        .build());
            }
            return true;
        }
    }

    @Override
    protected boolean needAutoResume() {
        writeLock();
        try {
            if (this.state == JobState.PAUSED) {
                return ScheduleRule.isNeedAutoSchedule(this);
            }
            return false;
        } finally {
            writeUnlock();
        }
    }

    @Override
    public String getStatistic() {
        Map<String, Object> summary = this.jobStatistic.summary();
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(summary);
    }

    /**
     * Get all shard IDs from the Kinesis stream.
     * This method should call Kinesis API to list shards.
     */
    private List<String> getAllKinesisShards() throws UserException {
        convertCustomProperties(false);
        // TODO: Implement Kinesis API call to list shards
        // For now, return the current shards or custom shards
        if (!customKinesisShards.isEmpty()) {
            return customKinesisShards;
        }
        // Placeholder: In actual implementation, this would call KinesisUtil.getAllKinesisShards()
        // return KinesisUtil.getAllKinesisShards(region, stream, endpoint, convertedCustomProperties);
        throw new UserException("Kinesis shard discovery not implemented yet. "
                + "Please specify shards explicitly using kinesis_shards property.");
    }

    /**
     * Create a KinesisRoutineLoadJob from CreateRoutineLoadInfo.
     */
    public static KinesisRoutineLoadJob fromCreateInfo(CreateRoutineLoadInfo info, ConnectContext ctx)
            throws UserException {
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(info.getDBName());

        long id = Env.getCurrentEnv().getNextId();
        KinesisDataSourceProperties kinesisProperties = 
                (KinesisDataSourceProperties) info.getDataSourceProperties();
        KinesisRoutineLoadJob kinesisRoutineLoadJob;

        if (kinesisProperties.isMultiTable()) {
            kinesisRoutineLoadJob = new KinesisRoutineLoadJob(id, info.getName(),
                    db.getId(),
                    kinesisProperties.getRegion(), kinesisProperties.getStream(),
                    ctx.getCurrentUserIdentity(), true);
        } else {
            OlapTable olapTable = db.getOlapTableOrDdlException(info.getTableName());
            checkMeta(olapTable, info.getRoutineLoadDesc());
            // Check load_to_single_tablet compatibility
            if (info.isLoadToSingleTablet()
                    && !(olapTable.getDefaultDistributionInfo() instanceof RandomDistributionInfo)) {
                throw new DdlException(
                        "if load_to_single_tablet set to true, the olap table must be with random distribution");
            }
            long tableId = olapTable.getId();
            kinesisRoutineLoadJob = new KinesisRoutineLoadJob(id, info.getName(),
                    db.getId(), tableId,
                    kinesisProperties.getRegion(), kinesisProperties.getStream(),
                    ctx.getCurrentUserIdentity());
        }

        kinesisRoutineLoadJob.setOptional(info);
        kinesisRoutineLoadJob.checkCustomProperties();

        return kinesisRoutineLoadJob;
    }

    private void checkCustomProperties() throws DdlException {
        // Validate custom properties if needed
    }

    private void updateNewShardProgress() throws UserException {
        try {
            for (String shardId : currentKinesisShards) {
                if (!((KinesisProgress) progress).containsShard(shardId)) {
                    List<String> newShards = Lists.newArrayList();
                    newShards.add(shardId);
                    List<Pair<String, String>> newShardPositions = getNewShardPositionsFromDefault(newShards);
                    Preconditions.checkState(newShardPositions.size() == 1);
                    for (Pair<String, String> shardPosition : newShardPositions) {
                        ((KinesisProgress) progress).addShardPosition(shardPosition);
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                                    .add("kinesis_shard_id", shardPosition.first)
                                    .add("begin_position", shardPosition.second)
                                    .add("msg", "The new shard has been added in job"));
                        }
                    }
                }
            }
        } catch (UserException e) {
            unprotectUpdateState(JobState.PAUSED,
                    new ErrorReason(InternalErrorCode.PARTITIONS_ERR, e.getMessage()), false);
            throw e;
        }
    }

    private List<Pair<String, String>> getNewShardPositionsFromDefault(List<String> newShards)
            throws UserException {
        List<Pair<String, String>> shardPositions = Lists.newArrayList();
        String defaultPosition = convertedDefaultPosition();
        for (String shardId : newShards) {
            shardPositions.add(Pair.of(shardId, defaultPosition));
        }
        return shardPositions;
    }

    protected void setOptional(CreateRoutineLoadInfo info) throws UserException {
        super.setOptional(info);
        KinesisDataSourceProperties kinesisDataSourceProperties =
                (KinesisDataSourceProperties) info.getDataSourceProperties();
        
        // Set endpoint if provided
        if (kinesisDataSourceProperties.getEndpoint() != null) {
            this.endpoint = kinesisDataSourceProperties.getEndpoint();
        }

        // Set custom shards and positions
        if (CollectionUtils.isNotEmpty(kinesisDataSourceProperties.getKinesisShardPositions())) {
            setCustomKinesisShards(kinesisDataSourceProperties);
        }
        
        // Set custom properties
        if (MapUtils.isNotEmpty(kinesisDataSourceProperties.getCustomKinesisProperties())) {
            setCustomKinesisProperties(kinesisDataSourceProperties.getCustomKinesisProperties());
        }
    }

    private void setCustomKinesisShards(KinesisDataSourceProperties kinesisDataSourceProperties) throws LoadException {
        List<Pair<String, String>> shardPositions = kinesisDataSourceProperties.getKinesisShardPositions();
        for (Pair<String, String> shardPosition : shardPositions) {
            this.customKinesisShards.add(shardPosition.first);
            ((KinesisProgress) progress).addShardPosition(shardPosition);
        }
    }

    private void setCustomKinesisProperties(Map<String, String> kinesisProperties) {
        this.customProperties = kinesisProperties;
    }

    @Override
    public String dataSourcePropertiesJsonToString() {
        Map<String, String> dataSourceProperties = Maps.newHashMap();
        dataSourceProperties.put("region", region);
        dataSourceProperties.put("stream", stream);
        if (endpoint != null) {
            dataSourceProperties.put("endpoint", endpoint);
        }
        List<String> sortedShards = Lists.newArrayList(currentKinesisShards);
        Collections.sort(sortedShards);
        dataSourceProperties.put("currentKinesisShards", Joiner.on(",").join(sortedShards));
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(dataSourceProperties);
    }

    @Override
    public String customPropertiesJsonToString() {
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        // Mask sensitive information
        Map<String, String> maskedProperties = new HashMap<>(customProperties);
        if (maskedProperties.containsKey(KinesisConfiguration.KINESIS_SECRET_KEY.getName())) {
            maskedProperties.put(KinesisConfiguration.KINESIS_SECRET_KEY.getName(), "******");
        }
        if (maskedProperties.containsKey(KinesisConfiguration.KINESIS_SESSION_TOKEN.getName())) {
            maskedProperties.put(KinesisConfiguration.KINESIS_SESSION_TOKEN.getName(), "******");
        }
        return gson.toJson(maskedProperties);
    }

    @Override
    public Map<String, String> getDataSourceProperties() {
        Map<String, String> dataSourceProperties = Maps.newHashMap();
        dataSourceProperties.put("kinesis_region", region);
        dataSourceProperties.put("kinesis_stream", stream);
        if (endpoint != null) {
            dataSourceProperties.put("kinesis_endpoint", endpoint);
        }
        return dataSourceProperties;
    }

    @Override
    public Map<String, String> getCustomProperties() {
        Map<String, String> ret = new HashMap<>();
        customProperties.forEach((k, v) -> {
            // Mask sensitive values
            if (k.equals(KinesisConfiguration.KINESIS_SECRET_KEY.getName())
                    || k.equals(KinesisConfiguration.KINESIS_SESSION_TOKEN.getName())) {
                ret.put("property." + k, "******");
            } else {
                ret.put("property." + k, v);
            }
        });
        return ret;
    }

    @Override
    public void modifyProperties(AlterRoutineLoadCommand command) throws UserException {
        Map<String, String> jobProperties = command.getAnalyzedJobProperties();
        KinesisDataSourceProperties dataSourceProperties = 
                (KinesisDataSourceProperties) command.getDataSourceProperties();

        writeLock();
        try {
            if (getState() != JobState.PAUSED) {
                throw new DdlException("Only supports modification of PAUSED jobs");
            }

            modifyPropertiesInternal(jobProperties, dataSourceProperties);

            AlterRoutineLoadJobOperationLog log = new AlterRoutineLoadJobOperationLog(this.id,
                    jobProperties, dataSourceProperties);
            Env.getCurrentEnv().getEditLog().logAlterRoutineLoadJob(log);
        } finally {
            writeUnlock();
        }
    }

    private void modifyPropertiesInternal(Map<String, String> jobProperties,
                                          KinesisDataSourceProperties dataSourceProperties)
            throws DdlException {
        if (dataSourceProperties != null) {
            List<Pair<String, String>> shardPositions = Lists.newArrayList();
            Map<String, String> customKinesisProperties = Maps.newHashMap();

            if (MapUtils.isNotEmpty(dataSourceProperties.getOriginalDataSourceProperties())) {
                shardPositions = dataSourceProperties.getKinesisShardPositions();
                customKinesisProperties = dataSourceProperties.getCustomKinesisProperties();
            }

            // Update custom properties
            if (!customKinesisProperties.isEmpty()) {
                this.customProperties.putAll(customKinesisProperties);
                convertCustomProperties(true);
            }

            // Check and modify shard positions
            if (!shardPositions.isEmpty()) {
                ((KinesisProgress) progress).checkShards(shardPositions);
                ((KinesisProgress) progress).modifyPosition(shardPositions);
            }

            // Modify stream if provided
            if (!Strings.isNullOrEmpty(dataSourceProperties.getStream())) {
                this.stream = dataSourceProperties.getStream();
                this.progress = new KinesisProgress();
            }

            // Modify region if provided
            if (!Strings.isNullOrEmpty(dataSourceProperties.getRegion())) {
                this.region = dataSourceProperties.getRegion();
            }

            // Modify endpoint if provided
            if (!Strings.isNullOrEmpty(dataSourceProperties.getEndpoint())) {
                this.endpoint = dataSourceProperties.getEndpoint();
            }
        }

        if (!jobProperties.isEmpty()) {
            Map<String, String> copiedJobProperties = Maps.newHashMap(jobProperties);
            modifyCommonJobProperties(copiedJobProperties);
            this.jobProperties.putAll(copiedJobProperties);
            if (jobProperties.containsKey(CreateRoutineLoadInfo.PARTIAL_COLUMNS)) {
                this.isPartialUpdate = BooleanUtils.toBoolean(jobProperties.get(CreateRoutineLoadInfo.PARTIAL_COLUMNS));
            }
            if (jobProperties.containsKey(CreateRoutineLoadInfo.PARTIAL_UPDATE_NEW_KEY_POLICY)) {
                String policy = jobProperties.get(CreateRoutineLoadInfo.PARTIAL_UPDATE_NEW_KEY_POLICY);
                if ("ERROR".equalsIgnoreCase(policy)) {
                    this.partialUpdateNewKeyPolicy = TPartialUpdateNewRowPolicy.ERROR;
                } else {
                    this.partialUpdateNewKeyPolicy = TPartialUpdateNewRowPolicy.APPEND;
                }
            }
        }
        LOG.info("modify the properties of kinesis routine load job: {}, jobProperties: {}, datasource properties: {}",
                this.id, jobProperties, dataSourceProperties);
    }

    @Override
    public void replayModifyProperties(AlterRoutineLoadJobOperationLog log) {
        try {
            modifyPropertiesInternal(log.getJobProperties(), 
                    (KinesisDataSourceProperties) log.getDataSourceProperties());
        } catch (DdlException e) {
            LOG.error("failed to replay modify kinesis routine load job: {}", id, e);
        }
    }

    @Override
    public String getLag() {
        Map<String, Long> shardIdToLag = ((KinesisProgress) progress).getLag(cachedShardWithMillsBehindLatest);
        Gson gson = new Gson();
        return gson.toJson(shardIdToLag);
    }

    @Override
    public TFileCompressType getCompressType() {
        return TFileCompressType.PLAIN;
    }

    @Override
    public double getMaxFilterRatio() {
        return maxFilterRatio;
    }

    @Override
    public Long totalProgress() {
        return ((KinesisProgress) progress).totalProgress();
    }

    @Override
    public Long totalLag() {
        Map<String, Long> shardIdToLag = ((KinesisProgress) progress).getLag(cachedShardWithMillsBehindLatest);
        return shardIdToLag.values().stream()
                .filter(lag -> lag >= 0)
                .mapToLong(v -> v)
                .sum();
    }

    /**
     * Check if there is more data to consume from Kinesis shards.
     * 
     * For Kinesis, we always return true as a simple implementation,
     * since Kinesis doesn't provide an easy way to check if there's more data
     * without actually trying to consume it. The actual data availability
     * is handled during consumption with GetRecords API which returns
     * MillisBehindLatest to indicate how far behind the consumer is.
     * 
     * @param taskId The task ID
     * @param shardIdToSequenceNumber Map of shard IDs to sequence numbers
     * @return true if there may be more data to consume
     * @throws UserException if an error occurs
     */
    public boolean hasMoreDataToConsume(UUID taskId, Map<String, String> shardIdToSequenceNumber) 
            throws UserException {
        // For Kinesis, we optimistically return true and let the BE handle
        // the actual data availability check during consumption.
        // The BE will use GetRecords API which returns MillisBehindLatest
        // to determine if there's more data.
        // 
        // A more sophisticated implementation could call DescribeStream
        // or GetShardIterator to check shard status, but this adds latency.
        return true;
    }

    @Override
    public NereidsRoutineLoadTaskInfo toNereidsRoutineLoadTaskInfo() throws UserException {
        Expression deleteCondition = getDeleteCondition() != null
                ? NereidsLoadUtils.parseExpressionSeq(getDeleteCondition().toSql()).get(0)
                : null;
        Expression precedingFilter = getPrecedingFilter() != null
                ? NereidsLoadUtils.parseExpressionSeq(getPrecedingFilter().toSql()).get(0)
                : null;
        Expression whereExpr = getWhereExpr() != null
                ? NereidsLoadUtils.parseExpressionSeq(getWhereExpr().toSqlWithoutTbl()).get(0)
                : null;
        NereidsLoadTaskInfo.NereidsImportColumnDescs importColumnDescs = null;
        if (columnDescs != null) {
            importColumnDescs = new NereidsLoadTaskInfo.NereidsImportColumnDescs();
            for (ImportColumnDesc desc : columnDescs.descs) {
                Expression expression = desc.getExpr() != null
                        ? NereidsLoadUtils.parseExpressionSeq(desc.getExpr().toSqlWithoutTbl()).get(0)
                        : null;
                importColumnDescs.descs.add(new NereidsImportColumnDesc(desc.getColumnName(), expression));
            }
        }
        return new NereidsRoutineLoadTaskInfo(execMemLimit, new HashMap<>(jobProperties), maxBatchIntervalS,
                partitionNamesInfo, mergeType, deleteCondition, sequenceCol, maxFilterRatio, importColumnDescs,
                precedingFilter, whereExpr, columnSeparator, lineDelimiter, enclose, escape, sendBatchParallelism,
                loadToSingleTablet, isPartialUpdate, partialUpdateNewKeyPolicy, memtableOnSinkNode);
    }

    @Override
    public void updateCloudProgress() throws UserException {
        // Cloud mode not supported for Kinesis yet
    }

    @Override
    protected void updateCloudProgress(RLTaskTxnCommitAttachment attachment) {
        // Cloud mode not supported for Kinesis yet
    }
}
