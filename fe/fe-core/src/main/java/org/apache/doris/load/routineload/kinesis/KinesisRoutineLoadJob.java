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

package org.apache.doris.load.routineload.kinesis;

import org.apache.doris.analysis.ExprToSqlVisitor;
import org.apache.doris.analysis.ImportColumnDesc;
import org.apache.doris.analysis.ToSqlParams;
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
import org.apache.doris.datasource.kinesis.KinesisUtil;
import org.apache.doris.load.routineload.ErrorReason;
import org.apache.doris.load.routineload.LoadDataSourceType;
import org.apache.doris.load.routineload.RLTaskTxnCommitAttachment;
import org.apache.doris.load.routineload.RoutineLoadJob;
import org.apache.doris.load.routineload.RoutineLoadTaskInfo;
import org.apache.doris.load.routineload.ScheduleRule;
import org.apache.doris.nereids.load.NereidsImportColumnDesc;
import org.apache.doris.nereids.load.NereidsLoadTaskInfo;
import org.apache.doris.nereids.load.NereidsLoadUtils;
import org.apache.doris.nereids.load.NereidsRoutineLoadTaskInfo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.commands.AlterRoutineLoadCommand;
import org.apache.doris.nereids.trees.plans.commands.info.CreateRoutineLoadInfo;
import org.apache.doris.persist.AlterRoutineLoadJobOperationLog;
import org.apache.doris.persist.KinesisLatestPositionOperation;
import org.apache.doris.persist.KinesisShardTopologyOperation;
import org.apache.doris.proto.InternalService;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.thrift.TPartialUpdateNewRowPolicy;
import org.apache.doris.thrift.TStatusCode;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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
    private static final String SENSITIVE_PROPERTY_MASK = "******";

    public static final String KINESIS_FILE_CATALOG = "kinesis";

    @SerializedName("rg")
    private String region;
    @SerializedName("stm")
    private String stream;
    @SerializedName("ep")
    private String endpoint;

    // optional, user want to load shards(Kafka's cskp).
    @SerializedName("csks")
    private List<String> customKinesisShards = Lists.newArrayList();

    @SerializedName("topo")
    private KinesisShardTopology shardTopology = new KinesisShardTopology();

    // Default starting position for new shards.
    // Values: TRIM_HORIZON, LATEST, or a timestamp string.
    private String kinesisDefaultPosition = "";

    // custom Kinesis properties including AWS credentials and client settings.
    @SerializedName("prop")
    private Map<String, String> customProperties = Maps.newHashMap();
    private Map<String, String> convertedCustomProperties = Maps.newHashMap();

    // The latest offset of each partition fetched from kinesis server.
    // Will be updated periodically by calling hasMoreDataToConsume()
    private Map<String, Long> cachedShardWithMillsBehindLatest = Maps.newConcurrentMap();

    // Compatibility views for SHOW/old unit fixtures. Topology remains the only source of truth.
    private transient List<String> openKinesisShards = Lists.newArrayList();
    private transient List<String> closedKinesisShards = Lists.newArrayList();
    private transient List<String> newCurrentKinesisShards;
    // Newly discovered shard descriptors from Kinesis. This is a transient scan result; the
    // durable topology is merged under the job lock before task scheduling.
    private transient List<InternalService.PShardInfo> newCurrentKinesisShardInfos;
    private transient long sourceGeneration;

    // A tail scan belongs to job preparation, before task creation and beginTxn.
    private transient Future<InternalService.PProxyResult> latestSequenceFetch;
    private transient Set<String> latestSequenceShards = Collections.emptySet();
    private transient long latestSequenceDeadlineNs;

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

    @Override
    public void gsonPostProcess() throws IOException {
        super.gsonPostProcess();
        try {
            convertCustomProperties(true);
        } catch (DdlException e) {
            throw new IOException("Failed to restore Kinesis properties", e);
        }
        if (shardTopology == null) {
            shardTopology = new KinesisShardTopology();
        }
        Map<String, String> concretePositions = new HashMap<>();
        for (Map.Entry<String, String> entry : ((KinesisProgress) progress)
                .getShardIdToSequenceNumber().entrySet()) {
            String position = entry.getValue();
            if (position != null && !KinesisShardTopology.isLatest(position)) {
                concretePositions.put(entry.getKey(), position);
            }
        }
        shardTopology.reconcileConcretePositions(concretePositions);
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

    private void refreshShardViews() {
        if (!shardTopology.getNodes().isEmpty()) {
            openKinesisShards = shardTopology.getOpenShardIds();
            closedKinesisShards = shardTopology.getClosedShardIds();
        }
    }

    private List<String> getOpenShardView() {
        return shardTopology.getNodes().isEmpty()
                ? new ArrayList<>(openKinesisShards) : shardTopology.getOpenShardIds();
    }

    private List<String> getClosedShardView() {
        return shardTopology.getNodes().isEmpty()
                ? new ArrayList<>(closedKinesisShards) : shardTopology.getClosedShardIds();
    }

    @Override
    public void prepare() throws UserException {
        // should reset converted properties each time the job being prepared.
        // because the file info can be changed anytime.
        writeLock();
        try {
            convertCustomProperties(true);
            if (state != JobState.NEED_SCHEDULE || !shardTopology.isInitialSnapshotFinalized()) {
                return;
            }
            if (latestSequenceFetch == null) {
                Set<String> shards = getUnresolvedLatestShards();
                if (shards.isEmpty()) {
                    return;
                }
                int timeout = Config.kinesis_latest_sequence_timeout_second;
                if (timeout != -1 && timeout <= 0) {
                    throw new LoadException("kinesis_latest_sequence_timeout_second must be -1 or positive");
                }
                latestSequenceDeadlineNs = timeout == -1 ? 0
                        : System.nanoTime() + TimeUnit.SECONDS.toNanos(timeout);
                latestSequenceFetch = KinesisUtil.getLatestSequenceNumbersAsync(
                        region, stream, endpoint, convertedCustomProperties, shards, timeout);
                latestSequenceShards = shards;
                LOG.info("Resolving initial Kinesis LATEST positions, job: {}, shards: {}", id, shards);
            }
            if (latestSequenceDeadlineNs != 0 && System.nanoTime() - latestSequenceDeadlineNs >= 0) {
                resetLatestSequenceFetch();
                throw new LoadException("Kinesis latest sequence scan timed out before reaching the shard tips");
            }
            if (!latestSequenceFetch.isDone()) {
                return;
            }
            try {
                InternalService.PProxyResult result = latestSequenceFetch.get();
                if (result.getStatus().getStatusCode() != TStatusCode.OK.getValue()) {
                    throw new LoadException("Kinesis latest sequence scan failed: "
                            + result.getStatus().getErrorMsgsList());
                }
                Map<String, String> positions = result.getKinesisMetaResult().getShardLatestSequencesMap();
                if (!positions.keySet().equals(latestSequenceShards)) {
                    throw new LoadException("BE did not return all requested Kinesis latest positions");
                }
                for (String position : positions.values()) {
                    if (!position.matches("[0-9]+") && !KinesisProgress.POSITION_TRIM_HORIZON.equals(position)) {
                        throw new LoadException("Invalid resolved Kinesis position: " + position);
                    }
                }
                KinesisLatestPositionOperation operation = new KinesisLatestPositionOperation(id, positions);
                // A task must never observe these positions before the journal write succeeds.
                Env.getCurrentEnv().getEditLog().logKinesisLatestPosition(operation);
                replayLatestPosition(operation);
                LOG.info("Resolved initial Kinesis positions, job: {}, positions: {}", id, positions);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new LoadException("Interrupted while resolving Kinesis latest positions");
            } catch (ExecutionException | CancellationException e) {
                throw new LoadException("Failed to resolve Kinesis latest positions: " + e.getMessage());
            } finally {
                resetLatestSequenceFetch();
            }
        } finally {
            writeUnlock();
        }
    }

    private Set<String> getUnresolvedLatestShards() {
        return new HashSet<>(shardTopology.getUnresolvedLatestShardIds());
    }

    @Override
    protected void unprotectUpdateState(JobState jobState, ErrorReason reason, boolean isReplay) throws UserException {
        super.unprotectUpdateState(jobState, reason, isReplay);
        if (jobState == JobState.PAUSED || jobState.isFinalState()) {
            resetLatestSequenceFetch();
        }
    }

    private void resetLatestSequenceFetch() {
        if (latestSequenceFetch != null) {
            latestSequenceFetch.cancel(true);
            latestSequenceFetch = null;
        }
        latestSequenceShards = Collections.emptySet();
    }

    public void replayLatestPosition(KinesisLatestPositionOperation operation) {
        writeLock();
        try {
            shardTopology.resolveInitialPositions(operation.getShardPositions());
            refreshShardViews();
            operation.getShardPositions().forEach((shard, position) -> {
                String current = ((KinesisProgress) progress).getSequenceNumberByShard(shard);
                if (current == null || KinesisProgress.POSITION_LATEST.equalsIgnoreCase(current)
                        || KinesisProgress.LATEST_VAL.equals(current)) {
                    ((KinesisProgress) progress).addShardPosition(Pair.of(shard, position));
                }
            });
        } finally {
            writeUnlock();
        }
    }

    public void replayShardTopology(KinesisShardTopologyOperation operation) {
        writeLock();
        try {
            shardTopology.mergeShardInfos(operation.getShardInfos(), operation.getDefaultPosition(),
                    operation.getInitialPositions());
            refreshShardViews();
            updateNewShardProgress();
        } finally {
            writeUnlock();
        }
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
        if (convertedCustomProperties.containsKey("kinesis_default_pos")) {
            kinesisDefaultPosition = convertedCustomProperties.get("kinesis_default_pos");
            // Keep it in convertedCustomProperties so BE can use it
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
                if (!shardTopology.isInitialSnapshotFinalized() || !getUnresolvedLatestShards().isEmpty()
                        || shardTopology.getLineageError() != null) {
                    // prepare() will collect the scan result on a later scheduler round.
                    return;
                }
                List<String> allShards = shardTopology.getReadyShardIds();

                currentConcurrentTaskNum = Math.min(currentConcurrentTaskNum, allShards.size());
                // Divide only ready shards, including confirmed children whose parents still drain.
                for (int i = 0; i < currentConcurrentTaskNum; i++) {
                    Map<String, String> taskKinesisProgress = Maps.newHashMap();
                    for (int j = i; j < allShards.size(); j = j + currentConcurrentTaskNum) {
                        String shardId = allShards.get(j);
                        String position = ((KinesisProgress) progress).getSequenceNumberByShard(shardId);
                        if (position == null) {
                            position = shardTopology.getStartPosition(shardId);
                        }
                        Preconditions.checkNotNull(position,
                                "Missing Kinesis start position for shard " + shardId);
                        taskKinesisProgress.put(shardId, position);
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
        writeLock();
        try {
            int shardNum = shardTopology.getNodes().isEmpty()
                    ? openKinesisShards.size() + closedKinesisShards.size()
                    : shardTopology.getReadyShardIds().size();
            if (desireTaskConcurrentNum == 0) {
                desireTaskConcurrentNum = Config.max_routine_load_task_concurrent_num;
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("current concurrent task number is min"
                                + "(shard num: {}, desire task concurrent num: {}, config: {})",
                        shardNum, desireTaskConcurrentNum,
                        Config.max_routine_load_task_concurrent_num);
            }
            currentTaskConcurrentNum = Math.min(shardNum, Math.min(desireTaskConcurrentNum,
                    Config.max_routine_load_task_concurrent_num));
            return currentTaskConcurrentNum;
        } finally {
            writeUnlock();
        }
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

    private void updateProgressAndOffsetsCache(RLTaskTxnCommitAttachment attachment) {
        // Kept for existing FE unit fixtures; production transaction callbacks always pass txnId.
        long txnId = Long.MIN_VALUE;
        updateProgressAndOffsetsCache(attachment, txnId);
        shardTopology.completeVisibleShards(txnId);
        refreshShardViews();
    }

    private void updateProgressAndOffsetsCache(RLTaskTxnCommitAttachment attachment, long txnId) {
        KinesisProgress taskProgress = (KinesisProgress) attachment.getProgress();
        if (customKinesisShards.isEmpty()) {
            shardTopology.mergeChildShardInfos(taskProgress.getChildShardParentIds());
            refreshShardViews();
        }
        taskProgress.getShardIdToMillsBehindLatest().forEach(cachedShardWithMillsBehindLatest::put);
        for (String shardId : taskProgress.getClosedShardIds()) {
            shardTopology.markEndCommitted(shardId, txnId);
            cachedShardWithMillsBehindLatest.remove(shardId);
        }
        progress.update(attachment);
        updateNewShardProgress();
    }

    @Override
    public void afterVisible(TransactionState txnState, boolean txnOperated) {
        writeLock();
        try {
            if (txnOperated) {
                shardTopology.completeVisibleShards(txnState.getTransactionId());
                refreshShardViews();
                updateNewShardProgress();
            }
            super.afterVisible(txnState, txnOperated);
        } finally {
            writeUnlock();
        }
    }

    @Override
    public void replayOnCommitted(TransactionState txnState) {
        writeLock();
        try {
            super.replayOnCommitted(txnState);
        } finally {
            writeUnlock();
        }
    }

    @Override
    public void replayOnVisible(TransactionState txnState) {
        writeLock();
        try {
            shardTopology.completeVisibleShards(txnState.getTransactionId());
            refreshShardViews();
            updateNewShardProgress();
        } finally {
            writeUnlock();
        }
    }

    @Override
    protected void updateProgress(RLTaskTxnCommitAttachment attachment, TransactionState txnState)
            throws UserException {
        updateProgressAndOffsetsCache(attachment, txnState.getTransactionId());
        super.updateProgress(attachment);
    }

    @Override
    protected void replayUpdateProgress(RLTaskTxnCommitAttachment attachment, TransactionState txnState) {
        super.replayUpdateProgress(attachment);
        updateProgressAndOffsetsCache(attachment, txnState.getTransactionId());
    }

    @Override
    protected RoutineLoadTaskInfo unprotectRenewTask(RoutineLoadTaskInfo oldTask, boolean delaySchedule) {
        Set<String> assignedShards = new HashSet<>();
        for (RoutineLoadTaskInfo taskInfo : routineLoadTaskInfoList) {
            if (taskInfo != oldTask) {
                assignedShards.addAll(((KinesisTaskInfo) taskInfo).getShards());
            }
        }
        ConcurrentMap<String, String> shardPositions = Maps.newConcurrentMap();
        for (String shardId : shardTopology.getReadyShardIds()) {
            if (!assignedShards.contains(shardId)) {
                String position = ((KinesisProgress) progress).getSequenceNumberByShard(shardId);
                shardPositions.put(shardId, position == null ? shardTopology.getStartPosition(shardId) : position);
            }
        }
        routineLoadTaskInfoList.remove(oldTask);
        if (shardPositions.isEmpty()) {
            return null;
        }
        KinesisTaskInfo task = new KinesisTaskInfo((KinesisTaskInfo) oldTask, shardPositions, isMultiTable());
        task.setDelaySchedule(delaySchedule);
        routineLoadTaskInfoList.add(task);
        return task;
    }

    @Override
    protected void unprotectUpdateProgress() throws UserException {
        updateNewShardProgress();
    }

    @Override
    protected boolean refreshKafkaPartitions(boolean needAutoResume) throws UserException {
        // For Kinesis, we refresh shards instead of Kafka partitions
        if (this.state == JobState.RUNNING || this.state == JobState.NEED_SCHEDULE || needAutoResume) {
            return updateKinesisShards();
        }
        return true;
    }

    private boolean updateKinesisShards() throws UserException {
        String scanRegion;
        String scanStream;
        String scanEndpoint;
        Map<String, String> properties;
        long generation;
        writeLock();
        try {
            convertCustomProperties(true);
            scanRegion = region;
            scanStream = stream;
            scanEndpoint = endpoint;
            properties = new HashMap<>(convertedCustomProperties);
            generation = sourceGeneration;
            newCurrentKinesisShardInfos = null;
        } finally {
            writeUnlock();
        }
        try {
            List<InternalService.PShardInfo> infos = KinesisUtil.getAllKinesisShardInfos(
                    scanRegion, scanStream, scanEndpoint, properties);
            writeLock();
            try {
                if (generation != sourceGeneration) {
                    return false;
                }
                newCurrentKinesisShardInfos = infos;
            } finally {
                writeUnlock();
            }
            return true;
        } catch (Exception e) {
            writeLock();
            try {
                if (generation == sourceGeneration && state == JobState.NEED_SCHEDULE) {
                    unprotectUpdateState(JobState.PAUSED,
                            new ErrorReason(InternalErrorCode.PARTITIONS_ERR, e.getMessage()), false);
                }
            } finally {
                writeUnlock();
            }
            LOG.warn("Failed to discover Kinesis shards, job: {}", id, e);
            return false;
        }
    }

    @Override
    protected boolean unprotectNeedReschedule() throws UserException {
        if (this.state == JobState.RUNNING || this.state == JobState.NEED_SCHEDULE) {
            return isKinesisShardsChanged();
        }
        return false;
    }

    private boolean isKinesisShardsChanged() throws UserException {
        boolean legacyScan = newCurrentKinesisShardInfos == null && newCurrentKinesisShards != null;
        if (legacyScan) {
            newCurrentKinesisShardInfos = new ArrayList<>();
            for (String shardId : newCurrentKinesisShards) {
                newCurrentKinesisShardInfos.add(InternalService.PShardInfo.newBuilder().setShardId(shardId).build());
            }
            for (String shardId : openKinesisShards) {
                if (!newCurrentKinesisShards.contains(shardId)) {
                    newCurrentKinesisShardInfos.add(InternalService.PShardInfo.newBuilder()
                            .setShardId(shardId).setClosed(true).build());
                }
            }
        }
        if (newCurrentKinesisShardInfos == null) {
            return false;
        }
        List<InternalService.PShardInfo> infos = new ArrayList<>(newCurrentKinesisShardInfos);
        if (!customKinesisShards.isEmpty()) {
            infos.removeIf(info -> !customKinesisShards.contains(info.getShardId()));
            if (infos.size() != customKinesisShards.size()) {
                unprotectUpdateState(JobState.PAUSED, new ErrorReason(InternalErrorCode.CANNOT_RESUME_ERR,
                        "Some explicitly selected Kinesis shards are missing from ListShards"), false);
                return false;
            }
        }
        KinesisShardTopologyOperation operation = new KinesisShardTopologyOperation(id, infos,
                convertedDefaultPosition(), ((KinesisProgress) progress).getShardIdToSequenceNumber());
        KinesisShardTopology candidate = shardTopology.copy();
        candidate.mergeShardInfos(infos, operation.getDefaultPosition(), operation.getInitialPositions());
        if (!candidate.toJson().equals(shardTopology.toJson())) {
            // Persist discovery before exposing new scheduling candidates or resolving LATEST.
            Env.getCurrentEnv().getEditLog().logKinesisShardTopology(operation);
            shardTopology = candidate;
            updateNewShardProgress();
        }
        if (shardTopology.getLineageError() != null) {
            unprotectUpdateState(JobState.PAUSED,
                    new ErrorReason(InternalErrorCode.CANNOT_RESUME_ERR, shardTopology.getLineageError()), false);
            return false;
        }
        Set<String> assignedShards = new HashSet<>();
        for (RoutineLoadTaskInfo task : routineLoadTaskInfoList) {
            assignedShards.addAll(((KinesisTaskInfo) task).getShards());
        }
        return !assignedShards.equals(new HashSet<>(shardTopology.getReadyShardIds()));
    }

    @Override
    protected boolean needAutoResume() {
        writeLock();
        try {
            if (this.state == JobState.PAUSED && shardTopology.getLineageError() == null) {
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
        readLock();
        try {
            summary.put("openShardNum", getOpenShardView().size());
            summary.put("closedShardNum", getClosedShardView().size());
            summary.put("trackedShardNum", ((KinesisProgress) progress).getShardIdToSequenceNumber().size());
            summary.put("cachedMillisBehindLatestShardNum", cachedShardWithMillsBehindLatest.size());
            summary.put("totalMillisBehindLatest", totalLag());
            long maxMillisBehindLatest = cachedShardWithMillsBehindLatest.values().stream()
                    .filter(lag -> lag >= 0)
                    .mapToLong(v -> v)
                    .max()
                    .orElse(-1L);
            summary.put("maxMillisBehindLatest", maxMillisBehindLatest);
        } finally {
            readUnlock();
        }
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(summary);
    }

    /**
     * Create a KinesisRoutineLoadJob from CreateRoutineLoadInfo.
     */
    public static KinesisRoutineLoadJob fromCreateInfo(CreateRoutineLoadInfo info, ConnectContext ctx)
            throws UserException {
        if (Config.isCloudMode()) {
            throw new DdlException("Kinesis routine load does not support cloud mode");
        }
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

    private void updateNewShardProgress() {
        for (KinesisShardTopology.ShardNode node : shardTopology.getNodes().values()) {
            String shardId = node.getShardId();
            if (node.isConsumptionFinished()) {
                continue;
            }
            if (node.getInitialStartPosition() == null) {
                continue;
            }
            if (!((KinesisProgress) progress).containsShard(shardId)) {
                ((KinesisProgress) progress).addShardPosition(
                        Pair.of(shardId, node.getInitialStartPosition()));
                if (LOG.isDebugEnabled()) {
                    LOG.debug(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                            .add("kinesis_shard_id", shardId)
                            .add("begin_position", node.getInitialStartPosition())
                            .add("msg", "The new shard has been added in job"));
                }
            }
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
        List<String> sortedOpenShards = getOpenShardView();
        Collections.sort(sortedOpenShards);
        dataSourceProperties.put("openKinesisShards", Joiner.on(",").join(sortedOpenShards));

        List<String> sortedClosedShards = getClosedShardView();
        Collections.sort(sortedClosedShards);
        dataSourceProperties.put("closedKinesisShards", Joiner.on(",").join(sortedClosedShards));

        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(dataSourceProperties);
    }

    @Override
    public String customPropertiesJsonToString() {
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(getMaskedCustomProperties(""));
    }

    @Override
    public Map<String, String> getDataSourceProperties() {
        Map<String, String> dataSourceProperties = Maps.newHashMap();
        dataSourceProperties.put(KinesisConfiguration.KINESIS_REGION.getName(), region);
        dataSourceProperties.put("kinesis_stream", stream);
        if (endpoint != null) {
            dataSourceProperties.put("kinesis_endpoint", endpoint);
        }
        return dataSourceProperties;
    }

    @Override
    public Map<String, String> getCustomProperties() {
        return getMaskedCustomProperties("property.");
    }

    private Map<String, String> getMaskedCustomProperties(String keyPrefix) {
        Map<String, String> maskedProperties = new HashMap<>();
        customProperties.forEach((key, value) -> {
            if (KinesisConfiguration.KINESIS_ACCESS_KEY.getName().equalsIgnoreCase(key)
                    || KinesisConfiguration.KINESIS_SECRET_KEY.getName().equalsIgnoreCase(key)
                    || KinesisConfiguration.KINESIS_SESSION_TOKEN.getName().equalsIgnoreCase(key)) {
                maskedProperties.put(keyPrefix + key, SENSITIVE_PROPERTY_MASK);
            } else {
                maskedProperties.put(keyPrefix + key, value);
            }
        });
        return maskedProperties;
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
            throws UserException {
        List<Pair<String, String>> shardPositions = Lists.newArrayList();
        Map<String, String> customKinesisProperties = Maps.newHashMap();
        boolean resetProgress = false;
        boolean sourceChanged = false;
        boolean sourceGenerationBumped = false;
        boolean hasExplicitShardPositions = false;

        if (dataSourceProperties != null) {
            if (MapUtils.isNotEmpty(dataSourceProperties.getOriginalDataSourceProperties())) {
                shardPositions = dataSourceProperties.getKinesisShardPositions();
                customKinesisProperties = dataSourceProperties.getCustomKinesisProperties();
                hasExplicitShardPositions = !shardPositions.isEmpty();
                sourceChanged = true;
            }
            resetProgress = !Strings.isNullOrEmpty(dataSourceProperties.getStream());
            sourceChanged |= resetProgress
                    || !Strings.isNullOrEmpty(dataSourceProperties.getRegion())
                    || !Strings.isNullOrEmpty(dataSourceProperties.getEndpoint());
        }

        // Validate every failure-prone input before mutating Kinesis or common job state.
        if (hasExplicitShardPositions && !resetProgress) {
            ((KinesisProgress) progress).checkShards(shardPositions);
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
                this.partialUpdateNewKeyPolicy = "ERROR".equalsIgnoreCase(policy)
                        ? TPartialUpdateNewRowPolicy.ERROR : TPartialUpdateNewRowPolicy.APPEND;
            }
        }

        if (dataSourceProperties != null) {
            if (!customKinesisProperties.isEmpty()) {
                this.customProperties.putAll(customKinesisProperties);
                convertCustomProperties(true);
            }
            if (!Strings.isNullOrEmpty(dataSourceProperties.getStream())) {
                this.stream = dataSourceProperties.getStream();
            }
            if (!Strings.isNullOrEmpty(dataSourceProperties.getRegion())) {
                this.region = dataSourceProperties.getRegion();
            }
            if (!Strings.isNullOrEmpty(dataSourceProperties.getEndpoint())) {
                this.endpoint = dataSourceProperties.getEndpoint();
            }

            if (sourceChanged && !sourceGenerationBumped) {
                sourceGeneration++;
                resetLatestSequenceFetch();
                newCurrentKinesisShardInfos = null;
                sourceGenerationBumped = true;
            }
            if (resetProgress) {
                this.progress = new KinesisProgress();
                this.shardTopology.reset();
                this.openKinesisShards.clear();
                this.closedKinesisShards.clear();
                this.cachedShardWithMillsBehindLatest.clear();
            }
            if (hasExplicitShardPositions) {
                this.customKinesisShards.clear();
                for (Pair<String, String> shardPosition : shardPositions) {
                    this.customKinesisShards.add(shardPosition.first);
                }
            } else if (resetProgress) {
                this.customKinesisShards.clear();
            }
            if (!shardPositions.isEmpty()) {
                ((KinesisProgress) progress).modifyPosition(shardPositions);
                this.shardTopology.reset();
            }
        }
        LOG.info("modify the properties of kinesis routine load job: {}, jobProperties: {}, dataSourceProperties: {}",
                this.id, jobProperties, dataSourceProperties);
    }

    @Override
    public void replayModifyProperties(AlterRoutineLoadJobOperationLog log) {
        try {
            modifyPropertiesInternal(log.getJobProperties(),
                    (KinesisDataSourceProperties) log.getDataSourceProperties());
        } catch (UserException e) {
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
     * Kinesis does not provide a cheap FE-side API equivalent to Kafka's latest offset query.
     * So FE cannot rely on cached lag to block scheduling, otherwise a task can get stuck after
     * catching up once and never probe for newly arrived records. Keep polling and let BE's
     * GetRecords result decide whether this round has data.
     */
    public boolean hasMoreDataToConsume(UUID taskId, Map<String, String> shardIdToSequenceNumber)
            throws UserException {
        if (LOG.isDebugEnabled() && !cachedShardWithMillsBehindLatest.isEmpty()) {
            boolean allCaughtUp = true;
            for (String shardId : shardIdToSequenceNumber.keySet()) {
                Long millis = cachedShardWithMillsBehindLatest.get(shardId);
                if (millis == null || millis > 0) {
                    allCaughtUp = false;
                    break;
                }
            }
            if (allCaughtUp) {
                LOG.debug("All shards are caught up by cached MillisBehindLatest, but keep polling. job {}, task {}",
                        id, taskId);
            }
        }
        return true;
    }

    @Override
    public NereidsRoutineLoadTaskInfo toNereidsRoutineLoadTaskInfo() throws UserException {
        Expression deleteCondition = getDeleteCondition() != null
                ? NereidsLoadUtils.parseExpressionSeq(
                        getDeleteCondition().accept(ExprToSqlVisitor.INSTANCE,
                                ToSqlParams.WITHOUT_TABLE)).get(0)
                : null;
        Expression precedingFilter = getPrecedingFilter() != null
                ? NereidsLoadUtils.parseExpressionSeq(
                        getPrecedingFilter().accept(ExprToSqlVisitor.INSTANCE,
                                ToSqlParams.WITHOUT_TABLE)).get(0)
                : null;
        Expression whereExpr = getWhereExpr() != null
                ? NereidsLoadUtils.parseExpressionSeq(getWhereExpr().accept(
                ExprToSqlVisitor.INSTANCE, ToSqlParams.WITHOUT_TABLE)).get(0)
                : null;
        NereidsLoadTaskInfo.NereidsImportColumnDescs importColumnDescs = null;
        if (columnDescs != null) {
            importColumnDescs = new NereidsLoadTaskInfo.NereidsImportColumnDescs();
            for (ImportColumnDesc desc : columnDescs.descs) {
                Expression expression = desc.getExpr() != null
                        ? NereidsLoadUtils.parseExpressionSeq(desc.getExpr().accept(
                        ExprToSqlVisitor.INSTANCE, ToSqlParams.WITHOUT_TABLE)).get(0)
                        : null;
                importColumnDescs.descs.add(new NereidsImportColumnDesc(desc.getColumnName(), expression));
            }
        }
        return new NereidsRoutineLoadTaskInfo(execMemLimit, new HashMap<>(jobProperties), maxBatchIntervalS,
                partitionNamesInfo, mergeType, deleteCondition, sequenceCol, maxFilterRatio, importColumnDescs,
                precedingFilter, whereExpr, columnSeparator, lineDelimiter, enclose, escape, sendBatchParallelism,
                loadToSingleTablet, uniqueKeyUpdateMode, partialUpdateNewKeyPolicy, memtableOnSinkNode);
    }

    @Override
    public void updateCloudProgress() throws UserException {
        throw new UserException("Kinesis routine load does not support cloud mode");
    }

    @Override
    protected void updateCloudProgress(RLTaskTxnCommitAttachment attachment) {
        throw new IllegalStateException("Kinesis routine load does not support cloud mode");
    }
}
