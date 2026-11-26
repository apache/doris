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

package org.apache.doris.job.offset.jdbc;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.httpv2.entity.ResponseBody;
import org.apache.doris.job.cdc.DataSourceConfigKeys;
import org.apache.doris.job.cdc.split.AbstractSourceSplit;
import org.apache.doris.job.cdc.split.BinlogSplit;
import org.apache.doris.job.cdc.split.SnapshotSplit;
import org.apache.doris.job.common.DataSourceType;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.extensions.insert.streaming.StreamingInsertJob;
import org.apache.doris.job.extensions.insert.streaming.StreamingJdbcUrlNormalizer;
import org.apache.doris.job.offset.Offset;
import org.apache.doris.job.util.StreamingJobUtils;
import org.apache.doris.nereids.analyzer.UnboundTVFRelation;
import org.apache.doris.nereids.trees.expressions.Properties;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.InternalService.PRequestCdcClientResult;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.system.Backend;
import org.apache.doris.tablefunction.CdcStreamTableValuedFunction;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * OffsetProvider for cdc_stream TVF path.
 *
 * <p>Differs from JdbcSourceOffsetProvider (non-TVF path) in:
 * <ul>
 *   <li>offset commit: FE pulls actual end offset from BE via /api/getTaskOffset/{taskId} in
 *       beforeCommitted, stores in txn attachment (transactionally safe)</li>
 *   <li>cloud mode snapshot: attachment carries cumulative chunkHighWatermarkMap so that
 *       replayOnCloudMode can recover full state from the single latest attachment in MS</li>
 *   <li>recovery: state is rebuilt from txn replay (chunkHighWatermarkMap populated by
 *       replayOnCommitted/replayOnCloudMode -> updateOffset), not from EditLog</li>
 *   <li>updateOffset: during replay remainingSplits is empty so removeIf returns false naturally;
 *       chunkHighWatermarkMap is always updated unconditionally to support recovery</li>
 *   <li>replayIfNeed: checks currentOffset directly — snapshot triggers remainingSplits rebuild
 *       from meta + chunkHighWatermarkMap; binlog needs no action (currentOffset already set)</li>
 *   <li>image persistence: chw/bop/ts also flow through getPersistInfo (inherited) so state
 *       survives FE checkpoint after pre-checkpoint journal is GC'd; restoreFromPersistInfo
 *       reads them back on startup</li>
 * </ul>
 */
@Log4j2
public class JdbcTvfSourceOffsetProvider extends JdbcSourceOffsetProvider {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /**
     * No-arg constructor required by SourceOffsetProviderFactory.createSourceOffsetProvider().
     */
    public JdbcTvfSourceOffsetProvider() {
        super();
    }

    @Override
    public String getSourceType() {
        return CdcStreamTableValuedFunction.NAME;
    }

    /** Initializes provider state from TVF properties; called every schedule tick. */
    @Override
    public void ensureInitialized(Long jobId, Map<String, String> originTvfProps) throws JobException {
        String type = originTvfProps.get(DataSourceConfigKeys.TYPE);
        Preconditions.checkArgument(type != null, "type is required");
        DataSourceType resolvedType = DataSourceType.valueOf(type.toUpperCase());

        // Populate default slot/pub into sourceProperties so cleanMeta -> /api/close
        // carries the resolved names for cdcclient ownership-based cleanup.
        Map<String, String> effective = new HashMap<>(originTvfProps);
        effective.putIfAbsent(DataSourceConfigKeys.SKIP_SNAPSHOT_BACKFILL, "true");
        effective.put(DataSourceConfigKeys.JDBC_URL, StreamingJdbcUrlNormalizer.normalize(
                resolvedType, effective.get(DataSourceConfigKeys.JDBC_URL)));
        StreamingJobUtils.populateDefaultSourceProperties(resolvedType, effective, String.valueOf(jobId));
        if (getCompactSnapshotProgress(chunkHighWatermarkMap) != null
                && !Boolean.parseBoolean(effective.get(DataSourceConfigKeys.SKIP_SNAPSHOT_BACKFILL))) {
            throw new JobException("Cannot enable snapshot backfill after compact snapshot progress was committed");
        }
        // Always refresh fields that may be updated via ALTER JOB (e.g. credentials, parallelism).
        this.sourceProperties = effective;
        this.snapshotParallelism = Integer.parseInt(
                originTvfProps.getOrDefault(DataSourceConfigKeys.SNAPSHOT_PARALLELISM,
                        DataSourceConfigKeys.SNAPSHOT_PARALLELISM_DEFAULT));

        if (this.jobId != null) {
            return;
        }
        this.jobId = jobId;
        this.sourceType = resolvedType;
        String table = originTvfProps.get(DataSourceConfigKeys.TABLE);
        Preconditions.checkArgument(table != null, "table is required for cdc_stream TVF");
    }

    /**
     * Rewrites the cdc_stream TVF SQL with current offset meta and taskId,
     * so the BE knows where to start reading and can report
     * the end offset back via taskOffsetCache.
     */
    @Override
    public InsertIntoTableCommand rewriteTvfParams(InsertIntoTableCommand originCommand,
            Offset runningOffset, long taskId) {
        JdbcOffset offset = (JdbcOffset) runningOffset;
        Map<String, String> props = new HashMap<>();
        Plan rewritePlan = originCommand.getParsedPlan().get().rewriteUp(plan -> {
            if (plan instanceof UnboundTVFRelation) {
                UnboundTVFRelation originTvfRel = (UnboundTVFRelation) plan;
                props.putAll(originTvfRel.getProperties().getMap());
                props.put(DataSourceConfigKeys.SKIP_SNAPSHOT_BACKFILL,
                        sourceProperties.get(DataSourceConfigKeys.SKIP_SNAPSHOT_BACKFILL));
                props.put(CdcStreamTableValuedFunction.META_KEY, new Gson().toJson(buildTaskMeta(offset)));
                props.put(CdcStreamTableValuedFunction.JOB_ID_KEY, String.valueOf(jobId));
                props.put(CdcStreamTableValuedFunction.TASK_ID_KEY, String.valueOf(taskId));
                return new UnboundTVFRelation(
                        originTvfRel.getRelationId(), originTvfRel.getFunctionName(), new Properties(props));
            }
            return plan;
        });
        InsertIntoTableCommand cmd = new InsertIntoTableCommand((LogicalPlan) rewritePlan,
                Optional.empty(), Optional.empty(), Optional.empty(), true, Optional.empty());
        cmd.setJobId(originCommand.getJobId());
        return cmd;
    }

    Map<String, Object> buildTaskMeta(JdbcOffset offset) {
        Map<String, Object> meta = offset.generateMeta();
        if (offset.snapshotSplit()
                && Boolean.parseBoolean(sourceProperties.get(DataSourceConfigKeys.SKIP_SNAPSHOT_BACKFILL))) {
            synchronized (splitsLock) {
                // Only the first committed snapshot batch needs the client to find the minimum watermark.
                if (chunkHighWatermarkMap.isEmpty()) {
                    meta.put(DataSourceConfigKeys.COMPACT_SNAPSHOT_OFFSET, "1");
                }
            }
        }
        return meta;
    }

    /**
     * Returns the serialized JSON offset to store in txn commit attachment.
     *
     * <p>Calls BE /api/getTaskOffset/{taskId} to get the actual end offset recorded after
     * fetchRecordStream completes (stored in PipelineCoordinator.taskOffsetCache).
     *
     * <p>Snapshots skipping backfill use one marked entry containing the fixed starting watermark and
     * the last contiguous committed split. Jobs with legacy progress retain per-split offsets.
     * Cloud attachments include all progress because MS retains only the latest attachment.
     */
    @Override
    public String getCommitOffsetJson(Offset runningOffset, long taskId, List<Long> scanBackendIds) {
        List<Map<String, String>> currentTaskEndOffset = fetchTaskEndOffset(taskId, scanBackendIds);
        if (CollectionUtils.isEmpty(currentTaskEndOffset)) {
            return "";
        }

        if (((JdbcOffset) runningOffset).snapshotSplit()) {
            synchronized (splitsLock) {
                return new Gson().toJson(buildCumulativeSnapshotOffset(currentTaskEndOffset));
            }
        }
        return new Gson().toJson(currentTaskEndOffset);
    }

    /**
     * Queries each scan backend in order until one returns a non-empty offset for this taskId.
     * Only the BE that ran the cdc_stream TVF scan node will have the entry in taskOffsetCache.
     */
    protected List<Map<String, String>> fetchTaskEndOffset(long taskId, List<Long> scanBackendIds) {
        InternalService.PRequestCdcClientRequest request =
                InternalService.PRequestCdcClientRequest.newBuilder()
                        .setApi("/api/getTaskOffset/" + taskId).build();
        for (Long beId : scanBackendIds) {
            Backend backend = Env.getCurrentSystemInfo().getBackend(beId);
            if (backend == null) {
                log.info("Backend {} not found for task {}, skipping", beId, taskId);
                continue;
            }
            String rawResponse = null;
            try {
                TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
                Future<PRequestCdcClientResult> future = BackendServiceProxy.getInstance()
                        .requestCdcClient(address, request, Config.streaming_cdc_light_rpc_timeout_sec);
                InternalService.PRequestCdcClientResult result =
                        future.get(Config.streaming_cdc_light_rpc_timeout_sec, TimeUnit.SECONDS);
                TStatusCode code = TStatusCode.findByValue(result.getStatus().getStatusCode());
                if (code != TStatusCode.OK) {
                    log.warn("Failed to get task {} offset from BE {}: {}", taskId,
                            backend.getHost(), result.getStatus().getErrorMsgs(0));
                    continue;
                }
                rawResponse = result.getResponse();
                ResponseBody<List<Map<String, String>>> responseObj = OBJECT_MAPPER.readValue(
                        rawResponse,
                        new TypeReference<ResponseBody<List<Map<String, String>>>>() {});
                List<Map<String, String>> data = responseObj.getData();
                if (!CollectionUtils.isEmpty(data)) {
                    log.info("Fetched task {} offset from BE {}: {}", taskId, backend.getHost(), data);
                    return data;
                }
            } catch (TimeoutException te) {
                log.warn("cdc_client RPC timeout api=/api/getTaskOffset jobId={} taskId={} backend={}:{} "
                                + "timeout_sec={}",
                        jobId, taskId, backend.getHost(), backend.getBrpcPort(),
                        Config.streaming_cdc_light_rpc_timeout_sec);
            } catch (Exception ex) {
                log.warn("Get task offset error for task {} from BE {}, raw response: {}",
                        taskId, backend.getHost(), rawResponse, ex);
            }
        }
        return Collections.emptyList();
    }

    /**
     * Returns one compact progress entry, or preserves the original per-split format for legacy jobs.
     */
    private List<Map<String, String>> buildCumulativeSnapshotOffset(
            List<Map<String, String>> currentTaskSplits) {
        SnapshotSplit progress = getCompactSnapshotProgress(chunkHighWatermarkMap);
        Map<String, String> first = currentTaskSplits.get(0);
        Map<String, String> watermark = progress == null ? null : sourceWatermark(progress.getHighWatermark());
        if (watermark == null && chunkHighWatermarkMap.isEmpty()
                && Boolean.parseBoolean(sourceProperties.get(DataSourceConfigKeys.SKIP_SNAPSHOT_BACKFILL))) {
            // The client puts the first batch's minimum watermark first; keep it fixed for later batches.
            Preconditions.checkState("1".equals(first.get(DataSourceConfigKeys.COMPACT_SNAPSHOT_OFFSET)),
                    "CDC client does not support compact snapshot offsets; "
                            + "upgrade the CDC client before starting the job");
            watermark = sourceWatermark(first);
        }
        if (watermark != null) {
            // Tasks commit batches in split order; the client may reorder the response.
            // E.g. [(split 1, HW 100), (split 0, HW 120), (split 2, HW 150)] is stored as (split 2, HW 100).
            String endSplitId = currentTaskSplits.stream().map(split -> split.get(SPLIT_ID))
                    .max(Comparator.comparingInt(JdbcSourceOffsetProvider::splitIdOf)).get();
            watermark.put(SPLIT_ID, endSplitId);
            watermark.put(DataSourceConfigKeys.COMPACT_SNAPSHOT_OFFSET, "1");
            return Collections.singletonList(watermark);
        }
        if (!Config.isCloudMode()) {
            return currentTaskSplits;
        }
        // Per-split offsets in cloud mode must include all progress because MS keeps only the latest attachment.
        Set<String> currentSplitIds = currentTaskSplits.stream()
                .map(m -> m.get(SPLIT_ID)).collect(Collectors.toSet());

        List<Map<String, String>> result = new ArrayList<>();
        // Add all previously completed chunks (skip any that overlap with current task)
        if (MapUtils.isNotEmpty(chunkHighWatermarkMap)) {
            for (Map.Entry<String, Map<String, Map<String, String>>> tableEntry
                    : chunkHighWatermarkMap.entrySet()) {
                for (Map.Entry<String, Map<String, String>> splitEntry
                        : tableEntry.getValue().entrySet()) {
                    if (!currentSplitIds.contains(splitEntry.getKey())) {
                        Map<String, String> map = new HashMap<>(splitEntry.getValue());
                        map.put(SPLIT_ID, splitEntry.getKey());
                        result.add(map);
                    }
                }
            }
        }
        result.addAll(currentTaskSplits);
        return result;
    }

    @Override
    protected List<SnapshotSplit> recalculateRemainingSplits(
            Map<String, Map<String, Map<String, String>>> watermarks,
            Map<String, List<SnapshotSplit>> snapshotSplits) {
        synchronized (splitsLock) {
            Map<String, Map<String, String>> committed = new HashMap<>();
            watermarks.values().forEach(committed::putAll);
            SnapshotSplit progress = getCompactSnapshotProgress(watermarks);
            int end = progress == null ? -1 : splitIdOf(progress.getSplitId());
            remainingSplits = new ArrayList<>();
            finishedSplits = new ArrayList<>();
            for (List<SnapshotSplit> splits : snapshotSplits.values()) {
                for (SnapshotSplit split : splits) {
                    Map<String, String> watermark = committed.get(split.getSplitId());
                    if (progress != null && splitIdOf(split.getSplitId()) <= end) {
                        watermark = progress.getHighWatermark();
                    }
                    if (watermark != null) {
                        split.setHighWatermark(sourceWatermark(watermark));
                        finishedSplits.add(split);
                    } else {
                        remainingSplits.add(split);
                    }
                }
            }
            if (progress != null) {
                long covered = finishedSplits.stream().filter(split -> splitIdOf(split.getSplitId()) <= end).count();
                Preconditions.checkState(covered == end + 1L,
                        "Persisted split definitions do not cover compact snapshot progress");
            }
            return new ArrayList<>(remainingSplits.subList(0, Math.min(remainingSplits.size(), snapshotParallelism)));
        }
    }

    /** Returns the committed prefix's last split ID and fixed starting watermark, or null if not compacted. */
    private SnapshotSplit getCompactSnapshotProgress(Map<String, Map<String, Map<String, String>>> watermarks) {
        synchronized (splitsLock) {
            return watermarks.values().stream().flatMap(table -> table.entrySet().stream())
                    .filter(entry -> "1".equals(entry.getValue().get(DataSourceConfigKeys.COMPACT_SNAPSHOT_OFFSET)))
                    .max(Comparator.comparingInt(entry -> splitIdOf(entry.getKey())))
                    .map(entry -> new SnapshotSplit(entry.getKey(), null, null, null, null, entry.getValue()))
                    .orElse(null);
        }
    }

    private static Map<String, String> sourceWatermark(Map<String, String> watermark) {
        Map<String, String> source = new HashMap<>(watermark);
        source.remove(DataSourceConfigKeys.COMPACT_SNAPSHOT_OFFSET);
        return source;
    }

    /**
     * TVF path updateOffset.
     *
     * <p>Snapshot: always writes to chunkHighWatermarkMap (needed for cloud cumulative attachment
     * and FE-restart recovery). In normal flow removeIf finds the split in remainingSplits and
     * adds it to finishedSplits. During txn replay remainingSplits is empty so removeIf returns
     * false naturally — chunkHighWatermarkMap is still updated for replayIfNeed to use later.
     *
     * <p>Binlog: mirror startingOffset into binlogOffsetPersist so it survives FE checkpoint via
     * image (currentOffset has no @SerializedName).
     */
    @Override
    public void updateOffset(Offset offset) {
        JdbcOffset newOffset = (JdbcOffset) offset;
        if (newOffset.snapshotSplit()) {
            synchronized (splitsLock) {
                SnapshotSplit compactSnapshotProgress = getCompactSnapshotProgress(chunkHighWatermarkMap);
                SnapshotSplit compact = newOffset.getSplits().stream().map(split -> (SnapshotSplit) split)
                        .filter(split -> "1".equals(split.getHighWatermark()
                                .get(DataSourceConfigKeys.COMPACT_SNAPSHOT_OFFSET)))
                        .max(Comparator.comparingInt(split -> splitIdOf(split.getSplitId()))).orElse(null);
                if (compact != null) {
                    compactSnapshotProgress = compact;
                    int end = splitIdOf(compact.getSplitId());
                    Map<String, String> watermark = sourceWatermark(compact.getHighWatermark());
                    remainingSplits.removeIf(split -> {
                        if (splitIdOf(split.getSplitId()) > end) {
                            return false;
                        }
                        split.setHighWatermark(watermark);
                        finishedSplits.add(split);
                        return true;
                    });
                    // Replace covered entries with the latest compact progress.
                    chunkHighWatermarkMap.values().forEach(table ->
                            table.keySet().removeIf(id -> splitIdOf(id) <= end));
                    chunkHighWatermarkMap.computeIfAbsent(buildTableKey(), k -> new HashMap<>())
                            .put(compact.getSplitId(), compact.getHighWatermark());
                }
                for (AbstractSourceSplit split : newOffset.getSplits()) {
                    SnapshotSplit ss = (SnapshotSplit) split;
                    if (compactSnapshotProgress != null
                            && splitIdOf(ss.getSplitId()) <= splitIdOf(compactSnapshotProgress.getSplitId())) {
                        continue;
                    }
                    boolean removed = remainingSplits.removeIf(v -> {
                        if (v.getSplitId().equals(ss.getSplitId())) {
                            ss.setTableId(v.getTableId());
                            ss.setSplitKey(v.getSplitKey());
                            ss.setSplitStart(v.getSplitStart());
                            ss.setSplitEnd(v.getSplitEnd());
                            v.setHighWatermark(ss.getHighWatermark());
                            return true;
                        }
                        return false;
                    });
                    if (removed) {
                        finishedSplits.add(ss);
                    }
                    chunkHighWatermarkMap.computeIfAbsent(buildTableKey(), k -> new HashMap<>())
                            .put(ss.getSplitId(), ss.getHighWatermark());
                }
            }
        } else {
            synchronized (splitsLock) {
                // Mirror binlog offset into bop so it survives FE checkpoint
                BinlogSplit bs = (BinlogSplit) newOffset.getSplits().get(0);
                if (MapUtils.isEmpty(bs.getStartingOffset())) {
                    log.warn("Skip empty committed binlog offset for job {}", getJobId());
                    return;
                }
                binlogOffsetPersist = new HashMap<>(bs.getStartingOffset());
                binlogOffsetPersist.put(SPLIT_ID, BinlogSplit.BINLOG_SPLIT_ID);
                clearSnapshotState();
                currentOffset = newOffset;
                hasMoreData = true;
            }
            return;
        }
        this.currentOffset = newOffset;
    }

    /**
     * TVF path recovery. After FE checkpoint the pre-checkpoint journal is GC'd, so currentOffset
     * (no @SerializedName) may be null with prior commits — fall back to bop/chw restored from image.
     *
     * <ul>
     *   <li>snapshot: mid-snapshot restart — rebuild remainingSplits from meta + chunkHighWatermarkMap</li>
     *   <li>binlog: currentOffset already correct from updateOffset; nothing to do</li>
     * </ul>
     */
    @Override
    public void replayIfNeed(StreamingInsertJob job) throws JobException {
        synchronized (splitsLock) {
            this.cachedSyncTables = job.getSyncTables();
        }
        if (currentOffset == null) {
            // Post-checkpoint binlog: rebuild from bop persisted in image
            if (MapUtils.isNotEmpty(binlogOffsetPersist)) {
                currentOffset = new JdbcOffset(
                        Collections.singletonList(new BinlogSplit(binlogOffsetPersist)));
                log.info("Replaying TVF offset provider for job {}: restored binlog offset from persist",
                        job.getJobId());
                return;
            }
            // Fresh-create or post-checkpoint mid-snapshot: restore remainingSplits from meta
            // so getNextOffset() returns snapshot splits instead of an empty BinlogSplit.
            Map<String, List<SnapshotSplit>> snapshotSplits = StreamingJobUtils.restoreSplitsToJob(job.getJobId());
            if (MapUtils.isNotEmpty(snapshotSplits)) {
                // chw outer key may be "null.null" during journal replay (sourceProperties uninitialized); remap
                Map<String, Map<String, Map<String, String>>> effective =
                        MapUtils.isNotEmpty(chunkHighWatermarkMap)
                                ? remapChunkHighWatermarkMap(snapshotSplits)
                                : new HashMap<>();
                recalculateRemainingSplits(effective, snapshotSplits);
                if (!finishedSplits.isEmpty()) {
                    SnapshotSplit lastFinished = finishedSplits.get(finishedSplits.size() - 1);
                    currentOffset = new JdbcOffset(Collections.singletonList(lastFinished));
                }
                resumeCdcSplitProgressFromSplits();
                log.info("Replaying TVF offset provider for job {}: no current offset,"
                        + " restored {} remaining splits from meta (chw size={})",
                        job.getJobId(), remainingSplits.size(),
                        chunkHighWatermarkMap == null ? 0 : chunkHighWatermarkMap.size());
            } else {
                log.info("Replaying TVF offset provider for job {}: no committed txn,"
                        + " no snapshot splits in meta", job.getJobId());
            }
        } else if (currentOffset.snapshotSplit()) {
            log.info("Replaying TVF offset provider for job {}: restoring snapshot state from txn replay",
                    job.getJobId());
            Map<String, List<SnapshotSplit>> snapshotSplits = StreamingJobUtils.restoreSplitsToJob(job.getJobId());
            if (MapUtils.isNotEmpty(snapshotSplits)) {
                // During replay, buildTableKey() may have stored entries under "null.null"
                // because sourceProperties was not yet initialized. Remap all committed splits
                // under the actual table key so recalculateRemainingSplits can match them.
                Map<String, Map<String, Map<String, String>>> effectiveMap =
                        remapChunkHighWatermarkMap(snapshotSplits);
                List<SnapshotSplit> lastSnapshotSplits =
                        recalculateRemainingSplits(effectiveMap, snapshotSplits);
                // Rebuild first so noMoreSplits() can read splitter state from last.splitEnd.
                resumeCdcSplitProgressFromSplits();
                if (remainingSplits.isEmpty()) {
                    if (!lastSnapshotSplits.isEmpty()) {
                        currentOffset = new JdbcOffset(lastSnapshotSplits);
                    } else if (!isSnapshotOnlyMode() && noMoreSplits()) {
                        BinlogSplit binlogSplit = new BinlogSplit();
                        binlogSplit.setFinishedSplits(finishedSplits);
                        currentOffset = new JdbcOffset(Collections.singletonList(binlogSplit));
                    }
                }
            }
        } else {
            log.info("Replaying TVF offset provider for job {}: binlog offset already set, nothing to do",
                    job.getJobId());
        }
    }

    /**
     * Builds the chunkHighWatermarkMap outer key as schema.table (if schema is present)
     * or database.table, matching the format used by snapshotSplits keys in
     * recalculateRemainingSplits.
     */
    private String buildTableKey() {
        String schema = sourceProperties.get(DataSourceConfigKeys.SCHEMA);
        String qualifier = (schema != null && !schema.isEmpty())
                ? schema : sourceProperties.get(DataSourceConfigKeys.DATABASE);
        return qualifier + "." + sourceProperties.get(DataSourceConfigKeys.TABLE);
    }

    /**
     * Remaps chunkHighWatermarkMap to use the actual table key from snapshotSplits.
     *
     * <p>During FE-restart replay, buildTableKey() produces "null.null" because sourceProperties
     * is not yet initialized (ensureInitialized has not been called). This causes
     * recalculateRemainingSplits to miss all committed splits (key mismatch) and re-process
     * them, leading to data duplication.
     *
     * <p>The fix: flatten all inner splitId-&gt;highWatermark entries across every outer key and
     * re-key them under the actual table name read from the meta table (snapshotSplits key).
     * TVF always has exactly one table, so snapshotSplits has exactly one entry.
     *
     * <tableName -> splitId -> chunk of highWatermark>
     */
    private Map<String, Map<String, Map<String, String>>> remapChunkHighWatermarkMap(
            Map<String, List<SnapshotSplit>> snapshotSplits) {
        if (MapUtils.isEmpty(chunkHighWatermarkMap)) {
            return chunkHighWatermarkMap;
        }
        // Flatten all committed splitId -> highWatermark entries, ignoring the (possibly wrong) outer key
        Map<String, Map<String, String>> flatCommitted = new HashMap<>();
        for (Map<String, Map<String, String>> inner : chunkHighWatermarkMap.values()) {
            if (inner != null) {
                flatCommitted.putAll(inner);
            }
        }
        if (flatCommitted.isEmpty()) {
            return chunkHighWatermarkMap;
        }
        // Re-key under the actual table name from snapshotSplits (TVF has exactly one table).
        // getTableName() in recalculateRemainingSplits splits by "." and takes the last segment,
        // so using the plain table name as key (no dots) ensures a direct match.
        String actualTableKey = snapshotSplits.keySet().iterator().next();
        Map<String, Map<String, Map<String, String>>> remapped = new HashMap<>();
        remapped.put(actualTableKey, flatCommitted);
        return remapped;
    }

    @Override
    public String mergePersistInfo(String previousPersistInfo, String incomingPersistInfo) {
        // TVF records contain complete state rather than schema-omitting CDC updates.
        return incomingPersistInfo;
    }

    /**
     * Restore chw/bop/ts from image or job update journal JSON; preserves cumulative snapshot progress.
     */
    @Override
    public void restoreFromPersistInfo(String persistInfo) {
        if (persistInfo == null) {
            return;
        }
        try {
            JdbcSourceOffsetProvider tmp = GsonUtils.GSON.fromJson(persistInfo, JdbcSourceOffsetProvider.class);
            this.chunkHighWatermarkMap = tmp.getChunkHighWatermarkMap();
            this.binlogOffsetPersist = tmp.getBinlogOffsetPersist();
            this.tableSchemas = tmp.getTableSchemas();
            if (MapUtils.isNotEmpty(binlogOffsetPersist)) {
                this.currentOffset = new JdbcOffset(
                        Collections.singletonList(new BinlogSplit(binlogOffsetPersist)));
            }
            log.info("Restored TVF offset provider from persist: chw={}, bop={}, ts.len={}",
                    chunkHighWatermarkMap == null ? 0 : chunkHighWatermarkMap.size(),
                    binlogOffsetPersist == null ? 0 : binlogOffsetPersist.size(),
                    tableSchemas == null ? 0 : tableSchemas.length());
        } catch (Exception e) {
            log.warn("Failed to restore TVF offset provider from persistInfo", e);
        }
    }

    @Override
    public void applyEndOffsetToTask(Offset runningOffset, Offset endOffset) {
        if (!(runningOffset instanceof JdbcOffset) || !(endOffset instanceof JdbcOffset)) {
            return;
        }
        JdbcOffset running = (JdbcOffset) runningOffset;
        JdbcOffset end = (JdbcOffset) endOffset;
        // Snapshot tasks share their split objects with remainingSplits; updateOffset already fills them.
        if (!running.snapshotSplit()) {
            BinlogSplit rSplit = (BinlogSplit) running.getSplits().get(0);
            BinlogSplit eSplit = (BinlogSplit) end.getSplits().get(0);
            // deserializeOffset stores binlog position in startingOffset
            rSplit.setEndingOffset(eSplit.getStartingOffset());
        }
    }

}
