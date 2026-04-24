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
import org.apache.doris.job.offset.Offset;
import org.apache.doris.job.util.StreamingJobUtils;
import org.apache.doris.nereids.analyzer.UnboundTVFRelation;
import org.apache.doris.nereids.trees.expressions.Properties;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Future;
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

    /**
     * Initializes provider state and fetches snapshot splits from BE.
     * splitChunks is called here (rather than in StreamingInsertJob) to keep
     * all cdc_stream-specific init logic inside the provider.
     */
    @Override
    public void ensureInitialized(Long jobId, Map<String, String> originTvfProps) throws JobException {
        String type = originTvfProps.get(DataSourceConfigKeys.TYPE);
        Preconditions.checkArgument(type != null, "type is required");
        DataSourceType resolvedType = DataSourceType.valueOf(type.toUpperCase());

        // Populate default slot/pub into sourceProperties so cleanMeta -> /api/close
        // carries the resolved names for cdcclient ownership-based cleanup.
        Map<String, String> effective = new HashMap<>(originTvfProps);
        StreamingJobUtils.populateDefaultSourceProperties(resolvedType, effective, String.valueOf(jobId));
        // Always refresh fields that may be updated via ALTER JOB (e.g. credentials, parallelism).
        this.sourceProperties = effective;
        this.snapshotParallelism = Integer.parseInt(
                originTvfProps.getOrDefault(DataSourceConfigKeys.SNAPSHOT_PARALLELISM,
                        DataSourceConfigKeys.SNAPSHOT_PARALLELISM_DEFAULT));

        if (this.jobId != null) {
            return;
        }
        // One-time initialization below — safe to skip on FE restart because the provider
        // is reconstructed fresh (getPersistInfo returns null), so jobId is null then too.
        this.jobId = jobId;
        this.chunkHighWatermarkMap = new HashMap<>();
        this.sourceType = resolvedType;
        String table = originTvfProps.get(DataSourceConfigKeys.TABLE);
        Preconditions.checkArgument(table != null, "table is required for cdc_stream TVF");
    }

    /**
     * Called once on fresh job creation (not on FE restart).
     * Fetches snapshot splits from BE and persists them to the meta table.
     */
    @Override
    public void initOnCreate() throws JobException {
        String table = sourceProperties.get(DataSourceConfigKeys.TABLE);
        splitChunks(Collections.singletonList(table));
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
                props.put(CdcStreamTableValuedFunction.META_KEY, new Gson().toJson(offset.generateMeta()));
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

    /**
     * Returns the serialized JSON offset to store in txn commit attachment.
     *
     * <p>Calls BE /api/getTaskOffset/{taskId} to get the actual end offset recorded after
     * fetchRecordStream completes (stored in PipelineCoordinator.taskOffsetCache).
     *
     * <p>For cloud + snapshot: returns cumulative list (all previously completed chunks +
     * current task's new splits) so that replayOnCloudMode can recover full state from latest attachment.
     * For non-cloud snapshot / binlog: returns only current task's splits.
     */
    @Override
    public String getCommitOffsetJson(Offset runningOffset, long taskId, List<Long> scanBackendIds) {
        List<Map<String, String>> currentTaskEndOffset = fetchTaskEndOffset(taskId, scanBackendIds);
        if (CollectionUtils.isEmpty(currentTaskEndOffset)) {
            return "";
        }

        // Cloud + snapshot: prepend all previously completed chunks so the attachment is
        // self-contained for replayOnCloudMode (MS only keeps the latest attachment)
        if (Config.isCloudMode() && ((JdbcOffset) runningOffset).snapshotSplit()) {
            List<Map<String, String>> cumulative = buildCumulativeSnapshotOffset(currentTaskEndOffset);
            return new Gson().toJson(cumulative);
        }
        return new Gson().toJson(currentTaskEndOffset);
    }

    /**
     * Queries each scan backend in order until one returns a non-empty offset for this taskId.
     * Only the BE that ran the cdc_stream TVF scan node will have the entry in taskOffsetCache.
     */
    private List<Map<String, String>> fetchTaskEndOffset(long taskId, List<Long> scanBackendIds) {
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
                Future<PRequestCdcClientResult> future =
                        BackendServiceProxy.getInstance().requestCdcClient(address, request);
                InternalService.PRequestCdcClientResult result = future.get();
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
            } catch (Exception ex) {
                log.warn("Get task offset error for task {} from BE {}, raw response: {}",
                        taskId, backend.getHost(), rawResponse, ex);
            }
        }
        return Collections.emptyList();
    }

    /**
     * Merges existing chunkHighWatermarkMap (all previously completed chunks) with
     * current task's new splits, deduplicating by splitId.
     */
    private List<Map<String, String>> buildCumulativeSnapshotOffset(
            List<Map<String, String>> currentTaskSplits) {
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

    /**
     * TVF path updateOffset.
     *
     * <p>Snapshot: always writes to chunkHighWatermarkMap (needed for cloud cumulative attachment
     * and FE-restart recovery). In normal flow removeIf finds the split in remainingSplits and
     * adds it to finishedSplits. During txn replay remainingSplits is empty so removeIf returns
     * false naturally — chunkHighWatermarkMap is still updated for replayIfNeed to use later.
     *
     * <p>Binlog: currentOffset is set above; no extra state needed for TVF recovery path.
     */
    @Override
    public void updateOffset(Offset offset) {
        this.currentOffset = (JdbcOffset) offset;
        if (currentOffset.snapshotSplit()) {
            for (AbstractSourceSplit split : currentOffset.getSplits()) {
                SnapshotSplit ss = (SnapshotSplit) split;
                boolean removed = remainingSplits.removeIf(v -> {
                    if (v.getSplitId().equals(ss.getSplitId())) {
                        ss.setTableId(v.getTableId());
                        ss.setSplitKey(v.getSplitKey());
                        ss.setSplitStart(v.getSplitStart());
                        ss.setSplitEnd(v.getSplitEnd());
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
        // Binlog: currentOffset is already set; no binlogOffsetPersist needed for TVF path.
    }

    /**
     * TVF path recovery: offsetProviderPersist is always null (no EditLog write).
     * currentOffset is set by replayOnCommitted/replayOnCloudMode -> updateOffset before this runs.
     *
     * <ul>
     *   <li>snapshot: mid-snapshot restart — rebuild remainingSplits from meta + chunkHighWatermarkMap</li>
     *   <li>binlog: currentOffset already correct from updateOffset; nothing to do</li>
     * </ul>
     */
    @Override
    public void replayIfNeed(StreamingInsertJob job) throws JobException {
        if (currentOffset == null) {
            // No committed txn yet. If snapshot splits exist in the meta table (written by
            // initOnCreate), restore remainingSplits so getNextOffset() returns snapshot splits
            // instead of a BinlogSplit (which would incorrectly skip the snapshot phase).
            Map<String, List<SnapshotSplit>> snapshotSplits = StreamingJobUtils.restoreSplitsToJob(job.getJobId());
            if (MapUtils.isNotEmpty(snapshotSplits)) {
                recalculateRemainingSplits(new HashMap<>(), snapshotSplits);
                log.info("Replaying TVF offset provider for job {}: no committed txn,"
                        + " restored {} remaining splits from meta", job.getJobId(), remainingSplits.size());
            } else {
                log.info("Replaying TVF offset provider for job {}: no committed txn,"
                        + " no snapshot splits in meta", job.getJobId());
            }
            return;
        }
        if (currentOffset.snapshotSplit()) {
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
                if (remainingSplits.isEmpty()) {
                    if (!lastSnapshotSplits.isEmpty()) {
                        currentOffset = new JdbcOffset(lastSnapshotSplits);
                    } else if (!isSnapshotOnlyMode()) {
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

    /**
     * TVF path does not persist to EditLog; state is recovered via txn replay.
     * This override is defensive — the persistOffsetProviderIfNeed() call path
     * only runs in the non-TVF commitOffset flow and won't reach here.
     */
    @Override
    public String getPersistInfo() {
        return null;
    }

    @Override
    public void applyEndOffsetToTask(Offset runningOffset, Offset endOffset) {
        if (!(runningOffset instanceof JdbcOffset) || !(endOffset instanceof JdbcOffset)) {
            return;
        }
        JdbcOffset running = (JdbcOffset) runningOffset;
        JdbcOffset end = (JdbcOffset) endOffset;
        if (running.snapshotSplit()) {
            for (int i = 0; i < running.getSplits().size() && i < end.getSplits().size(); i++) {
                SnapshotSplit rSplit = (SnapshotSplit) running.getSplits().get(i);
                SnapshotSplit eSplit = (SnapshotSplit) end.getSplits().get(i);
                rSplit.setHighWatermark(eSplit.getHighWatermark());
            }
        } else {
            BinlogSplit rSplit = (BinlogSplit) running.getSplits().get(0);
            BinlogSplit eSplit = (BinlogSplit) end.getSplits().get(0);
            // deserializeOffset stores binlog position in startingOffset
            rSplit.setEndingOffset(eSplit.getStartingOffset());
        }
    }

}
