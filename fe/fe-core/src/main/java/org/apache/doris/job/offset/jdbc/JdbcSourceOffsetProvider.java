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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.httpv2.entity.ResponseBody;
import org.apache.doris.httpv2.rest.RestApiStatusCode;
import org.apache.doris.job.cdc.DataSourceConfigKeys;
import org.apache.doris.job.cdc.request.CompareOffsetRequest;
import org.apache.doris.job.cdc.request.FetchTableSplitsRequest;
import org.apache.doris.job.cdc.request.JobBaseConfig;
import org.apache.doris.job.cdc.split.AbstractSourceSplit;
import org.apache.doris.job.cdc.split.BinlogSplit;
import org.apache.doris.job.cdc.split.SnapshotSplit;
import org.apache.doris.job.common.DataSourceType;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.extensions.insert.streaming.DataSourceConfigValidator;
import org.apache.doris.job.extensions.insert.streaming.StreamingInsertJob;
import org.apache.doris.job.extensions.insert.streaming.StreamingJobProperties;
import org.apache.doris.job.offset.Offset;
import org.apache.doris.job.offset.SourceOffsetProvider;
import org.apache.doris.job.util.StreamingJobUtils;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.InternalService.PRequestCdcClientResult;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

@Getter
@Setter
@Log4j2
public class JdbcSourceOffsetProvider implements SourceOffsetProvider {
    public static final String SPLIT_ID = "splitId";
    private static final ObjectMapper objectMapper = new ObjectMapper();
    protected int snapshotParallelism = 1;
    protected Long jobId;
    protected DataSourceType sourceType;
    protected Map<String, String> sourceProperties = new HashMap<>();

    List<SnapshotSplit> remainingSplits = new ArrayList<>();
    List<SnapshotSplit> finishedSplits = new ArrayList<>();

    volatile JdbcOffset currentOffset;
    Map<String, String> endBinlogOffset;

    @SerializedName("chw")
    // tableID -> splitId -> chunk of highWatermark
    Map<String, Map<String, Map<String, String>>> chunkHighWatermarkMap;
    @SerializedName("bop")
    Map<String, String> binlogOffsetPersist;

    @SerializedName("ts")
    String tableSchemas;

    /** Split progress (task-commit view). */
    @SerializedName("csp")
    SplitProgress committedSplitProgress;

    volatile boolean hasMoreData = true;

    transient volatile String cloudCluster;

    // Route fetchEndOffset/compareOffset to the bound BE (synced from job, not persisted).
    transient volatile long boundBackendId;

    /** Split progress (cdc-fetch view), >= committedSplitProgress. Rebuilt on restart. */
    transient SplitProgress cdcSplitProgress = new SplitProgress();

    /** Cache of Job.syncTables, set by initSplitProgress / replayIfNeed. */
    transient List<String> cachedSyncTables;

    /** Guards cdcSplitProgress/committedSplitProgress/remainingSplits/finishedSplits. */
    protected final transient Object splitsLock = new Object();

    /**
     * No-arg constructor for subclass use.
     */
    public JdbcSourceOffsetProvider() {
        this.chunkHighWatermarkMap = new HashMap<>();
    }

    /**
     * Constructor for FROM Source TO Database.
     */
    public JdbcSourceOffsetProvider(Long jobId, DataSourceType sourceType, Map<String, String> sourceProperties) {
        this.jobId = jobId;
        this.sourceType = sourceType;
        this.sourceProperties = sourceProperties;
        this.chunkHighWatermarkMap = new HashMap<>();
        this.snapshotParallelism = Integer.parseInt(
                sourceProperties.getOrDefault(DataSourceConfigKeys.SNAPSHOT_PARALLELISM,
                        DataSourceConfigKeys.SNAPSHOT_PARALLELISM_DEFAULT));
    }

    // Refresh fields that may be changed via ALTER JOB; called before each use.
    @Override
    public void ensureInitialized(Long jobId, Map<String, String> newProps) throws JobException {
        this.sourceProperties = newProps;
        this.snapshotParallelism = Integer.parseInt(
                newProps.getOrDefault(DataSourceConfigKeys.SNAPSHOT_PARALLELISM,
                        DataSourceConfigKeys.SNAPSHOT_PARALLELISM_DEFAULT));
    }

    @Override
    public String getSourceType() {
        return "jdbc";
    }

    @Override
    public Offset getNextOffset(StreamingJobProperties jobProps, Map<String, String> properties) {
        synchronized (splitsLock) {
            JdbcOffset nextOffset = new JdbcOffset();
            if (!remainingSplits.isEmpty()) {
                int splitsNum = Math.min(remainingSplits.size(), snapshotParallelism);
                List<SnapshotSplit> snapshotSplits = new ArrayList<>(remainingSplits.subList(0, splitsNum));
                nextOffset.setSplits(snapshotSplits);
                return nextOffset;
            } else if (currentOffset != null && currentOffset.snapshotSplit() && noMoreSplits()) {
                // initial mode: snapshot to binlog. noMoreSplits() guards against switching while
                // splitting is still in progress (remainingSplits empty doesn't mean fully cut).
                // snapshot-only mode is intercepted by hasReachedEnd() before reaching here.
                BinlogSplit binlogSplit = new BinlogSplit();
                binlogSplit.setFinishedSplits(new ArrayList<>(finishedSplits));
                nextOffset.setSplits(Collections.singletonList(binlogSplit));
                return nextOffset;
            } else {
                // only binlog
                return currentOffset == null
                        ? new JdbcOffset(Collections.singletonList(new BinlogSplit())) : currentOffset;
            }
        }
    }

    @Override
    public String getShowCurrentOffset() {
        if (this.currentOffset != null) {
            if (currentOffset.snapshotSplit()) {
                List<? extends AbstractSourceSplit> splits = currentOffset.getSplits();
                return new Gson().toJson(splits);
            } else {
                BinlogSplit binlogSplit = (BinlogSplit) currentOffset.getSplits().get(0);
                HashMap<String, Object> showMap = new HashMap<>();
                showMap.put(SPLIT_ID, BinlogSplit.BINLOG_SPLIT_ID);
                if (binlogSplit.getStartingOffset() != null) {
                    showMap.putAll(binlogSplit.getStartingOffset());
                }
                return new Gson().toJson(showMap);
            }
        }
        return null;
    }

    @Override
    public String getShowMaxOffset() {
        if (endBinlogOffset != null) {
            return new Gson().toJson(endBinlogOffset);
        }
        return null;
    }

    /**
     * Should never call this.
     */
    @Override
    public InsertIntoTableCommand rewriteTvfParams(InsertIntoTableCommand originCommand, Offset nextOffset,
            long taskId) {
        throw new UnsupportedOperationException("rewriteTvfParams not supported for " + getClass().getSimpleName());
    }

    @Override
    public void updateOffset(Offset offset) {
        JdbcOffset newOffset = (JdbcOffset) offset;
        if (newOffset.snapshotSplit()) {
            synchronized (splitsLock) {
                List<? extends AbstractSourceSplit> splits = newOffset.getSplits();
                for (AbstractSourceSplit split : splits) {
                    SnapshotSplit snapshotSplit = (SnapshotSplit) split;
                    String splitId = split.getSplitId();
                    boolean remove = remainingSplits.removeIf(v -> {
                        if (v.getSplitId().equals(splitId)) {
                            snapshotSplit.setTableId(v.getTableId());
                            snapshotSplit.setSplitKey(v.getSplitKey());
                            snapshotSplit.setSplitStart(v.getSplitStart());
                            snapshotSplit.setSplitEnd(v.getSplitEnd());
                            return true;
                        }
                        return false;
                    });
                    if (remove) {
                        finishedSplits.add(snapshotSplit);
                        chunkHighWatermarkMap.computeIfAbsent(snapshotSplit.getTableId(), k -> new HashMap<>())
                                .put(snapshotSplit.getSplitId(), snapshotSplit.getHighWatermark());

                        // Advance committedSplitProgress to this committed chunk.
                        if (committedSplitProgress != null) {
                            applySplitToProgress(committedSplitProgress, snapshotSplit,
                                    getTableName(snapshotSplit.getTableId()));
                        }
                    } else {
                        // Replay before remainingSplits is restored, or a duplicate commit.
                        log.warn("Cannot find snapshot split {} in remainingSplits for job {}", splitId, getJobId());
                    }
                }
            }
        } else {
            BinlogSplit binlogSplit = (BinlogSplit) newOffset.getSplits().get(0);
            binlogOffsetPersist = new HashMap<>(binlogSplit.getStartingOffset());
            binlogOffsetPersist.put(SPLIT_ID, BinlogSplit.BINLOG_SPLIT_ID);
        }
        this.currentOffset = newOffset;
    }

    @Override
    public void setBoundBackendId(long boundBackendId) {
        this.boundBackendId = boundBackendId;
    }

    @Override
    public void fetchRemoteMeta(Map<String, String> properties) throws Exception {
        Backend backend = StreamingJobUtils.selectBackend(cloudCluster, boundBackendId);
        JobBaseConfig requestParams =
                new JobBaseConfig(getJobId().toString(), sourceType.name(), sourceProperties, getFrontendAddress());
        InternalService.PRequestCdcClientRequest request = InternalService.PRequestCdcClientRequest.newBuilder()
                .setApi("/api/fetchEndOffset")
                .setParams(new Gson().toJson(requestParams)).build();
        TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
        InternalService.PRequestCdcClientResult result = null;
        try {
            Future<PRequestCdcClientResult> future = BackendServiceProxy.getInstance()
                    .requestCdcClient(address, request, Config.streaming_cdc_light_rpc_timeout_sec);
            result = future.get(Config.streaming_cdc_light_rpc_timeout_sec, TimeUnit.SECONDS);
            TStatusCode code = TStatusCode.findByValue(result.getStatus().getStatusCode());
            if (code != TStatusCode.OK) {
                log.warn("Failed to get end offset from backend, {}", result.getStatus().getErrorMsgs(0));
                throw new JobException(
                        "Failed to get end offset from backend," + result.getStatus().getErrorMsgs(0) + ", response: "
                                + result.getResponse());
            }
            String response = result.getResponse();
            try {
                ResponseBody<Map<String, String>> responseObj = objectMapper.readValue(
                        response,
                        new TypeReference<ResponseBody<Map<String, String>>>() {
                        }
                );
                Map<String, String> newEndOffset = responseObj.getData();
                // null→value also counts as a change: upstream may have advanced while fetch was blocked.
                if (endBinlogOffset == null || !endBinlogOffset.equals(newEndOffset)) {
                    hasMoreData = true;
                }
                endBinlogOffset = newEndOffset;
            } catch (JsonProcessingException e) {
                log.warn("Failed to parse end offset response: {}", response);
                throw new JobException(response);
            }
        } catch (TimeoutException te) {
            log.warn("cdc_client RPC timeout api=/api/fetchEndOffset jobId={} backend={}:{} timeout_sec={}",
                    getJobId(), backend.getHost(), backend.getBrpcPort(),
                    Config.streaming_cdc_light_rpc_timeout_sec);
            throw new JobException("cdc_client RPC timeout: /api/fetchEndOffset jobId=" + getJobId());
        } catch (ExecutionException | InterruptedException ex) {
            log.warn("Get end offset error: ", ex);
            throw new JobException(ex);
        }
    }

    @Override
    public boolean hasMoreDataToConsume() {
        if (currentOffset == null) {
            return true;
        }

        synchronized (splitsLock) {
            if (currentOffset.snapshotSplit()) {
                if (!remainingSplits.isEmpty()) {
                    return true;
                }
                // Splitting still in progress: wait for next tick.
                if (!noMoreSplits()) {
                    return false;
                }
                // Splitting done: snapshot-only completes; initial mode falls through to binlog.
                return !isSnapshotOnlyMode();
            }

            if (!hasMoreData) {
                return false;
            }

            if (CollectionUtils.isNotEmpty(remainingSplits)) {
                return true;
            }
        }
        if (MapUtils.isEmpty(endBinlogOffset)) {
            return false;
        }
        try {
            if (!currentOffset.snapshotSplit()) {
                BinlogSplit binlogSplit = (BinlogSplit) currentOffset.getSplits().get(0);
                if (MapUtils.isEmpty(binlogSplit.getStartingOffset())) {
                    // snapshot to binlog phase
                    return true;
                }
                hasMoreData = compareOffset(endBinlogOffset, new HashMap<>(binlogSplit.getStartingOffset()));
                return hasMoreData;
            } else {
                // snapshot means has data to consume
                return true;
            }
        } catch (Exception ex) {
            log.info("Compare offset error: ", ex);
            return false;
        }
    }

    private boolean compareOffset(Map<String, String> offsetFirst, Map<String, String> offsetSecond)
            throws JobException {
        Backend backend = StreamingJobUtils.selectBackend(cloudCluster, boundBackendId);
        CompareOffsetRequest requestParams =
                new CompareOffsetRequest(getJobId(), sourceType.name(), sourceProperties,
                        getFrontendAddress(), offsetFirst, offsetSecond);
        InternalService.PRequestCdcClientRequest request = InternalService.PRequestCdcClientRequest.newBuilder()
                .setApi("/api/compareOffset")
                .setParams(new Gson().toJson(requestParams)).build();
        TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
        InternalService.PRequestCdcClientResult result = null;
        try {
            Future<PRequestCdcClientResult> future = BackendServiceProxy.getInstance()
                    .requestCdcClient(address, request, Config.streaming_cdc_light_rpc_timeout_sec);
            result = future.get(Config.streaming_cdc_light_rpc_timeout_sec, TimeUnit.SECONDS);
            TStatusCode code = TStatusCode.findByValue(result.getStatus().getStatusCode());
            if (code != TStatusCode.OK) {
                log.warn("Failed to compare offset , {}", result.getStatus().getErrorMsgs(0));
                throw new JobException(
                        "Failed to compare offset ," + result.getStatus().getErrorMsgs(0) + ", response: "
                                + result.getResponse());
            }
            String response = result.getResponse();
            try {
                ResponseBody<Integer> responseObj = objectMapper.readValue(
                        response,
                        new TypeReference<ResponseBody<Integer>>() {
                        }
                );
                return responseObj.getData() > 0;
            } catch (JsonProcessingException e) {
                log.warn("Failed to parse compare offset response: {}", response);
                throw new JobException("Failed to parse compare offset response: " + response);
            }
        } catch (TimeoutException te) {
            log.warn("cdc_client RPC timeout api=/api/compareOffset jobId={} backend={}:{} timeout_sec={}",
                    getJobId(), backend.getHost(), backend.getBrpcPort(),
                    Config.streaming_cdc_light_rpc_timeout_sec);
            throw new JobException("cdc_client RPC timeout: /api/compareOffset jobId=" + getJobId());
        } catch (ExecutionException | InterruptedException ex) {
            log.warn("Compare offset error: ", ex);
            throw new JobException(ex);
        }
    }

    @Override
    public Offset deserializeOffset(String offset) {
        try {
            // chunk is highWatermark, binlog is offset map
            List<Map<String, String>> offsetMeta =
                    objectMapper.readValue(offset, new TypeReference<List<Map<String, String>>>() {});
            List<SnapshotSplit> snapshotSplits = new ArrayList<>(offsetMeta.size());
            for (Map<String, String> ot : offsetMeta) {
                String splitId = ot.remove(SPLIT_ID);
                if (BinlogSplit.BINLOG_SPLIT_ID.equals(splitId)) {
                    BinlogSplit binlogSplit = new BinlogSplit();
                    binlogSplit.setSplitId(splitId);
                    binlogSplit.setStartingOffset(ot);
                    return new JdbcOffset(Collections.singletonList(binlogSplit));
                }
                SnapshotSplit snapshotSplit = new SnapshotSplit();
                snapshotSplit.setSplitId(splitId);
                snapshotSplit.setHighWatermark(ot);
                snapshotSplits.add(snapshotSplit);
            }
            return new JdbcOffset(snapshotSplits);
        } catch (JsonProcessingException e) {
            log.warn("Failed to deserialize offset: {}", offset, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public Offset deserializeOffsetProperty(String offset) {
        if (offset == null || offset.trim().isEmpty()) {
            return null;
        }
        // JSON format: {"file":"binlog.000003","pos":154} or {"lsn":"123456"}
        if (DataSourceConfigValidator.isJsonOffset(offset)) {
            try {
                Map<String, String> offsetMap = objectMapper.readValue(offset,
                        new TypeReference<Map<String, String>>() {});
                return new JdbcOffset(Collections.singletonList(new BinlogSplit(offsetMap)));
            } catch (Exception e) {
                log.warn("Failed to parse JSON offset: {}", offset, e);
                return null;
            }
        }
        return null;
    }

    @Override
    public void validateAlterOffset(String offset) throws Exception {
        if (!DataSourceConfigValidator.isJsonOffset(offset)) {
            throw new AnalysisException(
                    "ALTER JOB for CDC only supports JSON specific offset, "
                    + "e.g. '{\"file\":\"binlog.000001\",\"pos\":\"154\"}' for MySQL "
                    + "or '{\"lsn\":\"12345678\"}' for PostgreSQL");
        }
    }

    /**
     * Replay snapshot splits if needed
     */
    @Override
    public void replayIfNeed(StreamingInsertJob job) throws JobException {
        synchronized (splitsLock) {
            this.cachedSyncTables = job.getSyncTables();
        }

        String offsetProviderPersist = job.getOffsetProviderPersist();
        if (offsetProviderPersist != null) {
            JdbcSourceOffsetProvider replayFromPersist = GsonUtils.GSON.fromJson(offsetProviderPersist,
                    JdbcSourceOffsetProvider.class);
            this.binlogOffsetPersist = replayFromPersist.getBinlogOffsetPersist();
            this.chunkHighWatermarkMap = replayFromPersist.getChunkHighWatermarkMap();
            this.tableSchemas = replayFromPersist.getTableSchemas();
            synchronized (splitsLock) {
                this.committedSplitProgress = replayFromPersist.getCommittedSplitProgress() != null
                        ? replayFromPersist.getCommittedSplitProgress() : new SplitProgress();
                this.cdcSplitProgress = this.committedSplitProgress.copy();
            }
            log.info("Replaying offset provider for job {}, binlogOffset size {}, chunkHighWatermark size {}",
                    getJobId(),
                    binlogOffsetPersist == null ? 0 : binlogOffsetPersist.size(),
                    chunkHighWatermarkMap == null ? 0 : chunkHighWatermarkMap.size());
            if (MapUtils.isNotEmpty(binlogOffsetPersist)) {
                currentOffset = new JdbcOffset();
                currentOffset.setSplits(Collections.singletonList(new BinlogSplit(binlogOffsetPersist)));
            } else {
                Map<String, List<SnapshotSplit>> snapshotSplits = StreamingJobUtils.restoreSplitsToJob(job.getJobId());
                if (MapUtils.isNotEmpty(chunkHighWatermarkMap) && MapUtils.isNotEmpty(snapshotSplits)) {
                    List<SnapshotSplit> lastSnapshotSplits =
                            recalculateRemainingSplits(chunkHighWatermarkMap, snapshotSplits);
                    resumeCdcSplitProgressFromSplits();
                    if (this.remainingSplits.isEmpty()) {
                        if (!lastSnapshotSplits.isEmpty()) {
                            currentOffset = new JdbcOffset();
                            currentOffset.setSplits(lastSnapshotSplits);
                        } else if (!isSnapshotOnlyMode() && noMoreSplits()) {
                            // initial mode: rebuild binlog split for snapshot-to-binlog transition
                            currentOffset = new JdbcOffset();
                            BinlogSplit binlogSplit = new BinlogSplit();
                            binlogSplit.setFinishedSplits(finishedSplits);
                            currentOffset.setSplits(Collections.singletonList(binlogSplit));
                        } else if (isSnapshotOnlyMode()) {
                            log.info("Replaying offset provider for job {}: snapshot-only mode completed,"
                                    + " finishedSplits={}, skip currentOffset restoration",
                                    getJobId(), finishedSplits.size());
                        }
                        // else: splitter mid-flight, keep currentOffset as snapshotSplit.
                    }
                }
            }
        } else if (checkNeedSplitChunks(sourceProperties)
                    && CollectionUtils.isEmpty(remainingSplits)
                    && CollectionUtils.isEmpty(finishedSplits)
                    && MapUtils.isEmpty(chunkHighWatermarkMap)
                    && MapUtils.isEmpty(binlogOffsetPersist)) {
            // After the Job is created for the first time, starting from the initial offset,
            // the task for the first split is scheduled, When the task status is running or failed,
            // If FE restarts, the split needs to be restore from the meta again.
            log.info("Replaying offset provider for job {}, offsetProviderPersist is empty", getJobId());
            Map<String, List<SnapshotSplit>> snapshotSplits = StreamingJobUtils.restoreSplitsToJob(job.getJobId());
            recalculateRemainingSplits(new HashMap<>(), snapshotSplits);
            resumeCdcSplitProgressFromSplits();
        } else {
            log.info("No need to replay offset provider for job {}", getJobId());
        }
    }

    /** Rebuild cdcSplitProgress after restart so advanceSplits resumes mid-table instead of skipping it. */
    protected void resumeCdcSplitProgressFromSplits() {
        synchronized (splitsLock) {
            if (cachedSyncTables == null || cachedSyncTables.isEmpty()) {
                return;
            }
            SnapshotSplit mid = findResumeMidSplit(cachedSyncTables, finishedSplits, remainingSplits);
            if (mid != null) {
                applySplitToProgress(cdcSplitProgress, mid, getTableName(mid.getTableId()));
            } else {
                clearProgress(cdcSplitProgress);
            }
            log.info("Resume cdcSplitProgress for job {}: finishedSplits={}, remainingSplits={}, "
                            + "cdcSplitProgress=(table={}, nextStart={}, nextSplitId={})",
                    getJobId(), finishedSplits.size(), remainingSplits.size(),
                    cdcSplitProgress.getCurrentSplittingTable(),
                    Arrays.toString(cdcSplitProgress.getNextSplitStart()),
                    cdcSplitProgress.getNextSplitId());
        }
    }

    /**
     * Find the at-most-one table cut to mid (its largest-id split has non-null splitEnd).
     * Returns null when every table in {@code syncTables} is either untouched or fully cut.
     */
    static SnapshotSplit findResumeMidSplit(List<String> syncTables,
                                            List<SnapshotSplit> finishedSplits,
                                            List<SnapshotSplit> remainingSplits) {
        // Map keys are bare names so syncTables (bare) and SnapshotSplit.tableId (qualified) align.
        Map<String, SnapshotSplit> lastPerTable = new HashMap<>();
        pickLastById(finishedSplits, lastPerTable);
        pickLastById(remainingSplits, lastPerTable);
        for (String tbl : syncTables) {
            SnapshotSplit last = lastPerTable.get(getTableName(tbl));
            if (last != null && last.getSplitEnd() != null && last.getSplitEnd().length > 0) {
                return last;
            }
        }
        return null;
    }

    private static void pickLastById(List<SnapshotSplit> splits, Map<String, SnapshotSplit> out) {
        for (SnapshotSplit s : splits) {
            String key = getTableName(s.getTableId());
            SnapshotSplit prev = out.get(key);
            if (prev == null || splitIdOf(s.getSplitId()) > splitIdOf(prev.getSplitId())) {
                out.put(key, s);
            }
        }
    }

    /**
     * Assign the HW value to the synchronized Split,
     * and remove the Split from remainSplit and place it in finishedSplit.
     */
    protected List<SnapshotSplit> recalculateRemainingSplits(
            Map<String, Map<String, Map<String, String>>> chunkHighWatermarkMap,
            Map<String, List<SnapshotSplit>> snapshotSplits) {
        this.finishedSplits = new ArrayList<>();
        for (Map.Entry<String, Map<String, Map<String, String>>> entry : chunkHighWatermarkMap.entrySet()) {
            String tableId = entry.getKey();
            Map<String, Map<String, String>> splitIdToHighWatermark = entry.getValue();
            if (MapUtils.isEmpty(splitIdToHighWatermark)) {
                continue;
            }
            // db.schema.table
            String tableName = getTableName(tableId);
            if (tableName == null) {
                continue;
            }
            List<SnapshotSplit> tableSplits = snapshotSplits.get(tableName);
            if (CollectionUtils.isEmpty(tableSplits)) {
                continue;
            }
            tableSplits.removeIf(split -> {
                String splitId = split.getSplitId();
                Map<String, String> highWatermark = splitIdToHighWatermark.get(splitId);
                if (highWatermark != null) {
                    split.setHighWatermark(highWatermark);
                    finishedSplits.add(split);
                    return true;
                }
                return false;
            });
        }

        this.remainingSplits = snapshotSplits.values().stream()
                .flatMap(List::stream)
                .collect(Collectors.toList());

        // The splits that were last syncing before the restart
        int splitsNum = Math.min(remainingSplits.size(), snapshotParallelism);
        List<SnapshotSplit> lastSnapshotSplits = new ArrayList<>(remainingSplits.subList(0, splitsNum));
        return lastSnapshotSplits;
    }

    private static String getTableName(String tableId) {
        if (tableId == null) {
            return null;
        }
        String[] split = tableId.split("\\.");
        return split[split.length - 1];
    }

    @Override
    public String getPersistInfo() {
        return GsonUtils.GSON.toJson(this);
    }

    // ============ Async split progress (driven by scheduler each tick) ============

    /**
     * One-time setup at CREATE.
     * - initial/snapshot mode: init split progress; scheduler will drive advanceSplits() each tick.
     * - latest mode (and other non-splitting modes): open the remote reader (e.g. PG slot) so the
     *   binlog phase can start immediately; no snapshot splitting will happen.
     */
    @Override
    public void initOnCreate(List<String> syncTables) throws JobException {
        if (!checkNeedSplitChunks(sourceProperties)) {
            initSourceReader();
            return;
        }
        synchronized (splitsLock) {
            this.cachedSyncTables = syncTables;
            this.committedSplitProgress = new SplitProgress();
            this.cdcSplitProgress = new SplitProgress();
        }
    }

    @Override
    public int pendingSplitCount() {
        synchronized (splitsLock) {
            return remainingSplits.size();
        }
    }

    @Override
    public boolean noMoreSplits() {
        if (!checkNeedSplitChunks(sourceProperties)) {
            return true;
        }
        synchronized (splitsLock) {
            if (currentOffset != null && !currentOffset.snapshotSplit()) {
                return true;
            }
            return cdcSplitProgress.getCurrentSplittingTable() == null
                    && computeCdcRemainingTables().isEmpty();
        }
    }

    /** Tables not yet touched by cdc fetching. Caller must hold splitsLock. */
    private List<String> computeCdcRemainingTables() {
        if (cachedSyncTables == null || cachedSyncTables.isEmpty()) {
            return Collections.emptyList();
        }
        // SnapshotSplit.tableId is qualified ("schema.table"/"db.table"); cachedSyncTables is bare — normalize.
        Set<String> touched = new HashSet<>();
        for (SnapshotSplit s : finishedSplits) {
            touched.add(getTableName(s.getTableId()));
        }
        for (SnapshotSplit s : remainingSplits) {
            touched.add(getTableName(s.getTableId()));
        }
        if (cdcSplitProgress.getCurrentSplittingTable() != null) {
            touched.add(getTableName(cdcSplitProgress.getCurrentSplittingTable()));
        }
        List<String> result = new ArrayList<>(cachedSyncTables.size());
        for (String t : cachedSyncTables) {
            if (!touched.contains(getTableName(t))) {
                result.add(t);
            }
        }
        return result;
    }

    @Override
    public void advanceSplits() throws JobException {
        // Phase 1 (locked, fast): pick next table & snapshot the resume cursor.
        String tbl;
        Object[] startVal;
        Integer splitId;
        synchronized (splitsLock) {
            if (cdcSplitProgress.getCurrentSplittingTable() == null) {
                List<String> remaining = computeCdcRemainingTables();
                if (remaining.isEmpty()) {
                    return;
                }
                cdcSplitProgress.setCurrentSplittingTable(getTableName(remaining.get(0)));
                cdcSplitProgress.setNextSplitStart(null);
                cdcSplitProgress.setNextSplitId(null);
            }
            tbl = cdcSplitProgress.getCurrentSplittingTable();
            startVal = cdcSplitProgress.getNextSplitStart();
            splitId = cdcSplitProgress.getNextSplitId();
        }

        // Phase 2 (unlocked, slow): RPC. Keeps updateOffset / scheduler tick unblocked
        // so task dispatch can drain remainingSplits while we fetch the next batch.
        List<SnapshotSplit> batch = rpcFetchSplitsBatch(tbl, startVal, splitId);
        if (batch == null || batch.isEmpty()) {
            return;
        }

        // Phase 3 (locked, fast): compute newSplits + splitsOfTbl WITHOUT mutating in-memory.
        // Persist-before-publish keeps streaming_job_meta from lagging cdcSplitProgress, so a
        // crash never leaves an HW recorded for a split whose definition was not written.
        List<SnapshotSplit> newSplits;
        List<SnapshotSplit> splitsOfTbl;
        synchronized (splitsLock) {
            // replayIfNeed / pause-resume may have moved the cursor during the RPC — discard.
            if (!tbl.equals(cdcSplitProgress.getCurrentSplittingTable())
                    || !Objects.equals(splitId, cdcSplitProgress.getNextSplitId())) {
                log.info("advanceSplits discard batch for job {} table {}: state moved on "
                                + "during RPC (now table={}, splitId={})",
                        getJobId(), tbl,
                        cdcSplitProgress.getCurrentSplittingTable(),
                        cdcSplitProgress.getNextSplitId());
                return;
            }
            Set<String> existingIds = new HashSet<>();
            finishedSplits.forEach(s -> existingIds.add(s.getSplitId()));
            remainingSplits.forEach(s -> existingIds.add(s.getSplitId()));
            newSplits = new ArrayList<>();
            for (SnapshotSplit s : batch) {
                if (!existingIds.contains(s.getSplitId())) {
                    newSplits.add(s);
                }
            }
            if (newSplits.size() < batch.size()) {
                log.info("advanceSplits dedup'd {} duplicate splits (batch={}, new={}) for job {} table {}",
                        batch.size() - newSplits.size(), batch.size(), newSplits.size(), getJobId(), tbl);
            }
            // Post-batch meta state: finished + remaining + newSplits, filtered by table.
            List<SnapshotSplit> allForTbl = new ArrayList<>(
                    finishedSplits.size() + remainingSplits.size() + newSplits.size());
            allForTbl.addAll(finishedSplits);
            allForTbl.addAll(remainingSplits);
            allForTbl.addAll(newSplits);
            // tbl is bare (matches cachedSyncTables); SnapshotSplit.tableId is qualified.
            splitsOfTbl = allForTbl.stream()
                    .filter(s -> tbl.equals(getTableName(s.getTableId())))
                    .sorted(Comparator.comparingInt(s -> splitIdOf(s.getSplitId())))
                    .collect(Collectors.toList());
        }

        // Phase 4 (unlocked, slow): persist FIRST. On failure throw → PAUSE; autoResume retries
        // with the same (tbl, startVal, splitId), idempotent on UNIQUE KEY (id, job_id).
        try {
            StreamingJobUtils.upsertChunkList(getJobId(), tbl, splitsOfTbl);
        } catch (Exception e) {
            throw new JobException("Failed to persist chunk_list for job " + getJobId()
                    + " table " + tbl + ": " + e.getMessage(), e);
        }

        // Phase 5 (locked, fast): publish. Skip if cursor moved during Phase 4 (already in meta).
        synchronized (splitsLock) {
            if (!tbl.equals(cdcSplitProgress.getCurrentSplittingTable())
                    || !Objects.equals(splitId, cdcSplitProgress.getNextSplitId())) {
                log.info("advanceSplits discard publish for job {} table {}: state moved on "
                                + "after UPSERT (now table={}, splitId={})",
                        getJobId(), tbl,
                        cdcSplitProgress.getCurrentSplittingTable(),
                        cdcSplitProgress.getNextSplitId());
                return;
            }
            remainingSplits.addAll(newSplits);
            applySplitToProgress(cdcSplitProgress, batch.get(batch.size() - 1), tbl);
            log.info("advanceSplits jobId={} table={} request(nextStart={}, nextSplitId={}) "
                            + "published {} new splits, cdcSplitProgress -> (table={}, nextStart={}, nextSplitId={})",
                    getJobId(), tbl, Arrays.toString(startVal), splitId, newSplits.size(),
                    cdcSplitProgress.getCurrentSplittingTable(),
                    Arrays.toString(cdcSplitProgress.getNextSplitStart()),
                    cdcSplitProgress.getNextSplitId());
        }
    }

    /** Parse the trailing integer id from flink-cdc splitId format "tableId:id". */
    static int splitIdOf(String splitId) {
        if (splitId == null) {
            throw new IllegalArgumentException("splitId is null");
        }
        int colon = splitId.lastIndexOf(':');
        if (colon < 0 || colon == splitId.length() - 1) {
            throw new IllegalArgumentException("malformed splitId, expected 'tableId:id': " + splitId);
        }
        try {
            return Integer.parseInt(splitId.substring(colon + 1));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("malformed splitId, expected 'tableId:id': " + splitId, e);
        }
    }

    /** Reset progress to "no table being split" state. */
    private static void clearProgress(SplitProgress progress) {
        progress.setCurrentSplittingTable(null);
        progress.setNextSplitStart(null);
        progress.setNextSplitId(null);
    }

    /**
     * Apply a split's position to a progress object.
     * - splitEnd null/empty (final split of table) → clear all fields.
     * - splitEnd non-empty → set currentSplittingTable to tableName (bare, matching the
     *   form used in cachedSyncTables / snapshotTable), advance start/id.
     * tableName must be the bare name; SnapshotSplit.tableId is qualified (schema.table)
     * and would break the fetchSplits RPC contract if reused as currentSplittingTable.
     */
    private static void applySplitToProgress(SplitProgress progress, SnapshotSplit split, String tableName) {
        if (split.getSplitEnd() == null || split.getSplitEnd().length == 0) {
            clearProgress(progress);
        } else {
            progress.setCurrentSplittingTable(tableName);
            progress.setNextSplitStart(split.getSplitEnd());
            progress.setNextSplitId(splitIdOf(split.getSplitId()) + 1);
        }
    }

    /** RPC fetchSplits with (table, nextSplitStart, nextSplitId, batchSize). protected for UT subclass. */
    protected List<SnapshotSplit> rpcFetchSplitsBatch(String table, Object[] nextSplitStart, Integer nextSplitId)
            throws JobException {
        Backend backend = StreamingJobUtils.selectBackend(cloudCluster);
        FetchTableSplitsRequest requestParams = new FetchTableSplitsRequest(
                getJobId(), sourceType.name(), sourceProperties, getFrontendAddress(), table);
        requestParams.setNextSplitStart(nextSplitStart);
        requestParams.setNextSplitId(nextSplitId);
        requestParams.setBatchSize(Config.streaming_cdc_fetch_splits_batch_size);
        InternalService.PRequestCdcClientRequest request = InternalService.PRequestCdcClientRequest.newBuilder()
                .setApi("/api/fetchSplits")
                .setParams(new Gson().toJson(requestParams))
                .build();
        TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
        try {
            Future<PRequestCdcClientResult> future = BackendServiceProxy.getInstance()
                    .requestCdcClient(address, request, Config.streaming_cdc_heavy_rpc_timeout_sec);
            PRequestCdcClientResult result = future.get(
                    Config.streaming_cdc_heavy_rpc_timeout_sec, TimeUnit.SECONDS);
            TStatusCode code = TStatusCode.findByValue(result.getStatus().getStatusCode());
            if (code != TStatusCode.OK) {
                throw new JobException("fetchSplits backend error: " + result.getStatus().getErrorMsgs(0));
            }
            ResponseBody<List<SnapshotSplit>> resp = objectMapper.readValue(
                    result.getResponse(),
                    new TypeReference<ResponseBody<List<SnapshotSplit>>>() {});
            return resp.getData();
        } catch (TimeoutException te) {
            throw new JobException("fetchSplits RPC timeout: jobId=" + getJobId() + " table=" + table);
        } catch (Exception ex) {
            throw new JobException("fetchSplits failed: " + ex.getMessage());
        }
    }

    protected boolean checkNeedSplitChunks(Map<String, String> sourceProperties) {
        String startMode = sourceProperties.get(DataSourceConfigKeys.OFFSET);
        if (startMode == null) {
            return false;
        }
        return DataSourceConfigKeys.OFFSET_INITIAL.equalsIgnoreCase(startMode)
                || DataSourceConfigKeys.OFFSET_SNAPSHOT.equalsIgnoreCase(startMode);
    }

    protected boolean isSnapshotOnlyMode() {
        String offset = sourceProperties.get(DataSourceConfigKeys.OFFSET);
        return DataSourceConfigKeys.OFFSET_SNAPSHOT.equalsIgnoreCase(offset);
    }

    @Override
    public String getLag() {
        if (currentOffset == null || currentOffset.snapshotSplit()) {
            return "";
        }
        // Source is idle (last task consumed no data), report zero lag
        if (!hasMoreData) {
            return "0";
        }
        BinlogSplit binlogSplit = (BinlogSplit) currentOffset.getSplits().get(0);
        Map<String, String> offsetMap = binlogSplit.getStartingOffset();
        if (MapUtils.isEmpty(offsetMap)) {
            return "";
        }
        long eventTimeMs = extractEventTimeMs(offsetMap);
        if (eventTimeMs <= 0) {
            return "0";
        }
        long lagSec = (System.currentTimeMillis() - eventTimeMs) / 1000;
        return String.valueOf(Math.max(lagSec, 0));
    }

    /**
     * Extract event timestamp in milliseconds from binlog offset map.
     * MySQL: ts_sec (seconds), PostgreSQL: ts_usec (microseconds).
     */
    protected long extractEventTimeMs(Map<String, String> offsetMap) {
        try {
            String tsSec = offsetMap.get("ts_sec");
            if (tsSec != null) {
                return Long.parseLong(tsSec) * 1000;
            }
            String tsUsec = offsetMap.get("ts_usec");
            if (tsUsec != null) {
                return Long.parseLong(tsUsec) / 1000;
            }
        } catch (NumberFormatException e) {
            log.warn("Failed to parse event timestamp from offset: {}", offsetMap, e);
        }
        return -1;
    }

    @Override
    public boolean hasReachedEnd() {
        if (!isSnapshotOnlyMode()) {
            return false;
        }
        synchronized (splitsLock) {
            return CollectionUtils.isNotEmpty(finishedSplits)
                    && remainingSplits.isEmpty()
                    && noMoreSplits();
        }
    }

    /**
     * Source reader needs to be initialized here.
     * For example, PG slots need to be created first;
     * otherwise, conflicts will occur in multi-backends scenarios.
     */
    private void initSourceReader() throws JobException {
        Backend backend = StreamingJobUtils.selectBackend(cloudCluster);
        JobBaseConfig requestParams =
                new JobBaseConfig(getJobId().toString(), sourceType.name(), sourceProperties, getFrontendAddress());
        InternalService.PRequestCdcClientRequest request = InternalService.PRequestCdcClientRequest.newBuilder()
                .setApi("/api/initReader")
                .setParams(new Gson().toJson(requestParams)).build();
        TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
        InternalService.PRequestCdcClientResult result = null;
        try {
            Future<PRequestCdcClientResult> future = BackendServiceProxy.getInstance()
                    .requestCdcClient(address, request, Config.streaming_cdc_heavy_rpc_timeout_sec);
            result = future.get(Config.streaming_cdc_heavy_rpc_timeout_sec, TimeUnit.SECONDS);
            TStatusCode code = TStatusCode.findByValue(result.getStatus().getStatusCode());
            if (code != TStatusCode.OK) {
                log.warn("Failed to init job {} reader, {}", getJobId(), result.getStatus().getErrorMsgs(0));
                throw new JobException(
                        "Failed to init source reader," + result.getStatus().getErrorMsgs(0) + ", response: "
                                + result.getResponse());
            }
            String response = result.getResponse();
            try {
                ResponseBody<String> responseObj = objectMapper.readValue(
                        response,
                        new TypeReference<ResponseBody<String>>() {
                        }
                );
                if (responseObj.getCode() == RestApiStatusCode.OK.code) {
                    log.info("Init {} source reader successfully, response: {}", getJobId(), responseObj.getData());
                    return;
                } else {
                    throw new JobException("Failed to init source reader, error: " + responseObj.getData());
                }
            } catch (JobException jobex) {
                log.warn("Failed to init {} source reader, {}", getJobId(), response);
                throw new JobException(jobex.getMessage());
            } catch (Exception e) {
                log.warn("Failed to init {} source reader, {}", getJobId(), response);
                throw new JobException("Failed to init source reader, cause " + e.getMessage());
            }
        } catch (TimeoutException te) {
            log.warn("cdc_client RPC timeout api=/api/initReader jobId={} backend={}:{} timeout_sec={}",
                    getJobId(), backend.getHost(), backend.getBrpcPort(),
                    Config.streaming_cdc_heavy_rpc_timeout_sec);
            throw new JobException("cdc_client RPC timeout: /api/initReader jobId=" + getJobId());
        } catch (ExecutionException | InterruptedException ex) {
            log.warn("init source reader: ", ex);
            throw new JobException(ex);
        }
    }

    public void cleanMeta(Long jobId, long runtimeBackendId) throws JobException {
        // clean meta table
        StreamingJobUtils.deleteJobMeta(jobId);
        // Dropping the slot only succeeds on the BE owning the live reader (it stops its own engine
        // first, freeing the slot). Prefer the runtime BE (covers the unbound snapshot phase), then
        // the bound BE; both may be alive but not load-available, so route by isAlive. Only when
        // neither is alive fall back to a random BE to drop the now-inactive slot.
        Backend backend = aliveBackend(runtimeBackendId);
        if (backend == null) {
            backend = aliveBackend(boundBackendId);
        }
        if (backend == null) {
            backend = StreamingJobUtils.selectBackend(cloudCluster, boundBackendId);
        }
        JobBaseConfig requestParams =
                new JobBaseConfig(getJobId().toString(), sourceType.name(), sourceProperties, getFrontendAddress());
        InternalService.PRequestCdcClientRequest request = InternalService.PRequestCdcClientRequest.newBuilder()
                .setApi("/api/close")
                .setParams(new Gson().toJson(requestParams)).build();
        TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
        InternalService.PRequestCdcClientResult result = null;
        try {
            Future<PRequestCdcClientResult> future = BackendServiceProxy.getInstance()
                    .requestCdcClient(address, request, Config.streaming_cdc_light_rpc_timeout_sec);
            result = future.get(Config.streaming_cdc_light_rpc_timeout_sec, TimeUnit.SECONDS);
            TStatusCode code = TStatusCode.findByValue(result.getStatus().getStatusCode());
            if (code != TStatusCode.OK) {
                log.warn("Failed to close job {} source {}", jobId, result.getStatus().getErrorMsgs(0));
            }
        } catch (TimeoutException te) {
            log.warn("cdc_client RPC timeout api=/api/close jobId={} backend={}:{} timeout_sec={}",
                    jobId, backend.getHost(), backend.getBrpcPort(),
                    Config.streaming_cdc_light_rpc_timeout_sec);
        } catch (ExecutionException | InterruptedException ex) {
            log.warn("Close job error: ", ex);
        }
    }

    private static Backend aliveBackend(long backendId) {
        if (backendId <= 0) {
            return null;
        }
        Backend be = Env.getCurrentSystemInfo().getBackend(backendId);
        return be != null && be.isAlive() ? be : null;
    }

    private String getFrontendAddress() {
        return Env.getCurrentEnv().getMasterHost() + ":" + Env.getCurrentEnv().getMasterHttpPort();
    }


    /** Mirrors flink-cdc ChunkSplitterState. */
    @Getter
    @Setter
    public static class SplitProgress {
        @SerializedName("ct")
        private String currentSplittingTable;

        @SerializedName("ns")
        private Object[] nextSplitStart;

        @SerializedName("ni")
        private Integer nextSplitId;

        public SplitProgress() {}

        /** Deep copy for transferring committed -> cdc view on restart. */
        public SplitProgress copy() {
            SplitProgress c = new SplitProgress();
            c.currentSplittingTable = this.currentSplittingTable;
            c.nextSplitStart = this.nextSplitStart == null
                    ? null : Arrays.copyOf(this.nextSplitStart, this.nextSplitStart.length);
            c.nextSplitId = this.nextSplitId;
            return c;
        }
    }
}
