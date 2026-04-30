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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
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

    JdbcOffset currentOffset;
    Map<String, String> endBinlogOffset;

    @SerializedName("chw")
    // tableID -> splitId -> chunk of highWatermark
    Map<String, Map<String, Map<String, String>>> chunkHighWatermarkMap;
    @SerializedName("bop")
    Map<String, String> binlogOffsetPersist;

    @SerializedName("ts")
    String tableSchemas;

    volatile boolean hasMoreData = true;

    transient volatile String cloudCluster;

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
        JdbcOffset nextOffset = new JdbcOffset();
        if (!remainingSplits.isEmpty()) {
            int splitsNum = Math.min(remainingSplits.size(), snapshotParallelism);
            List<SnapshotSplit> snapshotSplits = new ArrayList<>(remainingSplits.subList(0, splitsNum));
            nextOffset.setSplits(snapshotSplits);
            return nextOffset;
        } else if (currentOffset != null && currentOffset.snapshotSplit()) {
            // initial mode: snapshot to binlog
            // snapshot-only mode must be intercepted by hasReachedEnd() before reaching here
            BinlogSplit binlogSplit = new BinlogSplit();
            binlogSplit.setFinishedSplits(finishedSplits);
            nextOffset.setSplits(Collections.singletonList(binlogSplit));
            return nextOffset;
        } else {
            // only binlog
            return currentOffset == null ? new JdbcOffset(Collections.singletonList(new BinlogSplit())) : currentOffset;
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
        this.currentOffset = (JdbcOffset) offset;
        if (currentOffset.snapshotSplit()) {
            List<? extends AbstractSourceSplit> splits = currentOffset.getSplits();
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
                } else {
                    log.warn("Cannot find snapshot split {} in remainingSplits for job {}", splitId, getJobId());
                }
            }
        } else {
            BinlogSplit binlogSplit = (BinlogSplit) currentOffset.getSplits().get(0);
            binlogOffsetPersist = new HashMap<>(binlogSplit.getStartingOffset());
            binlogOffsetPersist.put(SPLIT_ID, BinlogSplit.BINLOG_SPLIT_ID);
        }
    }

    @Override
    public void fetchRemoteMeta(Map<String, String> properties) throws Exception {
        Backend backend = StreamingJobUtils.selectBackend(cloudCluster);
        JobBaseConfig requestParams =
                new JobBaseConfig(getJobId().toString(), sourceType.name(), sourceProperties, getFrontendAddress());
        InternalService.PRequestCdcClientRequest request = InternalService.PRequestCdcClientRequest.newBuilder()
                .setApi("/api/fetchEndOffset")
                .setParams(new Gson().toJson(requestParams)).build();
        TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
        InternalService.PRequestCdcClientResult result = null;
        try {
            Future<PRequestCdcClientResult> future =
                    BackendServiceProxy.getInstance().requestCdcClient(address, request);
            result = future.get();
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

        if (currentOffset.snapshotSplit()) {
            if (isSnapshotOnlyMode() && remainingSplits.isEmpty()) {
                return false;
            }
            return true;
        }

        if (!hasMoreData) {
            return false;
        }

        if (CollectionUtils.isNotEmpty(remainingSplits)) {
            return true;
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
        Backend backend = StreamingJobUtils.selectBackend(cloudCluster);
        CompareOffsetRequest requestParams =
                new CompareOffsetRequest(getJobId(), sourceType.name(), sourceProperties,
                        getFrontendAddress(), offsetFirst, offsetSecond);
        InternalService.PRequestCdcClientRequest request = InternalService.PRequestCdcClientRequest.newBuilder()
                .setApi("/api/compareOffset")
                .setParams(new Gson().toJson(requestParams)).build();
        TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
        InternalService.PRequestCdcClientResult result = null;
        try {
            Future<PRequestCdcClientResult> future =
                    BackendServiceProxy.getInstance().requestCdcClient(address, request);
            result = future.get();
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
        String offsetProviderPersist = job.getOffsetProviderPersist();
        if (offsetProviderPersist != null) {
            JdbcSourceOffsetProvider replayFromPersist = GsonUtils.GSON.fromJson(offsetProviderPersist,
                    JdbcSourceOffsetProvider.class);
            this.binlogOffsetPersist = replayFromPersist.getBinlogOffsetPersist();
            this.chunkHighWatermarkMap = replayFromPersist.getChunkHighWatermarkMap();
            this.tableSchemas = replayFromPersist.getTableSchemas();
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
                    if (this.remainingSplits.isEmpty()) {
                        if (!lastSnapshotSplits.isEmpty()) {
                            currentOffset = new JdbcOffset();
                            currentOffset.setSplits(lastSnapshotSplits);
                        } else if (!isSnapshotOnlyMode()) {
                            // initial mode: rebuild binlog split for snapshot-to-binlog transition
                            currentOffset = new JdbcOffset();
                            BinlogSplit binlogSplit = new BinlogSplit();
                            binlogSplit.setFinishedSplits(finishedSplits);
                            currentOffset.setSplits(Collections.singletonList(binlogSplit));
                        } else {
                            // snapshot-only completed: leave currentOffset as null,
                            // hasReachedEnd() detects completion via finishedSplits
                            log.info("Replaying offset provider for job {}: snapshot-only mode completed,"
                                    + " finishedSplits={}, skip currentOffset restoration",
                                    getJobId(), finishedSplits.size());
                        }
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
        } else {
            log.info("No need to replay offset provider for job {}", getJobId());
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

    private String getTableName(String tableId) {
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

    public void splitChunks(List<String> createTbls) throws JobException {
        // todo: When splitting takes a long time, it needs to be changed to asynchronous.
        if (checkNeedSplitChunks(sourceProperties)) {
            Map<String, List<SnapshotSplit>> tableSplits = new LinkedHashMap<>();
            for (String tbl : createTbls) {
                List<SnapshotSplit> snapshotSplits = requestTableSplits(tbl);
                tableSplits.put(tbl, snapshotSplits);
            }
            // save chunk list to system table
            saveChunkMeta(tableSplits);
            this.remainingSplits = tableSplits.values().stream()
                    .flatMap(List::stream)
                    .collect(Collectors.toList());
        } else {
            // The source reader is automatically initialized when the split is obtained.
            // In latest mode, a separate init is required.init source reader
            initSourceReader();
        }
    }

    private void saveChunkMeta(Map<String, List<SnapshotSplit>> tableSplits) throws JobException {
        try {
            StreamingJobUtils.createMetaTableIfNotExist();
            StreamingJobUtils.insertSplitsToMeta(getJobId(), tableSplits);
        } catch (Exception e) {
            log.warn("save chunk meta error: ", e);
            throw new JobException(e.getMessage());
        }
    }

    private List<SnapshotSplit> requestTableSplits(String table) throws JobException {
        Backend backend = StreamingJobUtils.selectBackend(cloudCluster);
        FetchTableSplitsRequest requestParams =
                new FetchTableSplitsRequest(getJobId(), sourceType.name(),
                        sourceProperties, getFrontendAddress(), table);
        InternalService.PRequestCdcClientRequest request = InternalService.PRequestCdcClientRequest.newBuilder()
                .setApi("/api/fetchSplits")
                .setParams(new Gson().toJson(requestParams)).build();
        TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
        InternalService.PRequestCdcClientResult result = null;
        try {
            Future<PRequestCdcClientResult> future =
                    BackendServiceProxy.getInstance().requestCdcClient(address, request);
            result = future.get();
            TStatusCode code = TStatusCode.findByValue(result.getStatus().getStatusCode());
            if (code != TStatusCode.OK) {
                log.warn("Failed to get split from backend, {}", result.getStatus().getErrorMsgs(0));
                throw new JobException(
                        "Failed to get split from backend," + result.getStatus().getErrorMsgs(0) + ", response: "
                                + result.getResponse());
            }
            String response = result.getResponse();
            try {
                ResponseBody<List<SnapshotSplit>> responseObj = objectMapper.readValue(
                        response,
                        new TypeReference<ResponseBody<List<SnapshotSplit>>>() {
                        }
                );
                List<SnapshotSplit> splits = responseObj.getData();
                return splits;
            } catch (JsonProcessingException e) {
                log.warn("Failed to parse split response: {}", response);
                throw new JobException("Failed to parse split response: " + response);
            }
        } catch (ExecutionException | InterruptedException ex) {
            log.warn("Get splits error: ", ex);
            throw new JobException(ex);
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
    public void onTaskCommitted(long scannedRows, long loadBytes) {
        if (scannedRows == 0 && loadBytes == 0) {
            hasMoreData = false;
        }
    }

    @Override
    public boolean hasReachedEnd() {
        return isSnapshotOnlyMode()
                && CollectionUtils.isNotEmpty(finishedSplits)
                && remainingSplits.isEmpty();
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
            Future<PRequestCdcClientResult> future =
                    BackendServiceProxy.getInstance().requestCdcClient(address, request);
            result = future.get();
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
        } catch (ExecutionException | InterruptedException ex) {
            log.warn("init source reader: ", ex);
            throw new JobException(ex);
        }
    }

    public void cleanMeta(Long jobId) throws JobException {
        // clean meta table
        StreamingJobUtils.deleteJobMeta(jobId);
        Backend backend = StreamingJobUtils.selectBackend(cloudCluster);
        JobBaseConfig requestParams =
                new JobBaseConfig(getJobId().toString(), sourceType.name(), sourceProperties, getFrontendAddress());
        InternalService.PRequestCdcClientRequest request = InternalService.PRequestCdcClientRequest.newBuilder()
                .setApi("/api/close")
                .setParams(new Gson().toJson(requestParams)).build();
        TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
        InternalService.PRequestCdcClientResult result = null;
        try {
            Future<PRequestCdcClientResult> future =
                    BackendServiceProxy.getInstance().requestCdcClient(address, request);
            result = future.get();
            TStatusCode code = TStatusCode.findByValue(result.getStatus().getStatusCode());
            if (code != TStatusCode.OK) {
                log.warn("Failed to close job {} source {}", jobId, result.getStatus().getErrorMsgs(0));
            }
        } catch (ExecutionException | InterruptedException ex) {
            log.warn("Close job error: ", ex);
        }
    }

    private String getFrontendAddress() {
        return Env.getCurrentEnv().getMasterHost() + ":" + Env.getCurrentEnv().getMasterHttpPort();
    }
}
