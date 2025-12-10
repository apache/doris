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

import org.apache.doris.httpv2.entity.ResponseBody;
import org.apache.doris.job.common.DataSourceType;
import org.apache.doris.job.common.LoadConstants;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.extensions.insert.streaming.StreamingInsertJob;
import org.apache.doris.job.extensions.insert.streaming.StreamingJobProperties;
import org.apache.doris.job.offset.Offset;
import org.apache.doris.job.offset.SourceOffsetProvider;
import org.apache.doris.job.offset.jdbc.split.AbstractSourceSplit;
import org.apache.doris.job.offset.jdbc.split.BinlogSplit;
import org.apache.doris.job.offset.jdbc.split.SnapshotSplit;
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

import java.io.IOException;
import java.util.ArrayList;
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
    private Long jobId;
    private DataSourceType sourceType;
    private Map<String, String> sourceProperties = new HashMap<>();

    List<SnapshotSplit> remainingSplits = new ArrayList<>();
    List<SnapshotSplit> finishedSplits = new ArrayList<>();

    JdbcOffset currentOffset;
    Map<String, String> endBinlogOffset;

    @SerializedName("chw")
    // tableID -> splitId -> chunk of highWatermark
    Map<String, Map<String, Map<String, String>>> chunkHighWatermarkMap;
    @SerializedName("bop")
    Map<String, String> binlogOffsetPersist;

    boolean hasMoreData = true;

    public JdbcSourceOffsetProvider(Long jobId, DataSourceType sourceType, Map<String, String> sourceProperties) {
        this.jobId = jobId;
        this.sourceType = sourceType;
        this.sourceProperties = sourceProperties;
        this.chunkHighWatermarkMap = new HashMap<>();
    }

    @Override
    public String getSourceType() {
        return "jdbc";
    }

    @Override
    public Offset getNextOffset(StreamingJobProperties jobProps, Map<String, String> properties) {
        JdbcOffset nextOffset = new JdbcOffset();
        if (!remainingSplits.isEmpty()) {
            // snapshot read
            SnapshotSplit snapshotSplit = remainingSplits.get(0);
            nextOffset.setSplit(snapshotSplit);
            return nextOffset;
        } else if (currentOffset != null && currentOffset.getSplit().snapshotSplit()) {
            // snapshot to binlog
            BinlogSplit binlogSplit = new BinlogSplit();
            binlogSplit.setFinishedSplits(finishedSplits);
            nextOffset.setSplit(binlogSplit);
            return nextOffset;
        } else {
            // only binlog
            return currentOffset == null ? new JdbcOffset(new BinlogSplit()) : currentOffset;
        }
    }

    @Override
    public String getShowCurrentOffset() {
        if (this.currentOffset != null) {
            AbstractSourceSplit split = this.currentOffset.getSplit();
            if (split.snapshotSplit()) {
                SnapshotSplit snsplit = (SnapshotSplit) split;
                Map<String, Object> splitShow = new HashMap<>();
                splitShow.put("splitId", snsplit.getSplitId());
                splitShow.put("tableId", snsplit.getTableId());
                splitShow.put("splitKey", snsplit.getSplitKey());
                splitShow.put("splitStart", snsplit.getSplitStart());
                splitShow.put("splitEnd", snsplit.getSplitEnd());
                return new Gson().toJson(splitShow);
            } else {
                BinlogSplit binlogSplit = (BinlogSplit) split;
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

    @Override
    public InsertIntoTableCommand rewriteTvfParams(InsertIntoTableCommand originCommand, Offset nextOffset) {
        // todo: only for cdc tvf
        return null;
    }

    /**
     *
     */
    @Override
    public void updateOffset(Offset offset) {
        this.currentOffset = (JdbcOffset) offset;
        AbstractSourceSplit split = currentOffset.getSplit();
        if (split.snapshotSplit()) {
            SnapshotSplit snapshotSplit = (SnapshotSplit) split;
            String splitId = split.getSplitId();
            remainingSplits.removeIf(v -> {
                if (v.getSplitId().equals(splitId)) {
                    snapshotSplit.setTableId(v.getTableId());
                    snapshotSplit.setSplitKey(v.getSplitKey());
                    snapshotSplit.setSplitStart(v.getSplitStart());
                    snapshotSplit.setSplitEnd(v.getSplitEnd());
                    return true;
                }
                return false;
            });
            finishedSplits.add(snapshotSplit);
            chunkHighWatermarkMap.computeIfAbsent(snapshotSplit.getTableId(), k -> new HashMap<>())
                    .put(snapshotSplit.getSplitId(), snapshotSplit.getHighWatermark());
        } else {
            BinlogSplit binlogSplit = (BinlogSplit) split;
            binlogOffsetPersist = new HashMap<>(binlogSplit.getStartingOffset());
            binlogOffsetPersist.put(SPLIT_ID, BinlogSplit.BINLOG_SPLIT_ID);
        }
    }

    @Override
    public void fetchRemoteMeta(Map<String, String> properties) throws Exception {
        Backend backend = StreamingJobUtils.selectBackend(jobId);
        Map<String, Object> params = buildBaseParams();
        InternalService.PRequestCdcClientRequest request = InternalService.PRequestCdcClientRequest.newBuilder()
                .setApi("/api/fetchEndOffset")
                .setParams(new Gson().toJson(params)).build();
        TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
        InternalService.PRequestCdcClientResult result = null;
        try {
            Future<PRequestCdcClientResult> future =
                    BackendServiceProxy.getInstance().requestCdcClient(address, request);
            result = future.get();
            TStatusCode code = TStatusCode.findByValue(result.getStatus().getStatusCode());
            if (code != TStatusCode.OK) {
                log.error("Failed to get end offset from backend, {}", result.getStatus().getErrorMsgs(0));
                throw new JobException(
                        "Failed to get end offset from backend," + result.getStatus().getErrorMsgs(0) + ", response: "
                                + result.getResponse());
            }
            String response = result.getResponse();
            ResponseBody<Map<String, String>> responseObj = objectMapper.readValue(
                    response,
                    new TypeReference<ResponseBody<Map<String, String>>>() {
                    }
            );
            if (endBinlogOffset != null
                    && !endBinlogOffset.equals(responseObj.getData())) {
                hasMoreData = true;
            }
            endBinlogOffset = responseObj.getData();
        } catch (ExecutionException | InterruptedException | IOException ex) {
            log.error("Get end offset error: ", ex);
            throw new JobException(ex);
        }
    }

    @Override
    public boolean hasMoreDataToConsume() {
        if (!hasMoreData) {
            return false;
        }

        if (currentOffset == null) {
            return true;
        }

        if (CollectionUtils.isNotEmpty(remainingSplits)) {
            return true;
        }
        if (MapUtils.isEmpty(endBinlogOffset)) {
            return false;
        }
        try {
            if (!currentOffset.getSplit().snapshotSplit()) {
                BinlogSplit binlogSplit = (BinlogSplit) currentOffset.getSplit();
                return compareOffset(endBinlogOffset, new HashMap<>(binlogSplit.getStartingOffset()));
            } else {
                SnapshotSplit snapshotSplit = (SnapshotSplit) currentOffset.getSplit();
                if (MapUtils.isNotEmpty(snapshotSplit.getHighWatermark())) {
                    return compareOffset(endBinlogOffset, new HashMap<>(snapshotSplit.getHighWatermark()));
                }
            }
        } catch (Exception ex) {
            log.info("Compare offset error: ", ex);
            return false;
        }
        return false;
    }

    private boolean compareOffset(Map<String, String> offsetFirst, Map<String, String> offsetSecond)
            throws JobException {
        Backend backend = StreamingJobUtils.selectBackend(jobId);
        Map<String, Object> params = buildCompareOffsetParams(offsetFirst, offsetSecond);
        InternalService.PRequestCdcClientRequest request = InternalService.PRequestCdcClientRequest.newBuilder()
                .setApi("/api/compareOffset")
                .setParams(new Gson().toJson(params)).build();
        TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
        InternalService.PRequestCdcClientResult result = null;
        try {
            Future<PRequestCdcClientResult> future =
                    BackendServiceProxy.getInstance().requestCdcClient(address, request);
            result = future.get();
            TStatusCode code = TStatusCode.findByValue(result.getStatus().getStatusCode());
            if (code != TStatusCode.OK) {
                log.error("Failed to compare offset , {}", result.getStatus().getErrorMsgs(0));
                throw new JobException(
                        "Failed to compare offset ," + result.getStatus().getErrorMsgs(0) + ", response: "
                                + result.getResponse());
            }
            String response = result.getResponse();
            ResponseBody<Integer> responseObj = objectMapper.readValue(
                    response,
                    new TypeReference<ResponseBody<Integer>>() {
                    }
            );
            return responseObj.getData() > 0;
        } catch (ExecutionException | InterruptedException | IOException ex) {
            log.error("Compare offset error: ", ex);
            throw new JobException(ex);
        }
    }

    @Override
    public Offset deserializeOffset(String offset) {
        try {
            // chunk is highWatermark, binlog is offset map
            Map<String, String> offsetMeta = objectMapper.readValue(offset, new TypeReference<Map<String, String>>() {
            });
            String splitId = offsetMeta.remove(SPLIT_ID);
            if (BinlogSplit.BINLOG_SPLIT_ID.equals(splitId)) {
                BinlogSplit binlogSplit = new BinlogSplit();
                binlogSplit.setSplitId(splitId);
                binlogSplit.setStartingOffset(offsetMeta);
                return new JdbcOffset(binlogSplit);
            } else {
                SnapshotSplit snapshotSplit = new SnapshotSplit();
                snapshotSplit.setSplitId(splitId);
                snapshotSplit.setHighWatermark(offsetMeta);
                return new JdbcOffset(snapshotSplit);
            }
        } catch (JsonProcessingException e) {
            log.warn("Failed to deserialize offset: {}", offset, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public Offset deserializeOffsetProperty(String offset) {
        // todo: use for alter offset for job
        return null;
    }

    /**
     * Replay snapshot splits if needed
     */
    @Override
    public void replayIfNeed(StreamingInsertJob job) throws JobException {
        String offsetProviderPersist = job.getOffsetProviderPersist();
        if (job.getOffsetProviderPersist() == null) {
            return;
        }
        JdbcSourceOffsetProvider replayFromPersist = GsonUtils.GSON.fromJson(offsetProviderPersist,
                JdbcSourceOffsetProvider.class);
        this.binlogOffsetPersist = replayFromPersist.getBinlogOffsetPersist();
        this.chunkHighWatermarkMap = replayFromPersist.getChunkHighWatermarkMap();

        if (MapUtils.isNotEmpty(binlogOffsetPersist)) {
            currentOffset = new JdbcOffset();
            currentOffset.setSplit(new BinlogSplit(binlogOffsetPersist));
        } else {
            try {
                Map<String, List<SnapshotSplit>> snapshotSplits = StreamingJobUtils.restoreSplitsToJob(job.getJobId());
                if (MapUtils.isNotEmpty(chunkHighWatermarkMap) && MapUtils.isNotEmpty(snapshotSplits)) {
                    SnapshotSplit lastSnapshotSplit = recalculateRemainingSplits(chunkHighWatermarkMap, snapshotSplits);
                    if (this.remainingSplits.isEmpty()) {
                        currentOffset = new JdbcOffset();
                        currentOffset.setSplit(lastSnapshotSplit);
                    }
                }
            } catch (Exception ex) {
                log.warn("Replay snapshot splits error with job {} ", job.getJobId(), ex);
                throw new JobException(ex);
            }
        }
    }

    /**
     * Assign the HW value to the synchronized Split,
     * and remove the Split from remainSplit and place it in finishedSplit.
     */
    private SnapshotSplit recalculateRemainingSplits(Map<String, Map<String, Map<String, String>>> chunkHighWatermarkMap,
            Map<String, List<SnapshotSplit>> snapshotSplits) {
        if (this.finishedSplits == null) {
            this.finishedSplits = new ArrayList<>();
        }
        SnapshotSplit lastSnapshotSplit = null;
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
            lastSnapshotSplit = tableSplits.get(tableSplits.size() - 1);
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
        return lastSnapshotSplit;
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
        Backend backend = StreamingJobUtils.selectBackend(jobId);
        Map<String, Object> params = buildSplitParams(table);
        InternalService.PRequestCdcClientRequest request = InternalService.PRequestCdcClientRequest.newBuilder()
                .setApi("/api/fetchSplits")
                .setParams(new Gson().toJson(params)).build();
        TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
        InternalService.PRequestCdcClientResult result = null;
        try {
            Future<PRequestCdcClientResult> future =
                    BackendServiceProxy.getInstance().requestCdcClient(address, request);
            result = future.get();
            TStatusCode code = TStatusCode.findByValue(result.getStatus().getStatusCode());
            if (code != TStatusCode.OK) {
                log.error("Failed to get split from backend, {}", result.getStatus().getErrorMsgs(0));
                throw new JobException(
                        "Failed to get split from backend," + result.getStatus().getErrorMsgs(0) + ", response: "
                                + result.getResponse());
            }
            String response = result.getResponse();
            ResponseBody<List<SnapshotSplit>> responseObj = objectMapper.readValue(
                    response,
                    new TypeReference<ResponseBody<List<SnapshotSplit>>>() {
                    }
            );
            List<SnapshotSplit> splits = responseObj.getData();
            return splits;
        } catch (ExecutionException | InterruptedException | IOException ex) {
            log.error("Get splits error: ", ex);
            throw new JobException(ex);
        }
    }

    private Map<String, Object> buildBaseParams() {
        Map<String, Object> params = new HashMap<>();
        params.put("jobId", getJobId());
        params.put("dataSource", sourceType);
        params.put("config", sourceProperties);
        return params;
    }

    private Map<String, Object> buildCompareOffsetParams(Map<String, String> offsetFirst,
            Map<String, String> offsetSecond) {
        Map<String, Object> params = buildBaseParams();
        params.put("offsetFirst", offsetFirst);
        params.put("offsetSecond", offsetSecond);
        return params;
    }

    private Map<String, Object> buildSplitParams(String table) {
        Map<String, Object> params = buildBaseParams();
        params.put("snapshotTable", table);
        return params;
    }

    private boolean checkNeedSplitChunks(Map<String, String> sourceProperties) {
        String startMode = sourceProperties.get(LoadConstants.STARTUP_MODE);
        if (startMode == null) {
            return false;
        }
        return "snapshot".equalsIgnoreCase(startMode) || "initial".equalsIgnoreCase(startMode);
    }
}
