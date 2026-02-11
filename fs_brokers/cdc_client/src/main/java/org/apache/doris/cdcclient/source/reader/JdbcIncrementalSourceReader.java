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

package org.apache.doris.cdcclient.source.reader;

import org.apache.doris.cdcclient.source.deserialize.DebeziumJsonDeserializer;
import org.apache.doris.cdcclient.source.deserialize.SourceRecordDeserializer;
import org.apache.doris.cdcclient.source.factory.DataSource;
import org.apache.doris.job.cdc.DataSourceConfigKeys;
import org.apache.doris.job.cdc.request.FetchTableSplitsRequest;
import org.apache.doris.job.cdc.request.JobBaseConfig;
import org.apache.doris.job.cdc.request.JobBaseRecordRequest;
import org.apache.doris.job.cdc.split.AbstractSourceSplit;
import org.apache.doris.job.cdc.split.BinlogSplit;
import org.apache.doris.job.cdc.split.SnapshotSplit;

import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.assigner.HybridSplitAssigner;
import org.apache.flink.cdc.connectors.base.source.assigner.SnapshotSplitAssigner;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.cdc.connectors.base.source.meta.split.FinishedSnapshotSplitInfo;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplitState;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceRecords;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitState;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplitState;
import org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkEvent;
import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask;
import org.apache.flink.cdc.connectors.base.source.reader.external.Fetcher;
import org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit.STREAM_SPLIT_ID;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.relational.Column;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Data
public abstract class JdbcIncrementalSourceReader implements SourceReader {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcIncrementalSourceReader.class);
    private static ObjectMapper objectMapper = new ObjectMapper();
    private SourceRecordDeserializer<SourceRecord, List<String>> serializer;
    private Map<TableId, TableChanges.TableChange> tableSchemas;

    // Support for multiple snapshot splits
    private List<
                    SnapshotReaderContext<
                            org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit,
                            Fetcher<SourceRecords, SourceSplitBase>,
                            SnapshotSplitState>>
            snapshotReaderContexts;
    private Set<String> completedSplitIds = new HashSet<>();

    // Parallel polling support
    private ExecutorService pollExecutor;
    private List<CompletableFuture<PollResult>> activePollFutures;

    // Stream/binlog reader (single reader for stream split)
    private Fetcher<SourceRecords, SourceSplitBase> streamReader;
    private StreamSplit streamSplit;
    private StreamSplitState streamSplitState;
    protected FetchTask<SourceSplitBase> currentFetchTask;

    public JdbcIncrementalSourceReader() {
        this.serializer = new DebeziumJsonDeserializer();
        this.snapshotReaderContexts = new ArrayList<>();
    }

    @Override
    public void initialize(long jobId, DataSource dataSource, Map<String, String> config) {
        this.serializer.init(config);

        // Initialize thread pool for parallel polling
        int parallelism =
                Integer.parseInt(
                        config.getOrDefault(
                                DataSourceConfigKeys.SNAPSHOT_PARALLELISM,
                                DataSourceConfigKeys.SNAPSHOT_PARALLELISM_DEFAULT));
        this.pollExecutor =
                Executors.newFixedThreadPool(
                        parallelism,
                        r -> {
                            Thread t = new Thread(r);
                            t.setName("snapshot-reader-" + jobId + "-" + t.getId());
                            t.setDaemon(true);
                            return t;
                        });
        LOG.info("Initialized poll executor with parallelism: {}", parallelism);
    }

    @Override
    public List<AbstractSourceSplit> getSourceSplits(FetchTableSplitsRequest ftsReq) {
        LOG.info("Get table {} splits for job {}", ftsReq.getSnapshotTable(), ftsReq.getJobId());
        JdbcSourceConfig sourceConfig = getSourceConfig(ftsReq);
        List<org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit>
                remainingSnapshotSplits = new ArrayList<>();
        StreamSplit remainingStreamSplit = null;

        // Check startup mode - for PostgreSQL, we use similar logic as MySQL
        String startupMode = ftsReq.getConfig().get(DataSourceConfigKeys.OFFSET);
        if (DataSourceConfigKeys.OFFSET_INITIAL.equalsIgnoreCase(startupMode)) {
            remainingSnapshotSplits =
                    startSplitChunks(sourceConfig, ftsReq.getSnapshotTable(), ftsReq.getConfig());
        } else {
            // For non-initial mode, create a stream split
            Offset startingOffset = createInitialOffset();
            remainingStreamSplit =
                    new StreamSplit(
                            STREAM_SPLIT_ID,
                            startingOffset,
                            createNoStoppingOffset(),
                            new ArrayList<>(),
                            new HashMap<>(),
                            0);
        }

        List<AbstractSourceSplit> splits = new ArrayList<>();
        if (!remainingSnapshotSplits.isEmpty()) {
            for (org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit
                    snapshotSplit : remainingSnapshotSplits) {
                String splitId = snapshotSplit.splitId();
                String tableId = snapshotSplit.getTableId().identifier();
                Object[] splitStart = snapshotSplit.getSplitStart();
                Object[] splitEnd = snapshotSplit.getSplitEnd();
                List<String> splitKey = snapshotSplit.getSplitKeyType().getFieldNames();
                SnapshotSplit split =
                        new SnapshotSplit(splitId, tableId, splitKey, splitStart, splitEnd, null);
                splits.add(split);
            }
        } else {
            Offset startingOffset = remainingStreamSplit.getStartingOffset();
            BinlogSplit streamSplit = new BinlogSplit();
            streamSplit.setSplitId(remainingStreamSplit.splitId());
            streamSplit.setStartingOffset(startingOffset.getOffset());
            splits.add(streamSplit);
        }
        return splits;
    }

    @Override
    public SplitReadResult prepareAndSubmitSplit(JobBaseRecordRequest baseReq) throws Exception {
        Map<String, Object> offsetMeta = baseReq.getMeta();
        if (offsetMeta == null || offsetMeta.isEmpty()) {
            throw new RuntimeException("miss meta offset");
        }

        LOG.info("Job {} read split records with offset: {}", baseReq.getJobId(), offsetMeta);

        String splitId = String.valueOf(offsetMeta.get(SPLIT_ID));
        if (BinlogSplit.BINLOG_SPLIT_ID.equals(splitId)) {
            // Stream split mode
            return prepareStreamSplit(offsetMeta, baseReq);
        } else {
            // Extract snapshot split list
            List<org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit>
                    snapshotSplits = extractSnapshotSplits(offsetMeta, baseReq);
            return prepareSnapshotSplits(snapshotSplits, baseReq);
        }
    }

    /**
     * Extract snapshot splits from meta.
     *
     * <p>Only supports format: {"splits": [{"splitId": "xxx", ...},...]}
     *
     * @return List of snapshot splits
     */
    private List<org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit>
            extractSnapshotSplits(Map<String, Object> offsetMeta, JobBaseRecordRequest baseReq) {

        // Check if it contains "splits" array
        Object splitsObj = offsetMeta.get("splits");
        if (splitsObj == null) {
            throw new RuntimeException("Invalid meta format: missing 'splits' array");
        }

        if (!(splitsObj instanceof List)) {
            throw new RuntimeException("Invalid meta format: 'splits' must be an array");
        }

        // Parse splits array
        List<Map<String, Object>> splitMetaList = (List<Map<String, Object>>) splitsObj;
        if (splitMetaList.isEmpty()) {
            throw new RuntimeException("Invalid meta format: 'splits' array is empty");
        }

        List<org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit> snapshotSplits =
                new ArrayList<>();
        for (Map<String, Object> splitMeta : splitMetaList) {
            org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit split =
                    createSnapshotSplit(splitMeta, baseReq);
            snapshotSplits.add(split);
        }

        LOG.info("Extracted {} snapshot split(s) from meta", snapshotSplits.size());
        return snapshotSplits;
    }

    /** Prepare snapshot splits (unified handling for single or multiple splits) */
    private SplitReadResult prepareSnapshotSplits(
            List<org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit> splits,
            JobBaseRecordRequest baseReq)
            throws Exception {

        LOG.info("Preparing {} snapshot split(s) for reading", splits.size());

        // Cancel any active poll operations
        if (activePollFutures != null) {
            LOG.info(
                    "Cancelling {} active poll operations with jobId {}",
                    activePollFutures.size(),
                    baseReq.getJobId());
            activePollFutures.forEach(f -> f.cancel(true));
            activePollFutures.clear();
            activePollFutures = null;
        }

        // Clear previous contexts
        this.snapshotReaderContexts.clear();
        this.completedSplitIds.clear();

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        // Create reader for each split and submit
        for (int i = 0; i < splits.size(); i++) {
            final int index = i;
            org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit split =
                    splits.get(index);

            // Create independent reader (each has its own Debezium queue)
            Fetcher<SourceRecords, SourceSplitBase> reader = getSnapshotSplitReader(baseReq, index);

            // Create split state
            SnapshotSplitState splitState = new SnapshotSplitState(split);

            // Save context using generic SnapshotReaderContext
            SnapshotReaderContext<
                            org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit,
                            Fetcher<SourceRecords, SourceSplitBase>,
                            SnapshotSplitState>
                    context = new SnapshotReaderContext<>(split, reader, splitState);
            snapshotReaderContexts.add(context);

            futures.add(
                    CompletableFuture.runAsync(
                            () -> {
                                // Submit split (triggers async reading, data goes into reader's
                                // Debezium queue)
                                FetchTask<SourceSplitBase> splitFetchTask =
                                        createFetchTaskFromSplit(baseReq, split);
                                reader.submitTask(splitFetchTask);
                                LOG.info(
                                        "Created reader {}/{} and submitted split: {} (table: {})",
                                        index + 1,
                                        splits.size(),
                                        split.splitId(),
                                        split.getTableId().identifier());
                            },
                            pollExecutor));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        // Construct return result with all splits and states
        SplitReadResult result = new SplitReadResult();

        List<SourceSplit> allSplits = new ArrayList<>();
        Map<String, Object> allStates = new HashMap<>();

        for (SnapshotReaderContext<
                        org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit,
                        Fetcher<SourceRecords, SourceSplitBase>,
                        SnapshotSplitState>
                context : snapshotReaderContexts) {
            org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit split =
                    context.getSplit();
            allSplits.add(split);
            allStates.put(split.splitId(), context.getSplitState());
        }

        result.setSplits(allSplits);
        result.setSplitStates(allStates);

        LOG.info("Success prepared {} snapshot splits for reading", splits.size());
        return result;
    }

    /** Prepare stream split */
    private SplitReadResult prepareStreamSplit(
            Map<String, Object> offsetMeta, JobBaseRecordRequest baseReq) throws Exception {
        Tuple2<SourceSplitBase, Boolean> splitFlag = createStreamSplit(offsetMeta, baseReq);
        this.streamSplit = splitFlag.f0.asStreamSplit();
        this.streamReader = getBinlogSplitReader(baseReq);

        LOG.info("Prepare stream split: {}", this.streamSplit.toString());

        // Submit split
        FetchTask<SourceSplitBase> splitFetchTask =
                createFetchTaskFromSplit(baseReq, this.streamSplit);
        this.streamReader.submitTask(splitFetchTask);
        this.setCurrentFetchTask(splitFetchTask);

        this.streamSplitState = new StreamSplitState(this.streamSplit);

        SplitReadResult result = new SplitReadResult();
        result.setSplits(Collections.singletonList(this.streamSplit));

        Map<String, Object> statesMap = new HashMap<>();
        statesMap.put(this.streamSplit.splitId(), this.streamSplitState);
        result.setSplitStates(statesMap);

        LOG.info("Success prepared stream split: {}", this.streamSplit.toString());
        return result;
    }

    @Override
    public Iterator<SourceRecord> pollRecords() throws Exception {
        if (!snapshotReaderContexts.isEmpty()) {
            // Snapshot split mode
            return pollRecordsFromSnapshotReaders();
        } else if (streamReader != null) {
            // Stream split mode
            return pollRecordsFromStreamReader();
        } else {
            throw new RuntimeException("No active snapshot or stream reader available");
        }
    }

    /**
     * Poll records from multiple snapshot readers in parallel. Uses CompletableFuture.anyOf() to
     * return data from the first completed reader.
     *
     * <p>This implementation starts parallel polling on first call, then incrementally returns
     * results as each reader completes, improving response latency.
     */
    private Iterator<SourceRecord> pollRecordsFromSnapshotReaders() throws Exception {
        if (snapshotReaderContexts.isEmpty()) {
            return Collections.emptyIterator();
        }

        if (completedSplitIds.size() >= snapshotReaderContexts.size()) {
            LOG.info("All {} snapshot splits have been completed", snapshotReaderContexts.size());
            return Collections.emptyIterator();
        }

        // If no active polling, start new parallel polling round
        if (activePollFutures == null || activePollFutures.isEmpty()) {
            startParallelPolling();
        }

        // Wait for any reader to complete and return its data
        PollResult result = waitForAnyCompletion();

        if (result == null) {
            // All readers completed but no data available
            LOG.info("All snapshot splits have no data currently");
            activePollFutures = null;
            return Collections.emptyIterator();
        }

        // Return data from the first completed reader
        LOG.info(
                "{} Records received from snapshot split {}",
                result.sourceRecords.getSourceRecordList().size(),
                result.context.getSplit().splitId());

        SplitRecords splitRecords =
                new SplitRecords(
                        result.context.getSplit().splitId(), result.sourceRecords.iterator());

        return new FilteredRecordIterator(splitRecords, result.context.getSplitState());
    }

    /** Start parallel polling for all snapshot readers */
    private void startParallelPolling() {
        LOG.info(
                "Starting parallel polling for {} snapshot readers", snapshotReaderContexts.size());

        activePollFutures = new ArrayList<>();

        for (int i = 0; i < snapshotReaderContexts.size(); i++) {
            final int index = i;
            SnapshotReaderContext<
                            org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit,
                            Fetcher<SourceRecords, SourceSplitBase>,
                            SnapshotSplitState>
                    context = snapshotReaderContexts.get(index);

            CompletableFuture<PollResult> future =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    LOG.info("Polling from split {}", context.getSplit().splitId());
                                    Iterator<SourceRecords> dataIt =
                                            context.getReader().pollSplitRecords();

                                    if (dataIt != null && dataIt.hasNext()) {
                                        SourceRecords sourceRecords = dataIt.next();
                                        if (!sourceRecords.getSourceRecordList().isEmpty()) {
                                            return new PollResult(context, sourceRecords, index);
                                        }
                                    }
                                    LOG.info("No data from split {}", context.getSplit().splitId());
                                } catch (Exception e) {
                                    LOG.error(
                                            "Error polling from split {}",
                                            context.getSplit().splitId(),
                                            e);
                                    throw new RuntimeException(
                                            "Failed to poll split: " + context.getSplit().splitId(),
                                            e);
                                }
                                return null;
                            },
                            pollExecutor);

            activePollFutures.add(future);
        }
    }

    /**
     * Wait for any reader to complete and return its result. Removes completed futures from the
     * active list.
     *
     * @return PollResult from first completed reader with data, or null if all completed without
     *     data
     */
    private PollResult waitForAnyCompletion() throws Exception {
        while (!activePollFutures.isEmpty()) {
            // Wait for any future to complete
            CompletableFuture<Object> anyOf =
                    CompletableFuture.anyOf(activePollFutures.toArray(new CompletableFuture[0]));

            anyOf.join(); // Wait for at least one to complete

            // Find and process completed futures
            Iterator<CompletableFuture<PollResult>> iterator = activePollFutures.iterator();
            while (iterator.hasNext()) {
                CompletableFuture<PollResult> future = iterator.next();

                if (future.isDone()) {
                    iterator.remove(); // Remove from active list
                    PollResult result = future.get();
                    if (result != null) {
                        // Found a reader with data, return immediately
                        LOG.info(
                                "Got result from reader {}, {} futures remaining",
                                result.context.getSplit().splitId(),
                                activePollFutures.size());
                        completedSplitIds.add(result.context.getSplit().splitId());
                        return result;
                    }
                    // If result is null (no data), continue checking other futures
                }
            }
        }
        // All futures completed but none had data
        return null;
    }

    /** Result from polling a single snapshot reader */
    private static class PollResult {
        final SnapshotReaderContext<
                        org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit,
                        Fetcher<SourceRecords, SourceSplitBase>,
                        SnapshotSplitState>
                context;
        final SourceRecords sourceRecords;
        final int readerIndex;

        PollResult(
                SnapshotReaderContext<
                                org.apache.flink.cdc.connectors.base.source.meta.split
                                        .SnapshotSplit,
                                Fetcher<SourceRecords, SourceSplitBase>,
                                SnapshotSplitState>
                        context,
                SourceRecords sourceRecords,
                int readerIndex) {
            this.context = context;
            this.sourceRecords = sourceRecords;
            this.readerIndex = readerIndex;
        }
    }

    /** Poll records from stream reader */
    private Iterator<SourceRecord> pollRecordsFromStreamReader() throws InterruptedException {

        Preconditions.checkState(streamReader != null, "streamReader is null");
        Preconditions.checkNotNull(streamSplitState, "streamSplitState is null");

        Iterator<SourceRecords> dataIt = streamReader.pollSplitRecords();
        if (dataIt == null || !dataIt.hasNext()) {
            return Collections.emptyIterator();
        }

        SourceRecords sourceRecords = dataIt.next();
        SplitRecords splitRecords =
                new SplitRecords(streamSplit.splitId(), sourceRecords.iterator());

        if (!sourceRecords.getSourceRecordList().isEmpty()) {
            LOG.info("{} Records received from stream", sourceRecords.getSourceRecordList().size());
        }

        return new FilteredRecordIterator(splitRecords, streamSplitState);
    }

    protected abstract DataType fromDbzColumn(Column splitColumn);

    protected abstract Fetcher<SourceRecords, SourceSplitBase> getSnapshotSplitReader(
            JobBaseConfig jobConfig, int subtaskId);

    protected abstract Fetcher<SourceRecords, SourceSplitBase> getBinlogSplitReader(
            JobBaseConfig jobConfig);

    protected abstract OffsetFactory getOffsetFactory();

    protected abstract Offset createOffset(Map<String, ?> offset);

    protected abstract Offset createInitialOffset();

    protected abstract Offset createNoStoppingOffset();

    protected abstract JdbcDataSourceDialect getDialect(JdbcSourceConfig sourceConfig);

    private org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit
            createSnapshotSplit(Map<String, Object> offset, JobBaseConfig jobConfig) {
        SnapshotSplit snapshotSplit = objectMapper.convertValue(offset, SnapshotSplit.class);
        TableId tableId = TableId.parse(snapshotSplit.getTableId(), false);
        Object[] splitStart = snapshotSplit.getSplitStart();
        Object[] splitEnd = snapshotSplit.getSplitEnd();
        List<String> splitKeys = snapshotSplit.getSplitKey();
        Map<TableId, TableChanges.TableChange> tableSchemas = getTableSchemas(jobConfig);
        TableChanges.TableChange tableChange = tableSchemas.get(tableId);
        Preconditions.checkNotNull(
                tableChange, "Can not find table " + tableId + " in job " + jobConfig.getJobId());
        // only support one split key
        String splitKey = splitKeys.get(0);
        io.debezium.relational.Column splitColumn = tableChange.getTable().columnWithName(splitKey);
        RowType splitType = getSplitType(splitColumn);
        org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit split =
                new org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit(
                        tableId,
                        snapshotSplit.getSplitId(),
                        splitType,
                        splitStart,
                        splitEnd,
                        null,
                        tableSchemas);
        return split;
    }

    private RowType getSplitType(Column splitColumn) {
        return (RowType)
                DataTypes.ROW(
                                new DataTypes.Field[] {
                                    DataTypes.FIELD(
                                            splitColumn.name(), this.fromDbzColumn(splitColumn))
                                })
                        .getLogicalType();
    }

    private Tuple2<SourceSplitBase, Boolean> createStreamSplit(
            Map<String, Object> meta, JobBaseConfig config) {
        BinlogSplit streamSplit = objectMapper.convertValue(meta, BinlogSplit.class);
        List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos = new ArrayList<>();
        Offset minOffsetFinishSplits = null;
        Offset maxOffsetFinishSplits = null;
        if (CollectionUtils.isNotEmpty(streamSplit.getFinishedSplits())) {
            List<SnapshotSplit> splitWithHW = streamSplit.getFinishedSplits();
            List<SnapshotSplit> assignedSplitLists =
                    splitWithHW.stream()
                            .sorted(Comparator.comparing(AbstractSourceSplit::getSplitId))
                            .toList();

            for (SnapshotSplit split : assignedSplitLists) {
                // find the min offset
                Map<String, String> offsetMap = split.getHighWatermark();
                Offset sourceOffset = createOffset(offsetMap);
                if (minOffsetFinishSplits == null || sourceOffset.isBefore(minOffsetFinishSplits)) {
                    minOffsetFinishSplits = sourceOffset;
                }
                if (maxOffsetFinishSplits == null || sourceOffset.isAfter(maxOffsetFinishSplits)) {
                    maxOffsetFinishSplits = sourceOffset;
                }
                finishedSnapshotSplitInfos.add(
                        new FinishedSnapshotSplitInfo(
                                TableId.parse(split.getTableId()),
                                split.getSplitId(),
                                split.getSplitStart(),
                                split.getSplitEnd(),
                                sourceOffset,
                                getOffsetFactory()));
            }
        }

        Offset startOffset;
        Offset lastOffset =
                createOffset(
                        streamSplit.getStartingOffset() == null
                                ? new HashMap<>()
                                : streamSplit.getStartingOffset());
        if (minOffsetFinishSplits != null && lastOffset.getOffset().isEmpty()) {
            startOffset = minOffsetFinishSplits;
        } else if (!lastOffset.getOffset().isEmpty()) {
            lastOffset.getOffset().remove(SPLIT_ID);
            startOffset = lastOffset;
        } else {
            // The input offset from params is empty
            JdbcSourceConfig sourceConfig = getSourceConfig(config);
            startOffset = getStartOffsetFromConfig(sourceConfig);
        }

        boolean pureStreamPhase = false;
        if (maxOffsetFinishSplits == null) {
            pureStreamPhase = true;
        } else if (startOffset.isAtOrAfter(maxOffsetFinishSplits)) {
            // All the offsets of the current split are smaller than the offset of the stream,
            // indicating that the stream phase has been fully entered.
            pureStreamPhase = true;
            LOG.info(
                    "The stream phase has been fully entered, the current split is: {}",
                    startOffset);
        }

        StreamSplit split =
                new StreamSplit(
                        STREAM_SPLIT_ID,
                        startOffset,
                        createNoStoppingOffset(),
                        finishedSnapshotSplitInfos,
                        new HashMap<>(),
                        0);
        // filterTableSchema
        StreamSplit streamSplitFinal =
                StreamSplit.fillTableSchemas(split.asStreamSplit(), getTableSchemas(config));
        return Tuple2.of(streamSplitFinal, pureStreamPhase);
    }

    private Offset getStartOffsetFromConfig(JdbcSourceConfig sourceConfig) {
        StartupOptions startupOptions = sourceConfig.getStartupOptions();
        Offset startingOffset;
        switch (startupOptions.startupMode) {
            case LATEST_OFFSET:
                startingOffset = getDialect(sourceConfig).displayCurrentOffset(sourceConfig);
                break;
            case EARLIEST_OFFSET:
                startingOffset = createInitialOffset();
                break;
            case TIMESTAMP:
            case SPECIFIC_OFFSETS:
            case COMMITTED_OFFSETS:
            default:
                throw new IllegalStateException(
                        "Unsupported startup mode " + startupOptions.startupMode);
        }
        return startingOffset;
    }

    private List<org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit>
            startSplitChunks(
                    JdbcSourceConfig sourceConfig,
                    String snapshotTable,
                    Map<String, String> config) {
        List<TableId> remainingTables = new ArrayList<>();
        if (snapshotTable != null) {
            String schema = config.get(DataSourceConfigKeys.SCHEMA);
            remainingTables.add(new TableId(null, schema, snapshotTable));
        }
        List<org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit> remainingSplits =
                new ArrayList<>();
        HybridSplitAssigner<JdbcSourceConfig> splitAssigner =
                new HybridSplitAssigner<>(
                        sourceConfig,
                        1,
                        remainingTables,
                        true,
                        getDialect(sourceConfig),
                        getOffsetFactory(),
                        new MockSplitEnumeratorContext(1));
        splitAssigner.open();
        try {
            while (true) {
                Optional<SourceSplitBase> split = splitAssigner.getNext();
                if (split.isPresent()) {
                    org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit
                            snapshotSplit = split.get().asSnapshotSplit();
                    remainingSplits.add(snapshotSplit);
                } else {
                    break;
                }
            }
        } finally {
            closeChunkSplitterOnly(splitAssigner);
        }
        return remainingSplits;
    }

    /**
     * Close only the chunk splitter to avoid closing shared connection pools Similar to MySQL
     * implementation Note: HybridSplitAssigner wraps SnapshotSplitAssigner, so we need to get the
     * inner assigner first
     */
    private static void closeChunkSplitterOnly(HybridSplitAssigner<?> splitAssigner) {
        try {
            // First, get the inner SnapshotSplitAssigner from HybridSplitAssigner
            java.lang.reflect.Field snapshotAssignerField =
                    HybridSplitAssigner.class.getDeclaredField("snapshotSplitAssigner");
            snapshotAssignerField.setAccessible(true);
            SnapshotSplitAssigner<?> snapshotSplitAssigner =
                    (SnapshotSplitAssigner<?>) snapshotAssignerField.get(splitAssigner);

            if (snapshotSplitAssigner == null) {
                LOG.warn("snapshotSplitAssigner is null in HybridSplitAssigner");
                return;
            }

            // Call closeExecutorService() via reflection
            java.lang.reflect.Method closeExecutorMethod =
                    SnapshotSplitAssigner.class.getDeclaredMethod("closeExecutorService");
            closeExecutorMethod.setAccessible(true);
            closeExecutorMethod.invoke(snapshotSplitAssigner);

            // Call chunkSplitter.close() via reflection
            java.lang.reflect.Field chunkSplitterField =
                    SnapshotSplitAssigner.class.getDeclaredField("chunkSplitter");
            chunkSplitterField.setAccessible(true);
            Object chunkSplitter = chunkSplitterField.get(snapshotSplitAssigner);

            if (chunkSplitter != null) {
                java.lang.reflect.Method closeMethod = chunkSplitter.getClass().getMethod("close");
                closeMethod.invoke(chunkSplitter);
                LOG.info("Closed Source chunkSplitter JDBC connection");
            }
        } catch (Exception e) {
            LOG.warn("Failed to close chunkSplitter via reflection", e);
        }
    }

    // Method removed - reader cleanup is now handled in finishSplitRecords()

    protected abstract FetchTask<SourceSplitBase> createFetchTaskFromSplit(
            JobBaseConfig jobConfig, SourceSplitBase split);

    /** Get source config - to be implemented by subclasses */
    protected abstract JdbcSourceConfig getSourceConfig(JobBaseConfig config);

    /** Get source config - to be implemented by subclasses */
    protected abstract JdbcSourceConfig getSourceConfig(JobBaseConfig config, int subtaskId);

    @Override
    public Map<String, String> extractSnapshotStateOffset(Object splitState) {
        Preconditions.checkNotNull(splitState, "splitState is null");
        SourceSplitState sourceSplitState = (SourceSplitState) splitState;
        Offset highWatermark = sourceSplitState.asSnapshotSplitState().getHighWatermark();
        Map<String, String> offsetRes = new HashMap<>(highWatermark.getOffset());
        return offsetRes;
    }

    @Override
    public Map<String, String> extractBinlogStateOffset(Object splitState) {
        Preconditions.checkNotNull(splitState, "splitState is null");
        SourceSplitState sourceSplitState = (SourceSplitState) splitState;
        Offset startingOffset = sourceSplitState.asStreamSplitState().getStartingOffset();
        return new HashMap<>(startingOffset.getOffset());
    }

    @Override
    public Map<String, String> extractBinlogOffset(SourceSplit split) {
        Preconditions.checkNotNull(split, "split is null");
        SourceSplitBase postgresSplit = (SourceSplitBase) split;
        Map<String, String> offsetRes =
                new HashMap<>(postgresSplit.asStreamSplit().getStartingOffset().getOffset());
        return offsetRes;
    }

    @Override
    public boolean isBinlogSplit(SourceSplit split) {
        Preconditions.checkNotNull(split, "split is null");
        SourceSplitBase postgresSplit = (SourceSplitBase) split;
        return postgresSplit.isStreamSplit();
    }

    @Override
    public boolean isSnapshotSplit(SourceSplit split) {
        Preconditions.checkNotNull(split, "split is null");
        SourceSplitBase postgresSplit = (SourceSplitBase) split;
        return postgresSplit.isSnapshotSplit();
    }

    @Override
    public void finishSplitRecords() {
        // Cancel any active poll operations
        if (activePollFutures != null) {
            activePollFutures.forEach(f -> f.cancel(true));
            activePollFutures.clear();
            activePollFutures = null;
        }
        completedSplitIds.clear();
        // Clean up snapshot readers
        if (!snapshotReaderContexts.isEmpty()) {
            LOG.info("Closing {} snapshot readers", snapshotReaderContexts.size());
            for (SnapshotReaderContext<
                            org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit,
                            Fetcher<SourceRecords, SourceSplitBase>,
                            SnapshotSplitState>
                    context : snapshotReaderContexts) {
                if (context.getReader() != null) {
                    closeReaderInternal(context.getReader());
                }
            }
            snapshotReaderContexts.clear();
        }

        // Clean up stream reader
        if (streamReader != null) {
            LOG.info("Closing stream reader");
            closeReaderInternal(streamReader);
            streamReader = null;
            streamSplit = null;
            streamSplitState = null;
        }
    }

    private void closeReaderInternal(Fetcher<SourceRecords, SourceSplitBase> reader) {
        if (reader != null) {
            LOG.info("Close reader {}", reader.getClass().getCanonicalName());
            reader.close();
        }
    }

    private Map<TableId, TableChanges.TableChange> getTableSchemas(JobBaseConfig config) {
        Map<TableId, TableChanges.TableChange> schemas = this.getTableSchemas();
        if (schemas == null) {
            schemas = discoverTableSchemas(config);
            this.setTableSchemas(schemas);
        }
        return schemas;
    }

    protected abstract Map<TableId, TableChanges.TableChange> discoverTableSchemas(
            JobBaseConfig config);

    @Override
    public void close(JobBaseConfig jobConfig) {
        LOG.info("Close source reader for job {}", jobConfig.getJobId());

        // Cancel any active poll operations
        if (activePollFutures != null) {
            activePollFutures.forEach(f -> f.cancel(true));
            activePollFutures.clear();
            activePollFutures = null;
        }

        // Clean up all readers
        finishSplitRecords();

        if (tableSchemas != null) {
            tableSchemas.clear();
            tableSchemas = null;
        }
    }

    @Override
    public List<String> deserialize(Map<String, String> config, SourceRecord element)
            throws IOException {
        return serializer.deserialize(config, element);
    }

    /**
     * Filtered record iterator that only returns data change records, filtering out watermark,
     * heartbeat and other events. This is a private static inner class that encapsulates record
     * filtering logic, making the main method cleaner.
     */
    private class FilteredRecordIterator implements Iterator<SourceRecord> {
        private final Iterator<SourceRecord> sourceIterator;
        private final SourceSplitState splitState;
        private SourceRecord nextRecord;

        FilteredRecordIterator(SplitRecords currentSplitRecords, SourceSplitState splitState) {
            this.sourceIterator =
                    currentSplitRecords != null && !currentSplitRecords.isEmpty()
                            ? currentSplitRecords.getIterator()
                            : null;
            this.splitState = splitState;
        }

        @Override
        public boolean hasNext() {
            if (sourceIterator == null) {
                return false;
            }
            if (nextRecord != null) {
                return true;
            }

            while (sourceIterator.hasNext()) {
                SourceRecord element = sourceIterator.next();
                if (WatermarkEvent.isWatermarkEvent(element)) {
                    Offset watermark = getWatermark(element);
                    if (WatermarkEvent.isHighWatermarkEvent(element)
                            && splitState.isSnapshotSplitState()) {
                        splitState.asSnapshotSplitState().setHighWatermark(watermark);
                    }
                } else if (SourceRecordUtils.isHeartbeatEvent(element)) {
                    LOG.debug("Receive heartbeat event: {}", element);
                    if (splitState.isStreamSplitState()) {
                        Offset position = createOffset(element.sourceOffset());
                        splitState.asStreamSplitState().setStartingOffset(position);
                    }
                    nextRecord = element;
                    return true;
                } else if (SourceRecordUtils.isDataChangeRecord(element)) {
                    if (splitState.isStreamSplitState()) {
                        Offset position = createOffset(element.sourceOffset());
                        splitState.asStreamSplitState().setStartingOffset(position);
                    }
                    nextRecord = element;
                    return true;
                } else {
                    LOG.debug("Ignore event: {}", element);
                }
            }
            return false;
        }

        @Override
        public SourceRecord next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            SourceRecord record = nextRecord;
            nextRecord = null;
            return record;
        }

        private Offset getWatermark(SourceRecord watermarkEvent) {
            Map<String, ?> offset = watermarkEvent.sourceOffset();
            // Extract watermark from source record offset
            OffsetFactory offsetFactory = getOffsetFactory();
            Map<String, String> offsetStrMap = new HashMap<>();
            for (Map.Entry<String, ?> entry : offset.entrySet()) {
                offsetStrMap.put(
                        entry.getKey(),
                        entry.getValue() == null ? null : entry.getValue().toString());
            }
            return offsetFactory.newOffset(offsetStrMap);
        }
    }
}
