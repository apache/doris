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

import org.apache.doris.cdcclient.common.Constants;
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
import org.apache.flink.cdc.connectors.base.source.reader.external.IncrementalSourceScanFetcher;
import org.apache.flink.cdc.connectors.base.source.reader.external.IncrementalSourceStreamFetcher;
import org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;

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
    private Fetcher<SourceRecords, SourceSplitBase> currentReader;
    private Map<TableId, TableChanges.TableChange> tableSchemas;
    private SplitRecords currentSplitRecords;
    private SourceSplitBase currentSplit;
    protected FetchTask<SourceSplitBase> currentFetchTask;

    public JdbcIncrementalSourceReader() {
        this.serializer = new DebeziumJsonDeserializer();
    }

    @Override
    public void initialize(long jobId, DataSource dataSource, Map<String, String> config) {
        this.serializer.init(config);
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
    public SplitReadResult readSplitRecords(JobBaseRecordRequest baseReq) throws Exception {
        Map<String, Object> offsetMeta = baseReq.getMeta();
        if (offsetMeta == null || offsetMeta.isEmpty()) {
            throw new RuntimeException("miss meta offset");
        }
        LOG.info("Job {} read split records with offset: {}", baseReq.getJobId(), offsetMeta);

        //  If there is an active split being consumed, reuse it directly;
        //  Otherwise, create a new snapshot/stream split based on offset and start the reader.
        SourceSplitBase split = null;
        SplitRecords currentSplitRecords = this.getCurrentSplitRecords();
        if (currentSplitRecords == null) {
            Fetcher<SourceRecords, SourceSplitBase> currentReader = this.getCurrentReader();
            if (baseReq.isReload() || currentReader == null) {
                LOG.info(
                        "No current reader or reload {}, create new split reader for job {}",
                        baseReq.isReload(),
                        baseReq.getJobId());
                // build split
                Tuple2<SourceSplitBase, Boolean> splitFlag = createSourceSplit(offsetMeta, baseReq);
                split = splitFlag.f0;
                // closeBinlogReader();
                currentSplitRecords = pollSplitRecordsWithSplit(split, baseReq);
                this.setCurrentSplitRecords(currentSplitRecords);
                this.setCurrentSplit(split);
            } else if (currentReader instanceof IncrementalSourceStreamFetcher) {
                LOG.info("Continue poll records with current binlog reader");
                // only for binlog reader
                currentSplitRecords = pollSplitRecordsWithCurrentReader(currentReader);
                split = this.getCurrentSplit();
            } else {
                throw new RuntimeException("Should not happen");
            }
        } else {
            LOG.info(
                    "Continue read records with current split records, splitId: {}",
                    currentSplitRecords.getSplitId());
        }

        // build response with iterator
        SplitReadResult result = new SplitReadResult();
        SourceSplitState currentSplitState = null;
        SourceSplitBase currentSplit = this.getCurrentSplit();
        if (currentSplit.isSnapshotSplit()) {
            currentSplitState = new SnapshotSplitState(currentSplit.asSnapshotSplit());
        } else {
            currentSplitState = new StreamSplitState(currentSplit.asStreamSplit());
        }

        Iterator<SourceRecord> filteredIterator =
                new FilteredRecordIterator(currentSplitRecords, currentSplitState);

        result.setRecordIterator(filteredIterator);
        result.setSplitState(currentSplitState);
        result.setSplit(split);
        return result;
    }

    protected abstract DataType fromDbzColumn(Column splitColumn);

    protected abstract Fetcher<SourceRecords, SourceSplitBase> getSnapshotSplitReader(
            JobBaseConfig jobConfig);

    protected abstract Fetcher<SourceRecords, SourceSplitBase> getBinlogSplitReader(
            JobBaseConfig jobConfig);

    protected abstract OffsetFactory getOffsetFactory();

    protected abstract Offset createOffset(Map<String, ?> offset);

    protected abstract Offset createInitialOffset();

    protected abstract Offset createNoStoppingOffset();

    protected abstract JdbcDataSourceDialect getDialect(JdbcSourceConfig sourceConfig);

    protected Tuple2<SourceSplitBase, Boolean> createSourceSplit(
            Map<String, Object> offsetMeta, JobBaseConfig jobConfig) {
        Tuple2<SourceSplitBase, Boolean> splitRes = null;
        String splitId = String.valueOf(offsetMeta.get(SPLIT_ID));
        if (!BinlogSplit.BINLOG_SPLIT_ID.equals(splitId)) {
            org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit split =
                    createSnapshotSplit(offsetMeta, jobConfig);
            splitRes = Tuple2.of(split, false);
        } else {
            splitRes = createStreamSplit(offsetMeta, jobConfig);
        }
        return splitRes;
    }

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
        LOG.info("Create stream split from offset: {}", streamSplit.getStartingOffset());
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

    private SplitRecords pollSplitRecordsWithSplit(SourceSplitBase split, JobBaseConfig jobConfig)
            throws Exception {
        Preconditions.checkState(split != null, "split is null");
        SourceRecords sourceRecords = null;
        String currentSplitId = null;
        Fetcher<SourceRecords, SourceSplitBase> currentReader = null;
        LOG.info("Get a split: {}", split.toString());
        if (split.isSnapshotSplit()) {
            currentReader = getSnapshotSplitReader(jobConfig);
        } else if (split.isStreamSplit()) {
            currentReader = getBinlogSplitReader(jobConfig);
        }
        this.setCurrentReader(currentReader);
        FetchTask<SourceSplitBase> splitFetchTask = createFetchTaskFromSplit(jobConfig, split);
        currentReader.submitTask(splitFetchTask);
        currentSplitId = split.splitId();
        this.setCurrentFetchTask(splitFetchTask);
        // make split record available
        sourceRecords =
                pollUntilDataAvailable(currentReader, Constants.POLL_SPLIT_RECORDS_TIMEOUTS, 500);
        if (currentReader instanceof IncrementalSourceScanFetcher) {
            closeCurrentReader();
        }
        return new SplitRecords(currentSplitId, sourceRecords.iterator());
    }

    private SplitRecords pollSplitRecordsWithCurrentReader(
            Fetcher<SourceRecords, SourceSplitBase> currentReader) throws Exception {
        Iterator<SourceRecords> dataIt = null;
        if (currentReader instanceof IncrementalSourceStreamFetcher) {
            dataIt = currentReader.pollSplitRecords();
            return dataIt == null
                    ? null
                    : new SplitRecords(STREAM_SPLIT_ID, dataIt.next().iterator());
        } else {
            throw new IllegalStateException("Unsupported reader type.");
        }
    }

    /**
     * Split tasks are submitted asynchronously, and data is sent to the Debezium queue. Therefore,
     * there will be a time interval between retrieving data; it's necessary to fetch data until the
     * queue has data.
     */
    private SourceRecords pollUntilDataAvailable(
            Fetcher<SourceRecords, SourceSplitBase> reader, long maxWaitTimeMs, long pollIntervalMs)
            throws InterruptedException {
        long startTime = System.currentTimeMillis();
        long elapsedTime = 0;
        int attemptCount = 0;
        LOG.info("Polling until data available");
        Iterator<SourceRecords> lastDataIt = null;
        while (elapsedTime < maxWaitTimeMs) {
            attemptCount++;
            lastDataIt = reader.pollSplitRecords();
            if (lastDataIt != null && lastDataIt.hasNext()) {
                SourceRecords sourceRecords = lastDataIt.next();
                if (sourceRecords != null && !sourceRecords.getSourceRecordList().isEmpty()) {
                    LOG.info(
                            "Data available after {} ms ({} attempts). {} Records received.",
                            elapsedTime,
                            attemptCount,
                            sourceRecords.getSourceRecordList().size());
                    // todo: poll until heartbeat ?
                    return sourceRecords;
                }
            }

            // No records yet, continue polling
            if (elapsedTime + pollIntervalMs < maxWaitTimeMs) {
                Thread.sleep(pollIntervalMs);
                elapsedTime = System.currentTimeMillis() - startTime;
            } else {
                // Last attempt before timeout
                break;
            }
        }

        LOG.warn(
                "Timeout: No data (heartbeat or data change) received after {} ms ({} attempts).",
                elapsedTime,
                attemptCount);
        return new SourceRecords(new ArrayList<>());
    }

    private void closeCurrentReader() {
        Fetcher<SourceRecords, SourceSplitBase> currentReader = this.getCurrentReader();
        if (currentReader != null) {
            LOG.info("Close current reader {}", currentReader.getClass().getCanonicalName());
            currentReader.close();
            this.setCurrentReader(null);
        }
    }

    protected abstract FetchTask<SourceSplitBase> createFetchTaskFromSplit(
            JobBaseConfig jobConfig, SourceSplitBase split);

    /** Get source config - to be implemented by subclasses */
    protected abstract JdbcSourceConfig getSourceConfig(JobBaseConfig config);

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
        this.setCurrentSplitRecords(null);
        // Close after each read, the binlog client will occupy the connection.
        closeCurrentReader();
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
        closeCurrentReader();
        currentReader = null;
        currentSplitRecords = null;
        currentSplit = null;
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
     * heartbeat and other events. This is a private inner class that encapsulates record filtering
     * logic, making the main method cleaner.
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
