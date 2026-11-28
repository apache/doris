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

import org.apache.doris.cdcclient.common.CdcResourceManager.SnapshotQuota;
import org.apache.doris.cdcclient.common.ManagedReader;
import org.apache.doris.cdcclient.common.ReaderBatch;
import org.apache.doris.cdcclient.source.deserialize.DebeziumJsonDeserializer;
import org.apache.doris.cdcclient.source.deserialize.DeserializeResult;
import org.apache.doris.cdcclient.source.factory.DataSource;
import org.apache.doris.job.cdc.DataSourceConfigKeys;
import org.apache.doris.job.cdc.request.FetchTableSplitsRequest;
import org.apache.doris.job.cdc.request.JobBaseConfig;
import org.apache.doris.job.cdc.request.JobBaseRecordRequest;
import org.apache.doris.job.cdc.split.AbstractSourceSplit;
import org.apache.doris.job.cdc.split.BinlogSplit;
import org.apache.doris.job.cdc.split.SnapshotSplit;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.assigner.splitter.ChunkSplitter;
import org.apache.flink.cdc.connectors.base.source.assigner.state.ChunkSplitterState;
import org.apache.flink.cdc.connectors.base.source.assigner.state.ChunkSplitterState.ChunkBound;
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
import org.apache.flink.cdc.connectors.base.source.utils.JdbcChunkUtils;
import org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;

import static org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit.STREAM_SPLIT_ID;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.relational.Column;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Data
public abstract class JdbcIncrementalSourceReader extends AbstractCdcSourceReader {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcIncrementalSourceReader.class);
    private static ObjectMapper objectMapper = new ObjectMapper();

    private final List<
                    SnapshotReaderContext<
                            org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit,
                            Fetcher<SourceRecords, SourceSplitBase>,
                            SnapshotSplitState>>
            snapshotReaderContexts = new ArrayList<>();
    private final Set<String> completedSplitIds = new HashSet<>();
    private int nextSnapshotSplit;
    private int nextPollReader;

    // Stream/binlog reader (single reader for stream split)
    private ManagedReader<Fetcher<SourceRecords, SourceSplitBase>> streamReader;
    private StreamSplit streamSplit;
    private StreamSplitState streamSplitState;
    protected volatile FetchTask<SourceSplitBase> currentFetchTask;

    public JdbcIncrementalSourceReader() {
        this.serializer = new DebeziumJsonDeserializer();
    }

    @Override
    public void initialize(String jobId, DataSource dataSource, Map<String, String> config) {
        this.serializer.init(config);
    }

    /**
     * Fetch a batch of snapshot splits by driving flink-cdc {@link ChunkSplitter} directly.
     *
     * <p>Stateless: each RPC builds a fresh splitter from the (table, nextChunkStart, nextChunkId)
     * triple supplied by FE, fetches up to {@code batchSize} chunks, then closes the splitter.
     *
     * <p>Only INITIAL/SNAPSHOT startup modes call this RPC; binlog/latest modes never reach here.
     */
    @Override
    public List<AbstractSourceSplit> getSourceSplits(FetchTableSplitsRequest ftsReq) {
        LOG.info(
                "Get table {} splits for job {} (nextSplitId={}, nextSplitStart={})",
                ftsReq.getSnapshotTable(),
                ftsReq.getJobId(),
                ftsReq.getNextSplitId(),
                java.util.Arrays.toString(ftsReq.getNextSplitStart()));
        JdbcSourceConfig sourceConfig = getSourceConfig(ftsReq);
        String schema = ftsReq.getConfig().get(DataSourceConfigKeys.SCHEMA);
        TableId tableId = new TableId(null, schema, ftsReq.getSnapshotTable());

        int batchSize = ftsReq.getBatchSize() == null ? 100 : ftsReq.getBatchSize();
        ChunkSplitterState state = buildChunkSplitterState(sourceConfig, tableId, ftsReq);
        ChunkSplitter splitter = getDialect(sourceConfig).createChunkSplitter(sourceConfig, state);

        try {
            splitter.open();
            List<AbstractSourceSplit> result = new ArrayList<>();
            while (result.size() < batchSize) {
                Collection<org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit>
                        chunks = splitter.generateSplits(tableId);
                for (org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit chunk :
                        chunks) {
                    result.add(toDorisSnapshotSplit(chunk));
                }
                if (!splitter.hasNextChunk()) {
                    break;
                }
            }
            LOG.info(
                    "Fetched {} splits for table {} (resume nextSplitId={}); hasNextChunk={}",
                    result.size(),
                    tableId,
                    ftsReq.getNextSplitId(),
                    splitter.hasNextChunk());
            return result;
        } catch (Exception e) {
            throw new RuntimeException("Failed to generate splits for " + tableId, e);
        } finally {
            try {
                splitter.close();
            } catch (Exception e) {
                LOG.warn("Failed to close splitter for {}", tableId, e);
            }
        }
    }

    /**
     * null start -> NO_SPLITTING_TABLE_STATE (analyze + maybe evenly); non-null -> resume
     * mid-table. Cast pkValues[0] back to the JDBC driver's natural type (JSON round-trip
     * downgrades types).
     */
    private ChunkSplitterState buildChunkSplitterState(
            JdbcSourceConfig sourceConfig, TableId tableId, FetchTableSplitsRequest ftsReq) {
        Object[] pkValues = ftsReq.getNextSplitStart();
        if (pkValues == null || pkValues.length == 0) {
            return ChunkSplitterState.NO_SPLITTING_TABLE_STATE;
        }
        TableChanges.TableChange tableChange = getTableSchemas(ftsReq).get(tableId);
        Column splitColumn =
                JdbcChunkUtils.getSplitColumn(
                        tableChange.getTable(), sourceConfig.getChunkKeyColumn());
        Class<?> targetClass = resolveSplitKeyClass(tableId, splitColumn, ftsReq);
        Object[] castStart = convertBounds(pkValues, targetClass, objectMapper);
        int splitId = ftsReq.getNextSplitId() == null ? 0 : ftsReq.getNextSplitId();
        return new ChunkSplitterState(tableId, ChunkBound.middleOf(castStart[0]), splitId);
    }

    /**
     * flink-cdc SnapshotSplit -> Doris SnapshotSplit (drops splitKeyType, keeps key field names).
     */
    private SnapshotSplit toDorisSnapshotSplit(
            org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit chunk) {
        return new SnapshotSplit(
                chunk.splitId(),
                chunk.getTableId().identifier(),
                chunk.getSplitKeyType().getFieldNames(),
                chunk.getSplitStart(),
                chunk.getSplitEnd(),
                null);
    }

    @Override
    public SplitReadResult prepareAndSubmitSplit(JobBaseRecordRequest baseReq) throws Exception {
        Map<String, Object> offsetMeta = baseReq.getMeta();
        LOG.info("Job {} read split records with offset: {}", baseReq.getJobId(), offsetMeta);

        if (SourceReader.isBinlogSplitMeta(offsetMeta)) {
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

    /** Prepare snapshot splits (unified handling for single or multiple splits). */
    private SplitReadResult prepareSnapshotSplits(
            List<org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit> splits,
            JobBaseRecordRequest baseReq)
            throws Exception {
        SplitReadResult result;
        SnapshotQuota quota;
        synchronized (this) {
            LOG.info(
                    "Preparing {} snapshot split(s) for reading, jobId={}",
                    splits.size(),
                    baseReq.getJobId());
            // Close previous readers before clearing to avoid JDBC connection leak
            finishSplitRecords();
            // Construct return result with all splits and states, including pending splits.
            result = new SplitReadResult();
            result.setSplits(new ArrayList<>(splits));
            Map<String, Object> states = new HashMap<>();
            for (org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit split :
                    splits) {
                SnapshotSplitState state = new SnapshotSplitState(split);
                snapshotReaderContexts.add(new SnapshotReaderContext<>(split, state));
                states.put(split.splitId(), state);
            }
            result.setSplitStates(states);
            try {
                quota = prepareSnapshotReaders(baseReq, splits.size());
            } catch (RuntimeException failure) {
                finishSplitRecords();
                throw failure;
            }
        }
        try {
            startSnapshotReaders(quota);
            LOG.info(
                    "Success prepared {} snapshot splits for reading, jobId={}",
                    splits.size(),
                    baseReq.getJobId());
            return result;
        } catch (RuntimeException failure) {
            synchronized (this) {
                if (snapshotQuota == quota) {
                    finishSplitRecords();
                }
            }
            throw failure;
        }
    }

    /** Prepare stream split */
    private synchronized SplitReadResult prepareStreamSplit(
            Map<String, Object> offsetMeta, JobBaseRecordRequest baseReq) throws Exception {
        // Load tableSchemas from FE if available (avoids re-discover on restart)
        tryLoadTableSchemasFromRequest(baseReq);
        // If still null (incremental-only startup, or snapshot→binlog transition where FE never
        // persisted schema), do a JDBC discover so the deserializer has a baseline to diff against.
        if (this.tableSchemas == null) {
            LOG.info(
                    "No tableSchemas available for stream split, discovering via JDBC for job {}",
                    baseReq.getJobId());
            Map<TableId, TableChanges.TableChange> discovered = getTableSchemas(baseReq);
            setTableSchemas(new java.util.concurrent.ConcurrentHashMap<>(discovered));
            LOG.info(
                    "Discovered {} table schema(s) for job {}",
                    discovered.size(),
                    baseReq.getJobId());
        }
        Tuple2<SourceSplitBase, Boolean> splitFlag = createStreamSplit(offsetMeta, baseReq);
        StreamSplit newStreamSplit = splitFlag.f0.asStreamSplit();

        // offset guard: reuse only when request start == reader's consumed position. Compare by
        // compareTo (LSN), NOT equals -- PG drops lsn_proc/commit so same position differs by map.
        if (this.streamReader != null && this.streamSplitState != null) {
            Offset requestStart = newStreamSplit.getStartingOffset();
            Offset readerPos = this.streamSplitState.getStartingOffset();
            if (requestStart != null
                    && readerPos != null
                    && requestStart.compareTo(readerPos) == 0) {
                LOG.info(
                        "Reuse live stream reader for job {} at offset {}",
                        baseReq.getJobId(),
                        requestStart);
                // Refresh split so commitSourceOffset advances PG confirmed_lsn to the FE-committed
                // offset (== reader pos); poll/offset keep using streamSplitState.
                this.streamSplit = newStreamSplit;
                SplitReadResult reuseResult = new SplitReadResult();
                reuseResult.setSplits(Collections.singletonList(this.streamSplit));
                Map<String, Object> reuseStates = new HashMap<>();
                reuseStates.put(this.streamSplit.splitId(), this.streamSplitState);
                reuseResult.setSplitStates(reuseStates);
                return reuseResult;
            }
            LOG.info(
                    "Rebuild stream reader for job {}: requestStart={}, readerPos={}",
                    baseReq.getJobId(),
                    requestStart,
                    readerPos);
        }

        this.streamSplit = newStreamSplit;
        // Close previous stream reader (rebuild path) before creating a new one. This prevents
        // connection leaks when a cancelled task's reader is still active while a new task arrives.
        if (this.streamReader != null) {
            LOG.info(
                    "Closing previous stream reader before creating new one for job {}",
                    baseReq.getJobId());
            this.streamReader.close();
            this.streamReader = null;
            currentFetchTask = null;
        }

        // Rebuild path: fail loudly if the source position is gone (e.g. slot dropped) instead of
        // silently re-locating from a lost offset.
        validateStreamSource(newStreamSplit.getStartingOffset(), baseReq);

        this.streamReader =
                createIncrementalReader(
                        baseReq,
                        () -> getBinlogSplitReader(baseReq),
                        JdbcIncrementalSourceReader::closeFetcher);

        try {
            LOG.info("Prepare stream split: {}", this.streamSplit.toString());

            // Submit split
            FetchTask<SourceSplitBase> splitFetchTask =
                    createFetchTaskFromSplit(baseReq, this.streamSplit);
            this.streamReader.getReader().submitTask(splitFetchTask);
            this.setCurrentFetchTask(splitFetchTask);

            this.streamSplitState = new StreamSplitState(this.streamSplit);
        } catch (Exception failure) {
            try {
                finishSplitRecords();
            } catch (Exception cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }

        SplitReadResult result = new SplitReadResult();
        result.setSplits(Collections.singletonList(this.streamSplit));

        Map<String, Object> statesMap = new HashMap<>();
        statesMap.put(this.streamSplit.splitId(), this.streamSplitState);
        result.setSplitStates(statesMap);

        LOG.info("Success prepared stream split: {}", this.streamSplit.toString());
        return result;
    }

    // Source-specific check before (re)building the stream reader; default no-op.
    protected void validateStreamSource(Offset startingOffset, JobBaseRecordRequest baseReq)
            throws Exception {}

    @Override
    public ReaderBatch<SourceRecord> pollRecords() throws Exception {
        SnapshotQuota quota = snapshotQuota;
        if (quota != null) {
            // Snapshot split mode
            return pollRecordsFromSnapshotReaders(quota);
        } else if (streamReader != null) {
            // Stream split mode
            return pollRecordsFromStreamReader();
        } else {
            throw new RuntimeException("No active snapshot or stream reader available");
        }
    }

    /** Poll active snapshot readers in parallel and start pending splits as slots are released. */
    private ReaderBatch<SourceRecord> pollRecordsFromSnapshotReaders(SnapshotQuota quota)
            throws Exception {
        refreshCompletedSplits(quota);
        startSnapshotReaders(quota);
        synchronized (this) {
            if (snapshotQuota != quota) {
                return ReaderBatch.wrap(Collections.emptyIterator());
            }
            startParallelPolling();
            ReaderBatch<SourceRecord> result = pollReadyBatch();
            if (result == null && completedSplitIds.size() < snapshotReaderContexts.size()) {
                // Return periodically so the task can check cancellation and ownership.
                wait(100);
                if (snapshotQuota == quota) {
                    result = pollReadyBatch();
                }
            }
            return result == null ? ReaderBatch.wrap(Collections.emptyIterator()) : result;
        }
    }

    private void startSnapshotReaders(SnapshotQuota quota) {
        JobBaseRecordRequest request;
        int firstSplit;
        int splitCount;
        synchronized (this) {
            firstSplit = nextSnapshotSplit;
            splitCount = snapshotReaderContexts.size();
            request = snapshotRequest;
        }

        for (int i = firstSplit; i < splitCount; i++) {
            int index = i;
            // Construction may open a connection; cancellation must not wait for this lock.
            ManagedReader<Fetcher<SourceRecords, SourceSplitBase>> reader =
                    quota.createSnapshotReader(
                            () -> getSnapshotSplitReader(request, index),
                            JdbcIncrementalSourceReader::closeFetcher);
            if (reader == null) {
                return;
            }

            boolean cancelled;
            synchronized (this) {
                cancelled = snapshotQuota != quota;
                if (!cancelled) {
                    var context = snapshotReaderContexts.get(index);
                    var split = context.getSplit();
                    context.submit(
                            reader,
                            () -> {
                                // Submit split (triggers async reading, data goes into reader's
                                // Debezium queue)
                                reader.getReader()
                                        .submitTask(createFetchTaskFromSplit(request, split));
                                LOG.info(
                                        "Created reader {}/{} and submitted split: {} (table: {}), jobId={}",
                                        index + 1,
                                        splitCount,
                                        split.splitId(),
                                        split.getTableId().identifier(),
                                        request.getJobId());
                            },
                            snapshotPollExecutor);
                    nextSnapshotSplit++;
                }
            }
            if (cancelled) {
                reader.close();
                return;
            }
        }
        // With every split started, released slots can go directly to waiting tasks.
        quota.close();
    }

    /** Start parallel polling for active readers with no outstanding batch. */
    private void startParallelPolling() {
        for (int i = 0; i < nextSnapshotSplit; i++) {
            var context = snapshotReaderContexts.get(i);
            if (completedSplitIds.contains(context.getSplit().splitId())
                    || context.getPollFuture() != null
                    || context.getBatch() != null) {
                continue;
            }
            CompletableFuture<ReaderBatch<SourceRecord>> future =
                    context.getSubmitFuture()
                            .thenApplyAsync(
                                    ignored -> {
                                        try {
                                            return context.getManagedReader()
                                                    .pollBatch(
                                                            reader ->
                                                                    filteredRecords(
                                                                            reader
                                                                                    .pollSplitRecords(),
                                                                            context.getSplit()
                                                                                    .splitId(),
                                                                            context
                                                                                    .getSplitState()));
                                        } catch (Exception failure) {
                                            throw new CompletionException(failure);
                                        }
                                    },
                                    snapshotPollExecutor);
            context.setPollFuture(future);
            future.whenComplete(
                    (batch, failure) -> {
                        synchronized (this) {
                            notifyAll();
                        }
                    });
        }
    }

    /** Return the next nonempty ready batch, rotating to avoid starving siblings. */
    private ReaderBatch<SourceRecord> pollReadyBatch() throws Exception {
        for (int i = 0; i < nextSnapshotSplit; i++) {
            int index = (nextPollReader + i) % nextSnapshotSplit;
            var context = snapshotReaderContexts.get(index);
            CompletableFuture<ReaderBatch<SourceRecord>> future = context.getPollFuture();
            if (future != null && future.isDone()) {
                ReaderBatch<SourceRecord> batch = future.get();
                // Keep the future until filtering succeeds so cleanup can close a failed batch.
                boolean hasRecords = batch.hasNext();
                context.setPollFuture(null);
                context.setBatch(batch);
                if (hasRecords) {
                    nextPollReader = (index + 1) % nextSnapshotSplit;
                    return batch;
                }
                // Keep the closed batch in context for high-watermark completion checks.
                batch.close();
            }
        }
        return null;
    }

    private void refreshCompletedSplits(SnapshotQuota quota) {
        List<ManagedReader<Fetcher<SourceRecords, SourceSplitBase>>> completed = new ArrayList<>();
        synchronized (this) {
            if (snapshotQuota != quota) {
                return;
            }
            for (var context : snapshotReaderContexts) {
                ReaderBatch<SourceRecord> batch = context.getBatch();
                if (batch != null && batch.isClosed()) {
                    context.setBatch(null);
                    // A split is finished only after its high-watermark event has been consumed.
                    if (context.getSplitState().getHighWatermark() != null) {
                        completedSplitIds.add(context.getSplit().splitId());
                        completed.add(context.getManagedReader());
                        LOG.info(
                                "Finished consuming snapshot split, jobId={}, splitId={}, "
                                        + "completedSplits={}, totalSplits={}",
                                snapshotRequest.getJobId(),
                                context.getSplit().splitId(),
                                completedSplitIds.size(),
                                snapshotReaderContexts.size());
                    }
                }
            }
        }
        // Closing a connector may wait for its worker; keep cancellation free to take the lock.
        RuntimeException closeFailure = null;
        for (var reader : completed) {
            try {
                reader.close();
            } catch (RuntimeException failure) {
                if (closeFailure == null) {
                    closeFailure = failure;
                } else {
                    closeFailure.addSuppressed(failure);
                }
            }
        }
        if (closeFailure != null) {
            throw closeFailure;
        }
    }

    @Override
    public boolean isSnapshotFinished() {
        refreshCompletedSplits(snapshotQuota);
        synchronized (this) {
            return completedSplitIds.size() == snapshotReaderContexts.size();
        }
    }

    /** Poll records from stream reader */
    private ReaderBatch<SourceRecord> pollRecordsFromStreamReader() throws InterruptedException {
        ManagedReader<Fetcher<SourceRecords, SourceSplitBase>> managedReader = streamReader;
        StreamSplit split = streamSplit;
        StreamSplitState state = streamSplitState;
        if (managedReader == null || split == null || state == null) {
            LOG.info("Stream reader is null at poll start, returning empty");
            return ReaderBatch.wrap(Collections.emptyIterator());
        }

        return managedReader.pollBatch(
                reader -> filteredRecords(reader.pollSplitRecords(), split.splitId(), state));
    }

    private Iterator<SourceRecord> filteredRecords(
            Iterator<SourceRecords> polled, String splitId, SourceSplitState state) {
        if (polled == null || !polled.hasNext()) {
            return Collections.emptyIterator();
        }
        return new FilteredRecordIterator(
                new SplitRecords(splitId, polled.next().iterator()), state);
    }

    protected abstract DataType fromDbzColumn(Column splitColumn);

    protected abstract Fetcher<SourceRecords, SourceSplitBase> getSnapshotSplitReader(
            JobBaseConfig jobConfig, int subtaskId);

    protected abstract Fetcher<SourceRecords, SourceSplitBase> getBinlogSplitReader(
            JobBaseConfig jobConfig);

    static ExecutorService closeFetcher(Fetcher<SourceRecords, SourceSplitBase> fetcher) {
        fetcher.close();
        if (fetcher instanceof IncrementalSourceScanFetcher) {
            return ((IncrementalSourceScanFetcher) fetcher).getExecutorService();
        }
        // Flink CDC 3.6 exposes the scan executor, but only stores the stream executor privately.
        try {
            return (ExecutorService)
                    FieldUtils.readField(
                            (IncrementalSourceStreamFetcher) fetcher, "executorService", true);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("Cannot confirm stream reader termination", e);
        }
    }

    protected abstract OffsetFactory getOffsetFactory();

    protected abstract Offset createOffset(Map<String, ?> offset);

    protected abstract Offset createInitialOffset();

    protected abstract Offset createNoStoppingOffset();

    protected abstract JdbcDataSourceDialect getDialect(JdbcSourceConfig sourceConfig);

    private org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit
            createSnapshotSplit(Map<String, Object> offset, JobBaseConfig jobConfig) {
        SnapshotSplit snapshotSplit = objectMapper.convertValue(offset, SnapshotSplit.class);
        TableId tableId = TableId.parse(snapshotSplit.getTableId(), false);
        List<String> splitKeys = snapshotSplit.getSplitKey();
        Map<TableId, TableChanges.TableChange> tableSchemas = getTableSchemas(jobConfig);
        TableChanges.TableChange tableChange = tableSchemas.get(tableId);
        Preconditions.checkNotNull(
                tableChange, "Can not find table " + tableId + " in job " + jobConfig.getJobId());
        // only support one split key
        String splitKey = splitKeys.get(0);
        Column splitColumn = tableChange.getTable().columnWithName(splitKey);
        Preconditions.checkNotNull(
                splitColumn,
                "Split key column "
                        + splitKey
                        + " not found in table "
                        + tableId
                        + " for job "
                        + jobConfig.getJobId());
        Class<?> keyClass = resolveSplitKeyClass(tableId, splitColumn, jobConfig);
        Object[] splitStart = convertBounds(snapshotSplit.getSplitStart(), keyClass, objectMapper);
        Object[] splitEnd = convertBounds(snapshotSplit.getSplitEnd(), keyClass, objectMapper);
        RowType splitType = getSplitType(splitColumn);
        org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit split =
                new org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit(
                        tableId,
                        snapshotSplit.getSplitId(),
                        splitType,
                        splitStart,
                        splitEnd,
                        null,
                        Collections.singletonMap(tableId, tableChange));
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

            Map<TableId, TableChanges.TableChange> tableSchemas = getTableSchemas(config);
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
                TableId tid = TableId.parse(split.getTableId());
                TableChanges.TableChange tableChange = tableSchemas.get(tid);
                Preconditions.checkNotNull(
                        tableChange, "Can not find table " + tid + " in job " + config.getJobId());
                String splitKey = split.getSplitKey().get(0);
                Column splitColumn = tableChange.getTable().columnWithName(splitKey);
                Preconditions.checkNotNull(
                        splitColumn,
                        "Split key column "
                                + splitKey
                                + " not found in table "
                                + tid
                                + " for job "
                                + config.getJobId());
                Class<?> keyClass = resolveSplitKeyClass(tid, splitColumn, config);
                Object[] start = convertBounds(split.getSplitStart(), keyClass, objectMapper);
                Object[] end = convertBounds(split.getSplitEnd(), keyClass, objectMapper);
                finishedSnapshotSplitInfos.add(
                        new FinishedSnapshotSplitInfo(
                                tid,
                                split.getSplitId(),
                                start,
                                end,
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
            case SPECIFIC_OFFSETS:
                startingOffset = createOffset(startupOptions.getOffset());
                break;
            case TIMESTAMP:
            case COMMITTED_OFFSETS:
            default:
                throw new IllegalStateException(
                        "Unsupported startup mode " + startupOptions.startupMode);
        }
        return startingOffset;
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
    public synchronized void finishSplitRecords() {
        shutdownSnapshotPollExecutor();
        // Clean up snapshot readers
        if (nextSnapshotSplit > 0) {
            LOG.info("Closing {} snapshot readers", nextSnapshotSplit);
        }
        RuntimeException closeFailure = null;
        for (var context : snapshotReaderContexts) {
            try {
                context.close();
            } catch (RuntimeException failure) {
                if (closeFailure == null) {
                    closeFailure = failure;
                } else {
                    closeFailure.addSuppressed(failure);
                }
            }
        }
        snapshotReaderContexts.clear();
        completedSplitIds.clear();
        nextSnapshotSplit = 0;
        nextPollReader = 0;

        // Clean up stream reader
        if (streamReader != null) {
            LOG.info("Closing stream reader");
            streamReader.close();
            streamReader = null;
            currentFetchTask = null;
            streamSplit = null;
            streamSplitState = null;
        }
        if (closeFailure != null) {
            throw closeFailure;
        }
    }

    protected Map<TableId, TableChanges.TableChange> getTableSchemas(JobBaseConfig config) {
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
    public synchronized void close(JobBaseConfig jobConfig) {
        LOG.info("Close source reader for job {}", jobConfig.getJobId());
        finishSplitRecords();
        if (tableSchemas != null) {
            tableSchemas.clear();
            tableSchemas = null;
        }
    }

    @Override
    public DeserializeResult deserialize(Map<String, String> config, SourceRecord element)
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
                } else if (SourceRecordUtils.isSchemaChangeEvent(element)) {
                    // PostgresSchemaRecord is synthetic and has no source offset. Keep the current
                    // stream offset; the following DML or heartbeat advances it.
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
