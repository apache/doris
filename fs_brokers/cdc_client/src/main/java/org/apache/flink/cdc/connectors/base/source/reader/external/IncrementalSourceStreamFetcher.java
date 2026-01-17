/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.base.source.reader.external;

import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.FinishedSnapshotSplitInfo;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceRecords;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.shaded.guava31.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.kafka.connect.source.SourceRecord;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkEvent.isEndWatermarkEvent;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Fetcher to fetch data from table split, the split is the stream split {@link StreamSplit}. */
public class IncrementalSourceStreamFetcher implements Fetcher<SourceRecords, SourceSplitBase> {
    private static final Logger LOG = LoggerFactory.getLogger(IncrementalSourceStreamFetcher.class);

    private final FetchTask.Context taskContext;
    private final ExecutorService executorService;
    private final Set<TableId> pureStreamPhaseTables;

    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile boolean currentTaskRunning;
    private volatile Throwable readException;

    private FetchTask<SourceSplitBase> streamFetchTask;
    private StreamSplit currentStreamSplit;
    private Map<TableId, List<FinishedSnapshotSplitInfo>> finishedSplitsInfo;
    // tableId -> the max splitHighWatermark
    private Map<TableId, Offset> maxSplitHighWatermarkMap;
    private final boolean isBackfillSkipped;

    private static final long READER_CLOSE_TIMEOUT_SECONDS = 30L;

    public IncrementalSourceStreamFetcher(FetchTask.Context taskContext, int subTaskId) {
        this.taskContext = taskContext;
        ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("debezium-reader-" + subTaskId).build();
        this.executorService = Executors.newSingleThreadExecutor(threadFactory);
        this.currentTaskRunning = true;
        this.pureStreamPhaseTables = new HashSet<>();
        this.isBackfillSkipped = taskContext.getSourceConfig().isSkipSnapshotBackfill();
    }

    @Override
    public void submitTask(FetchTask<SourceSplitBase> fetchTask) {
        this.streamFetchTask = fetchTask;
        this.currentStreamSplit = fetchTask.getSplit().asStreamSplit();
        configureFilter();
        taskContext.configure(currentStreamSplit);
        this.queue = taskContext.getQueue();
        executorService.submit(
                () -> {
                    try {
                        streamFetchTask.execute(taskContext);
                    } catch (Exception e) {
                        LOG.error(
                                String.format(
                                        "Execute stream read task for stream split %s fail",
                                        currentStreamSplit),
                                e);
                        readException = e;
                    }
                });
    }

    @Override
    public boolean isFinished() {
        return currentStreamSplit == null || !currentTaskRunning;
    }

    @Nullable
    @Override
    public Iterator<SourceRecords> pollSplitRecords() throws InterruptedException {
        checkReadException();
        final List<SourceRecord> sourceRecords = new ArrayList<>();
        // what happens if currentTaskRunning
        if (currentTaskRunning) {
            List<DataChangeEvent> batch = queue.poll();
            for (DataChangeEvent event : batch) {
                if (isEndWatermarkEvent(event.getRecord())) {
                    LOG.info("Read split {} end watermark event", currentStreamSplit);
                    try {
                        stopReadTask();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    break;
                } else if (shouldEmit(event.getRecord())) {
                    sourceRecords.add(event.getRecord());
                } else {
                    LOG.debug("{} data change event should not emit", event);
                }
            }
            List<SourceRecords> sourceRecordsSet = new ArrayList<>();
            sourceRecordsSet.add(new SourceRecords(sourceRecords));
            return sourceRecordsSet.iterator();
        } else {
            return null;
        }
    }

    private void checkReadException() {
        if (readException != null) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Read split %s error due to %s.",
                            currentStreamSplit, readException.getMessage()),
                    readException);
        }
    }

    @Override
    public void close() {
        try {
            // gracefully stop streamFetchTask, e.g. during shutdown
            stopReadTask();
            if (executorService != null) {
                executorService.shutdown();
                if (!executorService.awaitTermination(
                        READER_CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    LOG.warn(
                            "Failed to close the stream fetcher in {} seconds.",
                            READER_CLOSE_TIMEOUT_SECONDS);
                }
            }
        } catch (Exception e) {
            LOG.error("Close stream fetcher error", e);
        }
    }

    /**
     * Returns the record should emit or not.
     *
     * <p>The watermark signal algorithm is the stream split reader only sends the change event that
     * belongs to its finished snapshot splits. For each snapshot split, the change event is valid
     * since the offset is after its high watermark.
     *
     * <pre> E.g: the data input is :
     *    snapshot-split-0 info : [0,    1024) highWatermark0
     *    snapshot-split-1 info : [1024, 2048) highWatermark1
     *  the data output is:
     *  only the change event belong to [0,    1024) and offset is after highWatermark0 should send,
     *  only the change event belong to [1024, 2048) and offset is after highWatermark1 should send.
     * </pre>
     */
    private boolean shouldEmit(SourceRecord sourceRecord) {
        if (taskContext.isDataChangeRecord(sourceRecord)) {
            TableId tableId = taskContext.getTableId(sourceRecord);
            Offset position = taskContext.getStreamOffset(sourceRecord);
            if (hasEnterPureStreamPhase(tableId, position)) {
                return true;
            }
            // only the table who captured snapshot splits need to filter
            if (finishedSplitsInfo.containsKey(tableId)) {
                // if backfill skipped, don't need to filter
                if (isBackfillSkipped) {
                    return true;
                }
                for (FinishedSnapshotSplitInfo splitInfo : finishedSplitsInfo.get(tableId)) {
                    if (taskContext.isRecordBetween(
                                    sourceRecord,
                                    splitInfo.getSplitStart(),
                                    splitInfo.getSplitEnd())
                            && position.isAfter(splitInfo.getHighWatermark())) {
                        return true;
                    }
                }
            }
            // not in the monitored splits scope, do not emit
            return false;
        }
        // always send the schema change event and signal event
        // we need record them to state of Flink
        return true;
    }

    private boolean hasEnterPureStreamPhase(TableId tableId, Offset position) {
        if (pureStreamPhaseTables.contains(tableId)) {
            return true;
        }
        // the existed tables those have finished snapshot reading
        if (maxSplitHighWatermarkMap.containsKey(tableId)
                && position.isAtOrAfter(maxSplitHighWatermarkMap.get(tableId))) {
            pureStreamPhaseTables.add(tableId);
            return true;
        }

        // Use still need to capture new sharding table if user disable scan new added table,
        // The history records for all new added tables(including sharding table and normal table)
        // will be capture after restore from a savepoint if user enable scan new added table
        if (!taskContext.getSourceConfig().isScanNewlyAddedTableEnabled()) {
            // the new added sharding table without history records
            return !maxSplitHighWatermarkMap.containsKey(tableId)
                    && taskContext.getTableFilter().isIncluded(tableId);
        }
        return false;
    }

    private void configureFilter() {
        List<FinishedSnapshotSplitInfo> finishedSplitInfos =
                currentStreamSplit.getFinishedSnapshotSplitInfos();
        Map<TableId, List<FinishedSnapshotSplitInfo>> splitsInfoMap = new HashMap<>();
        Map<TableId, Offset> tableIdOffsetPositionMap = new HashMap<>();
        // startup mode which is stream only
        if (taskContext.getSourceConfig().getStartupOptions().isStreamOnly()) {
            for (TableId tableId : currentStreamSplit.getTableSchemas().keySet()) {
                tableIdOffsetPositionMap.put(tableId, currentStreamSplit.getStartingOffset());
            }
        }
        // startup mode which includes snapshot phase
        else {
            for (FinishedSnapshotSplitInfo finishedSplitInfo : finishedSplitInfos) {
                TableId tableId = finishedSplitInfo.getTableId();
                List<FinishedSnapshotSplitInfo> list =
                        splitsInfoMap.getOrDefault(tableId, new ArrayList<>());
                list.add(finishedSplitInfo);
                splitsInfoMap.put(tableId, list);

                Offset highWatermark = finishedSplitInfo.getHighWatermark();
                Offset maxHighWatermark = tableIdOffsetPositionMap.get(tableId);
                if (maxHighWatermark == null || highWatermark.isAfter(maxHighWatermark)) {
                    tableIdOffsetPositionMap.put(tableId, highWatermark);
                }
            }
        }
        this.finishedSplitsInfo = splitsInfoMap;
        this.maxSplitHighWatermarkMap = tableIdOffsetPositionMap;
        this.pureStreamPhaseTables.clear();
    }

    public void stopReadTask() throws Exception {
        this.currentTaskRunning = false;

        if (streamFetchTask != null) {
            streamFetchTask.close();
            LOG.info("Stream fetch task for split {} closed successfully", currentStreamSplit);
        }

        if (taskContext != null) {
            taskContext.close();
            LOG.info("Task context for split {} closed successfully", currentStreamSplit);
        }
    }
}
