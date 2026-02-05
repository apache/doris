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

package org.apache.doris.cdcclient.service;

import org.apache.doris.cdcclient.common.Constants;
import org.apache.doris.cdcclient.common.Env;
import org.apache.doris.cdcclient.model.response.RecordWithMeta;
import org.apache.doris.cdcclient.sink.DorisBatchStreamLoad;
import org.apache.doris.cdcclient.source.reader.SourceReader;
import org.apache.doris.cdcclient.source.reader.SplitReadResult;
import org.apache.doris.job.cdc.request.FetchRecordRequest;
import org.apache.doris.job.cdc.request.WriteRecordRequest;
import org.apache.doris.job.cdc.split.BinlogSplit;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils.SCHEMA_HEARTBEAT_EVENT_KEY_NAME;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import io.debezium.data.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/** Pipeline coordinator. */
@Component
public class PipelineCoordinator {
    private static final Logger LOG = LoggerFactory.getLogger(PipelineCoordinator.class);
    private static final String SPLIT_ID = "splitId";
    // jobId
    private final Map<Long, DorisBatchStreamLoad> batchStreamLoadMap = new ConcurrentHashMap<>();
    // taskId -> writeFailReason
    private final Map<String, String> taskErrorMaps = new ConcurrentHashMap<>();
    private final ThreadPoolExecutor executor;
    private static final int MAX_CONCURRENT_TASKS = 10;
    private static final int QUEUE_CAPACITY = 128;
    private static ObjectMapper objectMapper = new ObjectMapper();

    public PipelineCoordinator() {
        this.executor =
                new ThreadPoolExecutor(
                        MAX_CONCURRENT_TASKS,
                        MAX_CONCURRENT_TASKS,
                        60L,
                        TimeUnit.SECONDS,
                        new LinkedBlockingQueue<>(QUEUE_CAPACITY),
                        r -> {
                            Thread t =
                                    new Thread(
                                            r, "async-write-record-" + System.currentTimeMillis());
                            t.setDaemon(false);
                            return t;
                        },
                        new ThreadPoolExecutor.AbortPolicy());
    }

    public RecordWithMeta fetchRecords(FetchRecordRequest fetchRecordRequest) throws Exception {
        SourceReader sourceReader = Env.getCurrentEnv().getReader(fetchRecordRequest);
        SplitReadResult readResult = sourceReader.prepareAndSubmitSplit(fetchRecordRequest);
        return buildRecordResponse(sourceReader, fetchRecordRequest, readResult);
    }

    /**
     * Build RecordWithMeta response
     *
     * <p>This method polls records until: 1. Data is received AND heartbeat is received (normal
     * case) 2. Timeout is reached (with heartbeat wait protection)
     */
    private RecordWithMeta buildRecordResponse(
            SourceReader sourceReader, FetchRecordRequest fetchRecord, SplitReadResult readResult)
            throws Exception {
        RecordWithMeta recordResponse = new RecordWithMeta();
        try {
            boolean isSnapshotSplit = sourceReader.isSnapshotSplit(readResult.getSplit());
            long startTime = System.currentTimeMillis();
            boolean shouldStop = false;
            boolean hasReceivedData = false;
            boolean lastMessageIsHeartbeat = false;
            int heartbeatCount = 0;
            int recordCount = 0;
            LOG.info(
                    "Start fetching records for jobId={}, isSnapshotSplit={}",
                    fetchRecord.getJobId(),
                    isSnapshotSplit);
            while (!shouldStop) {
                Iterator<SourceRecord> recordIterator = sourceReader.pollRecords();

                if (!recordIterator.hasNext()) {
                    Thread.sleep(100);

                    // Check if should stop
                    long elapsedTime = System.currentTimeMillis() - startTime;
                    boolean timeoutReached = elapsedTime > Constants.POLL_SPLIT_RECORDS_TIMEOUTS;

                    if (shouldStop(
                            isSnapshotSplit,
                            hasReceivedData,
                            lastMessageIsHeartbeat,
                            elapsedTime,
                            Constants.POLL_SPLIT_RECORDS_TIMEOUTS,
                            timeoutReached)) {
                        break;
                    }
                    continue;
                }

                while (recordIterator.hasNext()) {
                    SourceRecord element = recordIterator.next();

                    // Check if this is a heartbeat message
                    if (isHeartbeatEvent(element)) {
                        heartbeatCount++;

                        // Mark last message as heartbeat
                        if (!isSnapshotSplit) {
                            lastMessageIsHeartbeat = true;
                        }

                        // If already have data or timeout, stop when heartbeat received
                        long elapsedTime = System.currentTimeMillis() - startTime;
                        boolean timeoutReached =
                                elapsedTime > Constants.POLL_SPLIT_RECORDS_TIMEOUTS;

                        if (hasReceivedData || timeoutReached) {
                            LOG.info(
                                    "Heartbeat received after {} data records, stopping",
                                    recordResponse.getRecords().size());
                            shouldStop = true;
                            break;
                        }
                        // Skip heartbeat messages if we haven't received data yet
                        continue;
                    }

                    // Process data messages
                    List<String> serializedRecords =
                            sourceReader.deserialize(fetchRecord.getConfig(), element);
                    if (!CollectionUtils.isEmpty(serializedRecords)) {
                        recordCount++;
                        recordResponse.getRecords().addAll(serializedRecords);
                        hasReceivedData = true;
                        lastMessageIsHeartbeat = false;
                    }
                }
            }
            LOG.info(
                    "Fetched {} records and {} heartbeats in {} ms for jobId={}",
                    recordCount,
                    heartbeatCount,
                    System.currentTimeMillis() - startTime,
                    fetchRecord.getJobId());
        } finally {
            cleanupReaderResources(sourceReader, fetchRecord.getJobId(), readResult);
        }

        // Extract and set offset metadata
        List<Map<String, String>> offsetMeta = extractOffsetMeta(sourceReader, readResult);
        recordResponse.setMeta(offsetMeta);

        return recordResponse;
    }

    public CompletableFuture<Void> writeRecordsAsync(WriteRecordRequest writeRecordRequest) {
        Preconditions.checkNotNull(writeRecordRequest.getToken(), "token must not be null");
        Preconditions.checkNotNull(writeRecordRequest.getTaskId(), "taskId must not be null");
        Preconditions.checkNotNull(writeRecordRequest.getTargetDb(), "targetDb must not be null");
        return CompletableFuture.runAsync(
                () -> {
                    try {
                        LOG.info(
                                "Start processing async write record, jobId={} taskId={}",
                                writeRecordRequest.getJobId(),
                                writeRecordRequest.getTaskId());
                        writeRecords(writeRecordRequest);
                        LOG.info(
                                "Successfully processed async write record, jobId={} taskId={}",
                                writeRecordRequest.getJobId(),
                                writeRecordRequest.getTaskId());
                    } catch (Exception ex) {
                        closeJobStreamLoad(writeRecordRequest.getJobId());
                        String rootCauseMessage = ExceptionUtils.getRootCauseMessage(ex);
                        taskErrorMaps.put(writeRecordRequest.getTaskId(), rootCauseMessage);
                        LOG.error(
                                "Failed to process async write record, jobId={} taskId={}",
                                writeRecordRequest.getJobId(),
                                writeRecordRequest.getTaskId(),
                                ex);
                    }
                },
                executor);
    }

    /**
     * Read data from SourceReader and write it to Doris, while returning meta information.
     *
     * <p>Snapshot split: Returns immediately after reading; otherwise, returns after the
     * maxInterval.
     *
     * <p>Binlog split: Fetches data at the maxInterval. Returns immediately if no data is found; if
     * found, checks if the last record is a heartbeat record. If it is, returns immediately;
     * otherwise, fetches again until the heartbeat deadline.
     *
     * <p>Heartbeat events will carry the latest offset.
     */
    public void writeRecords(WriteRecordRequest writeRecordRequest) throws Exception {
        SourceReader sourceReader = Env.getCurrentEnv().getReader(writeRecordRequest);
        DorisBatchStreamLoad batchStreamLoad = null;
        long scannedRows = 0L;
        int heartbeatCount = 0;
        SplitReadResult readResult = null;
        try {
            // 1. submit split async
            readResult = sourceReader.prepareAndSubmitSplit(writeRecordRequest);
            batchStreamLoad = getOrCreateBatchStreamLoad(writeRecordRequest);

            boolean isSnapshotSplit = sourceReader.isSnapshotSplit(readResult.getSplit());
            long startTime = System.currentTimeMillis();
            long maxIntervalMillis = writeRecordRequest.getMaxInterval() * 1000;
            boolean shouldStop = false;
            boolean lastMessageIsHeartbeat = false;
            LOG.info(
                    "Start polling records for jobId={} taskId={}, isSnapshotSplit={}",
                    writeRecordRequest.getJobId(),
                    writeRecordRequest.getTaskId(),
                    isSnapshotSplit);

            // 2. poll record
            while (!shouldStop) {
                Iterator<SourceRecord> recordIterator = sourceReader.pollRecords();

                if (!recordIterator.hasNext()) {
                    Thread.sleep(100);

                    // Check if should stop
                    long elapsedTime = System.currentTimeMillis() - startTime;
                    boolean timeoutReached =
                            maxIntervalMillis > 0 && elapsedTime >= maxIntervalMillis;

                    if (shouldStop(
                            isSnapshotSplit,
                            scannedRows > 0,
                            lastMessageIsHeartbeat,
                            elapsedTime,
                            maxIntervalMillis,
                            timeoutReached)) {
                        break;
                    }
                    continue;
                }

                while (recordIterator.hasNext()) {
                    SourceRecord element = recordIterator.next();

                    // Check if this is a heartbeat message
                    if (isHeartbeatEvent(element)) {
                        heartbeatCount++;

                        // Mark last message as heartbeat (only for binlog split)
                        if (!isSnapshotSplit) {
                            lastMessageIsHeartbeat = true;
                        }

                        // If already timeout, stop immediately when heartbeat received
                        long elapsedTime = System.currentTimeMillis() - startTime;
                        boolean timeoutReached =
                                maxIntervalMillis > 0 && elapsedTime >= maxIntervalMillis;

                        if (!isSnapshotSplit && timeoutReached) {
                            LOG.info(
                                    "Binlog split max interval reached and heartbeat received, stopping data reading");
                            shouldStop = true;
                            break;
                        }
                        // Skip heartbeat messages during normal processing
                        continue;
                    }

                    // Process data messages
                    List<String> serializedRecords =
                            sourceReader.deserialize(writeRecordRequest.getConfig(), element);

                    if (!CollectionUtils.isEmpty(serializedRecords)) {
                        String database = writeRecordRequest.getTargetDb();
                        String table = extractTable(element);
                        for (String record : serializedRecords) {
                            scannedRows++;
                            batchStreamLoad.writeRecord(database, table, record.getBytes());
                        }
                        // Mark last message as data (not heartbeat)
                        lastMessageIsHeartbeat = false;
                    }
                }
            }
            LOG.info(
                    "Fetched {} records and {} heartbeats in {} ms for jobId={} taskId={}",
                    scannedRows,
                    heartbeatCount,
                    System.currentTimeMillis() - startTime,
                    writeRecordRequest.getJobId(),
                    writeRecordRequest.getTaskId());

        } finally {
            cleanupReaderResources(sourceReader, writeRecordRequest.getJobId(), readResult);
        }

        // 3. Extract offset from split state
        List<Map<String, String>> metaResponse = extractOffsetMeta(sourceReader, readResult);
        // 4. wait all stream load finish
        batchStreamLoad.forceFlush();

        // 5. request fe api update offset
        String currentTaskId = batchStreamLoad.getCurrentTaskId();
        // The offset must be reset before commitOffset to prevent the next taskId from being create
        // by the fe.
        batchStreamLoad.resetTaskId();
        batchStreamLoad.commitOffset(
                currentTaskId, metaResponse, scannedRows, batchStreamLoad.getLoadStatistic());
    }

    public static boolean isHeartbeatEvent(SourceRecord record) {
        Schema valueSchema = record.valueSchema();
        return valueSchema != null
                && SCHEMA_HEARTBEAT_EVENT_KEY_NAME.equalsIgnoreCase(valueSchema.name());
    }

    /**
     * Determine if we should stop polling.
     *
     * @param isSnapshotSplit whether this is a snapshot split
     * @param hasData whether we have received any data
     * @param lastMessageIsHeartbeat whether the last message is a heartbeat
     * @param elapsedTime total elapsed time in milliseconds
     * @param maxIntervalMillis max interval in milliseconds
     * @param timeoutReached whether timeout is reached
     * @return true if should stop, false if should continue
     */
    private boolean shouldStop(
            boolean isSnapshotSplit,
            boolean hasData,
            boolean lastMessageIsHeartbeat,
            long elapsedTime,
            long maxIntervalMillis,
            boolean timeoutReached) {

        // 1. Snapshot split with data: if no more data in queue, stop immediately (no need to wait
        // for timeout)
        // snapshot split will be written to the debezium queue all at once.
        // multiple snapshot splits are handled in the source reader.
        if (isSnapshotSplit) {
            LOG.info(
                    "Snapshot split finished, no more data available. Total elapsed: {} ms",
                    elapsedTime);
            return true;
        }

        // 2. Not timeout yet: continue waiting
        if (!timeoutReached) {
            return false;
        }

        // === Below are checks after timeout is reached ===

        // 3. No data received after timeout: stop
        if (!hasData) {
            LOG.info("No data received after timeout, stopping. Elapsed: {} ms", elapsedTime);
            return true;
        }

        // 5. Binlog split + last message is heartbeat: stop immediately
        if (lastMessageIsHeartbeat) {
            LOG.info("Binlog split timeout and last message is heartbeat, stopping");
            return true;
        }

        // 6. Binlog split + no heartbeat yet: wait for heartbeat with timeout protection
        if (elapsedTime > maxIntervalMillis + Constants.DEBEZIUM_HEARTBEAT_INTERVAL_MS * 3) {
            LOG.warn(
                    "Binlog split heartbeat wait timeout after {} ms, force stopping. Total elapsed: {} ms",
                    elapsedTime - maxIntervalMillis,
                    elapsedTime);
            return true;
        }

        // Continue waiting for heartbeat
        return false;
    }

    private synchronized DorisBatchStreamLoad getOrCreateBatchStreamLoad(
            WriteRecordRequest writeRecordRequest) {
        DorisBatchStreamLoad batchStreamLoad =
                batchStreamLoadMap.computeIfAbsent(
                        writeRecordRequest.getJobId(),
                        k -> {
                            LOG.info(
                                    "Create DorisBatchStreamLoad for jobId={}",
                                    writeRecordRequest.getJobId());
                            return new DorisBatchStreamLoad(
                                    writeRecordRequest.getJobId(),
                                    writeRecordRequest.getTargetDb());
                        });
        batchStreamLoad.setCurrentTaskId(writeRecordRequest.getTaskId());
        batchStreamLoad.setFrontendAddress(writeRecordRequest.getFrontendAddress());
        batchStreamLoad.setToken(writeRecordRequest.getToken());
        batchStreamLoad.setLoadProps(writeRecordRequest.getStreamLoadProps());
        batchStreamLoad.getLoadStatistic().clear();
        return batchStreamLoad;
    }

    public void closeJobStreamLoad(Long jobId) {
        DorisBatchStreamLoad batchStreamLoad = batchStreamLoadMap.remove(jobId);
        if (batchStreamLoad != null) {
            LOG.info("Close DorisBatchStreamLoad for jobId={}", jobId);
            batchStreamLoad.close();
            batchStreamLoad = null;
        }
    }

    private String extractTable(SourceRecord record) {
        Struct value = (Struct) record.value();
        return value.getStruct(Envelope.FieldName.SOURCE).getString("table");
    }

    public String getTaskFailReason(String taskId) {
        String taskReason = taskErrorMaps.remove(taskId);
        return taskReason == null ? "" : taskReason;
    }

    /**
     * Clean up reader resources: commit source offset and finish split records.
     *
     * @param sourceReader the source reader
     * @param jobId the job id
     * @param readResult the read result containing split information
     */
    private void cleanupReaderResources(
            SourceReader sourceReader, Long jobId, SplitReadResult readResult) {
        try {
            // The LSN in the commit is the current offset, which is the offset from the last
            // successful write.
            // Therefore, even if a subsequent write fails, it will not affect the commit.
            if (readResult != null && readResult.getSplit() != null) {
                sourceReader.commitSourceOffset(jobId, readResult.getSplit());
            }
        } finally {
            // This must be called after `commitSourceOffset`; otherwise,
            // PG's confirmed lsn will not proceed.
            // This operation must be performed before `batchStreamLoad.commitOffset`;
            // otherwise, fe might create the next task for this job.
            sourceReader.finishSplitRecords();
        }
    }

    /**
     * Extract offset metadata from split state.
     *
     * <p>This method handles both snapshot splits and binlog splits, extracting the appropriate
     * offset information through the SourceReader interface. For snapshot splits:
     * [{"splitId":"tbl:1",...},...]
     *
     * <p>For Binlog Split:
     * [{"splitId":"binlog_split","fileName":"mysql-bin.000001","pos":"12345",...}]
     *
     * @param sourceReader the source reader
     * @param readResult the read result containing splits and split states
     * @return offset metadata map
     * @throws RuntimeException if split state is null or split type is unknown
     */
    private List<Map<String, String>> extractOffsetMeta(
            SourceReader sourceReader, SplitReadResult readResult) throws JsonProcessingException {
        Preconditions.checkNotNull(readResult, "readResult must not be null");

        if (readResult.getSplitState() == null) {
            throw new RuntimeException("split state is null");
        }

        SourceSplit split = readResult.getSplit();
        List<Map<String, String>> commitOffsets = new ArrayList<>();
        if (sourceReader.isSnapshotSplit(split)) {
            // Unified format for both single and multiple splits
            List<SourceSplit> allSplits = readResult.getSplits();
            Map<String, Object> allStates = readResult.getSplitStates();

            for (SourceSplit currentSplit : allSplits) {
                String splitId = currentSplit.splitId();
                Object currentState = allStates.get(splitId);
                Preconditions.checkNotNull(
                        currentState, "Split state not found for splitId: " + splitId);

                Map<String, String> highWatermark =
                        sourceReader.extractSnapshotStateOffset(currentState);
                Map<String, String> splitInfo = new HashMap<>();
                splitInfo.put(SourceReader.SPLIT_ID, splitId);
                splitInfo.putAll(highWatermark);
                commitOffsets.add(splitInfo);
            }
        } else if (sourceReader.isBinlogSplit(split)) {
            Map<String, String> offsetRes =
                    sourceReader.extractBinlogStateOffset(readResult.getSplitState());
            offsetRes.put(SPLIT_ID, BinlogSplit.BINLOG_SPLIT_ID);
            commitOffsets.add(offsetRes);
        } else {
            throw new RuntimeException("Unknown split type: " + split.getClass().getName());
        }
        return commitOffsets;
    }
}
