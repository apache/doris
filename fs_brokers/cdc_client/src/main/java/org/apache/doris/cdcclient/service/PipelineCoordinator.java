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
            long startTime = System.currentTimeMillis();
            boolean shouldStop = false;
            boolean hasReceivedData = false;
            int heartbeatCount = 0;
            int recordCount = 0;

            while (!shouldStop) {
                long elapsedTime = System.currentTimeMillis() - startTime;
                boolean timeoutReached = elapsedTime > Constants.POLL_SPLIT_RECORDS_TIMEOUTS;

                // Timeout protection: force stop if waiting for heartbeat exceeds threshold
                // After reaching timeout, wait at most DEBEZIUM_HEARTBEAT_INTERVAL_MS * 3 for
                // heartbeat
                if (timeoutReached
                        && elapsedTime
                                > Constants.POLL_SPLIT_RECORDS_TIMEOUTS
                                        + Constants.DEBEZIUM_HEARTBEAT_INTERVAL_MS * 3) {
                    LOG.warn(
                            "Heartbeat wait timeout after {} ms since timeout reached, force stopping. "
                                    + "Total elapsed: {} ms",
                            elapsedTime - Constants.POLL_SPLIT_RECORDS_TIMEOUTS,
                            elapsedTime);
                    break;
                }

                Iterator<SourceRecord> recordIterator =
                        sourceReader.pollRecords(readResult.getSplitState());

                if (!recordIterator.hasNext()) {
                    // If we have data and reached timeout, continue waiting for heartbeat
                    if (hasReceivedData && timeoutReached) {
                        Thread.sleep(100);
                        continue;
                    }
                    // Otherwise, just wait for more data
                    Thread.sleep(100);
                    continue;
                }

                while (recordIterator.hasNext()) {
                    SourceRecord element = recordIterator.next();

                    // Check if this is a heartbeat message
                    if (isHeartbeatEvent(element)) {
                        heartbeatCount++;
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
        Map<String, String> offsetMeta = extractOffsetMeta(sourceReader, readResult);
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
     * Read data from SourceReader and write it to Doris, while returning meta information. 1. poll
     * interval record. 2. Try to retrieve the heartbeat message, as it returns the latest offset,
     * preventing the next task from having to skip a large number of records.
     */
    public void writeRecords(WriteRecordRequest writeRecordRequest) throws Exception {
        SourceReader sourceReader = Env.getCurrentEnv().getReader(writeRecordRequest);
        DorisBatchStreamLoad batchStreamLoad = null;
        Map<String, String> metaResponse = new HashMap<>();
        long scannedRows = 0L;
        long scannedBytes = 0L;
        int heartbeatCount = 0;
        SplitReadResult readResult = null;
        try {
            // 1. submit split async
            readResult = sourceReader.prepareAndSubmitSplit(writeRecordRequest);
            batchStreamLoad = getOrCreateBatchStreamLoad(writeRecordRequest);
            long startTime = System.currentTimeMillis();
            long maxIntervalMillis = writeRecordRequest.getMaxInterval() * 1000;
            boolean shouldStop = false;

            // 2. poll record
            while (!shouldStop) {
                long elapsedTime = System.currentTimeMillis() - startTime;
                boolean timeoutReached = maxIntervalMillis > 0 && elapsedTime >= maxIntervalMillis;

                // Timeout protection: force stop if waiting for heartbeat exceeds threshold
                // After reaching maxInterval, wait at most DEBEZIUM_HEARTBEAT_INTERVAL_MS * 3 for
                // heartbeat
                if (timeoutReached
                        && elapsedTime
                                > maxIntervalMillis
                                        + Constants.DEBEZIUM_HEARTBEAT_INTERVAL_MS * 3) {
                    LOG.warn(
                            "Heartbeat wait timeout after {} ms since timeout reached, force stopping. "
                                    + "Total elapsed: {} ms",
                            elapsedTime - maxIntervalMillis,
                            elapsedTime);
                    break;
                }

                Iterator<SourceRecord> recordIterator =
                        sourceReader.pollRecords(readResult.getSplitState());

                if (!recordIterator.hasNext()) {
                    Thread.sleep(100);
                    continue;
                }

                while (recordIterator.hasNext()) {
                    SourceRecord element = recordIterator.next();
                    // Check if this is a heartbeat message
                    if (isHeartbeatEvent(element)) {
                        heartbeatCount++;
                        if (timeoutReached) {
                            LOG.info(
                                    "Max interval reached and heartbeat received, stopping data reading");
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
                            byte[] dataBytes = record.getBytes();
                            scannedBytes += dataBytes.length;
                            batchStreamLoad.writeRecord(database, table, dataBytes);
                        }
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
        metaResponse = extractOffsetMeta(sourceReader, readResult);
        try {
            // 4. wait all stream load finish
            batchStreamLoad.forceFlush();

            // 5. request fe api update offset
            batchStreamLoad.commitOffset(metaResponse, scannedRows, scannedBytes);
        } finally {
            batchStreamLoad.resetTaskId();
        }
    }

    public static boolean isHeartbeatEvent(SourceRecord record) {
        Schema valueSchema = record.valueSchema();
        return valueSchema != null
                && SCHEMA_HEARTBEAT_EVENT_KEY_NAME.equalsIgnoreCase(valueSchema.name());
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
     * offset information and adding the split ID.
     *
     * @param sourceReader the source reader
     * @param readResult the read result containing split and split state
     * @return offset metadata map
     * @throws RuntimeException if split state is null or split type is unknown
     */
    private Map<String, String> extractOffsetMeta(
            SourceReader sourceReader, SplitReadResult readResult) {
        Preconditions.checkNotNull(readResult, "readResult must not be null");

        if (readResult.getSplitState() == null) {
            throw new RuntimeException("split state is null");
        }

        SourceSplit split = readResult.getSplit();
        Map<String, String> offsetRes;

        // Set meta information for hw (high watermark)
        if (sourceReader.isSnapshotSplit(split)) {
            offsetRes = sourceReader.extractSnapshotStateOffset(readResult.getSplitState());
            offsetRes.put(SPLIT_ID, split.splitId());
        } else if (sourceReader.isBinlogSplit(split)) {
            // Set meta for binlog event
            offsetRes = sourceReader.extractBinlogStateOffset(readResult.getSplitState());
            offsetRes.put(SPLIT_ID, BinlogSplit.BINLOG_SPLIT_ID);
        } else {
            throw new RuntimeException("Unknown split type: " + split.getClass().getName());
        }

        return offsetRes;
    }
}
