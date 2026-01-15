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
        SplitReadResult readResult = sourceReader.readSplitRecords(fetchRecordRequest);
        return buildRecordResponse(sourceReader, fetchRecordRequest, readResult);
    }

    /** build RecordWithMeta */
    private RecordWithMeta buildRecordResponse(
            SourceReader sourceReader, FetchRecordRequest fetchRecord, SplitReadResult readResult)
            throws Exception {
        RecordWithMeta recordResponse = new RecordWithMeta();
        SourceSplit split = readResult.getSplit();
        int count = 0;
        try {
            // Serialize records and add them to the response (collect from iterator)
            Iterator<SourceRecord> iterator = readResult.getRecordIterator();
            while (iterator != null && iterator.hasNext()) {
                SourceRecord element = iterator.next();
                List<String> serializedRecords =
                        sourceReader.deserialize(fetchRecord.getConfig(), element);
                if (!CollectionUtils.isEmpty(serializedRecords)) {
                    recordResponse.getRecords().addAll(serializedRecords);
                    count += serializedRecords.size();
                    if (sourceReader.isBinlogSplit(split)) {
                        // put offset for event
                        Map<String, String> lastMeta =
                                sourceReader.extractBinlogStateOffset(readResult.getSplitState());
                        lastMeta.put(SPLIT_ID, BinlogSplit.BINLOG_SPLIT_ID);
                        recordResponse.setMeta(lastMeta);
                    }
                }
            }

            if (readResult.getSplitState() != null) {
                // Set meta information for hw
                if (sourceReader.isSnapshotSplit(split)) {
                    Map<String, String> offsetRes =
                            sourceReader.extractSnapshotStateOffset(readResult.getSplitState());
                    offsetRes.put(SPLIT_ID, split.splitId());
                    recordResponse.setMeta(offsetRes);
                }

                // set meta for binlog event
                if (sourceReader.isBinlogSplit(split)) {
                    Map<String, String> offsetRes =
                            sourceReader.extractBinlogStateOffset(readResult.getSplitState());
                    offsetRes.put(SPLIT_ID, BinlogSplit.BINLOG_SPLIT_ID);
                    recordResponse.setMeta(offsetRes);
                }
            } else {
                throw new RuntimeException("split state is null");
            }

            sourceReader.commitSourceOffset(fetchRecord.getJobId(), readResult.getSplit());
            return recordResponse;
        } finally {
            // This must be called after commitSourceOffset; otherwise, PG's confirmed lsn will not
            // proceed.
            if (sourceReader != null) {
                sourceReader.finishSplitRecords();
            }
        }
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

    /** Read data from SourceReader and write it to Doris, while returning meta information. */
    public void writeRecords(WriteRecordRequest writeRecordRequest) throws Exception {
        SourceReader sourceReader = Env.getCurrentEnv().getReader(writeRecordRequest);
        DorisBatchStreamLoad batchStreamLoad = null;
        Map<String, String> metaResponse = new HashMap<>();
        long scannedRows = 0L;
        long scannedBytes = 0L;
        SplitReadResult readResult = null;
        try {
            readResult = sourceReader.readSplitRecords(writeRecordRequest);
            batchStreamLoad =
                    getOrCreateBatchStreamLoad(
                            writeRecordRequest.getJobId(), writeRecordRequest.getTargetDb());
            batchStreamLoad.setCurrentTaskId(writeRecordRequest.getTaskId());
            batchStreamLoad.setFrontendAddress(writeRecordRequest.getFrontendAddress());
            batchStreamLoad.setToken(writeRecordRequest.getToken());

            // Record start time for maxInterval check
            long startTime = System.currentTimeMillis();
            long maxIntervalMillis = writeRecordRequest.getMaxInterval() * 1000;

            // Use iterators to read and write.
            Iterator<SourceRecord> iterator = readResult.getRecordIterator();
            while (iterator != null && iterator.hasNext()) {
                SourceRecord element = iterator.next();
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
                // Check if maxInterval has been exceeded
                long elapsedTime = System.currentTimeMillis() - startTime;
                if (maxIntervalMillis > 0 && elapsedTime >= maxIntervalMillis) {
                    LOG.info(
                            "Max interval {} seconds reached, stopping data reading",
                            writeRecordRequest.getMaxInterval());
                    break;
                }
            }

            // get offset from split state
            try {
                if (readResult.getSplitState() != null) {
                    // Set meta information for hw
                    if (sourceReader.isSnapshotSplit(readResult.getSplit())) {
                        Map<String, String> offsetRes =
                                sourceReader.extractSnapshotStateOffset(readResult.getSplitState());
                        offsetRes.put(SPLIT_ID, readResult.getSplit().splitId());
                        metaResponse = offsetRes;
                    }

                    // set meta for binlog event
                    if (sourceReader.isBinlogSplit(readResult.getSplit())) {
                        Map<String, String> offsetRes =
                                sourceReader.extractBinlogStateOffset(readResult.getSplitState());
                        offsetRes.put(SPLIT_ID, BinlogSplit.BINLOG_SPLIT_ID);
                        metaResponse = offsetRes;
                    }
                } else {
                    throw new RuntimeException("split state is null");
                }

                // wait all stream load finish
                batchStreamLoad.forceFlush();

                // request fe api
                batchStreamLoad.commitOffset(metaResponse, scannedRows, scannedBytes);

                // commit source offset if need
                sourceReader.commitSourceOffset(
                        writeRecordRequest.getJobId(), readResult.getSplit());
            } finally {
                batchStreamLoad.resetTaskId();
            }
        } finally {
            // This must be called after commitSourceOffset; otherwise, PG's confirmed lsn will not
            // proceed.
            if (sourceReader != null) {
                sourceReader.finishSplitRecords();
            }
        }
    }

    private DorisBatchStreamLoad getOrCreateBatchStreamLoad(Long jobId, String targetDb) {
        return batchStreamLoadMap.computeIfAbsent(
                jobId,
                k -> {
                    LOG.info("Create DorisBatchStreamLoad for jobId={}", jobId);
                    return new DorisBatchStreamLoad(jobId, targetDb);
                });
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
}
