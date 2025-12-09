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
import org.apache.doris.cdcclient.exception.StreamLoadException;
import org.apache.doris.cdcclient.model.request.WriteRecordReq;
import org.apache.doris.cdcclient.sink.DorisBatchStreamLoad;
import org.apache.doris.cdcclient.source.deserialize.DebeziumJsonDeserializer;
import org.apache.doris.cdcclient.source.deserialize.SourceRecordDeserializer;
import org.apache.doris.cdcclient.source.reader.SourceReader;
import org.apache.doris.cdcclient.source.reader.SplitReadResult;

import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils;
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
    private static final String PURE_BINLOG_PHASE = "pureBinlogPhase";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    // jobId
    private final Map<Long, DorisBatchStreamLoad> batchStreamLoadMap = new ConcurrentHashMap<>();
    private final SourceRecordDeserializer<SourceRecord, List<String>> serializer;
    private final ThreadPoolExecutor executor;
    private static final int MAX_CONCURRENT_TASKS = 10;
    private static final int QUEUE_CAPACITY = 128;

    public PipelineCoordinator() {
        this.serializer = new DebeziumJsonDeserializer();
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

    public CompletableFuture<Void> writeRecordsAsync(WriteRecordReq writeRecordReq) {
        Preconditions.checkNotNull(writeRecordReq.getToken(), "token must not be null");
        Preconditions.checkNotNull(writeRecordReq.getTaskId(), "taskId must not be null");
        Preconditions.checkNotNull(writeRecordReq.getTargetDb(), "targetDb must not be null");
        return CompletableFuture.runAsync(
                () -> {
                    try {
                        LOG.info(
                                "Start processing async write record, jobId={} taskId={}",
                                writeRecordReq.getJobId(),
                                writeRecordReq.getTaskId());
                        writeRecords(writeRecordReq);
                        LOG.info(
                                "Successfully processed async write record, jobId={} taskId={}",
                                writeRecordReq.getJobId(),
                                writeRecordReq.getTaskId());
                    } catch (Exception ex) {
                        LOG.error(
                                "Failed to process async write record, jobId={} taskId={}",
                                writeRecordReq.getJobId(),
                                writeRecordReq.getTaskId(),
                                ex);
                    }
                },
                executor);
    }

    /** Read data from SourceReader and write it to Doris, while returning meta information. */
    public void writeRecords(WriteRecordReq writeRecordReq) throws Exception {
        SourceReader<?, ?> sourceReader = Env.getCurrentEnv().getReader(writeRecordReq);
        DorisBatchStreamLoad batchStreamLoad = null;
        Map<String, String> metaResponse = new HashMap<>();
        try {
            SplitReadResult<?, ?> readResult = sourceReader.readSplitRecords(writeRecordReq);
            batchStreamLoad =
                    getOrCreateBatchStreamLoad(
                            writeRecordReq.getJobId(), writeRecordReq.getTargetDb());
            batchStreamLoad.setCurrentTaskId(writeRecordReq.getTaskId());
            batchStreamLoad.setFrontendAddress(writeRecordReq.getFrontendAddress());
            batchStreamLoad.setToken(writeRecordReq.getToken());
            boolean readBinlog = readResult.isReadBinlog();
            boolean pureBinlogPhase = readResult.isPureBinlogPhase();

            boolean hasData = false;
            // Record start time for maxInterval check
            long startTime = System.currentTimeMillis();
            long maxIntervalMillis = writeRecordReq.getMaxInterval() * 1000;

            long scannedRows = 0L;
            long scannedBytes = 0L;
            // Use iterators to read and write.
            Iterator<SourceRecord> iterator = readResult.getRecordIterator();
            while (iterator != null && iterator.hasNext()) {
                SourceRecord element = iterator.next();
                if (RecordUtils.isDataChangeRecord(element)) {
                    List<String> serializedRecords =
                            serializer.deserialize(writeRecordReq.getConfig(), element);
                    if (!CollectionUtils.isEmpty(serializedRecords)) {
                        String database = writeRecordReq.getTargetDb();
                        String table = extractTable(element);
                        hasData = true;
                        for (String record : serializedRecords) {
                            scannedRows++;
                            byte[] dataBytes = record.getBytes();
                            scannedBytes += dataBytes.length;
                            batchStreamLoad.writeRecord(database, table, dataBytes);
                        }

                        Map<String, String> lastMeta =
                                RecordUtils.getBinlogPosition(element).getOffset();
                        if (readBinlog && sourceReader.getSplitId(readResult.getSplit()) != null) {
                            lastMeta.put(SPLIT_ID, sourceReader.getSplitId(readResult.getSplit()));
                            // lastMeta.put(PURE_BINLOG_PHASE, String.valueOf(pureBinlogPhase));
                        }
                        metaResponse = lastMeta;
                    }
                } else {
                    LOG.info("Skip non-data record: {}", element.valueSchema());
                }
                // Check if maxInterval has been exceeded
                long elapsedTime = System.currentTimeMillis() - startTime;
                if (maxIntervalMillis > 0 && elapsedTime >= maxIntervalMillis) {
                    LOG.info(
                            "Max interval {} seconds reached, stopping data reading",
                            writeRecordReq.getMaxInterval());
                    break;
                }
            }
            if (!hasData) {
                // todo: need return the lastest heartbeat offset, means the maximum offset that the
                // current job can recover.
                if (readBinlog) {
                    // BinlogSplit binlogSplit =
                    //         OBJECT_MAPPER.convertValue(writeRecordReq.getMeta(),
                    // BinlogSplit.class);
                    // Map<String, String> metaResp = new
                    // HashMap<>(binlogSplit.getStartingOffset());
                    // metaResp.put(SPLIT_ID, BinlogSplit.BINLOG_SPLIT_ID);
                    Map<String, String> offsetRes =
                            sourceReader.extractBinlogOffset(readResult.getSplit());
                    batchStreamLoad.commitOffset(offsetRes, scannedRows, scannedBytes);
                    return;
                } else {
                    throw new RuntimeException("should not happen");
                }
            }

            // wait all stream load finish
            batchStreamLoad.forceFlush();
            // update offset meta
            if (!readBinlog) {
                Map<String, String> offsetRes =
                        sourceReader.extractSnapshotOffset(
                                readResult.getSplitState(), readResult.getSplit());
                if (offsetRes == null) {
                    // should not happen
                    throw new StreamLoadException(
                            "Chunk data cannot be obtained from highWatermark.");
                }
                metaResponse = offsetRes;
            }
            // request fe api
            batchStreamLoad.commitOffset(metaResponse, scannedRows, scannedBytes);
        } finally {
            sourceReader.finishSplitRecords();
            if (batchStreamLoad != null) {
                batchStreamLoad.resetTaskId();
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

    public void closeJob(Long jobId) {
        DorisBatchStreamLoad batchStreamLoad = batchStreamLoadMap.remove(jobId);
        if (batchStreamLoad != null) {
            LOG.info("Close DorisBatchStreamLoad for jobId={}", jobId);
            batchStreamLoad.close();
        }
    }

    private String extractTable(SourceRecord record) {
        Struct value = (Struct) record.value();
        return value.getStruct(Envelope.FieldName.SOURCE).getString("table");
    }
}
