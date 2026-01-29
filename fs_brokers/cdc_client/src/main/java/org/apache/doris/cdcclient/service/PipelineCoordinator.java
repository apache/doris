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
import org.apache.doris.cdcclient.exception.CommonException;
import org.apache.doris.cdcclient.exception.StreamException;
import org.apache.doris.cdcclient.model.response.RecordWithMeta;
import org.apache.doris.cdcclient.sink.DorisBatchStreamLoad;
import org.apache.doris.cdcclient.source.reader.SourceReader;
import org.apache.doris.cdcclient.source.reader.SplitReadResult;
import org.apache.doris.cdcclient.utils.ConfigUtil;
import org.apache.doris.job.cdc.DataSourceConfigKeys;
import org.apache.doris.job.cdc.request.FetchRecordRequest;
import org.apache.doris.job.cdc.request.WriteRecordRequest;
import org.apache.doris.job.cdc.split.BinlogSplit;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.BufferedOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import io.debezium.data.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

/** Pipeline coordinator. */
@Component
public class PipelineCoordinator {
    private static final Logger LOG = LoggerFactory.getLogger(PipelineCoordinator.class);
    private static final String SPLIT_ID = "splitId";
    // jobId
    private final Map<String, DorisBatchStreamLoad> batchStreamLoadMap = new ConcurrentHashMap<>();
    // taskId -> writeFailReason
    private final Map<String, String> taskErrorMaps = new ConcurrentHashMap<>();
    // taskId, offset
    private final Map<String, Map<String, String>> taskOffsetCache = new ConcurrentHashMap<>();
    private final ThreadPoolExecutor executor;
    private static final int MAX_CONCURRENT_TASKS = 10;
    private static final int QUEUE_CAPACITY = 128;
    private static ObjectMapper objectMapper = new ObjectMapper();
    private final byte[] LINE_DELIMITER = "\n".getBytes(StandardCharsets.UTF_8);

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

    /** return data for http_file_reader */
    public StreamingResponseBody fetchRecordStream(FetchRecordRequest fetchReq) throws Exception {
        SourceReader sourceReader;
        SplitReadResult readResult;
        try {
            if (fetchReq.getTaskId() == null && fetchReq.getMeta() == null) {
                LOG.info(
                        "Generate initial meta for fetch record request, jobId={}, taskId={}",
                        fetchReq.getJobId(),
                        fetchReq.getTaskId());
                // means the request did not originate from the job, only tvf
                Map<String, Object> meta = generateMeta(fetchReq.getConfig());
                fetchReq.setMeta(meta);
            }

            sourceReader = Env.getCurrentEnv().getReader(fetchReq);
            readResult = sourceReader.readSplitRecords(fetchReq);
        } catch (Exception ex) {
            throw new CommonException(ex);
        }

        return outputStream -> {
            try {
                buildRecords(sourceReader, fetchReq, readResult, outputStream);
            } catch (Exception ex) {
                LOG.error(
                        "Failed fetch record, jobId={}, taskId={}",
                        fetchReq.getJobId(),
                        fetchReq.getTaskId(),
                        ex);
                throw new StreamException(ex);
            }
        };
    }

    private void buildRecords(
            SourceReader sourceReader,
            FetchRecordRequest fetchRecord,
            SplitReadResult readResult,
            OutputStream rawOutputStream)
            throws Exception {
        SourceSplit split = readResult.getSplit();
        Map<String, String> lastMeta = null;
        int rowCount = 0;
        BufferedOutputStream bos = new BufferedOutputStream(rawOutputStream);
        try {
            // Serialize records and add them to the response (collect from iterator)
            Iterator<SourceRecord> iterator = readResult.getRecordIterator();
            while (iterator != null && iterator.hasNext()) {
                SourceRecord element = iterator.next();
                List<String> serializedRecords =
                        sourceReader.deserialize(fetchRecord.getConfig(), element);
                for (String record : serializedRecords) {
                    bos.write(record.getBytes(StandardCharsets.UTF_8));
                    bos.write(LINE_DELIMITER);
                }
                rowCount += serializedRecords.size();
            }
            // force flush buffer
            bos.flush();
        } finally {
            // The LSN in the commit is the current offset, which is the offset from the last
            // successful write.
            // Therefore, even if a subsequent write fails, it will not affect the commit.
            sourceReader.commitSourceOffset(fetchRecord.getJobId(), readResult.getSplit());

            // This must be called after commitSourceOffset; otherwise,
            // PG's confirmed lsn will not proceed.
            sourceReader.finishSplitRecords();
        }

        LOG.info(
                "Fetch records completed, jobId={}, taskId={}, splitId={}, rowCount={}",
                fetchRecord.getJobId(),
                fetchRecord.getTaskId(),
                split.splitId(),
                rowCount);

        if (readResult.getSplitState() != null) {
            // Set meta information for hw
            if (sourceReader.isSnapshotSplit(split)) {
                lastMeta = sourceReader.extractSnapshotStateOffset(readResult.getSplitState());
                lastMeta.put(SPLIT_ID, split.splitId());
            }

            // set meta for binlog event
            if (sourceReader.isBinlogSplit(split)) {
                lastMeta = sourceReader.extractBinlogStateOffset(readResult.getSplitState());
                lastMeta.put(SPLIT_ID, BinlogSplit.BINLOG_SPLIT_ID);
            }
        } else {
            throw new RuntimeException("split state is null");
        }

        if (StringUtils.isNotEmpty(fetchRecord.getTaskId())) {
            taskOffsetCache.put(fetchRecord.getTaskId(), lastMeta);
        }

        if (!isLong(fetchRecord.getJobId())) {
            // TVF requires closing the window after each execution,
            // while PG requires dropping the slot.
            sourceReader.close(fetchRecord);
        }
    }

    private boolean isLong(String s) {
        if (s == null || s.isEmpty()) return false;
        try {
            Long.parseLong(s);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /**
     * Generate split meta from request.offset. This only applies to TVF, so initial is not
     * supported because initial requires a job to obtain split information.
     */
    private Map<String, Object> generateMeta(Map<String, String> cdcConfig)
            throws JsonProcessingException {
        Map<String, Object> meta = new HashMap<>();
        String offset = cdcConfig.get(DataSourceConfigKeys.OFFSET);
        if (DataSourceConfigKeys.OFFSET_LATEST.equalsIgnoreCase(offset)
                || DataSourceConfigKeys.OFFSET_EARLIEST.equalsIgnoreCase(offset)) {
            meta.put(SPLIT_ID, BinlogSplit.BINLOG_SPLIT_ID);
        } else if (ConfigUtil.isJson(offset)) {
            Map<String, String> startOffset =
                    objectMapper.readValue(offset, new TypeReference<>() {});
            meta.put(SPLIT_ID, BinlogSplit.BINLOG_SPLIT_ID);
            meta.put("startingOffset", startOffset);
        } else {
            throw new RuntimeException("Unsupported offset: " + offset);
        }
        return meta;
    }

    /** pull data from api for test */
    public RecordWithMeta fetchRecords(FetchRecordRequest fetchRecordRequest) throws Exception {
        SourceReader sourceReader = Env.getCurrentEnv().getReader(fetchRecordRequest);
        SplitReadResult readResult = sourceReader.readSplitRecords(fetchRecordRequest);
        return buildRecordResponse(sourceReader, fetchRecordRequest, readResult);
    }

    /** build RecordWithMeta for test */
    private RecordWithMeta buildRecordResponse(
            SourceReader sourceReader, FetchRecordRequest fetchRecord, SplitReadResult readResult)
            throws Exception {
        RecordWithMeta recordResponse = new RecordWithMeta();
        SourceSplit split = readResult.getSplit();
        try {
            // Serialize records and add them to the response (collect from iterator)
            Iterator<SourceRecord> iterator = readResult.getRecordIterator();
            while (iterator != null && iterator.hasNext()) {
                SourceRecord element = iterator.next();
                List<String> serializedRecords =
                        sourceReader.deserialize(fetchRecord.getConfig(), element);
                if (!CollectionUtils.isEmpty(serializedRecords)) {
                    recordResponse.getRecords().addAll(serializedRecords);
                }
            }
        } finally {
            // The LSN in the commit is the current offset, which is the offset from the last
            // successful write.
            // Therefore, even if a subsequent write fails, it will not affect the commit.
            sourceReader.commitSourceOffset(fetchRecord.getJobId(), readResult.getSplit());

            // This must be called after commitSourceOffset; otherwise,
            // PG's confirmed lsn will not proceed.
            sourceReader.finishSplitRecords();
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
        } finally {
            if (readResult != null) {
                // The LSN in the commit is the current offset, which is the offset from the last
                // successful write.
                // Therefore, even if a subsequent write fails, it will not affect the commit.
                sourceReader.commitSourceOffset(
                        writeRecordRequest.getJobId(), readResult.getSplit());
            }

            // This must be called after commitSourceOffset; otherwise,
            // PG's confirmed lsn will not proceed.
            // This operation must be performed before batchStreamLoad.commitOffset;
            // otherwise, fe might issue the next task for this job.
            sourceReader.finishSplitRecords();
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
        } finally {
            batchStreamLoad.resetTaskId();
        }
    }

    private DorisBatchStreamLoad getOrCreateBatchStreamLoad(String jobId, String targetDb) {
        return batchStreamLoadMap.computeIfAbsent(
                jobId,
                k -> {
                    LOG.info("Create DorisBatchStreamLoad for jobId={}", jobId);
                    return new DorisBatchStreamLoad(jobId, targetDb);
                });
    }

    public void closeJobStreamLoad(String jobId) {
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

    public Map<String, String> getOffsetWithTaskId(String taskId) {
        Map<String, String> taskOffset = taskOffsetCache.remove(taskId);
        return taskOffset == null ? new HashMap<>() : taskOffset;
    }
}
