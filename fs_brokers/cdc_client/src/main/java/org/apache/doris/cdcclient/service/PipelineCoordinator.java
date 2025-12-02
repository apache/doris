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

import static org.apache.flink.cdc.connectors.mysql.source.assigners.MySqlBinlogSplitAssigner.BINLOG_SPLIT_ID;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.data.Envelope;
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
import org.apache.commons.collections.CollectionUtils;
import org.apache.doris.cdcclient.common.Env;
import org.apache.doris.cdcclient.model.request.WriteRecordReq;
import org.apache.doris.cdcclient.model.response.WriteMetaResp;
import org.apache.doris.cdcclient.sink.DorisBatchStreamLoad;
import org.apache.doris.cdcclient.source.deserialize.DebeziumJsonDeserializer;
import org.apache.doris.cdcclient.source.deserialize.SourceRecordDeserializer;
import org.apache.doris.cdcclient.source.reader.SourceReader;
import org.apache.doris.cdcclient.source.reader.SplitReadResult;
import org.apache.doris.cdcclient.utils.HttpUtil;
import org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
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
        return CompletableFuture.runAsync(
                () -> {
                    WriteMetaResp response = null;
                    Exception error = null;

                    try {
                        LOG.info(
                                "Start processing async write record, jobId={}",
                                writeRecordReq.getJobId());
                        response = writeRecords(writeRecordReq);
                        LOG.info(
                                "Successfully processed async write record, jobId={}",
                                writeRecordReq.getJobId());

                    } catch (Exception ex) {
                        LOG.error(
                                "Failed to process async write record, jobId={}",
                                writeRecordReq.getJobId(),
                                ex);
                        error = ex;
                    } finally {
                        // commitTransaction(writeRecordReq.getJobId(), response, error);
                    }
                },
                executor);
    }

    /** commit transaction. */
    private void commitTransaction(Long jobId, WriteMetaResp response, Exception error) {
        try {
            // 序列化为JSON
            String jsonBody = OBJECT_MAPPER.writeValueAsString("");
            HttpPost httpPost = new HttpPost("url");
            httpPost.setHeader(HttpHeaders.CONTENT_TYPE, "application/json; charset=UTF-8");
            httpPost.setEntity(new StringEntity(jsonBody, StandardCharsets.UTF_8));

            LOG.info("Calling callback URL: {}, jobId={}", "url", jobId);

            try (CloseableHttpClient httpClient = HttpUtil.getHttpClient()) {
                try (CloseableHttpResponse httpResponse = httpClient.execute(httpPost)) {
                    int statusCode = httpResponse.getStatusLine().getStatusCode();
                    String responseBody =
                            httpResponse.getEntity() != null
                                    ? EntityUtils.toString(httpResponse.getEntity())
                                    : "";

                    LOG.info(
                            "Callback completed, jobId={}, statusCode={}, response={}",
                            jobId,
                            statusCode,
                            responseBody);

                    if (statusCode < 200 || statusCode >= 300) {
                        LOG.warn(
                                "Callback returned non-2xx status, jobId={}, statusCode={}",
                                jobId,
                                statusCode);
                    }
                }
            }

        } catch (Exception ex) {
            LOG.error("Failed to call callback URL: {}, jobId={}", "", jobId, ex);
        }
    }

    /** Read data from SourceReader and write it to Doris, while returning meta information. */
    public WriteMetaResp writeRecords(WriteRecordReq writeRecordReq) throws Exception {
        SourceReader<?, ?> sourceReader = Env.getCurrentEnv().getReader(writeRecordReq);
        WriteMetaResp recordResponse = new WriteMetaResp();
        SplitReadResult<?, ?> readResult = sourceReader.readSplitRecords(writeRecordReq);

        DorisBatchStreamLoad batchStreamLoad =
                getOrCreateBatchStreamLoad(writeRecordReq.getJobId());
        boolean readBinlog = readResult.isReadBinlog();
        boolean pureBinlogPhase = readResult.isPureBinlogPhase();

        boolean hasData = false;
        // Record start time for maxInterval check
        long startTime = System.currentTimeMillis();
        long maxIntervalMillis = writeRecordReq.getMaxInterval() * 1000;

        // Use iterators to read and write.
        Iterator<SourceRecord> iterator = readResult.getRecordIterator();
        while (iterator != null && iterator.hasNext()) {
            SourceRecord element = iterator.next();
            if (RecordUtils.isDataChangeRecord(element)) {
                List<String> serializedRecords =
                        serializer.deserialize(writeRecordReq.getConfig(), element);
                if (!CollectionUtils.isEmpty(serializedRecords)) {
                    String database = writeRecordReq.getTargetDatabase();
                    String table = extractTable(element);
                    hasData = true;
                    for (String record : serializedRecords) {
                        batchStreamLoad.writeRecord(database, table, record.getBytes());
                    }

                    Map<String, String> lastMeta =
                            RecordUtils.getBinlogPosition(element).getOffset();
                    if (readBinlog && sourceReader.getSplitId(readResult.getSplit()) != null) {
                        lastMeta.put(SPLIT_ID, sourceReader.getSplitId(readResult.getSplit()));
                        lastMeta.put(PURE_BINLOG_PHASE, String.valueOf(pureBinlogPhase));
                    }
                    recordResponse.setMeta(lastMeta);
                }
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
        if (hasData) {
            // wait all stream load finish
            batchStreamLoad.forceFlush();
        }

        sourceReader.finishSplitRecords();
        // update offset meta
        if (!readBinlog) {
            Map<String, String> offsetRes =
                    sourceReader.extractSnapshotOffset(
                            readResult.getSplitState(), readResult.getSplit());
            if (offsetRes != null) {
                recordResponse.setMeta(offsetRes);
            }
        }

        if (!hasData) {
            if (readBinlog) {
                Map<String, String> offsetRes =
                        sourceReader.extractBinlogOffset(readResult.getSplit(), pureBinlogPhase);
                if (offsetRes != null) {
                    recordResponse.setMeta(offsetRes);
                } else {
                    // Fallback to request meta if extraction fails
                    Map<String, String> fallbackOffset = new HashMap<>(writeRecordReq.getMeta());
                    fallbackOffset.put(SPLIT_ID, BINLOG_SPLIT_ID);
                    fallbackOffset.put(PURE_BINLOG_PHASE, String.valueOf(pureBinlogPhase));
                    recordResponse.setMeta(fallbackOffset);
                }
            } else {
                recordResponse.setMeta(writeRecordReq.getMeta());
            }
        }
        return recordResponse;
    }

    private DorisBatchStreamLoad getOrCreateBatchStreamLoad(Long jobId) {
        return batchStreamLoadMap.computeIfAbsent(
                jobId,
                k -> {
                    LOG.info("Create DorisBatchStreamLoad for jobId={}", jobId);
                    return new DorisBatchStreamLoad(jobId);
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
