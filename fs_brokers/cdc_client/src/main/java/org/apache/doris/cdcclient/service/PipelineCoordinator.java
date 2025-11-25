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
import org.apache.doris.cdcclient.model.JobConfig;
import org.apache.doris.cdcclient.model.req.FetchRecordReq;
import org.apache.doris.cdcclient.model.req.WriteRecordReq;
import org.apache.doris.cdcclient.model.resp.RecordWithMeta;
import org.apache.doris.cdcclient.model.resp.WriteMetaResp;
import org.apache.doris.cdcclient.sink.DorisBatchStreamLoad;
import org.apache.doris.cdcclient.source.deserialize.DebeziumJsonDeserializer;
import org.apache.doris.cdcclient.source.deserialize.SourceRecordDeserializer;
import org.apache.doris.cdcclient.source.reader.SourceReader;
import org.apache.doris.cdcclient.source.reader.SplitReadResult;

import io.debezium.data.Envelope;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Pipeline coordinator. */
@Component
public class PipelineCoordinator {
    private static final Logger LOG = LoggerFactory.getLogger(PipelineCoordinator.class);
    private static final String SPLIT_ID = "splitId";
    private static final String PURE_BINLOG_PHASE = "pureBinlogPhase";

    // jobId
    private final Map<Long, DorisBatchStreamLoad> batchStreamLoadMap = new ConcurrentHashMap<>();
    private final SourceRecordDeserializer<SourceRecord, List<String>> serializer;

    public PipelineCoordinator() {
        this.serializer = new DebeziumJsonDeserializer();
    }

    /** Read data from SourceReader and return it with meta information. */
    public RecordWithMeta read(FetchRecordReq recordReq) throws Exception {
        SourceReader<?, ?> reader =
                Env.getCurrentEnv()
                        .getReader(
                                recordReq.getJobId(),
                                recordReq.getDataSource(),
                                recordReq.getConfig());
        return reader.read(recordReq);
    }

    /** Read data from SourceReader and write it to Doris, while returning meta information. */
    public WriteMetaResp readAndWrite(WriteRecordReq writeRecordReq) throws Exception {
        SourceReader<?, ?> sourceReader =
                Env.getCurrentEnv()
                        .getReader(
                                writeRecordReq.getJobId(),
                                writeRecordReq.getDataSource(),
                                writeRecordReq.getConfig());
        JobConfig jobConfig =
                new JobConfig(
                        writeRecordReq.getJobId(),
                        writeRecordReq.getDataSource(),
                        writeRecordReq.getConfig());

        Map<String, String> offsetMeta = writeRecordReq.getMeta();
        WriteMetaResp recordResponse = new WriteMetaResp();

        SplitReadResult<?, ?> readResult = sourceReader.readSplitRecords(writeRecordReq);

        if (readResult == null || readResult.isEmpty()) {
            setDefaultMeta(recordResponse, offsetMeta, readResult);
            return recordResponse;
        }

        DorisBatchStreamLoad batchStreamLoad =
                getOrCreateBatchStreamLoad(writeRecordReq.getJobId());
        boolean readBinlog = readResult.isReadBinlog();
        boolean pureBinlogPhase = readResult.isPureBinlogPhase();

        // 直接使用迭代器，边读边写
        Iterator<SourceRecord> iterator = readResult.getRecordIterator();
        while (iterator != null && iterator.hasNext()) {
            SourceRecord element = iterator.next();
            if (RecordUtils.isDataChangeRecord(element)) {
                List<String> serializedRecords =
                        serializer.deserialize(jobConfig.getConfig(), element);
                if (!CollectionUtils.isEmpty(serializedRecords)) {
                    String database = "doris_cdc"; // doris database
                    String table = extractTable(element);

                    for (String record : serializedRecords) {
                        batchStreamLoad.writeRecord(database, table, record.getBytes());
                    }

                    Map<String, String> lastMeta =
                            RecordUtils.getBinlogPosition(element).getOffset();
                    if (readBinlog && readResult.getSplitId() != null) {
                        lastMeta.put(SPLIT_ID, readResult.getSplitId());
                        lastMeta.put(PURE_BINLOG_PHASE, String.valueOf(pureBinlogPhase));
                    }
                    recordResponse.setMeta(lastMeta);
                }
            }
        }

        // wait stream load finish
        batchStreamLoad.forceFlush();
        return recordResponse;
    }

    private DorisBatchStreamLoad getOrCreateBatchStreamLoad(Long jobId) {
        return batchStreamLoadMap.computeIfAbsent(
                jobId,
                k -> {
                    LOG.info("Create DorisBatchStreamLoad for jobId={}", jobId);
                    return new DorisBatchStreamLoad();
                });
    }

    public void closeJob(Long jobId) {
        DorisBatchStreamLoad batchStreamLoad = batchStreamLoadMap.remove(jobId);
        if (batchStreamLoad != null) {
            LOG.info("Close DorisBatchStreamLoad for jobId={}", jobId);
            batchStreamLoad.close();
        }
    }

    private void setDefaultMeta(
            WriteMetaResp recordResponse,
            Map<String, String> offsetMeta,
            SplitReadResult<?, ?> readResult) {
        if (readResult == null) {
            recordResponse.setMeta(offsetMeta);
            return;
        }

        boolean readBinlog = readResult.isReadBinlog();
        if (readBinlog) {
            Map<String, String> offsetRes;
            if (readResult.getDefaultOffset() != null) {
                offsetRes = new HashMap<>(readResult.getDefaultOffset());
            } else {
                offsetRes = new HashMap<>(offsetMeta);
            }
            if (readResult.getSplitId() != null) {
                offsetRes.put(SPLIT_ID, readResult.getSplitId());
            }
            offsetRes.put(PURE_BINLOG_PHASE, String.valueOf(readResult.isPureBinlogPhase()));
            recordResponse.setMeta(offsetRes);
        } else {
            recordResponse.setMeta(offsetMeta);
        }
    }

    private String extractTable(SourceRecord record) {
        Struct value = (Struct) record.value();
        return value.getStruct(Envelope.FieldName.SOURCE).getString("table");
    }
}
