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

package org.apache.doris.cdcclient.itcase;

import org.apache.doris.cdcclient.common.Env;
import org.apache.doris.cdcclient.service.PipelineCoordinator;
import org.apache.doris.cdcclient.source.reader.SourceReader;
import org.apache.doris.job.cdc.DataSourceConfigKeys;
import org.apache.doris.job.cdc.request.FetchRecordRequest;
import org.apache.doris.job.cdc.request.FetchTableSplitsRequest;
import org.apache.doris.job.cdc.request.JobBaseConfig;
import org.apache.doris.job.cdc.split.AbstractSourceSplit;
import org.apache.doris.job.cdc.split.BinlogSplit;
import org.apache.doris.job.cdc.split.SnapshotSplit;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

/**
 * Test-only driver that exercises the real cdc_client TVF read path against a containerized
 * database, mirroring the production sequence used by the BE HttpScanner:
 *
 * <ol>
 *   <li>{@code /api/fetchSplits} -> {@link SourceReader#getSourceSplits} computes the chunk
 *       boundaries (flink-cdc ChunkSplitter). See {@link #fetchAllSnapshotSplits}.
 *   <li>{@code /api/fetchRecordStream} -> {@link PipelineCoordinator#fetchRecordStream} streams the
 *       deserialized JSON rows; the per-task offset is published into the coordinator's task-offset
 *       cache. See {@link #readSnapshot} / {@link #readBinlogUntil}.
 *   <li>{@code /api/getTaskOffset} -> {@link PipelineCoordinator#getOffsetWithTaskId} returns that
 *       offset.
 * </ol>
 *
 * <p>A numeric (Long) jobId is used so the reader is kept across snapshot and binlog calls,
 * matching the job-driven TVF path that performs snapshot + incremental.
 *
 * <p>The job context (and its open connections) lives in the process-wide {@link Env} singleton, so
 * every harness MUST be {@link #close() closed} (use try-with-resources) and every test MUST use a
 * unique {@code jobId} to stay isolated.
 */
public final class CdcClientReadHarness implements AutoCloseable {

    private static final int SPLIT_BATCH_SIZE = 1000;

    private final PipelineCoordinator coordinator = new PipelineCoordinator(10);
    private final String jobId;
    private final String dataSource;
    private final Map<String, String> config;
    private final AtomicLong taskSeq = new AtomicLong();

    private Map<String, String> lastBinlogOffset;

    private CdcClientReadHarness(String jobId, String dataSource, Map<String, String> config) {
        this.jobId = jobId;
        this.dataSource = dataSource;
        this.config = config;
    }

    /** Build a harness for a MySQL source backed by a Testcontainers instance. */
    public static CdcClientReadHarness mysql(
            String jobId,
            String host,
            int port,
            String user,
            String password,
            String database,
            String includeTables,
            String offset) {
        Map<String, String> config = new HashMap<>();
        config.put(
                DataSourceConfigKeys.JDBC_URL,
                "jdbc:mysql://" + host + ":" + port + "/" + database + "?serverTimezone=UTC");
        config.put(DataSourceConfigKeys.USER, user);
        config.put(DataSourceConfigKeys.PASSWORD, password);
        config.put(DataSourceConfigKeys.DATABASE, database);
        config.put(DataSourceConfigKeys.INCLUDE_TABLES, includeTables);
        config.put(DataSourceConfigKeys.OFFSET, offset);
        config.put(DataSourceConfigKeys.SNAPSHOT_PARALLELISM, "1");
        return new CdcClientReadHarness(jobId, "MYSQL", config);
    }

    /** Build a harness for a PostgreSQL source backed by a Testcontainers instance. */
    public static CdcClientReadHarness postgres(
            String jobId,
            String host,
            int port,
            String user,
            String password,
            String database,
            String schema,
            String includeTables,
            String offset) {
        Map<String, String> config = new HashMap<>();
        config.put(
                DataSourceConfigKeys.JDBC_URL,
                "jdbc:postgresql://" + host + ":" + port + "/" + database);
        config.put(DataSourceConfigKeys.USER, user);
        config.put(DataSourceConfigKeys.PASSWORD, password);
        config.put(DataSourceConfigKeys.DATABASE, database);
        config.put(DataSourceConfigKeys.SCHEMA, schema);
        config.put(DataSourceConfigKeys.INCLUDE_TABLES, includeTables);
        config.put(DataSourceConfigKeys.OFFSET, offset);
        config.put(DataSourceConfigKeys.SNAPSHOT_PARALLELISM, "1");
        // FE-assigned names so the reader owns and pre-creates the slot/publication.
        config.put(DataSourceConfigKeys.SLOT_NAME, DataSourceConfigKeys.defaultSlotName(jobId));
        config.put(
                DataSourceConfigKeys.PUBLICATION_NAME,
                DataSourceConfigKeys.defaultPublicationName(jobId));
        return new CdcClientReadHarness(jobId, "POSTGRES", config);
    }

    public String jobId() {
        return jobId;
    }

    public Map<String, String> lastBinlogOffset() {
        return lastBinlogOffset;
    }

    private JobBaseConfig baseConfig() {
        return new JobBaseConfig(jobId, dataSource, config, null);
    }

    /**
     * Compute every snapshot chunk for one table by advancing the FE-style cursor (nextSplitStart /
     * nextSplitId) until the final chunk (the one whose splitEnd is null) is reached.
     */
    public List<SnapshotSplit> fetchAllSnapshotSplits(String table) {
        SourceReader reader = Env.getCurrentEnv().getReader(baseConfig());
        List<SnapshotSplit> all = new ArrayList<>();
        Object[] nextSplitStart = null;
        Integer nextSplitId = null;
        while (true) {
            FetchTableSplitsRequest req = new FetchTableSplitsRequest();
            req.setJobId(jobId);
            req.setDataSource(dataSource);
            req.setConfig(config);
            req.setSnapshotTable(table);
            req.setNextSplitStart(nextSplitStart);
            req.setNextSplitId(nextSplitId);
            req.setBatchSize(SPLIT_BATCH_SIZE);

            List<AbstractSourceSplit> batch = reader.getSourceSplits(req);
            if (batch == null || batch.isEmpty()) {
                break;
            }
            for (AbstractSourceSplit split : batch) {
                all.add((SnapshotSplit) split);
            }
            SnapshotSplit last = (SnapshotSplit) batch.get(batch.size() - 1);
            // The last chunk has an open upper bound (splitEnd == null).
            if (last.getSplitEnd() == null) {
                break;
            }
            nextSplitStart = last.getSplitEnd();
            nextSplitId = all.size();
        }
        return all;
    }

    /** Read the full snapshot for the given splits in one blocking stream. */
    public SnapshotResult readSnapshot(List<SnapshotSplit> splits) throws Exception {
        List<Map<String, Object>> splitMaps = new ArrayList<>();
        for (SnapshotSplit split : splits) {
            splitMaps.add(toMap(split));
        }
        Map<String, Object> meta = new HashMap<>();
        meta.put("splits", splitMaps);

        String taskId = nextTaskId();
        List<String> records = streamRecords(meta, taskId);
        return new SnapshotResult(records, coordinator.getOffsetWithTaskId(taskId));
    }

    /** Convenience: compute splits for a single table and read its full snapshot. */
    public SnapshotResult readFullSnapshot(String table) throws Exception {
        return readSnapshot(fetchAllSnapshotSplits(table));
    }

    /**
     * Enter the binlog phase from a completed snapshot and poll until {@code expectedCount} rows
     * arrive or {@code timeout} elapses.
     */
    public List<String> readBinlogUntil(
            SnapshotResult snapshot,
            List<SnapshotSplit> snapshotSplits,
            int expectedCount,
            Duration timeout)
            throws Exception {
        Map<String, Map<String, String>> hwBySplitId = new HashMap<>();
        for (Map<String, String> offset : snapshot.splitOffsets()) {
            Map<String, String> hw = new HashMap<>(offset);
            String splitId = hw.remove(SourceReader.SPLIT_ID);
            hwBySplitId.put(splitId, hw);
        }

        List<Map<String, Object>> finished = new ArrayList<>();
        for (SnapshotSplit split : snapshotSplits) {
            Map<String, Object> splitMap = toMap(split);
            splitMap.put("highWatermark", hwBySplitId.get(split.getSplitId()));
            finished.add(splitMap);
        }

        Map<String, Object> meta = new HashMap<>();
        meta.put(SourceReader.SPLIT_ID, BinlogSplit.BINLOG_SPLIT_ID);
        meta.put("finishedSplits", finished);
        return pollBinlog(meta, expectedCount, timeout);
    }

    /** Continue the binlog phase from a previously returned binlog offset. */
    public List<String> readBinlogFromOffset(
            Map<String, String> startingOffset, int expectedCount, Duration timeout)
            throws Exception {
        return pollBinlog(binlogMeta(startingOffset), expectedCount, timeout);
    }

    private List<String> pollBinlog(
            Map<String, Object> firstMeta, int expectedCount, Duration timeout) throws Exception {
        List<String> records = new ArrayList<>();
        Map<String, Object> meta = firstMeta;
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        while (records.size() < expectedCount && System.nanoTime() < deadlineNanos) {
            String taskId = nextTaskId();
            records.addAll(streamRecords(meta, taskId));

            List<Map<String, String>> offsetMeta = coordinator.getOffsetWithTaskId(taskId);
            Map<String, String> offset = new HashMap<>(offsetMeta.get(0));
            offset.remove(SourceReader.SPLIT_ID);
            this.lastBinlogOffset = offset;
            meta = binlogMeta(offset);
        }
        return records;
    }

    /** Invoke the production stream endpoint and collect the newline-delimited JSON records. */
    private List<String> streamRecords(Map<String, Object> meta, String taskId) throws Exception {
        FetchRecordRequest req = new FetchRecordRequest();
        req.setJobId(jobId);
        req.setDataSource(dataSource);
        req.setConfig(config);
        req.setMeta(meta);
        req.setTaskId(taskId);

        StreamingResponseBody body = coordinator.fetchRecordStream(req);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        body.writeTo(out);

        List<String> records = new ArrayList<>();
        String payload = out.toString(StandardCharsets.UTF_8);
        for (String line : payload.split("\n")) {
            if (!line.isEmpty()) {
                records.add(line);
            }
        }
        return records;
    }

    /**
     * Serialize a split into the map shape cdc_client expects in the request meta. Built by hand
     * (not via Jackson) to avoid coupling the test to the module's Jackson version alignment.
     */
    private static Map<String, Object> toMap(SnapshotSplit split) {
        Map<String, Object> map = new HashMap<>();
        map.put("splitId", split.getSplitId());
        map.put("tableId", split.getTableId());
        map.put("splitKey", split.getSplitKey());
        map.put("splitStart", split.getSplitStart());
        map.put("splitEnd", split.getSplitEnd());
        map.put("highWatermark", split.getHighWatermark());
        return map;
    }

    private Map<String, Object> binlogMeta(Map<String, String> startingOffset) {
        Map<String, Object> meta = new HashMap<>();
        meta.put(SourceReader.SPLIT_ID, BinlogSplit.BINLOG_SPLIT_ID);
        meta.put("startingOffset", startingOffset);
        return meta;
    }

    private String nextTaskId() {
        return jobId + "-t" + taskSeq.incrementAndGet();
    }

    @Override
    public void close() {
        Env.getCurrentEnv().close(jobId);
    }

    /** Snapshot rows (JSON) plus the high-watermark offset of every split. */
    public static final class SnapshotResult {
        private final List<String> records;
        private final List<Map<String, String>> splitOffsets;

        SnapshotResult(List<String> records, List<Map<String, String>> splitOffsets) {
            this.records = records;
            this.splitOffsets = splitOffsets;
        }

        public List<String> records() {
            return records;
        }

        public List<Map<String, String>> splitOffsets() {
            return splitOffsets;
        }
    }
}
