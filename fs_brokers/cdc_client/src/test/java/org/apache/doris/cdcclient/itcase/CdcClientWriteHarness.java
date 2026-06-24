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
import org.apache.doris.job.cdc.request.FetchTableSplitsRequest;
import org.apache.doris.job.cdc.request.JobBaseConfig;
import org.apache.doris.job.cdc.request.WriteRecordRequest;
import org.apache.doris.job.cdc.split.AbstractSourceSplit;
import org.apache.doris.job.cdc.split.BinlogSplit;
import org.apache.doris.job.cdc.split.SnapshotSplit;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Test driver for the from-to {@code writeRecords} path: drives the read + deserialize + streamload
 * + commit-offset pipeline against a containerized source and a {@link MockDorisServer} standing in
 * for the BE stream-load and FE commit-offset endpoints. No real Doris cluster needed.
 *
 * <p>Uses a numeric jobId/taskId because {@code commitOffset} parses them as {@code Long}.
 */
final class CdcClientWriteHarness implements AutoCloseable {

    private static final int SPLIT_BATCH_SIZE = 1000;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final PipelineCoordinator coordinator = new PipelineCoordinator(10);
    private final String jobId;
    private final String dataSource;
    private final Map<String, String> config;
    private final String targetDb;
    private final MockDorisServer mock;
    private final AtomicLong taskSeq = new AtomicLong();

    // Threaded across rounds, mirroring how FE persists and replays them.
    private String lastTableSchemas;
    private Map<String, String> lastBinlogOffset;

    private CdcClientWriteHarness(
            String jobId,
            String dataSource,
            Map<String, String> config,
            String targetDb,
            MockDorisServer mock) {
        this.jobId = jobId;
        this.dataSource = dataSource;
        this.config = config;
        this.targetDb = targetDb;
        this.mock = mock;
    }

    static CdcClientWriteHarness mysql(
            String jobId,
            String host,
            int port,
            String user,
            String password,
            String database,
            String includeTables,
            String offset,
            String targetDb,
            MockDorisServer mock) {
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

        // Point cdc_client's stream-load at the mock BE.
        Env.getCurrentEnv().setBackendHttpPort(mock.port());
        Env.getCurrentEnv().setClusterToken("test");
        return new CdcClientWriteHarness(jobId, "MYSQL", config, targetDb, mock);
    }

    static CdcClientWriteHarness postgres(
            String jobId,
            String host,
            int port,
            String user,
            String password,
            String database,
            String schema,
            String includeTables,
            String offset,
            String targetDb,
            MockDorisServer mock) {
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
        // FE-assigned names so the reader owns and pre-creates the slot/publication.
        config.put(DataSourceConfigKeys.SLOT_NAME, DataSourceConfigKeys.defaultSlotName(jobId));
        config.put(
                DataSourceConfigKeys.PUBLICATION_NAME,
                DataSourceConfigKeys.defaultPublicationName(jobId));

        Env.getCurrentEnv().setBackendHttpPort(mock.port());
        Env.getCurrentEnv().setClusterToken("test");
        return new CdcClientWriteHarness(jobId, "POSTGRES", config, targetDb, mock);
    }

    /** Shrink the snapshot chunk size so a small table still splits into multiple chunks. */
    CdcClientWriteHarness withSplitSize(int splitSize) {
        config.put(DataSourceConfigKeys.SNAPSHOT_SPLIT_SIZE, String.valueOf(splitSize));
        return this;
    }

    /**
     * Override the MySQL JDBC url's {@code serverTimezone}, which both the driver and Debezium use
     * to interpret TIMESTAMP columns. MySQL only — the url is rebuilt from the original host/port/db.
     */
    CdcClientWriteHarness withServerTimezone(String zoneId) {
        String url = config.get(DataSourceConfigKeys.JDBC_URL);
        int q = url.indexOf('?');
        String base = q < 0 ? url : url.substring(0, q);
        config.put(DataSourceConfigKeys.JDBC_URL, base + "?serverTimezone=" + zoneId);
        return this;
    }

    private JobBaseConfig baseConfig() {
        return new JobBaseConfig(jobId, dataSource, config, null);
    }

    /** Compute every snapshot chunk for one table (mirrors FE's /api/fetchSplits cursor). */
    List<SnapshotSplit> fetchAllSnapshotSplits(String table) {
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
            if (last.getSplitEnd() == null) {
                break;
            }
            nextSplitStart = last.getSplitEnd();
            nextSplitId = all.size();
        }
        return all;
    }

    /** Run the snapshot through the from-to path: read -> deserialize -> streamload -> commit. */
    void writeSnapshot(List<SnapshotSplit> splits) throws Exception {
        List<Map<String, Object>> splitMaps = new ArrayList<>();
        for (SnapshotSplit split : splits) {
            splitMaps.add(toMap(split));
        }
        Map<String, Object> meta = new HashMap<>();
        meta.put("splits", splitMaps);
        runWrite(meta);
    }

    /**
     * Enter the binlog phase from a completed snapshot and read one window. This establishes (and
     * commits) the baseline tableSchemas before any later DDL, so a subsequent ALTER is detectable.
     */
    void enterBinlog(List<SnapshotSplit> splits) throws Exception {
        runWrite(finishedSplitsMeta(splits, committedSnapshotOffsets()));
    }

    /**
     * Enter the binlog phase straight from the configured non-snapshot startup mode
     * (latest/earliest/specific-offset/timestamp) with no preceding snapshot. The reader resolves
     * the configured OFFSET on this first window and commits a concrete position; later reads resume
     * via {@link #continueBinlog}. Mirrors FE's binlog-only meta: {@code {splitId: binlog-split}}
     * with neither startingOffset nor finishedSplits.
     */
    void enterBinlogFromStartupMode() throws Exception {
        Map<String, Object> meta = new HashMap<>();
        meta.put("splitId", BinlogSplit.BINLOG_SPLIT_ID);
        runWrite(meta);
    }

    /**
     * Read the binlog phase straight from the configured startup mode, polling until
     * {@code expectedRecords} rows are stream-loaded or the timeout elapses. The first window
     * resolves the configured OFFSET (so for specific-offset/earliest it replays pre-existing
     * changes from that point forward); later windows resume from the committed offset. Returns only
     * the rows loaded during this call.
     */
    List<String> readBinlogFromStartupMode(int expectedRecords, Duration timeout) throws Exception {
        int before = mock.loadedRecords().size();
        Map<String, Object> meta = new HashMap<>();
        meta.put("splitId", BinlogSplit.BINLOG_SPLIT_ID);
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        while (mock.loadedRecords().size() - before < expectedRecords
                && System.nanoTime() < deadlineNanos) {
            runWrite(meta);
            meta = new HashMap<>();
            meta.put("splitId", BinlogSplit.BINLOG_SPLIT_ID);
            meta.put("startingOffset", lastBinlogOffset);
        }
        List<String> all = mock.loadedRecords();
        return new ArrayList<>(all.subList(before, all.size()));
    }

    /** Continue the binlog phase from the last committed offset until expected rows or timeout. */
    List<String> continueBinlog(int expectedRecords, Duration timeout) throws Exception {
        int before = mock.loadedRecords().size();
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        while (mock.loadedRecords().size() - before < expectedRecords
                && System.nanoTime() < deadlineNanos) {
            Map<String, Object> meta = new HashMap<>();
            meta.put("splitId", BinlogSplit.BINLOG_SPLIT_ID);
            meta.put("startingOffset", lastBinlogOffset);
            runWrite(meta);
        }
        List<String> all = mock.loadedRecords();
        return new ArrayList<>(all.subList(before, all.size()));
    }

    private void runWrite(Map<String, Object> meta) throws Exception {
        coordinator.writeRecords(buildRequest(meta));
        capture();
    }

    /** Capture tableSchemas and the binlog offset from the last commit, to replay next round. */
    private void capture() throws Exception {
        JsonNode commit = MAPPER.readTree(mock.committedOffset());
        JsonNode ts = commit.get("tableSchemas");
        if (ts != null && !ts.isNull()) {
            lastTableSchemas = ts.asText();
        }
        List<Map<String, String>> offsets = parseOffsetList(mock.committedOffset());
        if (offsets.size() == 1
                && BinlogSplit.BINLOG_SPLIT_ID.equals(offsets.get(0).get("splitId"))) {
            Map<String, String> offset = new HashMap<>(offsets.get(0));
            offset.remove("splitId");
            lastBinlogOffset = offset;
        }
    }

    /**
     * Continue from a completed snapshot into the binlog phase and keep polling (each writeRecords
     * is one maxInterval window) until {@code expectedRecords} new rows are stream-loaded or the
     * timeout elapses. Returns only the rows stream-loaded during this binlog phase.
     */
    List<String> writeBinlogUntil(List<SnapshotSplit> splits, int expectedRecords, Duration timeout)
            throws Exception {
        int before = mock.loadedRecords().size();
        Map<String, Object> meta = finishedSplitsMeta(splits, committedSnapshotOffsets());
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        while (mock.loadedRecords().size() - before < expectedRecords
                && System.nanoTime() < deadlineNanos) {
            coordinator.writeRecords(buildRequest(meta));
            meta = new HashMap<>();
            meta.put("splitId", BinlogSplit.BINLOG_SPLIT_ID);
            meta.put("startingOffset", committedBinlogOffset());
        }
        List<String> all = mock.loadedRecords();
        return new ArrayList<>(all.subList(before, all.size()));
    }

    private Map<String, Object> finishedSplitsMeta(
            List<SnapshotSplit> splits, List<Map<String, String>> snapshotOffsets) {
        Map<String, Map<String, String>> hwBySplitId = new HashMap<>();
        for (Map<String, String> offset : snapshotOffsets) {
            Map<String, String> hw = new HashMap<>(offset);
            String splitId = hw.remove("splitId");
            hwBySplitId.put(splitId, hw);
        }
        List<Map<String, Object>> finished = new ArrayList<>();
        for (SnapshotSplit split : splits) {
            Map<String, Object> m = toMap(split);
            m.put("highWatermark", hwBySplitId.get(split.getSplitId()));
            finished.add(m);
        }
        Map<String, Object> meta = new HashMap<>();
        meta.put("splitId", BinlogSplit.BINLOG_SPLIT_ID);
        meta.put("finishedSplits", finished);
        return meta;
    }

    private List<Map<String, String>> committedSnapshotOffsets() throws Exception {
        return parseOffsetList(mock.committedOffset());
    }

    private Map<String, String> committedBinlogOffset() throws Exception {
        Map<String, String> offset = new HashMap<>(parseOffsetList(mock.committedOffset()).get(0));
        offset.remove("splitId");
        return offset;
    }

    private List<Map<String, String>> parseOffsetList(String commitJson) throws Exception {
        JsonNode commit = MAPPER.readTree(commitJson);
        String offsetStr = commit.get("offset").asText();
        return MAPPER.readValue(offsetStr, new TypeReference<List<Map<String, String>>>() {});
    }

    private WriteRecordRequest buildRequest(Map<String, Object> meta) {
        WriteRecordRequest req = new WriteRecordRequest();
        req.setJobId(jobId);
        req.setDataSource(dataSource);
        req.setConfig(config);
        req.setFrontendAddress(mock.hostPort());
        req.setMeta(meta);
        req.setTableSchemas(lastTableSchemas);
        req.setTaskId(String.valueOf(taskSeq.incrementAndGet()));
        req.setTargetDb(targetDb);
        req.setToken("test-token");
        req.setMaxInterval(3);
        req.setTaskTimeoutMs(60_000);
        req.setStreamLoadProps(new HashMap<>());
        return req;
    }

    List<String> loadedRecords() {
        return mock.loadedRecords();
    }

    List<String> executedDdls() {
        return mock.executedDdls();
    }

    String committedOffset() {
        return mock.committedOffset();
    }

    @Override
    public void close() {
        Env.getCurrentEnv().close(jobId);
    }

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
}
