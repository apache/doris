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

package org.apache.doris.load;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.load.loadv2.LoadManager;
import org.apache.doris.statistics.util.StatisticsUtil;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TLoadJob;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * Master-only daemon that periodically persists final-state import task snapshots into the
 * internal physical table {@code __internal_schema.loads_history}. That table is a real Doris
 * OLAP table (created by {@link org.apache.doris.catalog.InternalSchemaInitializer}); users query
 * it directly and it is the long-term retention version of {@code information_schema.loads}.
 *
 * <p>Data flow: collect final-state records from the same unified sources as
 * {@code information_schema.loads} (the {@link LoadManager} runtime jobs + Stream Load records
 * read on demand from BE RocksDB), never from the {@code SHOW STREAM LOAD} FE cache or the
 * {@code audit_log} plugin. Records are upserted by their stable {@code record_key} into a
 * UNIQUE KEY table, so re-scanning the same record (periodic rescan, FE restart, master switch,
 * or duplicate rows from a source) collapses to one logical history row.
 *
 * <p>Watermark: finish-time high-water marks are kept in memory only (not persisted to editlog).
 * On restart / master switch the watermarks reset to 0 and the most recent source window is
 * re-scanned once; the idempotent upsert absorbs the overlap. This is the intended
 * "simple watermark + idempotent fallback" model.
 *
 * <p>Failures (internal table missing, sync SQL error) only WARN and are retried next cycle; they
 * never affect the import execution path or the {@code loads} query path.
 */
public class LoadsHistorySyncer extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(LoadsHistorySyncer.class);

    // Physical history table name under __internal_schema. Users query it directly:
    // SELECT ... FROM __internal_schema.loads_history.
    public static final String LOADS_HISTORY_TABLE = "loads_history";

    // Physical column order of __internal_schema.loads_history (see InternalSchema.LOADS_HISTORY_SCHEMA):
    // the two UNIQUE KEY columns first, then the 20 unified "loads" columns.
    private static final String INSERT_COLUMNS =
            "`finish_time`, `record_key`, `job_id`, `label`, `state`, `progress`, `type`,"
            + " `etl_info`, `task_info`, `error_msg`, `create_time`, `etl_start_time`,"
            + " `etl_finish_time`, `load_start_time`, `load_finish_time`, `url`, `job_details`,"
            + " `transaction_id`, `error_tablets`, `user`, `comment`, `first_error_msg`";

    // Bound the rows written per INSERT statement so one cycle issues several small statements
    // instead of one huge VALUES list.
    private static final int WRITE_BATCH_SIZE = 200;

    // Per-BE cap on Stream Load records read per cycle, bounding memory for one sync pass.
    private static final int STREAM_LOAD_READ_CAP = 50000;

    // In-memory finish-time high-water marks, one per source. Reset to 0 on restart.
    private long loadManagerWatermarkMs = 0;
    private long streamLoadWatermarkMs = 0;

    public LoadsHistorySyncer() {
        super("loads-history-syncer", Config.loads_history_sync_interval_second * 1000L);
    }

    @Override
    protected void runAfterCatalogReady() {
        if (!Config.enable_loads_history) {
            return;
        }
        // Only the master FE writes history. The daemon is started under
        // startMasterOnlyDaemonThreads(), but guard defensively in case of role changes.
        if (!Env.getCurrentEnv().isMaster()) {
            return;
        }
        if (!FeConstants.enableInternalSchemaDb) {
            return;
        }

        long start = System.currentTimeMillis();
        int synced = 0;
        try {
            synced += syncLoadManagerRecords();
            synced += syncStreamLoadRecords();
        } catch (Exception e) {
            // Any failure only warns; the next cycle retries. Never affect the import path.
            LOG.warn("Failed to sync records into {}.{}", FeConstants.INTERNAL_DB_NAME,
                    LOADS_HISTORY_TABLE, e);
        }
        if (synced > 0) {
            LOG.info("Synced {} final-state records into {}.{}, cost {} ms", synced,
                    FeConstants.INTERNAL_DB_NAME, LOADS_HISTORY_TABLE,
                    System.currentTimeMillis() - start);
        }
    }

    private int syncLoadManagerRecords() {
        LoadManager loadManager = Env.getCurrentEnv().getLoadManager();
        if (loadManager == null) {
            return 0;
        }
        List<LoadJobHistoryRecord> records = withinRetention(
                loadManager.getLoadJobHistoryRecords(loadManagerWatermarkMs));
        if (records.isEmpty()) {
            return 0;
        }
        try {
            int written = writeRecords(records);
            // Advance the watermark only after all batches succeeded, so a failed batch is fully
            // re-scanned next cycle (idempotent upsert absorbs any overlap).
            loadManagerWatermarkMs = Math.max(loadManagerWatermarkMs, maxFinishTime(records));
            return written;
        } catch (Exception e) {
            LOG.warn("Failed to write LoadManager records into {}.{}; watermark not advanced,"
                    + " will retry next cycle", FeConstants.INTERNAL_DB_NAME, LOADS_HISTORY_TABLE, e);
            return 0;
        }
    }

    private int syncStreamLoadRecords() {
        StreamLoadRecordMgr mgr = Env.getCurrentEnv().getStreamLoadRecordMgr();
        if (mgr == null) {
            return 0;
        }
        ImmutableMap<Long, Backend> backends;
        try {
            backends = Env.getCurrentSystemInfo().getAllBackendsByAllCluster();
        } catch (AnalysisException e) {
            LOG.warn("Failed to load backends for loads_history stream load sync", e);
            return 0;
        }
        List<LoadJobHistoryRecord> records = Lists.newArrayList();
        for (Backend backend : backends.values()) {
            if (!backend.isAlive()) {
                continue;
            }
            // A single unavailable BE only skips its own records; the method tolerates it.
            records.addAll(mgr.getStreamLoadHistoryRecords(backend, streamLoadWatermarkMs,
                    STREAM_LOAD_READ_CAP));
        }
        records = withinRetention(records);
        if (records.isEmpty()) {
            return 0;
        }
        try {
            int written = writeRecords(records);
            streamLoadWatermarkMs = Math.max(streamLoadWatermarkMs, maxFinishTime(records));
            return written;
        } catch (Exception e) {
            LOG.warn("Failed to write stream load records into {}.{}; watermark not advanced,"
                    + " will retry next cycle", FeConstants.INTERNAL_DB_NAME, LOADS_HISTORY_TABLE, e);
            return 0;
        }
    }

    /**
     * Keep only records whose finish time lands inside the internal table's dynamic-partition
     * window [now - retentionDays, now]. Records outside the window have no target partition and
     * would fail the INSERT; skipping them is an accepted data gap (matches the retention policy),
     * and it keeps one stray record from blocking the whole batch (and thus the watermark).
     */
    private static List<LoadJobHistoryRecord> withinRetention(List<LoadJobHistoryRecord> records) {
        long now = System.currentTimeMillis();
        long retentionDays = Math.max(1, Config.loads_history_retention_day);
        long lowerBoundMs = now - retentionDays * 24L * 60L * 60L * 1000L;
        List<LoadJobHistoryRecord> kept = Lists.newArrayListWithCapacity(records.size());
        for (LoadJobHistoryRecord record : records) {
            long finish = record.getFinishTimeMs();
            if (finish >= lowerBoundMs && finish <= now) {
                kept.add(record);
            }
        }
        return kept;
    }

    private static long maxFinishTime(List<LoadJobHistoryRecord> records) {
        long max = 0;
        for (LoadJobHistoryRecord record : records) {
            if (record.getFinishTimeMs() > max) {
                max = record.getFinishTimeMs();
            }
        }
        return max;
    }

    /**
     * Upsert the given records into the internal history table in bounded batches. Returns the
     * number of records written. Throws if any batch fails, so the caller can leave its watermark
     * unadvanced and re-scan the whole window next cycle (the idempotent upsert absorbs overlap).
     */
    private int writeRecords(List<LoadJobHistoryRecord> records) throws Exception {
        if (records == null || records.isEmpty()) {
            return 0;
        }
        int written = 0;
        StringBuilder values = new StringBuilder();
        int batchCount = 0;
        for (LoadJobHistoryRecord record : records) {
            if (batchCount > 0) {
                values.append(",");
            }
            values.append(toValuesTuple(record));
            batchCount++;
            if (batchCount >= WRITE_BATCH_SIZE) {
                execInsert(values.toString());
                written += batchCount;
                values.setLength(0);
                batchCount = 0;
            }
        }
        if (batchCount > 0) {
            execInsert(values.toString());
            written += batchCount;
        }
        return written;
    }

    private void execInsert(String valuesClause) throws Exception {
        String sql = "INSERT INTO `" + InternalCatalog.INTERNAL_CATALOG_NAME + "`.`"
                + FeConstants.INTERNAL_DB_NAME + "`.`" + LOADS_HISTORY_TABLE + "` ("
                + INSERT_COLUMNS + ") VALUES " + valuesClause;
        StatisticsUtil.execUpdate(sql);
    }

    // Package-private for unit testing of escaping / NULL handling.
    static String toValuesTuple(LoadJobHistoryRecord record) {
        TLoadJob row = record.getRow();
        // finish_time is the typed DATETIME partition column; record_key is the dedup key.
        String finishTime = TimeUtils.longToTimeString(record.getFinishTimeMs(),
                TimeUtils.getDatetimeMsFormatWithTimeZone());
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        appendString(sb, finishTime).append(",");
        appendString(sb, record.getRecordKey()).append(",");
        appendString(sb, row.getJobId()).append(",");
        appendString(sb, row.getLabel()).append(",");
        appendString(sb, row.getState()).append(",");
        appendString(sb, row.getProgress()).append(",");
        appendString(sb, row.getType()).append(",");
        appendString(sb, row.getEtlInfo()).append(",");
        appendString(sb, row.getTaskInfo()).append(",");
        appendString(sb, row.getErrorMsg()).append(",");
        appendString(sb, row.getCreateTime()).append(",");
        appendString(sb, row.getEtlStartTime()).append(",");
        appendString(sb, row.getEtlFinishTime()).append(",");
        appendString(sb, row.getLoadStartTime()).append(",");
        appendString(sb, row.getLoadFinishTime()).append(",");
        appendString(sb, row.getUrl()).append(",");
        appendString(sb, row.getJobDetails()).append(",");
        appendString(sb, row.getTransactionId()).append(",");
        appendString(sb, row.getErrorTablets()).append(",");
        appendString(sb, row.getUser()).append(",");
        appendString(sb, row.getComment()).append(",");
        appendString(sb, row.getFirstErrorMsg());
        sb.append(")");
        return sb.toString();
    }

    private static StringBuilder appendString(StringBuilder sb, String value) {
        if (value == null) {
            return sb.append("NULL");
        }
        return sb.append("'").append(StatisticsUtil.escapeSQL(value)).append("'");
    }
}
