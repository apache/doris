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

package org.apache.doris.nereids.stats;

import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.statistics.ResultRow;
import org.apache.doris.statistics.util.StatisticsUtil;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Persistence of manually injected (pinned) hbo statistics into the internal database
 * ({@code __internal_schema.hbo_statistics}), controlled by
 * {@code Config.hbo_persist_pinned_to_internal_db}.
 *
 * <p>The in-memory {@link HboPlanStatisticsManager} stays authoritative on the read path;
 * SET/DELETE are written through synchronously (best effort, failures only logged) and a FE
 * loads the whole table into memory lazily on first use, so pinned entries survive a FE
 * restart. All FEs share the table through the backend storage.
 */
public class HboStatisticsStore {
    private static final Logger LOG = LogManager.getLogger(HboStatisticsStore.class);

    private static final String INTERNAL_DB = FeConstants.INTERNAL_DB_NAME;
    private static final String TABLE = "hbo_statistics";
    private static final String FULL_QUALIFIED =
            InternalCatalog.INTERNAL_CATALOG_NAME + "." + INTERNAL_DB + "." + TABLE;
    /** max length (bytes) of the struct_info varchar column */
    private static final int STRUCT_MAX_BYTES = 65533;

    private HboStatisticsStore() {
    }

    /** Create the table if it does not exist. */
    public static void ensureTable() throws Exception {
        StatisticsUtil.execUpdate(createDdl());
    }

    /**
     * Upsert one pinned entry (UNIQUE KEY fingerprint replaces on conflict).
     */
    public static void persist(String fingerprint, long rows,
            HboPlanStatisticsManager.PinnedType type, String structCanonical, long createTimeMs) {
        try {
            ensureTable();
            HboPlanStatisticsManager.PinnedType statsType = type == null
                    ? HboPlanStatisticsManager.PinnedType.EXACT : type;
            String struct = truncateUtf8(structCanonical == null ? "" : structCanonical, STRUCT_MAX_BYTES);
            // the fingerprint kind is only known after the entry was applied once, so the stored
            // value stays UNKNOWN (it is reported by HBO SHOW from the in-memory entry)
            String sql = "INSERT INTO " + FULL_QUALIFIED
                    + " (`fingerprint`, `row_count`, `stats_type`, `fingerprint_kind`, `struct_info`,"
                    + " `create_time_ms`) VALUES ('"
                    + StatisticsUtil.escapeSQL(fingerprint) + "', " + rows + ", '"
                    + StatisticsUtil.escapeSQL(statsType.name().toLowerCase(java.util.Locale.ROOT)) + "', '"
                    + StatisticsUtil.escapeSQL(
                            HboPlanStatisticsManager.FingerprintKind.UNKNOWN.name().toLowerCase(java.util.Locale.ROOT))
                    + "', '" + StatisticsUtil.escapeSQL(struct) + "', " + createTimeMs + ")";
            StatisticsUtil.execUpdate(sql);
        } catch (Exception t) {
            LOG.warn("failed to persist hbo pinned statistics for fingerprint {}", fingerprint, t);
        }
    }

    /**
     * Remove a pinned entry.
     *
     * @return true when the row was removed (or the table had no such row), false when the
     *         best-effort removal failed (e.g. internal schema not ready yet)
     */
    public static boolean delete(String fingerprint) {
        try {
            ensureTable();
            String sql = "DELETE FROM " + FULL_QUALIFIED + " WHERE `fingerprint` = '"
                    + StatisticsUtil.escapeSQL(fingerprint) + "'";
            StatisticsUtil.execUpdate(sql);
            return true;
        } catch (Exception t) {
            LOG.warn("failed to delete hbo pinned statistics for fingerprint {}", fingerprint, t);
            return false;
        }
    }

    /**
     * Load all pinned entries from the internal table.
     *
     * @return the loaded entries, or {@code null} when the load failed (e.g. the internal schema
     *         is not ready yet); the caller decides when to retry
     */
    public static List<HboPlanStatisticsManager.PinnedHboStatistics> loadAll() {
        List<HboPlanStatisticsManager.PinnedHboStatistics> result = new ArrayList<>();
        try {
            ensureTable();
            List<ResultRow> rows = StatisticsUtil.execStatisticQuery(
                    "SELECT `fingerprint`, `row_count`, `stats_type`, `struct_info`, `create_time_ms` FROM "
                            + FULL_QUALIFIED);
            for (ResultRow row : rows) {
                try {
                    HboPlanStatisticsManager.PinnedType type =
                            HboPlanStatisticsManager.PinnedType.fromName(row.get(2));
                    result.add(new HboPlanStatisticsManager.PinnedHboStatistics(
                            row.get(0),
                            Long.parseLong(row.get(1)),
                            type == null ? HboPlanStatisticsManager.PinnedType.EXACT : type,
                            row.get(3) == null ? "" : row.get(3),
                            Long.parseLong(row.get(4))));
                } catch (NumberFormatException e) {
                    LOG.warn("skip malformed hbo pinned statistics row {}", row, e);
                }
            }
            return result;
        } catch (Exception t) {
            LOG.warn("failed to load hbo pinned statistics from internal table", t);
            return null;
        }
    }

    private static String createDdl() {
        // follow the internal-table convention (see InternalSchemaInitializer) so that CREATE
        // never fails on clusters whose min_replication_num_per_tablet exceeds 1
        int replication = Math.max(1, Config.min_replication_num_per_tablet);
        return "CREATE TABLE IF NOT EXISTS `" + InternalCatalog.INTERNAL_CATALOG_NAME + "`.`" + INTERNAL_DB
                + "`.`" + TABLE + "` (\n"
                + "  `fingerprint` varchar(64) NOT NULL COMMENT \"\",\n"
                + "  `row_count` bigint NOT NULL COMMENT \"\",\n"
                + "  `stats_type` varchar(32) NOT NULL COMMENT \"\",\n"
                + "  `fingerprint_kind` varchar(16) NOT NULL COMMENT \"\",\n"
                + "  `struct_info` varchar(" + STRUCT_MAX_BYTES + ") NULL COMMENT \"\",\n"
                + "  `create_time_ms` bigint NOT NULL COMMENT \"\"\n"
                + ") ENGINE = olap\n"
                + "UNIQUE KEY(`fingerprint`)\n"
                + "COMMENT \"Doris internal hbo pinned statistics table, DO NOT MODIFY IT\"\n"
                + "DISTRIBUTED BY HASH(`fingerprint`)\n"
                + "BUCKETS 1\n"
                + "PROPERTIES (\"replication_num\" = \"" + replication + "\")";
    }

    /**
     * Truncate to at most {@code maxBytes} UTF-8 bytes without splitting a multi-byte character
     * (the varchar column limit is byte-based).
     */
    private static String truncateUtf8(String value, int maxBytes) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length <= maxBytes) {
            return value;
        }
        // a cut inside a multi-byte sequence points at a continuation byte; back off to the
        // preceding lead byte so that only complete characters are kept
        int end = maxBytes;
        while (end > 0 && (bytes[end] & 0xC0) == 0x80) {
            end--;
        }
        return new String(bytes, 0, end, StandardCharsets.UTF_8);
    }
}
