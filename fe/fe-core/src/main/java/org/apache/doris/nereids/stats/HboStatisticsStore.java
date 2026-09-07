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

import org.apache.doris.common.FeConstants;
import org.apache.doris.statistics.ResultRow;
import org.apache.doris.statistics.util.StatisticsUtil;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
    private static final String FULL_QUALIFIED = "internal." + INTERNAL_DB + "." + TABLE;
    private static final int STRUCT_MAX_LEN = 65533;

    private static final String DDL = "CREATE TABLE IF NOT EXISTS `internal`.`" + INTERNAL_DB + "`.`" + TABLE
            + "` (\n"
            + "  `fingerprint` varchar(64) NOT NULL COMMENT \"\",\n"
            + "  `row_count` bigint NOT NULL COMMENT \"\",\n"
            + "  `node_type` varchar(1024) NULL COMMENT \"\",\n"
            + "  `struct_info` varchar(" + STRUCT_MAX_LEN + ") NULL COMMENT \"\",\n"
            + "  `create_time_ms` bigint NOT NULL COMMENT \"\"\n"
            + ") ENGINE = olap\n"
            + "UNIQUE KEY(`fingerprint`)\n"
            + "COMMENT \"Doris internal hbo pinned statistics table, DO NOT MODIFY IT\"\n"
            + "DISTRIBUTED BY HASH(`fingerprint`)\n"
            + "BUCKETS 1\n"
            + "PROPERTIES (\"replication_num\" = \"1\")";

    private HboStatisticsStore() {
    }

    /** Create the table if it does not exist. */
    public static void ensureTable() throws Exception {
        StatisticsUtil.execUpdate(DDL);
    }

    /** Upsert one pinned entry (UNIQUE KEY fingerprint replaces on conflict). */
    public static void persist(String fingerprint, long rows, String nodeType, String structCanonical,
            long createTimeMs) {
        try {
            ensureTable();
            String struct = structCanonical == null ? "" : structCanonical;
            if (struct.length() > STRUCT_MAX_LEN) {
                struct = struct.substring(0, STRUCT_MAX_LEN);
            }
            String sql = "INSERT INTO " + FULL_QUALIFIED
                    + " (`fingerprint`, `row_count`, `node_type`, `struct_info`, `create_time_ms`) VALUES ('"
                    + escape(fingerprint) + "', " + rows + ", '" + escape(nodeType) + "', '"
                    + escape(struct) + "', " + createTimeMs + ")";
            StatisticsUtil.execUpdate(sql);
        } catch (Throwable t) {
            LOG.warn("failed to persist hbo pinned statistics for fingerprint {}", fingerprint, t);
        }
    }

    /** Remove a pinned entry. */
    public static void delete(String fingerprint) {
        try {
            ensureTable();
            String sql = "DELETE FROM " + FULL_QUALIFIED + " WHERE `fingerprint` = '" + escape(fingerprint) + "'";
            StatisticsUtil.execUpdate(sql);
        } catch (Throwable t) {
            LOG.warn("failed to delete hbo pinned statistics for fingerprint {}", fingerprint, t);
        }
    }

    /** Load all pinned entries into memory. Returns empty list when the table is not present. */
    public static List<HboPlanStatisticsManager.PinnedHboStatistics> loadAll() {
        List<HboPlanStatisticsManager.PinnedHboStatistics> result = new ArrayList<>();
        try {
            ensureTable();
            List<ResultRow> rows = StatisticsUtil.execStatisticQuery(
                    "SELECT `fingerprint`, `row_count`, `node_type`, `struct_info`, `create_time_ms` FROM "
                            + FULL_QUALIFIED);
            for (ResultRow row : rows) {
                try {
                    result.add(new HboPlanStatisticsManager.PinnedHboStatistics(
                            row.get(0),
                            Long.parseLong(row.get(1)),
                            row.get(2) == null ? "" : row.get(2),
                            row.get(3) == null ? "" : row.get(3),
                            Long.parseLong(row.get(4))));
                } catch (NumberFormatException e) {
                    LOG.warn("skip malformed hbo pinned statistics row {}", row, e);
                }
            }
        } catch (Throwable t) {
            LOG.warn("failed to load hbo pinned statistics from internal table", t);
        }
        return result;
    }

    private static String escape(String value) {
        return value == null ? "" : value.replace("'", "''");
    }
}
