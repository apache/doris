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

suite("test_row_ttl_compaction", "nonConcurrent") {
    def physicalRowCount = { table ->
        if (isCloudMode()) {
            return null
        }
        def metaUrl = sql_return_maparray("SHOW TABLETS FROM ${table}").get(0).MetaUrl
        def (code, out, err) = curl("GET", metaUrl)
        logger.info("tablet meta response: code=${code}, err=${err}")
        assertEquals(0, code)
        def tabletMeta = parseJson(out.trim())
        return tabletMeta.rs_metas.sum { rowset -> rowset.num_rows as long } as long
    }

    sql "DROP TABLE IF EXISTS row_ttl_dup_compaction"
    sql """
        CREATE TABLE row_ttl_dup_compaction (
            k INT,
            event_time DATETIMEV2(6),
            v INT
        ) DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "disable_auto_compaction" = "true",
            "function_column.enable_row_ttl" = "true",
            "function_column.ttl_col" = "event_time",
            "function_column.ttl" = "0",
            "function_column.ttl_time_zone" = "+08:00"
        )
    """
    sql "INSERT INTO row_ttl_dup_compaction VALUES (1, now(6) - INTERVAL 1 DAY, 10)"
    sql "INSERT INTO row_ttl_dup_compaction VALUES (2, now(6) + INTERVAL 1 DAY, 20)"
    order_qt_dup_before_compaction "SELECT k, v FROM row_ttl_dup_compaction ORDER BY k"
    trigger_and_wait_compaction("row_ttl_dup_compaction", "full")
    order_qt_dup_after_compaction "SELECT k, v FROM row_ttl_dup_compaction ORDER BY k"
    if (!isCloudMode()) {
        def dupPhysicalRowCount = physicalRowCount("row_ttl_dup_compaction")
        qt_dup_physical_reclamation "SELECT ${dupPhysicalRowCount} AS physical_row_count"
    }

    sql "DROP TABLE IF EXISTS row_ttl_mow_compaction"
    sql """
        CREATE TABLE row_ttl_mow_compaction (
            k INT,
            event_time DATETIMEV2(6),
            v INT
        ) UNIQUE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "disable_auto_compaction" = "true",
            "function_column.enable_row_ttl" = "true",
            "function_column.ttl_col" = "event_time",
            "function_column.ttl" = "0",
            "function_column.ttl_time_zone" = "+08:00"
        )
    """
    // The first cumulative compaction isolates post-merge delete-sign filtering: all TTL values
    // are in the future, while the latest versions of keys 1 and 3 are delete-sign rows.
    sql "INSERT INTO row_ttl_mow_compaction VALUES (1, now(6) + INTERVAL 1 DAY, 10)"
    sql """
        INSERT INTO row_ttl_mow_compaction(k, event_time, v, __DORIS_DELETE_SIGN__)
        VALUES (1, now(6) + INTERVAL 1 DAY, 10, 1)
    """
    sql "INSERT INTO row_ttl_mow_compaction VALUES (3, now(6) + INTERVAL 1 DAY, 30)"
    sql """
        INSERT INTO row_ttl_mow_compaction(k, event_time, v, __DORIS_DELETE_SIGN__)
        VALUES (3, now(6) + INTERVAL 1 DAY, 30, 1)
    """
    sql "INSERT INTO row_ttl_mow_compaction VALUES (2, now(6) + INTERVAL 1 DAY, 20)"
    trigger_and_wait_compaction("row_ttl_mow_compaction", "cumulative")

    // The second cumulative compaction isolates post-merge TTL filtering. Five delta rowsets meet
    // the default cumulative compaction threshold while only the refreshed key 2 remains visible.
    sql "INSERT INTO row_ttl_mow_compaction VALUES (4, now(6) - INTERVAL 1 DAY, 40)"
    sql "INSERT INTO row_ttl_mow_compaction VALUES (5, now(6) - INTERVAL 1 DAY, 50)"
    sql "INSERT INTO row_ttl_mow_compaction VALUES (6, now(6) - INTERVAL 1 DAY, 60)"
    sql "INSERT INTO row_ttl_mow_compaction VALUES (7, now(6) - INTERVAL 1 DAY, 70)"
    sql "INSERT INTO row_ttl_mow_compaction VALUES (2, now(6) + INTERVAL 1 DAY, 20)"
    trigger_and_wait_compaction("row_ttl_mow_compaction", "cumulative")
    order_qt_mow_after_compaction "SELECT k, v FROM row_ttl_mow_compaction ORDER BY k"
    trigger_and_wait_compaction("row_ttl_mow_compaction", "full")
    if (!isCloudMode()) {
        def mowPhysicalRowCount = physicalRowCount("row_ttl_mow_compaction")
        qt_mow_physical_reclamation "SELECT ${mowPhysicalRowCount} AS physical_row_count"
    }

    sql "DROP TABLE IF EXISTS row_ttl_mor_no_resurrection"
    sql """
        CREATE TABLE row_ttl_mor_no_resurrection (
            k INT,
            event_time DATETIMEV2(6),
            v STRING
        ) UNIQUE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "false",
            "disable_auto_compaction" = "true",
            "function_column.enable_row_ttl" = "true",
            "function_column.ttl_col" = "event_time",
            "function_column.ttl" = "0",
            "function_column.ttl_time_zone" = "+08:00"
        )
    """
    sql "INSERT INTO row_ttl_mor_no_resurrection VALUES (1, now(6) + INTERVAL 1 DAY, 'old-live')"
    sql "INSERT INTO row_ttl_mor_no_resurrection VALUES (2, now(6) + INTERVAL 1 DAY, 'base-live')"
    // Full compaction builds the base rowset and advances the cumulative point.
    trigger_and_wait_compaction("row_ttl_mor_no_resurrection", "full")
    // Five delta rowsets meet the default cumulative compaction threshold.
    sql "INSERT INTO row_ttl_mor_no_resurrection VALUES (1, now(6) - INTERVAL 1 DAY, 'new-expired')"
    sql "INSERT INTO row_ttl_mor_no_resurrection VALUES (1, now(6) - INTERVAL 2 DAY, 'newer-expired')"
    sql "INSERT INTO row_ttl_mor_no_resurrection VALUES (1, now(6) - INTERVAL 3 DAY, 'newest-expired')"
    sql "INSERT INTO row_ttl_mor_no_resurrection VALUES (1, now(6) - INTERVAL 4 DAY, 'latest-expired')"
    sql "INSERT INTO row_ttl_mor_no_resurrection VALUES (3, now(6) + INTERVAL 1 DAY, 'cumulative-live')"
    order_qt_mor_before_compaction "SELECT k, v FROM row_ttl_mor_no_resurrection ORDER BY k"
    trigger_and_wait_compaction("row_ttl_mor_no_resurrection", "cumulative")
    order_qt_mor_after_cumulative "SELECT k, v FROM row_ttl_mor_no_resurrection ORDER BY k"
    trigger_and_wait_compaction("row_ttl_mor_no_resurrection", "full")
    order_qt_mor_after_full "SELECT k, v FROM row_ttl_mor_no_resurrection ORDER BY k"
    if (!isCloudMode()) {
        def morPhysicalRowCount = physicalRowCount("row_ttl_mor_no_resurrection")
        qt_mor_physical_reclamation "SELECT ${morPhysicalRowCount} AS physical_row_count"
    }
}
