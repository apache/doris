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

suite("test_row_ttl_alter", "nonConcurrent") {
    def metas = { table ->
        sql_return_maparray("SHOW TABLETS FROM ${table}").collect { tablet ->
            def (code, out, err) = curl("GET", tablet.MetaUrl)
            assertEquals(0, code)
            parseJson(out.trim())
        }
    }
    def rowsets = { table ->
        metas(table).collectMany { it.rs_metas }.collect { it.rowset_id_v2 }.sort()
    }
    def physicalRows = { table ->
        metas(table).collectMany { it.rs_metas }.sum { it.num_rows as long }
    }
    // Exercise both merge implementations and all supported key models. DATE and
    // DATETIME source cells retain their original types across policy changes.
    [false, true].each { vertical ->
        setBeConfigTemporary([enable_vertical_compaction: vertical,
                              vertical_compaction_num_columns_per_group: 1]) {
            ["dup", "mow", "mor"].each { model ->
                def table = "row_ttl_alter_${model}_${vertical}"
                def key = model == "dup" ? "DUPLICATE" : "UNIQUE"
                def mow = model == "dup" ? "" :
                        '\"enable_unique_key_merge_on_write\"=\"' + (model == "mow") + '\",'
                def sourceType = vertical ? "DATEV2" : "DATETIMEV2(6)"
                sql "DROP TABLE IF EXISTS ${table}"
                sql """CREATE TABLE ${table} (k INT, event_time ${sourceType} NULL, v INT)
                    ${key} KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
                    PROPERTIES ("replication_num"="1", ${mow} "disable_auto_compaction"="true",
                        "function_column.enable_row_ttl"="true", "function_column.ttl_col"="event_time",
                        "function_column.ttl"="36500 day", "function_column.ttl_time_zone"="+00:00")"""
                sql """INSERT INTO ${table} VALUES
                    (1, '2000-01-01', 10), (2, '2099-01-01', 20), (3, NULL, 30),
                    (4, now() - INTERVAL 2 DAY, 40)"""
                order_qt_initial "SELECT k, v FROM ${table} ORDER BY k"
                def initialRowsets = isCloudMode() ? [] : rowsets(table)
                sql """ALTER TABLE ${table} SET ("function_column.ttl"="3 day")"""
                order_qt_shorter_without_rewrite "SELECT k, v FROM ${table} ORDER BY k"
                if (!isCloudMode()) {
                    qt_no_rewrite "SELECT ${initialRowsets == rowsets(table)}"
                    qt_initial_physical "SELECT ${physicalRows(table)}"
                }
                trigger_and_wait_compaction(table, "full")
                if (!isCloudMode()) {
                    qt_three_day_policy_physical "SELECT ${physicalRows(table)}"
                }
                sql """ALTER TABLE ${table} SET ("function_column.ttl"="0")"""
                order_qt_shortened "SELECT k, v FROM ${table} ORDER BY k"
                // No new writes: full compaction must revisit the single existing rowset.
                trigger_and_wait_compaction(table, "full")
                if (!isCloudMode()) {
                    qt_single_rowset_reclaimed "SELECT ${physicalRows(table)}"
                }
                test {
                    sql """ALTER TABLE ${table} SET ("function_column.ttl"="36500 day")"""
                    exception "Increasing function_column.ttl is not supported"
                }
                order_qt_reclaimed_stays_absent "SELECT k, v FROM ${table} ORDER BY k"
                sql "INSERT INTO ${table} VALUES (5, '2000-01-01', 50), (6, '2099-01-01', 60)"
                trigger_and_wait_compaction(table, "full")
                order_qt_new_write_current_policy "SELECT k, v FROM ${table} ORDER BY k"
                if (!isCloudMode()) {
                    qt_new_write_physical "SELECT ${physicalRows(table)}"
                }
            }
        }
    }

    sql "DROP TABLE IF EXISTS row_ttl_alter_direct"
    sql """CREATE TABLE row_ttl_alter_direct (k INT) DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ("replication_num"="1",
            "disable_auto_compaction"="true", "function_column.enable_row_ttl"="true")"""
    sql """INSERT INTO row_ttl_alter_direct(k, __DORIS_TTL_COL__)
        VALUES (1, 0), (2, 9223372036854775807), (3, NULL)"""
    trigger_and_wait_compaction("row_ttl_alter_direct", "full")
    order_qt_direct_single_gc "SELECT * FROM row_ttl_alter_direct ORDER BY k"
    if (!isCloudMode()) {
        qt_direct_physical "SELECT ${physicalRows('row_ttl_alter_direct')}"
    }
    test {
        sql """ALTER TABLE row_ttl_alter_direct SET ("function_column.ttl"="1 day")"""
        exception "function_column.ttl can only be changed on a source-column row TTL table"
    }
}
