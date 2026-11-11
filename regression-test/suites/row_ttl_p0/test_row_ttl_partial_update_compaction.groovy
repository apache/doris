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

suite("test_row_ttl_partial_update_compaction", "nonConcurrent") {
    for (boolean vertical : [false, true]) {
        setBeConfigTemporary([enable_vertical_compaction: vertical,
                              vertical_compaction_num_columns_per_group: 1]) {
            for (boolean sourceColumn : [false, true]) {
                for (String compact : ["none", "cumulative", "full"]) {
                    sql "DROP TABLE IF EXISTS row_ttl_partial_compaction"
                    def ttlProperties = sourceColumn ? ', "function_column.ttl_col"="event_time", "function_column.ttl"="0", "function_column.ttl_time_zone"="+00:00"' : ''
                    sql """
                        CREATE TABLE row_ttl_partial_compaction (
                            k INT,
                            event_time DATETIMEV2(6) DEFAULT '2099-01-01 00:00:00',
                            v INT,
                            payload INT DEFAULT '99'
                        ) UNIQUE KEY(k)
                        DISTRIBUTED BY HASH(k) BUCKETS 1
                        PROPERTIES ("replication_num"="1", "disable_auto_compaction"="true",
                            "enable_unique_key_merge_on_write"="true",
                            "enable_unique_key_skip_bitmap_column"="true",
                            "function_column.enable_row_ttl"="true" ${ttlProperties})
                    """
                    for (int key : 1..5) {
                        if (sourceColumn) {
                            sql "INSERT INTO row_ttl_partial_compaction VALUES (${key}, '2000-01-01', 0, ${key})"
                        } else {
                            sql """INSERT INTO row_ttl_partial_compaction(k,event_time,v,payload,__DORIS_TTL_COL__)
                                VALUES (${key}, '2000-01-01', 0, ${key}, 0)"""
                        }
                    }
                    if (compact != "none") {
                        trigger_and_wait_compaction("row_ttl_partial_compaction", compact)
                    }
                    def physicalRows = {
                        def metaUrl = sql_return_maparray("SHOW TABLETS FROM row_ttl_partial_compaction")[0].MetaUrl
                        def (code, out, err) = curl("GET", metaUrl)
                        assertEquals(0, code)
                        parseJson(out.trim()).rs_metas.sum { it.num_rows as long }
                    }
                    def rowsBeforeRejection = isCloudMode() ? null : physicalRows()
                    def ttlColumn = sourceColumn ? "event_time" : "__DORIS_TTL_COL__"
                    def ttlValue = sourceColumn ? "'2099-01-01'" : "4102444800000000"
                    try {
                        sql "SET enable_unique_key_partial_update = true"
                        test {
                            sql "INSERT INTO row_ttl_partial_compaction(k,v) VALUES (1,10),(6,60)"
                            exception "Partial update is not supported on tables with row TTL"
                        }
                        test {
                            sql "INSERT INTO row_ttl_partial_compaction(k,v) SELECT 1,10"
                            exception "Partial update is not supported on tables with row TTL"
                        }
                        test {
                            sql "INSERT INTO row_ttl_partial_compaction(k,${ttlColumn}) VALUES (1,${ttlValue})"
                            exception "Partial update is not supported on tables with row TTL"
                        }
                    } finally {
                        sql "SET enable_unique_key_partial_update = false"
                    }
                    for (boolean updatesTtl : [false, true]) {
                        for (String mode : ["partial_columns", "UPDATE_FIXED_COLUMNS", "UPDATE_FLEXIBLE_COLUMNS"]) {
                            streamLoad {
                                table "row_ttl_partial_compaction"
                                set 'format', 'json'
                                set 'read_json_by_line', 'true'
                                if (mode == "partial_columns") {
                                    set 'partial_columns', 'true'
                                } else {
                                    set 'unique_key_update_mode', mode
                                }
                                if (mode != "UPDATE_FLEXIBLE_COLUMNS") {
                                    set 'columns', updatesTtl ? "k,${ttlColumn}" : 'k,v'
                                }
                                def jsonValue = sourceColumn ? '"2099-01-01"' : '4102444800000000'
                                def input = updatesTtl
                                        ? "{\"k\":2,\"${ttlColumn}\":${jsonValue}}\n{\"k\":7,\"${ttlColumn}\":${jsonValue}}\n"
                                        : '{"k":2,"v":20}\n{"k":7,"v":70}\n'
                                inputStream new ByteArrayInputStream(input.getBytes())
                                time 10000
                                check { result, exception, startTime, endTime ->
                                    if (exception != null) { throw exception }
                                    def response = parseJson(result)
                                    assertEquals("Fail", response.Status)
                                    assertTrue(response.Message.contains(
                                        "Partial update is not supported on tables with row TTL"))
                                }
                            }
                        }
                    }
                    if (!isCloudMode()) {
                        "qt_rejected_unchanged_${vertical}_${sourceColumn}_${compact}"(
                            "SELECT ${rowsBeforeRejection} = ${physicalRows()} AS unchanged")
                    }
                    "order_qt_rejected_${vertical}_${sourceColumn}_${compact}"(
                        "SELECT k,event_time,v,payload FROM row_ttl_partial_compaction ORDER BY k")
                    // Ordinary UPSERT supplies defaults independently of the reclaimed history.
                    sql "INSERT INTO row_ttl_partial_compaction(k,v) VALUES (1,10),(6,60)"
                    sql "UPDATE row_ttl_partial_compaction SET payload=7 WHERE k=1"
                    sql "UPDATE row_ttl_partial_compaction SET v=v+1 WHERE k=1"
                    // Force the query-based DELETE path rather than the light delete optimization.
                    sql "DELETE FROM row_ttl_partial_compaction WHERE abs(k)=6"
                    "order_qt_full_write_${vertical}_${sourceColumn}_${compact}"(
                        "SELECT k,event_time,v,payload FROM row_ttl_partial_compaction ORDER BY k")
                    trigger_and_wait_compaction("row_ttl_partial_compaction", "full")
                    if (!isCloudMode()) {
                        "qt_physical_gc_${vertical}_${sourceColumn}_${compact}"(
                            "SELECT ${physicalRows()} AS physical_rows")
                    }
                }
            }
        }
    }

    sql "DROP TABLE IF EXISTS row_ttl_partial_rollup"
    sql """CREATE TABLE row_ttl_partial_rollup (
        k INT, sub_k INT, event_time DATETIMEV2(6) DEFAULT '2099-01-01 00:00:00', v INT
    ) UNIQUE KEY(k, sub_k) DISTRIBUTED BY HASH(k) BUCKETS 1
    PROPERTIES ("replication_num"="1", "disable_auto_compaction"="true",
        "enable_unique_key_merge_on_write"="true", "function_column.enable_row_ttl"="true",
        "function_column.ttl_col"="event_time", "function_column.ttl"="0",
        "function_column.ttl_time_zone"="+00:00")"""
    def baseTablet = sql_return_maparray("SHOW TABLETS FROM row_ttl_partial_rollup")[0].TabletId
    sql "ALTER TABLE row_ttl_partial_rollup ADD ROLLUP row_ttl_partial_mv(sub_k,k,v)"
    waitingMVTaskFinishedByMvName(context.dbName, "row_ttl_partial_rollup", "row_ttl_partial_mv")
    sql "INSERT INTO row_ttl_partial_rollup VALUES (1,1,'2000-01-01',10)"
    sql "INSERT INTO row_ttl_partial_rollup VALUES (2,2,'2099-01-01',20)"
    // Compact only the base index; the rollup deliberately keeps its original rowsets.
    trigger_and_wait_compaction("row_ttl_partial_rollup", "full", 300, [] as String[], [baseTablet])
    try {
        sql "SET enable_unique_key_partial_update = true"
        test {
            sql "INSERT INTO row_ttl_partial_rollup(k,sub_k,v) VALUES (1,1,11)"
            exception "Partial update is not supported on tables with row TTL"
        }
    } finally {
        sql "SET enable_unique_key_partial_update = false"
    }
    sql "UPDATE row_ttl_partial_rollup SET v=v+1 WHERE k=2"
    explain {
        sql "SELECT /*+ NO_USE_MV(row_ttl_partial_rollup.row_ttl_partial_mv) */ k,v FROM row_ttl_partial_rollup"
        contains "row_ttl_partial_rollup(row_ttl_partial_rollup)"
    }
    explain {
        sql "SELECT /*+ USE_MV(row_ttl_partial_rollup.row_ttl_partial_mv) */ k,v FROM row_ttl_partial_rollup"
        contains "row_ttl_partial_rollup(row_ttl_partial_mv)"
    }
    order_qt_base "SELECT /*+ NO_USE_MV(row_ttl_partial_rollup.row_ttl_partial_mv) */ k,v FROM row_ttl_partial_rollup ORDER BY k"
    order_qt_rollup """SELECT /*+ USE_MV(row_ttl_partial_rollup.row_ttl_partial_mv) */
        k,v FROM row_ttl_partial_rollup ORDER BY k"""
}
