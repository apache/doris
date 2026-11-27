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

suite("topn_virtual_slot_on_overlapping_segments", "nonConcurrent") {
    def tableName = "topn_vslot_overlap"

    def backendIdToIp = [:]
    def backendIdToHttpPort = [:]
    getBackendIpHttpPort(backendIdToIp, backendIdToHttpPort)
    backendIdToIp.each { backendId, beIp ->
        def bePort = backendIdToHttpPort.get(backendId)
        def (code, out, err) = show_be_config(beIp, bePort)
        assertEquals(0, code)
        def configList = parseJson(out.trim())
        def writeBufferSize = configList.find { it[0] == "write_buffer_size" }
        assertTrue(writeBufferSize != null, "write_buffer_size is not reported by BE")
        assertTrue(Long.parseLong(writeBufferSize[2]) <= 209715200L,
                "write_buffer_size must be <= 209715200")
    }

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
            key_col INT NOT NULL,
            filter_col INT NOT NULL,
            value_col INT NOT NULL,
            padding_col STRING
        ) DUPLICATE KEY(key_col)
        DISTRIBUTED BY HASH(key_col) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "disable_auto_compaction" = "true"
        )
    """

    sql "SET experimental_enable_virtual_slot_for_cse = true"
    sql "SET enable_segment_limit_pushdown = true"
    sql "SET enable_sql_cache = false"

    // Ensure create multiple memtable segments with the default write buffer size.
    sql """
        INSERT INTO ${tableName}
        SELECT
            CAST((number * 7919) % 1000000 AS INT),
            CAST(number % 1000 AS INT),
            CAST(number % 1000 AS INT),
            repeat(CAST(number AS STRING), 2048)
        FROM numbers("number" = "100000")
    """

    sql "DELETE FROM ${tableName} WHERE filter_col = 100000"

    def tablets = sql_return_maparray("SHOW TABLETS FROM ${tableName}")
    assertEquals(1, tablets.size())
    def (code, out, err) = curl("GET", tablets[0].MetaUrl)
    assertEquals(0, code)
    def metadata = parseJson(out.trim())
    def overlappingRowset = metadata.rs_metas.find {
        it.segments_overlap_pb == "OVERLAPPING" && it.num_segments > 1
    }
    assertTrue(overlappingRowset != null,
            "expected a multi-segment OVERLAPPING rowset with default BE configuration")

    def query = """
        SELECT key_col, key_col + value_col AS computed_value
        FROM ${tableName}
        WHERE (key_col + value_col) > 3 AND (key_col + value_col) < 1000000
        ORDER BY key_col ASC
        LIMIT 20
    """

    explain {
        sql query
        verbose true
        contains "SORT LIMIT: 20"
        contains "__DORIS_VIRTUAL_COL__"
    }

    qt_topn_virtual_slot query

    // Supplementary regression test for #60772.
    qt_select_all """
        SELECT *
        FROM ${tableName}
        ORDER BY key_col ASC
        LIMIT 10
    """
}
