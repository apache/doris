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

suite("test_point_query_sequence_visibility", "p0,nonConcurrent") {
    def originalInsertStrict = sql("SHOW VARIABLES LIKE 'enable_insert_strict'")[0][1]
    def originalPartialUpdate = sql("SHOW VARIABLES LIKE 'enable_unique_key_partial_update'")[0][1]
    for (def sinkMemtable : [false, true]) {
        def table = "point_query_sequence_${sinkMemtable}"
        sql "SET enable_memtable_on_sink_node=${sinkMemtable}"
        sql "DROP TABLE IF EXISTS ${table}"
        sql """CREATE TABLE ${table} (
            k INT NOT NULL, seq BIGINT NOT NULL, v BIGINT, payload STRING DEFAULT 'default'
        ) UNIQUE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 32
        PROPERTIES("replication_num"="1", "enable_unique_key_merge_on_write"="true",
                   "store_row_column"="true", "function_column.sequence_col"="seq")"""
        // Most writers in the partition receive only EOS on each load.
        for (int key = 1; key <= 8; key++) {
            sql "INSERT INTO ${table} VALUES (${key}, 1, ${key * 10}, 'initial')"
        }
        this."order_qt_sequence_1_${sinkMemtable}"("SELECT COUNT(*), SUM(v) FROM ${table}")
        sql "INSERT INTO ${table} VALUES (1, 2, 100, 'newer')"
        sql "INSERT INTO ${table} VALUES (1, 1, 999, 'older')"
        explain {
            sql "SELECT seq,v,payload FROM ${table} WHERE k=1"
            contains "SHORT-CIRCUIT"
        }
        this."order_qt_sequence_2_${sinkMemtable}"("SELECT seq,v,payload FROM ${table} WHERE k=1")
        try {
            sql "SET enable_unique_key_partial_update=true"
            sql "SET enable_insert_strict=false"
            sql "INSERT INTO ${table}(k,seq,v) VALUES (1,3,111),(9,3,90)"
        } finally {
            sql "SET enable_insert_strict=${originalInsertStrict}"
            sql "SET enable_unique_key_partial_update=${originalPartialUpdate}"
        }
        this."order_qt_sequence_3_${sinkMemtable}"("SELECT seq,v,payload FROM ${table} WHERE k=1")
        this."order_qt_sequence_4_${sinkMemtable}"("SELECT seq,v,payload FROM ${table} WHERE k=9")
        sql "DELETE FROM ${table} WHERE k=2"
        this."order_qt_sequence_5_${sinkMemtable}"("SELECT * FROM ${table} WHERE k=2")
        sql "INSERT INTO ${table} VALUES(2,4,222,'reinserted')"
        this."order_qt_sequence_6_${sinkMemtable}"("SELECT seq,v,payload FROM ${table} WHERE k=2")
        sql "INSERT INTO ${table} SELECT * FROM ${table} WHERE k<0"
        this."order_qt_sequence_7_${sinkMemtable}"("SELECT COUNT(*),SUM(v) FROM ${table}")
        def visibleVersion = sql_return_maparray("SHOW PARTITIONS FROM ${table}")[0].VisibleVersion as long
        def tablets = sql_return_maparray("SHOW TABLETS FROM ${table}")
        assertEquals(32, tablets.size())
        tablets.each { tablet ->
            assertTrue((tablet.Version as long) >= visibleVersion,
                    "tablet ${tablet.TabletId} did not publish version ${visibleVersion}: ${tablet}")
        }
    }
}
