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

suite("test_duplicate_table_hll") {

    for (def use_nereids : [true, false]) {
        if (use_nereids) {
            sql "set enable_nereids_planner=true"
            sql "set enable_fallback_to_original_planner=false"
            sql "set enable_nereids_dml=true;"
        } else {
            sql "set enable_nereids_planner=false"
            sql "set enable_nereids_dml=false;"
        }
        sql "sync;"

        def tbName = "test_duplicate_hll1"
        sql "DROP TABLE IF EXISTS ${tbName}"
        sql """ CREATE TABLE IF NOT EXISTS ${tbName} ( k int, v hll ) DUPLICATE KEY(k)
                DISTRIBUTED BY HASH(k) BUCKETS 1 properties("replication_num" = "1"); """

        def result = sql "show create table ${tbName}"
        logger.info("${result}")
        assertTrue(result.toString().containsIgnoreCase('`v` HLL NOT NULL'))

        def tbNameAgg = "test_duplicate_hll_agg1"
        sql "DROP TABLE IF EXISTS ${tbNameAgg}"
        sql """ CREATE TABLE IF NOT EXISTS ${tbNameAgg} ( k int, v hll hll_union ) AGGREGATE KEY(k)
                DISTRIBUTED BY HASH(k) BUCKETS 1 properties("replication_num" = "1"); """

        sql """ insert into ${tbNameAgg} values
                (1,hll_empty()),(2, hll_hash(100)),
                (2,hll_hash(0)),(2, hll_hash(4875)),
                (2,hll_hash(9234)),(2, hll_hash(45)),
                (2,hll_hash(0)),(2,hll_hash(100000)),
                (3,hll_hash(0)),(3,hll_hash(1)); """
        qt_sql "select k, hll_cardinality(v) from ${tbNameAgg} order by k;"
        qt_sql "select HLL_UNION_AGG(v) from ${tbNameAgg};"

        // 1. insert from aggregate table
        sql "insert into ${tbName} select * from ${tbNameAgg};"
        qt_from_agg "select k, hll_cardinality(v) from ${tbName} order by k, hll_cardinality(v);"
        qt_from_agg "select HLL_UNION_AGG(v) from ${tbName};"
        // 2. insert into values
        sql """ insert into ${tbName} values (4, hll_hash(100)), (1, hll_hash(999)), (2, hll_hash(0));"""
        qt_from_values "select k, hll_cardinality(v) from ${tbName} order by k, hll_cardinality(v);"
        qt_from_values "select k, hll_cardinality(hll_union(v)) from ${tbName} group by k order by k, hll_cardinality(hll_union(v));"
        qt_from_values "select HLL_UNION_AGG(v) from ${tbName};"
        // 3. insert from duplicate table
        sql "insert into ${tbName} select * from ${tbName};"
        qt_from_dup "select k, hll_cardinality(v) from ${tbName} order by k, hll_cardinality(v);"
        qt_from_dup "select k, hll_cardinality(hll_union(v)) from ${tbName} group by k order by k, hll_cardinality(hll_union(v));"
        qt_from_dup "select HLL_UNION_AGG(v) from ${tbName};"

        sql "DROP TABLE IF EXISTS ${tbName};"
        sql "DROP TABLE IF EXISTS ${tbNameAgg};"


        tbName = "test_duplicate_hll3"
        sql "DROP TABLE IF EXISTS ${tbName}"
        test {
            sql """ CREATE TABLE IF NOT EXISTS ${tbName} (k hll, v int) DUPLICATE KEY(k)
                DISTRIBUTED BY HASH(k) BUCKETS 1 properties("replication_num" = "1"); """
            exception "Key column can not set complex type:k"
        }
    }
}
