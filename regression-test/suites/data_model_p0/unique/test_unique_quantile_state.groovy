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

suite("test_unique_table_quantile_state") {

    for (def enable_mow : [true, false]) {
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

            def tbName = "test_unique_quantile_state1"
            sql "DROP TABLE IF EXISTS ${tbName}"
            sql """ CREATE TABLE IF NOT EXISTS ${tbName} ( k int, v QUANTILE_STATE ) UNIQUE KEY(k)
                    DISTRIBUTED BY HASH(k) BUCKETS 1 properties("replication_num" = "1", "enable_unique_key_merge_on_write" = "${enable_mow}"); """

            def result = sql "show create table ${tbName}"
            logger.info("${result}")
            assertTrue(result.toString().containsIgnoreCase('`v` QUANTILE_STATE NOT NULL'))

            def tbNameAgg = "test_unique_quantile_state_agg1"
            sql "DROP TABLE IF EXISTS ${tbNameAgg}"
            sql """ CREATE TABLE IF NOT EXISTS ${tbNameAgg} ( k int, v QUANTILE_STATE QUANTILE_UNION NOT NULL ) AGGREGATE KEY(k)
                    DISTRIBUTED BY HASH(k) BUCKETS 1 properties("replication_num" = "1"); """

            sql """ insert into ${tbNameAgg} values
                    (1,to_quantile_state(-1, 2048)),(2,to_quantile_state(0, 2048)),
                    (2,to_quantile_state(1, 2048)),(3,to_quantile_state(0, 2048)),
                    (3,to_quantile_state(1, 2048)),(3,to_quantile_state(2, 2048));"""
            qt_sql "select k, quantile_percent(v, 0), quantile_percent(v, 0.5), quantile_percent(v, 1) from ${tbNameAgg} order by k;"

            // 1. insert from aggregate table
            sql "insert into ${tbName} select * from ${tbNameAgg};"
            qt_from_agg "select k, quantile_percent(v, 0) c1, quantile_percent(v, 0.5) c2, quantile_percent(v, 1) c3 from ${tbName} order by k, c1, c2, c3;"
            // 2. insert into values
            sql """ insert into ${tbName} values (1, to_quantile_state(-2, 2048)), (1, to_quantile_state(0, 2048)), (2, to_quantile_state(-100, 2048));"""
            qt_from_values "select k, quantile_percent(v, 0) c1, quantile_percent(v, 0.5) c2, quantile_percent(v, 1) c3 from ${tbName} order by k, c1, c2, c3;"
            qt_from_values """ select k, quantile_percent(QUANTILE_UNION(v), 0) c1, quantile_percent(QUANTILE_UNION(v), 0.5) c2, quantile_percent(QUANTILE_UNION(v), 1) c3
                    from ${tbName} group by k order by k, c1, c2, c3; """
            // 3. insert from UNIQUE table
            sql "insert into ${tbName} select * from ${tbName};"
            qt_from_uniq "select k, quantile_percent(v, 0) c1, quantile_percent(v, 0.5) c2, quantile_percent(v, 1) c3 from ${tbName} order by k, c1, c2, c3;"
            qt_from_uniq """ select k, quantile_percent(QUANTILE_UNION(v), 0) c1, quantile_percent(QUANTILE_UNION(v), 0.5) c2, quantile_percent(QUANTILE_UNION(v), 1) c3
                    from ${tbName} group by k order by k, c1, c2, c3; """

            sql "DROP TABLE IF EXISTS ${tbName};"
            sql "DROP TABLE IF EXISTS ${tbNameAgg};"


            tbName = "test_unique_quantile_state3"
            sql "DROP TABLE IF EXISTS ${tbName}"
            test {
                sql """ CREATE TABLE IF NOT EXISTS ${tbName} (k QUANTILE_STATE, v int) UNIQUE KEY(k)
                    DISTRIBUTED BY HASH(k) BUCKETS 1 properties("replication_num" = "1"); """
                exception "Key column can not set complex type:k"
            }
        }
    }
}
