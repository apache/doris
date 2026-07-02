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

suite("test_ivm_binlog_order_1") {
    // Scenario 1: AGG MV with MOW base table → no FOJ
    sql """drop materialized view if exists test_ivm_bor_agg_mv;"""
    sql """drop table if exists test_ivm_bor_agg_t1;"""
    sql """
        CREATE TABLE test_ivm_bor_agg_t1 (k1 INT, v1 INT)
        UNIQUE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES ("replication_num"="1","binlog.enable"="true","binlog.format"="ROW",
            "binlog.need_historical_value"="true","enable_unique_key_merge_on_write"="true");
    """
    sql """
        CREATE MATERIALIZED VIEW test_ivm_bor_agg_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2 PROPERTIES ('replication_num'='1')
        AS SELECT k1, SUM(v1) AS total FROM test_ivm_bor_agg_t1 GROUP BY k1;
    """
    sql """INSERT INTO test_ivm_bor_agg_t1 VALUES (1, 10), (2, 20);"""
    explain {
        sql "LOGICAL PLAN REFRESH MATERIALIZED VIEW test_ivm_bor_agg_mv INCREMENTAL"
        notContains "type=FULL_OUTER_JOIN"
    }

    // Scenario 2: Non-AGG MV with DUP_KEYS base table → no FOJ
    sql """drop materialized view if exists test_ivm_bor_dup_mv;"""
    sql """drop table if exists test_ivm_bor_dup_t1;"""
    sql """
        CREATE TABLE test_ivm_bor_dup_t1 (k1 INT, v1 INT)
        DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES ("replication_num"="1","binlog.enable"="true","binlog.format"="ROW");
    """
    sql """
        CREATE MATERIALIZED VIEW test_ivm_bor_dup_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2 PROPERTIES ('replication_num'='1')
        AS SELECT k1, v1 FROM test_ivm_bor_dup_t1;
    """
    sql """INSERT INTO test_ivm_bor_dup_t1 VALUES (1, 10);"""
    explain {
        sql "LOGICAL PLAN REFRESH MATERIALIZED VIEW test_ivm_bor_dup_mv INCREMENTAL"
        notContains "type=FULL_OUTER_JOIN"
    }

    // Scenario 3: Non-AGG MV with MOW base table → should contain FOJ
    sql """drop materialized view if exists test_ivm_bor_mow_mv;"""
    sql """drop table if exists test_ivm_bor_mow_t1;"""
    sql """
        CREATE TABLE test_ivm_bor_mow_t1 (k1 INT, v1 INT)
        UNIQUE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES ("replication_num"="1","binlog.enable"="true","binlog.format"="ROW",
            "binlog.need_historical_value"="true","enable_unique_key_merge_on_write"="true");
    """
    sql """
        CREATE MATERIALIZED VIEW test_ivm_bor_mow_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2 PROPERTIES ('replication_num'='1')
        AS SELECT k1, v1 FROM test_ivm_bor_mow_t1;
    """
    sql """INSERT INTO test_ivm_bor_mow_t1 VALUES (1, 10);"""
    explain {
        sql "LOGICAL PLAN REFRESH MATERIALIZED VIEW test_ivm_bor_mow_mv INCREMENTAL"
        contains "type=FULL_OUTER_JOIN"
    }

    // Cleanup
    sql """drop materialized view if exists test_ivm_bor_agg_mv;"""
    sql """drop table if exists test_ivm_bor_agg_t1;"""
    sql """drop materialized view if exists test_ivm_bor_dup_mv;"""
    sql """drop table if exists test_ivm_bor_dup_t1;"""
}
