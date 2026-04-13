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

suite("test_ivm_agg_4") {

    // =========================================================
    // Part 15: Expressions in agg arguments — SUM(v1 + v2), MIN(v1 * 2)
    //          IVM does NOT support complex expressions inside agg functions
    //          (only bare column Slots are allowed after NormalizeAggregate).
    //          Verify that CREATE MV with INCREMENTAL correctly rejects this.
    // =========================================================

    sql """drop materialized view if exists test_ivm_agg_mtmv_expr_mv;"""
    sql """drop table if exists test_ivm_agg_mtmv_expr_base;"""

    sql """
        CREATE TABLE test_ivm_agg_mtmv_expr_base (
            k1 INT,
            grp INT,
            v1 INT,
            v2 INT,
            binlog_op TINYINT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    test {
        sql """
            CREATE MATERIALIZED VIEW test_ivm_agg_mtmv_expr_mv
            BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES (
                'replication_num' = '1'
            )
            AS SELECT grp, SUM(v1 + v2) AS sum_add, MIN(v1 * 2) AS min_double, COUNT(*) AS cnt
               FROM test_ivm_agg_mtmv_expr_base
               GROUP BY grp;
        """
        exception "aggregate argument must be a Slot"
    }
}
