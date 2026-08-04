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

// Regression test for issue #66030: a short-circuit point query on a table
// partitioned by LIST used to throw IllegalStateException because the tablet
// list was not pruned to a single tablet when the matched partition contained
// more than one tablet. The legacy distribution-prune path taken for point
// queries could not prune (no column filters for the nereids-planned conjuncts),
// so PointQueryExecutor.setScanRangeLocations hit its single-tablet checkState
// with an un-pruned list. With multiple buckets per partition the bug surfaces.

suite("test_point_query_list_partition") {
    def realDb = "regression_test_serving_p0"
    def tableName = realDb + ".tbl_point_query_list_partition"
    sql "CREATE DATABASE IF NOT EXISTS ${realDb}"

    sql """DROP TABLE IF EXISTS ${tableName}"""
    sql """
              CREATE TABLE IF NOT EXISTS ${tableName} (
                `pk` varchar(64) NOT NULL,
                `_id` bigint NOT NULL
              ) ENGINE=OLAP
              UNIQUE KEY(`pk`, `_id`)
              PARTITION BY LIST(`pk`) (
                  PARTITION `p_abcd` VALUES IN ("abcd"),
                  PARTITION `p_efgh` VALUES IN ("efgh")
              )
              DISTRIBUTED BY HASH(`pk`, `_id`) BUCKETS 3
              PROPERTIES (
              "replication_allocation" = "tag.location.default: 1",
              "store_row_column" = "true",
              "enable_unique_key_merge_on_write" = "true",
              "light_schema_change" = "true",
              "storage_format" = "V2")
              """

    sql """INSERT INTO ${tableName} VALUES ('abcd', 1)"""
    sql """INSERT INTO ${tableName} VALUES ('abcd', 2)"""
    sql """INSERT INTO ${tableName} VALUES ('efgh', 1)"""

    // The point query covers the full unique key, so it must be planned as a
    // short-circuit point query. Confirm the short-circuit path is taken so
    // this test actually exercises the buggy code path.
    def explain = sql """EXPLAIN SELECT * FROM ${tableName} WHERE pk = 'abcd' AND _id = 1"""
    assertTrue(explain.toString().contains("SHORT-CIRCUIT"))

    // Before the fix this threw: (1105, 'IllegalStateException, msg: null').
    // The point query must succeed and return exactly the matched row.
    def result1 = sql """SELECT * FROM ${tableName} WHERE pk = 'abcd' AND _id = 1"""
    assertEquals(1, result1.size())

    // Point query hitting a different LIST partition must also succeed.
    def result2 = sql """SELECT * FROM ${tableName} WHERE pk = 'efgh' AND _id = 1"""
    assertEquals(1, result2.size())

    // A point query that matches no row should return zero rows, not throw.
    def result3 = sql """SELECT * FROM ${tableName} WHERE pk = 'abcd' AND _id = 999"""
    assertEquals(0, result3.size())

    sql """DROP TABLE IF EXISTS ${tableName}"""
}
