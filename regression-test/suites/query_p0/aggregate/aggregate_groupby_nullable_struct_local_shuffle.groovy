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

suite("aggregate_groupby_nullable_struct_local_shuffle") {
    // IF(cond, NULL, named_struct(...)) yields logical NULL structs whose hidden field payloads
    // differ per row. GROUP BY must treat them as one group no matter how the rows are hashed
    // by the local shuffle before aggregation.
    def tableName = "agg_groupby_nullable_struct_ls"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE ${tableName} (
            pk INT NOT NULL
        )
        DUPLICATE KEY(pk)
        DISTRIBUTED BY HASH(pk) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        )
    """
    sql """ INSERT INTO ${tableName} VALUES (2), (7), (11) """

    def groupQuery = """
        SELECT k, COUNT(*) AS c
        FROM (
            SELECT IF(pk IN (2, 7), NULL,
                      named_struct('x', CASE WHEN pk = 2 THEN 0
                                             WHEN pk = 7 THEN -2147483648
                                             ELSE 1 END)) AS k
            FROM ${tableName}
        ) z
        GROUP BY k
    """
    def summaryQuery = """
        SELECT COUNT(*) AS groups_n, SUM(c) AS rows_n, MIN(c) AS min_c, MAX(c) AS max_c
        FROM (${groupQuery}) q
    """

    // Force a local hash shuffle on the struct key before the single-phase aggregation.
    sql """ SET enable_local_shuffle_planner = true """
    sql """ SET enable_query_cache = false """
    sql """ SET agg_phase = 1 """
    sql """ SET enable_bucketed_hash_agg = false """
    sql """ SET experimental_use_serial_exchange = true """
    sql """ SET ignore_storage_data_distribution = true """
    sql """ SET parallel_pipeline_task_num = 3 """

    sql """ SET enable_local_shuffle = true """
    sql """ SET enable_new_shuffle_hash_method = true """
    order_qt_local_shuffle_crc32c_groups "${groupQuery}"
    order_qt_local_shuffle_crc32c_summary "${summaryQuery}"

    sql """ SET enable_new_shuffle_hash_method = false """
    order_qt_local_shuffle_legacy_hash_groups "${groupQuery}"
    order_qt_local_shuffle_legacy_hash_summary "${summaryQuery}"

    sql """ SET enable_local_shuffle = false """
    order_qt_no_local_shuffle_groups "${groupQuery}"
    order_qt_no_local_shuffle_summary "${summaryQuery}"
}
