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

suite("test_legacy_merge_agg_bucket_route") {
    sql "DROP TABLE IF EXISTS legacy_merge_agg_bucket_route"
    sql """
        CREATE TABLE legacy_merge_agg_bucket_route (
            k1 VARCHAR(32) NOT NULL,
            k2 INT NOT NULL
        ) DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 3
        PROPERTIES ("replication_num" = "1")
    """

    sql "SET enable_sql_cache=false"
    sql "SET enable_query_cache=false"
    sql "SET parallel_pipeline_task_num=8"
    sql "SET ignore_storage_data_distribution=true"
    sql "SET enable_local_exchange_before_agg=true"
    sql "SET enable_bucketed_hash_agg=false"
    sql "SET enable_spill=false"

    def query = """
        SELECT COUNT(DISTINCT k1), COUNT(k1)
        FROM (SELECT DISTINCT k1, k2 FROM legacy_merge_agg_bucket_route) s
    """
    def modes = [
        [name: "legacy", shuffle: true, planner: false],
        [name: "no_shuffle", shuffle: false, planner: false],
        [name: "fe", shuffle: true, planner: true]
    ]

    for (def data : ["unique", "repeated"]) {
        sql "TRUNCATE TABLE legacy_merge_agg_bucket_route"
        if (data == "unique") {
            sql """
                INSERT INTO legacy_merge_agg_bucket_route
                SELECT concat('key_', cast(number AS VARCHAR)), cast(number % 7 AS INT)
                FROM numbers('number'='128')
            """
        } else {
            // Each k1 has three distinct k2 values, and each pair occurs twice.
            sql """
                INSERT INTO legacy_merge_agg_bucket_route
                SELECT concat('key_', cast(number % 128 AS VARCHAR)), cast(number % 3 AS INT)
                FROM numbers('number'='768')
            """
        }

        for (def phase : [3, 4]) {
            sql "SET agg_phase=${phase}"
            for (def streaming : [true, false]) {
                sql "SET enable_distinct_streaming_aggregation=${streaming}"
                for (def mode : modes) {
                    sql "SET experimental_enable_local_shuffle=${mode.shuffle}"
                    sql "SET experimental_enable_local_shuffle_planner=${mode.planner}"
                    "order_qt_${data}_p${phase}_d${streaming}_m${mode.name}"(query)
                }
            }
        }
    }

    sql "SET agg_phase=0"
    sql "SET enable_distinct_streaming_aggregation=true"
    sql "SET experimental_enable_local_shuffle=true"
    sql "SET experimental_enable_local_shuffle_planner=false"
    order_qt_phase0_legacy query

    sql "SET agg_phase=3"
    sql "SET parallel_pipeline_task_num=1"
    order_qt_single_task_legacy query

    sql "SET parallel_pipeline_task_num=8"
    sql "SET ignore_storage_data_distribution=false"
    order_qt_storage_distribution_legacy query
}
