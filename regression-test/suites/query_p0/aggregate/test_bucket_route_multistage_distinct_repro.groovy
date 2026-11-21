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

suite("test_bucket_route_multistage_distinct_repro", "p0") {
    def tableName = "bucket_route_multistage_distinct_repro"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
            k1 VARCHAR(32) NOT NULL,
            k2 INT NOT NULL
        ) DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 3
        PROPERTIES ('replication_num'='1')
    """
    sql """
        INSERT INTO ${tableName}
        SELECT concat('key_', cast(number AS VARCHAR)), cast(number % 7 AS INT)
        FROM numbers('number'='128')
    """
    sql "SET enable_sql_cache=false"
    sql "SET enable_query_cache=false"
    sql "SET parallel_pipeline_task_num=8"
    sql "SET ignore_storage_data_distribution=true"
    sql "SET enable_local_exchange_before_agg=true"
    sql "SET enable_spill=false"

    def query = """
        SELECT COUNT(DISTINCT k1), COUNT(k1)
        FROM (SELECT DISTINCT k1, k2 FROM ${tableName}) s
    """
    def checkMatrix = { shape ->
        for (def streaming : [true, false]) {
            sql "SET enable_distinct_streaming_aggregation=${streaming}"
            for (def phase : [3, 4]) {
                sql "SET agg_phase=${phase}"
                for (def shuffle : [true, false]) {
                    sql "SET experimental_enable_local_shuffle=${shuffle}"
                    "order_qt_${shape}_phase${phase}_stream${streaming}_shuffle${shuffle}"(query)
                }
            }
        }
        sql "SET enable_distinct_streaming_aggregation=true"
        sql "SET experimental_enable_local_shuffle=true"
        sql "SET agg_phase=0"
        "order_qt_${shape}_automatic_phase"(query)
    }

    checkMatrix("unique_keys")
    sql """
        INSERT INTO ${tableName}
        SELECT concat('key_', cast(number AS VARCHAR)), cast(number % 7 + 7 AS INT)
        FROM numbers('number'='128')
    """
    sql """
        INSERT INTO ${tableName}
        SELECT concat('key_', cast(number AS VARCHAR)), cast(number % 7 AS INT)
        FROM numbers('number'='128')
    """
    checkMatrix("multiple_values")
    sql "DROP TABLE ${tableName}"
}
