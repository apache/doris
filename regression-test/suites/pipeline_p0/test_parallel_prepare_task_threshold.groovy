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

suite("test_parallel_prepare_task_threshold") {
    sql "DROP TABLE IF EXISTS test_parallel_prepare_task_threshold"
    sql """
        CREATE TABLE test_parallel_prepare_task_threshold (k INT, v INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 4
        PROPERTIES("replication_num" = "1")
    """
    sql """
        INSERT INTO test_parallel_prepare_task_threshold
        SELECT number, number * 10 FROM numbers("number" = "32")
    """

    sql "SET parallel_pipeline_task_num = 4"
    sql "SET enable_local_shuffle = true"
    sql "SET enable_local_shuffle_planner = true"
    def query = """
        WITH grouped AS (
            SELECT k % 4 AS k, COUNT(*) AS n, SUM(v) AS total
            FROM test_parallel_prepare_task_threshold GROUP BY k % 4
        )
        SELECT l.k, l.n, r.total FROM grouped l JOIN grouped r ON l.k = r.k
        UNION ALL
        SELECT r.k, r.n, l.total FROM grouped l JOIN grouped r ON l.k = r.k
    """

    // Four instances used to remain serial at threshold 4, regardless of pipeline count.
    // Keep both planner modes covered because either FE or BE can insert local exchanges.
    sql "SET parallel_prepare_threshold = 4"
    order_qt_fe_planned query
    sql "SET enable_local_shuffle_planner = false"
    order_qt_be_planned query

    sql "SET parallel_prepare_threshold = 0"
    order_qt_always_parallel query
    sql "SET parallel_prepare_threshold = 100000"
    order_qt_serial query

    sql "SET parallel_pipeline_task_num = 1"
    sql "SET parallel_prepare_threshold = 0"
    order_qt_single_instance query
}
