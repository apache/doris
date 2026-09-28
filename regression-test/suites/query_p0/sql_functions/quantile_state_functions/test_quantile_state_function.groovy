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

suite("test_quantile_state_function") {
    qt_sql_quantile_state_base64_1 """
        select quantile_state_to_base64(quantile_state_empty())
    """

    qt_sql_quantile_state_base64_2 """
        select quantile_state_from_base64(null)
    """

    qt_sql_quantile_state_base64_3 """
        select quantile_state_to_base64(
            quantile_state_from_base64(
                quantile_state_to_base64(quantile_state_empty())
            )
        ) = quantile_state_to_base64(quantile_state_empty())
    """

    qt_sql_quantile_state_base64_4 """
        select quantile_state_to_base64(
            quantile_state_from_base64(
                quantile_state_to_base64(to_quantile_state(1.0, 2048))
            )
        ) = quantile_state_to_base64(to_quantile_state(1.0, 2048))
    """

    qt_sql_quantile_state_base64_5 """
        select quantile_state_to_base64(to_quantile_state(1.0, 2048))
    """

    qt_sql_quantile_state_base64_6 """
        select quantile_state_from_base64('invalid')
    """

    qt_sql_quantile_state_base64_7 """
        select quantile_state_from_base64('not_base64!')
    """

    qt_sql_quantile_state_base64_8 """
        select quantile_state_from_base64('')
    """

    qt_sql_quantile_state_base64_9 """
        select length(quantile_state_to_base64(to_quantile_state(1.0, 2048))) > 0
    """

    qt_sql_quantile_state_base64_10 """
        select quantile_state_from_base64(quantile_state_to_base64(null))
    """

    qt_sql_quantile_state_base64_11 """
        select quantile_state_to_base64(to_quantile_state(10.0, 2048))
    """

    qt_sql_quantile_state_base64_12 """
         select quantile_state_to_base64(
            quantile_state_from_base64(
                quantile_state_to_base64(
                    quantile_state_from_base64(
                        quantile_state_to_base64(to_quantile_state(10.0, 2048))
                    )
                )
            )
        ) = quantile_state_to_base64(to_quantile_state(10.0, 2048))
    """

    sql "DROP TABLE IF EXISTS test_quantile_state_cow"
    sql """
        CREATE TABLE test_quantile_state_cow (
            topic INT NOT NULL,
            chunk INT NOT NULL,
            q QUANTILE_STATE NOT NULL
        ) DUPLICATE KEY(topic, chunk)
        DISTRIBUTED BY HASH(topic, chunk) BUCKETS 4
        PROPERTIES("replication_num" = "1")
    """
    // Each stored state has 4096 inputs, exceeding the EXPLICIT-state limit.
    sql """
        INSERT INTO test_quantile_state_cow
        SELECT number % 2, (number DIV 2) % 4,
               quantile_union(to_quantile_state(10 + 100 * (number % 2), 2048))
        FROM numbers("number" = "32768")
        GROUP BY 1, 2
    """

    def query = """
        WITH shared AS (SELECT * FROM test_quantile_state_cow),
        topics AS (SELECT topic, quantile_union(q) AS q FROM shared GROUP BY topic),
        total AS (SELECT quantile_union(q) AS q FROM shared)
        SELECT topic, quantile_percent(topics.q, 0), quantile_percent(topics.q, 0.5),
               quantile_percent(topics.q, 1),
               quantile_percent(total.q, 0), quantile_percent(total.q, 1)
        FROM topics CROSS JOIN total ORDER BY topic
    """
    sql "SET enable_cte_materialize = false"
    qt_cow_independent query
    sql "SET enable_cte_materialize = true"
    sql "SET inline_cte_referenced_threshold = 0"
    explain {
        sql(query)
        contains "MultiCastDataSinks"
    }
    // Both plans are checked against fixed results: topic values are 10 or 110.
    qt_cow_shared query

    qt_cow_window """
        SELECT topic, chunk, quantile_percent(q, 0), quantile_percent(q, 1)
        FROM (
            SELECT topic, chunk,
                   quantile_union(q) OVER (ORDER BY topic, chunk
                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS q
            FROM test_quantile_state_cow
        ) t ORDER BY topic, chunk
    """

    // Each state has 4096 samples. Process the shared 10 state first so both
    // groups start from it before group 1 merges 110; group 0 must stay at 10.
    qt_cow_join_expand """
        SELECT g.k, quantile_percent(quantile_union(t.q), 1)
        FROM (
            SELECT number % 2 AS id,
                   quantile_union(to_quantile_state(10 + 100 * (number % 2), 2048)) AS q
            FROM numbers("number" = "8192") GROUP BY 1 ORDER BY 1 LIMIT 2
        ) t
        JOIN (SELECT 0 AS k UNION ALL SELECT 1) g ON t.id = 0 OR g.k = 1
        GROUP BY g.k ORDER BY g.k
    """
    qt_cow_explode_expand """
        SELECT k, quantile_percent(quantile_union(q), 1)
        FROM (
            SELECT number % 2 AS id,
                   quantile_union(to_quantile_state(10 + 100 * (number % 2), 2048)) AS q
            FROM numbers("number" = "8192") GROUP BY 1 ORDER BY 1 LIMIT 2
        ) t
        LATERAL VIEW explode(IF(id = 0, [0, 1], [1])) e AS k
        GROUP BY k ORDER BY k
    """
}
