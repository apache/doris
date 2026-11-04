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

suite("test_date_expression_filter", "p0,external,hive,external_docker") {
    if (!context.config.otherConfigs.get("enableHiveTest")?.equalsIgnoreCase("true")) {
        logger.info("Hive external tests are disabled")
        return
    }
    def host = context.config.otherConfigs.get("externalEnvIp")
    def port = context.config.otherConfigs.get("hive2HdfsPort")
    def defaultFS = "hdfs://${host}:${port}"
    def rows = """SELECT CAST(1 AS INT) AS id,
                        CAST('9999-12-31 23:59:59' AS DATETIME(6)) AS dt,
                        CAST('2025-01-01' AS DATETIME(6)) AS filter_dt
                  UNION ALL SELECT 2, CAST('2020-01-01' AS DATETIME(6)),
                                       CAST('2020-01-01' AS DATETIME(6))"""
    sql "set short_circuit_evaluation=true"
    for (def format : ["orc", "parquet"]) {
        def path = "${defaultFS}/user/doris/tmp_data/test_date_expression_filter/${UUID.randomUUID()}/exp_"
        def exported = sql """SELECT * FROM (${rows}) AS input
            INTO OUTFILE "${path}" FORMAT AS ${format}
            PROPERTIES("fs.defaultFS"="${defaultFS}", "hadoop.username"="doris")"""
        def source = """HDFS("uri"="${exported[0][3]}",
                             "hadoop.username"="doris", "format"="${format}")"""
        for (def function : ["WEEKS_ADD", "HOURS_ADD", "SECOND_CEIL", "YEAR_CEIL", "DAYS_ADD"]) {
            // The outer predicate must not evaluate a partial date function before the inner WHERE.
            order_qt_filtered_date """WITH cte1 AS (
                SELECT CAST(${function}(dt,5) AS DATETIME) AS d,
                       APPROX_COUNT_DISTINCT(id) AS n
                FROM ${source}
                WHERE filter_dt <= (SELECT MIN('2024-07-01') FROM ${source})
                  AND CAST(WEEKS_SUB(filter_dt,0) AS DATETIME)='2012-05-05 11:11:11'
                GROUP BY d ORDER BY d,n
            ), cte2 AS (SELECT * FROM cte1 WHERE d='2019-10-23 16:50:42')
            SELECT * FROM cte2"""
        }
        order_qt_valid_rows """WITH cte AS (
            SELECT CAST(WEEKS_ADD(dt,5) AS DATETIME) AS d, APPROX_COUNT_DISTINCT(id) AS n
            FROM ${source} WHERE filter_dt='2020-01-01' GROUP BY d
        ) SELECT DATE_FORMAT(d,'%Y-%m-%d %H:%i:%s'),n FROM cte WHERE d='2020-02-05'"""
        // Shared CTE predicates may be collected and pushed back into a producer with a hidden WHERE.
        def inlineThreshold = sql("SHOW VARIABLES LIKE 'inline_cte_referenced_threshold'")[0][1]
        sql "set inline_cte_referenced_threshold=0"
        try {
            def branch = """SELECT DATE_FORMAT(d,'%Y-%m-%d %H:%i:%s') FROM (
                SELECT CAST(WEEKS_ADD(dt,5) AS DATETIME) AS d FROM filtered_input
            ) s WHERE d='2020-02-05'"""
            def sharedCte = """WITH filtered_input AS (
                SELECT dt FROM ${source} WHERE filter_dt='2020-01-01'
            ) ${branch} UNION ALL ${branch}"""
            order_qt_shared_cte sharedCte
        } finally {
            sql "set inline_cte_referenced_threshold=${inlineThreshold}"
        }
        // Overflow on a row that actually participates in date arithmetic must still be reported.
        test {
            sql "SELECT WEEKS_ADD(dt,5) FROM ${source}"
            exception "Operation week_add of 9999-12-31 23:59:59, 5 out of range"
        }
    }
    sql "DROP TABLE IF EXISTS test_date_expression_filter_partition_pruning"
    // Explicit partitions make the safe alias predicate's pruning boundary observable.
    sql """CREATE TABLE test_date_expression_filter_partition_pruning (dt DATE NOT NULL, id INT)
        DUPLICATE KEY(dt, id)
        PARTITION BY RANGE(dt) (
            PARTITION p1 VALUES [('2020-01-01'), ('2020-01-02')),
            PARTITION p2 VALUES [('2020-01-02'), ('2020-01-03')),
            PARTITION p3 VALUES [('2020-01-03'), ('2020-01-04'))
        )
        DISTRIBUTED BY HASH(dt) BUCKETS 3
        PROPERTIES("replication_num"="1")"""
    sql "INSERT INTO test_date_expression_filter_partition_pruning VALUES ('2020-01-01',1),('2020-01-02',2),('2020-01-03',3)"
    for (def offset : [0, 1]) {
        def query = """SELECT id FROM (
            SELECT DAYS_ADD(dt,${offset}) AS d,id FROM test_date_expression_filter_partition_pruning
        ) s WHERE d='2020-01-02'"""
        explain {
            sql query
            contains "partitions=1/3 (p${2 - offset})"
        }
        order_qt_partition_pruning query
    }
    // A zero-offset alias stays safe even with an unrelated lower WHERE.
    def zeroOffsetQuery = """SELECT id FROM (
        SELECT DAYS_ADD(dt,0) AS d,id FROM test_date_expression_filter_partition_pruning WHERE id > 0
    ) s WHERE d='2020-01-02'"""
    explain {
        sql zeroOffsetQuery
        contains "partitions=1/3 (p2)"
    }
    order_qt_zero_offset_partition_pruning zeroOffsetQuery
    order_qt_null "SELECT WEEKS_ADD(CAST(NULL AS DATETIME),5)"
}
