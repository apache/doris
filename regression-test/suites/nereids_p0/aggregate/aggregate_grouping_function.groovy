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

suite("aggregate_grouping_function") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
    sql "DROP TABLE IF EXISTS test_aggregate_grouping_function"

    sql """
        CREATE TABLE IF NOT EXISTS `test_aggregate_grouping_function` (
        `dt_date` varchar(1000) NULL COMMENT "",
        `name` varchar(1000) NULL COMMENT "",
        `num1` bigint(20) SUM NOT NULL COMMENT "",
        `num2` bigint(20) SUM NOT NULL COMMENT ""
        ) ENGINE=OLAP
        AGGREGATE KEY(`dt_date`, `name`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`dt_date`) BUCKETS 32
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        )
        ;
        """

    sql """INSERT INTO test_aggregate_grouping_function values ('2022-08-01', "aaa", 1,2),('2022-08-01', "bbb", 1,2),('2022-08-01', "ccc", 1,2),
            ('2022-08-02', "aaa", 1,2),('2022-08-02', "bbb", 1,2),('2022-08-02', "ccc", 1,2),('2022-08-03', "aaa", 1,2),
            ('2022-08-03', "bbb", 1,2),('2022-08-03', "ccc", 1,2);"""

    qt_select """
        WITH base_table AS (
		SELECT dt_date, name, sum(num1) AS sum1
			, sum(num2) AS sum2
			, sum(num1) / sum(num2) AS ratio
		FROM test_aggregate_grouping_function
		GROUP BY dt_date, name
            )
        SELECT grouping_id(sum1), dt_date, name
            , sum(sum2)
        FROM base_table
        GROUP BY GROUPING SETS ((dt_date), (dt_date, name, sum1))
        ORDER BY dt_date, name;
    """

    qt_select """
        WITH base_table AS (
		SELECT dt_date, name, sum(num1) AS sum1
			, sum(num2) AS sum2
			, sum(num1) / sum(num2) AS ratio
		FROM test_aggregate_grouping_function
                GROUP BY dt_date, name
            )
        SELECT grouping_id(ratio), dt_date, name
            , sum(sum2)
        FROM base_table
        GROUP BY GROUPING SETS ((dt_date), (dt_date, name, ratio))
        ORDER BY dt_date, name;
    """

    qt_select """
        WITH base_table AS (
		SELECT dt_date, name, sum(num1) AS sum1
			, sum(num2) AS sum2
			, sum(num1) / sum(num2) AS ratio
		FROM test_aggregate_grouping_function
		GROUP BY dt_date, name
        ), 
        base_table2 AS (
            SELECT *
            FROM base_table
        )
        SELECT grouping_id(ratio), dt_date, name
            , sum(sum2)
        FROM base_table2
        GROUP BY GROUPING SETS ((dt_date), (dt_date, name, ratio))
        ORDER BY dt_date, name;
    """

    sql "DROP TABLE test_aggregate_grouping_function"
}