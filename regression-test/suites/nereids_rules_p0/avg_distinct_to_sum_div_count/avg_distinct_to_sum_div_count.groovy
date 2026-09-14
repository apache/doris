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

suite("avg_distinct_to_sum_div_count") {
    sql "drop table if exists t1;"
    sql """ create table t1 (
            pk int,
            c1 decimal(12,2)  not null)
            properties("replication_num" = "1");"""

    sql "insert into t1 values(1,12.32),(2,123.23);"
    sql "drop table if exists testctas;"
    sql """create table  testctas properties("replication_num" = "1") select sum(distinct c1),avg(distinct c1) from t1;"""
    qt_ctas "select * from testctas;"

    qt_bigint_avg_distinct_overflow """
        SELECT COUNT(*) AS matched_rows, MAX(c) AS distinct_count
        FROM (
            SELECT AVG(DISTINCT x) AS a, COUNT(DISTINCT y) AS c
            FROM (
                SELECT CAST(9223372036854775807 AS BIGINT) AS x, CAST(1 AS INT) AS y
                UNION ALL
                SELECT CAST(9223372036854775806 AS BIGINT) AS x, CAST(2 AS INT) AS y
            ) t
        ) q
        WHERE a > 0
    """
}
