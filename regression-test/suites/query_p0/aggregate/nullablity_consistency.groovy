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

suite("aggregate_nullability_consistency") {
    sql """DROP TABLE IF EXISTS t114;"""
    sql """CREATE TABLE t114 (col1 varchar(11451) not null, col2 int not null, col3 int not null)
    UNIQUE KEY(col1)
    DISTRIBUTED BY HASH(col1)
    BUCKETS 3
    PROPERTIES(
            "replication_num"="1",
            "enable_unique_key_merge_on_write"="true"
    );"""
    sql """insert into t114 values('1994',1644, 1994);"""

    sql """DROP TABLE IF EXISTS t115;"""
    sql """CREATE TABLE t115 (col1 varchar(32) not null, col2 int not null, col3 int not null, col4 int not null)
    DISTRIBUTED BY HASH(col3)
    BUCKETS 3
    PROPERTIES(
            "replication_num"="1"
    );"""

    sql """insert into t115 values("1994", 1994, 1995, 1996);"""

    qt_sql """WITH a_cte AS (
            SELECT *
                    FROM   t114
    )

    SELECT
    col1
    FROM      (
            SELECT
            lower(b.col1) col1
                    FROM      a_cte a
                    LEFT JOIN t115 b
                    ON        a.col2=b.col2
                    UNION ALL
                    SELECT
                    lower(b.col1) col1
                    FROM      a_cte a
                    LEFT JOIN t115 b
                    ON        a.col2=b.col2) tt
    GROUP BY
    col1"""


}