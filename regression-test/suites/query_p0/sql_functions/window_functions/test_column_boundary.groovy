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

suite("test_column_boundary","nonConcurrent") {
    sql """ DROP TABLE IF EXISTS test_column_boundary """
    sql """
        CREATE TABLE IF NOT EXISTS test_column_boundary (
            u_id int NULL COMMENT "",
            u_city varchar(40) NULL COMMENT ""
        ) ENGINE=OLAP
        DUPLICATE KEY(`u_id`, `u_city`)
        DISTRIBUTED BY HASH(`u_id`, `u_city`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
    );
    """

    sql """ DROP TABLE IF EXISTS test_column_boundary2 """
    sql """
        CREATE TABLE IF NOT EXISTS test_column_boundary2 (
            u_id int NULL COMMENT "",
            u_city varchar(40) NULL COMMENT ""
        ) ENGINE=OLAP
        DUPLICATE KEY(`u_id`, `u_city`)
        DISTRIBUTED BY HASH(`u_id`, `u_city`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
    );
    """

    sql """ insert into test_column_boundary2 select number, number + random() from numbers("number" = "1000000"); """
    Integer count = 0;
    Integer maxCount = 270;
    while (count < maxCount) {
        log.info("count: ${count}")
        sql """ insert into test_column_boundary select * from test_column_boundary2; """
        count++
        sleep(100);
    }
    sql """ set parallel_pipeline_task_num = 1; """

    qt_sql_1 """ select count() from test_column_boundary; """ // 256000000 rows

    try {
        GetDebugPoint().enableDebugPointForAllBEs("AnalyticSinkLocalState._remove_unused_rows")
        // before column size will be too large 
        qt_sql_2 """ select sum(res) from (select count() over(partition by u_city) as res from test_column_boundary) as t; """
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("AnalyticSinkLocalState._remove_unused_rows")
    }
    sql """ DROP TABLE IF EXISTS test_column_boundary """
}