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

suite("test_column_boundary") {
    sql """ DROP TABLE IF EXISTS test_column_boundary """
    sql """
        CREATE TABLE IF NOT EXISTS test_column_boundary (
            u_id int NULL COMMENT "",
            u_city varchar(20) NULL COMMENT ""
        ) ENGINE=OLAP
        DUPLICATE KEY(`u_id`, `u_city`)
        DISTRIBUTED BY HASH(`u_id`, `u_city`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
    );
    """

    sql """ insert into test_column_boundary select number, number + random() from numbers("number" = "1000000"); """
    Integer count = 0;
    Integer maxCount = 8;
    while (count < maxCount) {
        sql """  insert into test_column_boundary select * from test_column_boundary;"""
        count++
        sleep(100);
    }
    sql """ set parallel_pipeline_task_num = 1; """

    qt_sql_1 """ select count() from test_column_boundary; """ // 256000000 rows
    test {
        // column size is too large
        sql """ select sum(res) from (select count() over(partition by u_city) as res from test_column_boundary) as t; """
        exception "string column length is too large"
    }
    sql """ DROP TABLE IF EXISTS test_column_boundary """
}



