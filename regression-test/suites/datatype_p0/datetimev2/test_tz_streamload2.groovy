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

suite("test_tz_streamload2", "nonConcurrent") {
    def table1 = "global_timezone_test"
    
    sql "drop table if exists ${table1}"

    sql "SET GLOBAL time_zone = 'Asia/Shanghai'"
    
    sql """
    CREATE TABLE IF NOT EXISTS ${table1} (
      `id` int NULL,
      `dt_datetime` datetime NULL,
      `dt_date` date NULL,
    ) ENGINE=OLAP
    DUPLICATE KEY(`id`)
    DISTRIBUTED BY HASH(`id`) BUCKETS 3
    PROPERTIES (
        "replication_num" = "1"
    )
    """

    // case1 stream load set timezone = UTC
    // same as case3
    /*
    1	2024-04-11T08:00:13	2024-04-11
    2	2024-04-10T22:00:13	2024-04-11
    */
    streamLoad {
        table "${table1}"
        set 'column_separator', ','
        set 'timezone', 'UTC'
        file "test_global_timezone_streamload2.csv"
        time 20000
    }
    sql "sync"
    qt_global_offset "select * from ${table1} order by id"
    sql "truncate table ${table1}"

    // case2 not set timezone
    /*
    1	2024-04-11T16:00:13	2024-04-11
    2	2024-04-11T06:00:13	2024-04-11
    */
    streamLoad {
        table "${table1}"
        set 'column_separator', ','
        file "test_global_timezone_streamload2.csv"
        time 20000
    }
    sql "sync"
    qt_global_offset "select * from ${table1} order by id"
    sql "truncate table ${table1}"

    // case3 not set timezone but default is UTC
    // same as case1
    /*
    1	2024-04-11T08:00:13	2024-04-11
    2	2024-04-10T22:00:13	2024-04-11
    */
    sql "SET GLOBAL time_zone = 'UTC'"
    streamLoad {
        table "${table1}"
        set 'column_separator', ','
        file "test_global_timezone_streamload2.csv"
        time 20000
    }
    sql "sync"
    qt_global_offset "select * from ${table1} order by id"

    sql "UNSET GLOBAL VARIABLE time_zone"
}