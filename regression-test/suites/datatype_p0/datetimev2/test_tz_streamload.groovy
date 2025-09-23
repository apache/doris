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

suite("test_tz_streamload") {
    def table1 = "global_timezone_test"
    
    sql "drop table if exists ${table1}"
    
    sql """
    CREATE TABLE IF NOT EXISTS ${table1} (
      `id` int NULL,
      `dt_datetime` datetime NULL,
      `dt_date` date NULL,
      `dt_datetimev2` datetimev2(3) NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`id`)
    DISTRIBUTED BY HASH(`id`) BUCKETS 3
    PROPERTIES (
        "replication_num" = "1"
    )
    """
    
    // case1：UTC
    sql "SET GLOBAL time_zone = 'UTC'"
    
    streamLoad {
        table "${table1}"
        set 'column_separator', ','
        file "test_global_timezone_streamload.csv"
        time 20000
    }
    sql "sync"
    qt_global_utc "select * from ${table1} order by id"
    
    // case2：Asia/Shanghai
    sql "truncate table ${table1}"
    sql "SET GLOBAL time_zone = 'Asia/Shanghai'"
    
    streamLoad {
        table "${table1}"
        set 'column_separator', ','
        file "test_global_timezone_streamload.csv"
        time 20000
    }
    sql "sync"
    qt_global_shanghai "select * from ${table1} order by id"
    
    // case3：+08:00
    sql "truncate table ${table1}"
    sql "SET GLOBAL time_zone = '+08:00'"
    
    streamLoad {
        table "${table1}"
        set 'column_separator', ','
        file "test_global_timezone_streamload.csv"
        time 20000
    }
    sql "sync"
    qt_global_offset "select * from ${table1} order by id"
    
    // case4
    sql "truncate table ${table1}"
    
    streamLoad {
        table "${table1}"
        set 'column_separator', ','
        set 'timezone', 'UTC'
        file "test_global_timezone_streamload.csv"
        time 20000
    }
    sql "sync"
    qt_explicit_timezone "select * from ${table1} order by id"
    
    // case5
    sql "truncate table ${table1}"
    sql "SET GLOBAL time_zone = 'UTC'"
    
    streamLoad {
        table "${table1}"
        set 'column_separator', ','
        file "test_global_timezone_streamload.csv"
        time 20000
    }
    sql "sync"
    qt_date_timezone_conversion "select id, dt_date from ${table1} order by id"
}