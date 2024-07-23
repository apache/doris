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
    def table1 = "timezone"
    def table2 = "datetime"

    sql "drop table if exists ${table1}"
    sql """
    CREATE TABLE IF NOT EXISTS ${table1} (
      `k1` datetimev2(3) NULL,
      `k2` datev2 NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`k1`)
    DISTRIBUTED BY HASH(`k1`) BUCKETS 3
    PROPERTIES (
    "replication_num" = "1"
    )
    """

    sql "drop table if exists ${table2}"
    sql """
    CREATE TABLE ${table2} (
        id int NULL,
        createTime datetime NULL
    )ENGINE=OLAP
    UNIQUE KEY(`id`)
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(`id`) BUCKETS 3
    PROPERTIES (
        "replication_num" = "1",
        "colocate_with" = "lineitem_orders",
        "enable_unique_key_merge_on_write" = "true"
    );
    """

    streamLoad {
        table "${table1}"
        set 'column_separator', ','
        set 'timezone', '+02:00'
        file "test_tz_streamload.csv"
        time 20000
    }
    sql "sync"
    qt_table1 "select * from ${table1} order by k1"

    streamLoad { // contain more complex format
        table "${table2}"
        set 'column_separator', ','
        set 'columns', 'id,createTime,createTime=date_add(createTime, INTERVAL 8 HOUR)'
        // use default timezone for this
        file "test_tz_streamload2.csv"
        time 20000
    }
    sql "sync"
    qt_table2 "select * from ${table2} order by id"

    // test rounding for date type. from hour to date.
    sql "drop table if exists d"
    sql """
        CREATE TABLE d (
            `k1` int,
            `k2` date,
            `k3` datetime
        )
        DISTRIBUTED BY HASH(k1) BUCKETS 3
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    streamLoad {
        table "d"
        set 'column_separator', ','
        set 'timezone', 'UTC'
        file "only_date.csv"
        time 20000
    }
    sql "sync"
    qt_table1 "select * from d order by k1, k2, k3"
}
