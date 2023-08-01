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
    def dbName = "tz_streamload"
    def tableName = "timezone"

    sql "drop table if exists ${tableName}"
    sql """
    CREATE TABLE IF NOT EXISTS ${tableName} (
      `k1` datetimev2(3) NULL,
      `k2` datev2 NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`k1`)
    DISTRIBUTED BY HASH(`k1`) BUCKETS 3
    PROPERTIES (
    "replication_num" = "1"
    )
    """

    streamLoad {
        table "${tableName}"
        set 'column_separator', ','
        set 'time_zone', '+02:00'
        file "test_tz_streamload.csv"
        time 10000
    }
    sql "sync"

    qt_all "select * from ${tableName} order by k1"
}
