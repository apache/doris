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

suite("test_with_suffix") {
    def tableName = "datetime"

    sql "drop table if exists ${tableName}"
    sql """
    CREATE TABLE IF NOT EXISTS ${tableName} (
      `k1` datetimev2(3) NULL,
      `k2` datetimev2(6) NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`k1`)
    DISTRIBUTED BY HASH(`k1`) BUCKETS 3
    PROPERTIES (
    "replication_num" = "1"
    )
    """

    sql """ insert into ${tableName} values
        ("2020-01-12 12:12:12.123", "2020-01-12 12:12:12.123"),
        ("2020-02-12 12:12:12.123Z", "2020-02-12 12:12:12.123Z"),
        ("2020-03-12 12:12:12.UTC", "2020-03-12 12:12:12.UTC"),
        ("2020-04-12 12:12:12.123456XYZ", "2020-04-12 12:12:12.123456XYZ") """

    streamLoad {
        table "${tableName}"
        set 'column_separator', ','
        file "test_with_suffix.csv"
        time 10000
    }
    sql "sync"

    qt_all "select * from ${tableName} order by k1"
}
