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

suite("test_streamload") {
    def table = "test_datetimev2_streamload"

    sql "drop table if exists ${table}"

    sql """
    CREATE TABLE IF NOT EXISTS `${table}` (
      `col` datetimev2(3) NULL COMMENT ""
    ) ENGINE=OLAP
    UNIQUE KEY(`col`)
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(`col`) BUCKETS 8
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "in_memory" = "false",
    "storage_format" = "V2"
    )
    """

    streamLoad {
        table "${table}"

        file 'datetimes.csv'
    }
    qt_select_all "select * from ${table} order by col"

    qt_sql_cast_datetimev2 " select cast(col as datetimev2(5)) col1 from ${table} order by col1; "
}
