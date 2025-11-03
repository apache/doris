
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

suite("test_csv_with_skip_lines", "p0") {
    def tableName = "test_csv_with_skip_lines"

    // create table
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `k1` int(20) NULL,
            `k2` bigint(20) NULL,
            `v1` tinyint(4)  NULL,
            `v2` string  NULL,
            `v3` date  NULL,
            `v4` datetime  NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`, `k2`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 3
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    // skip 3 lines and file have 4 lines
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'skip_lines', '3'

        file 'csv_with_skip_lines.csv'
    }

    sql "sync"
    qt_sql "select * from ${tableName} order by k1, k2"

    // skip 4 lines and file have 4 lines
    sql """truncate table ${tableName}"""
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'skip_lines', '4'

        file 'csv_with_skip_lines.csv'
    }

    sql "sync"
    qt_sql "select * from ${tableName} order by k1, k2"

    // skip 5 lines and file have 4 lines
    sql """truncate table ${tableName}"""
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'skip_lines', '5'

        file 'csv_with_skip_lines.csv'
    }

    sql "sync"
    qt_sql "select * from ${tableName} order by k1, k2"

    // skip 3 lines and set format = csv_with_names ==>>> skip 1 line
    sql """truncate table ${tableName}"""
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'format', 'csv_with_names'
        set 'skip_lines', '3'

        file 'csv_with_skip_lines.csv'
    }

    sql "sync"
    qt_sql "select * from ${tableName} order by k1, k2"

    // skip 3 lines and set format = csv_with_names_and_types ==>>> skip 2 line
    sql """truncate table ${tableName}"""
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'format', 'csv_with_names_and_types'
        set 'skip_lines', '3'

        file 'csv_with_skip_lines.csv'
    }

    sql "sync"
    qt_sql "select * from ${tableName} order by k1, k2"

    // skip 3 lines and set format = csv ==>>> skip 3 line
    sql """truncate table ${tableName}"""
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'skip_lines', '3'

        file 'csv_with_skip_lines.csv'
    }

    sql "sync"
    qt_sql "select * from ${tableName} order by k1, k2"


    // drop drop
    sql """ DROP TABLE IF EXISTS ${tableName} """
}
