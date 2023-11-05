
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

suite("test_partial_update_replace_if_not_null", "p0") {

    def tableName = "test_partial_update_replace_if_not_null1"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE ${tableName} ( 
                `k` int(11) NULL, 
                `v1` BIGINT NULL,
                `v2` BIGINT NULL,
                `v3` BIGINT NULL,
                `v4` BIGINT NULL,
                `v5` BIGINT NULL
            ) ENGINE = OLAP UNIQUE KEY(`k`) 
            COMMENT 'OLAP' DISTRIBUTED BY HASH(`k`) BUCKETS 1 
            PROPERTIES ( 
                "replication_allocation" = "tag.location.default: 1", 
                "storage_format" = "V2", 
                "enable_unique_key_merge_on_write" = "true", 
                "light_schema_change" = "true", 
                "disable_auto_compaction" = "true",
                "enable_unique_key_replace_if_not_null" = "true"
            );
    """
    sql """insert into ${tableName} values(1,1,1,1,1,1),(2,2,2,2,2,2),(3,3,3,3,3,3),(4,4,4,4,4,4),(5,5,5,5,5,5);"""
    qt_sql """select * from ${tableName} order by k;"""

    // test the normal partial update stream load
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'partial_columns', 'true'
        set 'columns', 'k,v1'
        set 'strict_mode', 'false'

        file 'replace1.csv'
        time 10000 // limit inflight 10s
    }
    sql "sync"
    qt_sql """select * from ${tableName} order by k;"""

    // test the partial update stream load with explict null values
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'partial_columns', 'true'
        set 'columns', 'k,v1,v3,v5'
        set 'strict_mode', 'false'

        file 'replace2.csv'
        time 10000 // limit inflight 10s
    }
    sql "sync"
    qt_sql """select * from ${tableName} order by k;"""

    // test the partial update stream load with implicit null values
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'partial_columns', 'true'
        set 'columns', 'k,v1,v3,v5'
        set 'strict_mode', 'false'

        file 'replace3.csv'
        time 10000 // limit inflight 10s
    }
    sql "sync"
    qt_sql """select * from ${tableName} order by k;"""

    // corner case: read old values for all columns with explicit null values
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'partial_columns', 'true'
        set 'columns', 'k,v1,v3,v5'
        set 'strict_mode', 'false'

        file 'replace4.csv'
        time 10000 // limit inflight 10s
    }
    sql "sync"
    qt_sql """select * from ${tableName} order by k;"""

    // corner case: read old values for all columns with implicit null values
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'partial_columns', 'true'
        set 'columns', 'k,v1,v3,v5'
        set 'strict_mode', 'false'

        file 'replace5.csv'
        time 10000 // limit inflight 10s
    }
    sql "sync"
    qt_sql """select * from ${tableName} order by k;"""

    // csv file, include all columns
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'partial_columns', 'true'
        set 'columns', 'k,v1,v2,v3,v4,v5'
        set 'strict_mode', 'false'

        file 'replace6.csv'
        time 10000 // limit inflight 10s
    }
    sql "sync"
    qt_sql """select * from ${tableName} order by k;"""

    // test the partial update stream load with json file
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'format', 'json'
        set 'partial_columns', 'true'
        set 'columns', 'k,v2,v3,v4,v5'
        set 'read_json_by_line', 'true'
        set 'strict_mode', 'false'

        file 'replace7.json'
        time 10000 // limit inflight 10s
    }
    sql "sync"
    qt_sql """select * from ${tableName} order by k;"""

    // json file, include all columns
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'format', 'json'
        set 'partial_columns', 'true'
        set 'columns', 'k,v1,v2,v3,v4,v5'
        set 'read_json_by_line', 'true'
        set 'strict_mode', 'false'

        file 'replace8.json'
        time 10000 // limit inflight 10s
    }
    sql "sync"
    qt_sql """select * from ${tableName} order by k;"""

    sql """ DROP TABLE IF EXISTS ${tableName};"""
}
