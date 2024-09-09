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

suite("test_default_bitmap_empty") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    def tableName = "test_default_bitmap_empty"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName}
        (
            k TINYINT,
            v1 bitmap NOT NULL DEFAULT bitmap_empty,
            v2 INT
        )
        UNIQUE KEY(K)
        DISTRIBUTED BY HASH(k)
        PROPERTIES("replication_num" = "1");
    """

    // test insert into.
    sql " insert into ${tableName} (k, v2) values (1, 1); "
    sql " insert into ${tableName} (k, v2) values (2, 2); "
    sql " insert into ${tableName} (k, v2) values (3, 3); "
    sql " insert into ${tableName} (k, v2) values (4, 4); "
    sql "sync"
    qt_insert_into1 """ select bitmap_count(v1) from ${tableName}; """

    // test csv stream load.
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'columns', 'k, v1=bitmap_empty(), v2'

        file 'test_default_bitmap_empty_streamload.csv'

        time 10000 // limit inflight 10s
    }

    sql "sync"

    qt_stream_load_csv1 """ select bitmap_count(v1) from ${tableName}; """

    // test partial update
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName}
        (
            k TINYINT,
            v1 bitmap NOT NULL DEFAULT bitmap_empty,
            v2 INT
        )
        UNIQUE KEY(K)
        DISTRIBUTED BY HASH(k)
        PROPERTIES("replication_num" = "1");
    """

    sql "set enable_unique_key_partial_update=true;" 
    sql "set enable_insert_strict=false;"

    sql " insert into ${tableName} (k, v2) values (1, 1); "
    sql " insert into ${tableName} (k, v2) values (2, 2); "
    sql " insert into ${tableName} (k, v2) values (3, 3); "
    sql " insert into ${tableName} (k, v2) values (4, 4); "
    sql "sync"

    qt_select_2 "select bitmap_count(v1) from ${tableName};"

    streamLoad {
        table "${tableName}"

        set 'partial_columns', 'true'
        set 'column_separator', ','
        set 'columns', 'k, v2'

        file 'test_default_bitmap_empty_streamload.csv'

        time 10000 // limit inflight 10s
    }

    sql "sync"

    qt_stream_load_csv2 """ select bitmap_count(v1) from ${tableName}; """

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName}
        (
            k TINYINT,
            v1 bitmap BITMAP_UNION default BITMAP_EMPTY,
            v2 INT replace_if_not_null
        )
        aggregate KEY(K)
        DISTRIBUTED BY HASH(k)
        PROPERTIES("replication_num" = "1");
    """

    // test insert into.
    sql " insert into ${tableName} (k, v2) values (1, 1); "
    sql " insert into ${tableName} (k, v2) values (2, 2); "
    sql " insert into ${tableName} (k, v2) values (3, 3); "
    sql " insert into ${tableName} (k, v2) values (4, 4); "
    sql "sync"
    qt_insert_into3 """ select bitmap_count(v1) from ${tableName}; """

    // test csv stream load.
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'columns', 'k, v1=bitmap_empty(), v2'

        file 'test_default_bitmap_empty_streamload.csv'

        time 10000 // limit inflight 10s
    }

    sql "sync"

    qt_stream_load_csv3 """ select bitmap_count(v1) from ${tableName}; """
}