
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

suite("test_primary_key_partial_update", "p0") {
    def tableName = "test_primary_key_partial_update"

    // create table
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE ${tableName} (
                `id` int(11) NOT NULL COMMENT "用户 ID",
                `name` varchar(65533) NOT NULL COMMENT "用户姓名",
                `score` int(11) NOT NULL COMMENT "用户得分",
                `test` int(11) NULL COMMENT "null test",
                `dft` int(11) DEFAULT "4321")
                UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
                PROPERTIES("replication_num" = "1", "enable_unique_key_merge_on_write" = "true")
    """
    // insert 2 lines
    sql """
        insert into ${tableName} values(2, "doris2", 2000, 223, 1)
    """

    sql """
        insert into ${tableName} values(1, "doris", 1000, 123, 1)
    """

    // skip 3 lines and file have 4 lines
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'partial_columns', 'true'
        set 'columns', 'id,score'

        file 'basic.csv'
        time 10000 // limit inflight 10s
    }

    sql "sync"

    qt_select_default """
        select * from ${tableName} order by id;
    """

    // partial update a row multiple times in one stream load
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'partial_columns', 'true'
        set 'columns', 'id,score'

        file 'basic_with_duplicate.csv'
        time 10000 // limit inflight 10s
    }

    sql "sync"

    qt_partial_update_in_one_stream_load """
        select * from ${tableName} order by id;
    """

    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'partial_columns', 'true'
        set 'columns', 'id,score'

        file 'basic_with_duplicate2.csv'
        time 10000 // limit inflight 10s
    }

    sql "sync"

    qt_partial_update_in_one_stream_load """
        select * from ${tableName} order by id;
    """

    streamLoad {
        table "${tableName}"
        set 'column_separator', ','
        set 'format', 'csv'
        set 'partial_columns', 'true'
        set 'columns', 'id,name,score'

        file 'basic_with_new_keys.csv'
        time 10000 // limit inflight 10s
    }

    sql "sync"

    qt_partial_update_in_one_stream_load """
        select * from ${tableName} order by id;
    """

    streamLoad {
        table "${tableName}"
        set 'column_separator', ','
        set 'format', 'csv'
        set 'partial_columns', 'false'
        set 'columns', 'id,name,score'

        file 'basic_with_new_keys.csv'
        time 10000 // limit inflight 10s
    }

    sql "sync"

    qt_partial_update_in_one_stream_load """
        select * from ${tableName} order by id;
    """

    streamLoad {
        table "${tableName}"
        set 'column_separator', ','
        set 'format', 'csv'
        set 'partial_columns', 'true'
        set 'columns', 'id,name,score'

        file 'basic_with_new_keys_and_invalid.csv'
        time 10000// limit inflight 10s

        check {result, exception, startTime, endTime ->
            assertTrue(exception == null)
            def json = parseJson(result)
            assertEquals("Fail", json.Status)
        }
    }

    qt_partial_update_in_one_stream_load """
        select * from ${tableName} order by id;
    """

    streamLoad {
        table "${tableName}"
        set 'column_separator', ','
        set 'column_separator', ','
        set 'format', 'csv'
        set 'partial_columns', 'true'
        set 'columns', 'id,score'

        file 'basic_invalid.csv'
        time 10000// limit inflight 10s

        check {result, exception, startTime, endTime ->
            assertTrue(exception == null)
            def json = parseJson(result)
            assertEquals("Fail", json.Status)
            assertTrue(json.Message.contains("[DATA_QUALITY_ERROR]too many filtered rows"))
            assertEquals(3, json.NumberTotalRows)
            assertEquals(1, json.NumberLoadedRows)
            assertEquals(2, json.NumberFilteredRows)
        }
    }
    sql "sync"
    qt_partial_update_in_one_stream_load """
        select * from ${tableName} order by id;
    """

    // drop drop
    sql """ DROP TABLE IF EXISTS ${tableName} """
}
