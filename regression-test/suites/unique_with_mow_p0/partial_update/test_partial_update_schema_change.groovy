
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

suite("test_partial_update_schema_change", "p0") {

    /* ============================================== light schema change cases: ============================================== */

    // test add value column
    def tableName = "test_partial_update_light_schema_change_add_column"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE ${tableName} (
                `c0` int NULL,
                `c1` int NULL,
                `c2` int NULL,
                `c3` int NULL,
                `c4` int NULL,
                `c5` int NULL,
                `c6` int NULL,
                `c7` int NULL,
                `c8` int NULL,
                `c9` int NULL)
                UNIQUE KEY(`c0`) DISTRIBUTED BY HASH(`c0`) BUCKETS 1
                PROPERTIES(
                    "replication_num" = "1",
                    "light_schema_change" = "true",
                    "enable_unique_key_merge_on_write" = "true")
    """

    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'columns', 'c0, c1, c2, c3, c4, c5, c6, c7, c8, c9'

        file 'schema_change/load.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(1, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
        }
    }

    sql "sync"

    qt_sql1 " select * from ${tableName} order by c0 "
    
    // schema change
    sql " ALTER table ${tableName} add column c10 INT DEFAULT '0' "
    def try_times=100
    while(true){
        def res = sql " SHOW ALTER TABLE COLUMN WHERE TableName = '${tableName}' ORDER BY CreateTime DESC LIMIT 1 "
        Thread.sleep(1200)
        if(res[0][9].toString() == "FINISHED"){
            break;
        }
        assert(try_times>0)
        try_times--
    }
    
    // test load data without new column
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'partial_columns', 'true'
        set 'columns', 'c0, c1, c2'

        file 'schema_change/load_without_new_column.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(1, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
        }
    }

    sql "sync"

    // check data, new column is filled by default value.
    qt_sql2 " select * from ${tableName} order by c0 "

    // test load data with new column
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'partial_columns', 'true'
        set 'columns', 'c0, c1, c2, c10'

        file 'schema_change/load_with_new_column.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(1, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
        }
    }

    sql "sync"

    // check data, new column is filled by given value.
    qt_sql3 " select * from ${tableName} order by c0 "

    sql """ DROP TABLE IF EXISTS ${tableName} """


    // test delete value column
    tableName = "test_partial_update_light_schema_change_delete_column"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE ${tableName} (
                `c0` int NULL,
                `c1` int NULL,
                `c2` int NULL,
                `c3` int NULL,
                `c4` int NULL,
                `c5` int NULL,
                `c6` int NULL,
                `c7` int NULL,
                `c8` int NULL,
                `c9` int NULL)
                UNIQUE KEY(`c0`) DISTRIBUTED BY HASH(`c0`) BUCKETS 1
                PROPERTIES(
                    "replication_num" = "1",
                    "light_schema_change" = "true",
                    "enable_unique_key_merge_on_write" = "true")
    """

    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'columns', 'c0, c1, c2, c3, c4, c5, c6, c7, c8, c9'        

        file 'schema_change/load.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(1, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
        }
    }

    sql "sync"

    qt_sql4 " select * from ${tableName} order by c0 "
    
    // schema change
    sql " ALTER table ${tableName} DROP COLUMN c8 "
    try_times=100
    while(true){
        def res = sql " SHOW ALTER TABLE COLUMN WHERE TableName = '${tableName}' ORDER BY CreateTime DESC LIMIT 1 "
        Thread.sleep(1200)
        if(res[0][9].toString() == "FINISHED"){
            break;
        }
        assert(try_times>0)
        try_times--
    }

    // test load data without delete column
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'partial_columns', 'true'
        set 'columns', 'c0, c1, c2'

        file 'schema_change/load_with_delete_column.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(1, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
        }
    }

    sql "sync"

    qt_sql5 " select * from ${tableName} order by c0 "

    // test load data with delete column, stream load will ignore the
    // non-existing column
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'partial_columns', 'true'
        set 'columns', 'c0, c1, c7, c8'

        file 'schema_change/load_without_delete_column.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            // check result, which is fail for loading delete column.
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(1, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
        }
    }

    sql "sync"

    qt_sql6 " select * from ${tableName} order by c0 "

    sql """ DROP TABLE IF EXISTS ${tableName} """


    // test update value column
    tableName = "test_partial_update_light_schema_change_update_column"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE ${tableName} (
                `c0` int NULL,
                `c1` int NULL,
                `c2` int NULL,
                `c3` int NULL,
                `c4` int NULL,
                `c5` int NULL,
                `c6` int NULL,
                `c7` int NULL,
                `c8` int NULL,
                `c9` int NULL)
                UNIQUE KEY(`c0`) DISTRIBUTED BY HASH(`c0`) BUCKETS 1
                PROPERTIES(
                    "replication_num" = "1",
                    "light_schema_change" = "true",
                    "enable_unique_key_merge_on_write" = "true")
    """

    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'columns', 'c0, c1, c2, c3, c4, c5, c6, c7, c8, c9'

        file 'schema_change/load.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(1, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
        }
    }

    sql "sync"

    qt_sql7 " select * from ${tableName} order by c0 "
    
    // schema change
    sql " ALTER table ${tableName} MODIFY COLUMN c2 double "
    try_times=100
    while(true){
        def res = sql " SHOW ALTER TABLE COLUMN WHERE TableName = '${tableName}' ORDER BY CreateTime DESC LIMIT 1 "
        Thread.sleep(1200)
        if(res[0][9].toString() == "FINISHED"){
            break;
        }
        assert(try_times>0)
        try_times--
    }

    // test load data with update column
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'partial_columns', 'true'
        set 'columns', 'c0, c1, c2'

        file 'schema_change/load_with_update_column.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(1, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
        }
    }

    sql "sync"

    qt_sql8 " select * from ${tableName} order by c0 "

    sql """ DROP TABLE IF EXISTS ${tableName} """


    // test add key column
    tableName = "test_partial_update_light_schema_change_add_key_column"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE ${tableName} (
                `c0` int NULL)
                UNIQUE KEY(`c0`) DISTRIBUTED BY HASH(`c0`) BUCKETS 1
                PROPERTIES(
                    "replication_num" = "1",
                    "light_schema_change" = "true",
                    "enable_unique_key_merge_on_write" = "true")
    """

    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'columns', 'c0'

        file 'schema_change/load1.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(1, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
        }
    }

    sql "sync"

    qt_sql9 " select * from ${tableName} order by c0 "
    
    // schema change
    sql " ALTER table ${tableName} ADD COLUMN c1 int key null "
    try_times=100
    while(true){
        def res = sql " SHOW ALTER TABLE COLUMN WHERE TableName = '${tableName}' ORDER BY CreateTime DESC LIMIT 1 "
        Thread.sleep(1200)
        if(res[0][9].toString() == "FINISHED"){
            break;
        }
        assert(try_times>0)
        try_times--
    }

    sql " ALTER table ${tableName} ADD COLUMN c2 int null "
    try_times=100
    while(true){
        def res = sql " SHOW ALTER TABLE COLUMN WHERE TableName = '${tableName}' ORDER BY CreateTime DESC LIMIT 1 "
        Thread.sleep(1200)
        if(res[0][9].toString() == "FINISHED"){
            break;
        }
        assert(try_times>0)
        try_times--
    }

    // test load data with all key column, should fail because
    // it don't have any value columns
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'partial_columns', 'true'
        set 'columns', 'c0, c1'

        file 'schema_change/load_with_key_column.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertEquals(1, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
        }
    }

    sql "sync"

    sql """ DROP TABLE IF EXISTS ${tableName} """


    // test create index
    tableName = "test_partial_update_light_schema_change_create_index"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE ${tableName} (
                `c0` int NULL,
                `c1` int NULL,
                `c2` int NULL,
                `c3` int NULL,
                `c4` int NULL,
                `c5` int NULL,
                `c6` int NULL,
                `c7` int NULL,
                `c8` int NULL,
                `c9` int NULL)
                UNIQUE KEY(`c0`) DISTRIBUTED BY HASH(`c0`) BUCKETS 1
                PROPERTIES(
                    "replication_num" = "1",
                    "light_schema_change" = "true",
                    "enable_unique_key_merge_on_write" = "true")
    """

    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'columns', 'c0, c1, c2, c3, c4, c5, c6, c7, c8, c9'
        

        file 'schema_change/load.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(1, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
        }
    }

    sql "sync"

    qt_sql10 " select * from ${tableName} order by c0 "
    
    sql " CREATE INDEX test ON ${tableName} (c1) USING BITMAP "
    try_times=100
    while(true){
        def res = sql " SHOW ALTER TABLE COLUMN WHERE TableName = '${tableName}' ORDER BY CreateTime DESC LIMIT 1 "
        Thread.sleep(1200)
        if(res[0][9].toString() == "FINISHED"){
            break;
        }
        assert(try_times>0)
        try_times--
    }

    //test load data with create index
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'partial_columns', 'true'
        set 'columns', 'c0, c1, c2'

        file 'schema_change/load_with_create_index.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(1, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
        }
    }

    sql "sync"

    qt_sql11 " select * from ${tableName} order by c0 "

    sql """ DROP TABLE IF EXISTS ${tableName} """

    // test change properties
    tableName = "test_partial_update_light_schema_change_properties"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE ${tableName} (
                `c0` int NULL,
                `c1` int NULL,
                `c2` int NULL,
                `c3` int NULL,
                `c4` int NULL,
                `c5` int NULL,
                `c6` int NULL,
                `c7` int NULL,
                `c8` int NULL,
                `c9` int NULL)
                UNIQUE KEY(`c0`) DISTRIBUTED BY HASH(`c0`) BUCKETS 1
                PROPERTIES(
                    "replication_num" = "1",
                    "light_schema_change" = "true",
                    "enable_unique_key_merge_on_write" = "true")
    """

    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'columns', 'c0, c1, c2, c3, c4, c5, c6, c7, c8, c9'

        file 'schema_change/load.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(1, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
        }
    }

    sql "sync"

    qt_sql12 " select * from ${tableName} order by c0 "
    
    if (!isCloudMode()) {
        sql " ALTER TABLE ${tableName} set ('in_memory' = 'false') "
    }

    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'partial_columns', 'true'
        set 'columns', 'c0, c1, c2'

        file 'schema_change/load_with_change_properties.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(1, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
        }
    }

    sql "sync"

    qt_sql13 " select * from ${tableName} order by c0 "

    sql """ DROP TABLE IF EXISTS ${tableName} """

    /* ============================================== schema change cases: ============================================== */

    // test add value column
    tableName = "test_partial_update_schema_change_add_column"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE ${tableName} (
                `c0` int NULL,
                `c1` int NULL,
                `c2` int NULL,
                `c3` int NULL,
                `c4` int NULL,
                `c5` int NULL,
                `c6` int NULL,
                `c7` int NULL,
                `c8` int NULL,
                `c9` int NULL)
                UNIQUE KEY(`c0`) DISTRIBUTED BY HASH(`c0`) BUCKETS 1
                PROPERTIES(
                    "replication_num" = "1",
                    "enable_unique_key_merge_on_write" = "true")
    """

    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'columns', 'c0, c1, c2, c3, c4, c5, c6, c7, c8, c9'

        file 'schema_change/load.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(1, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
        }
    }

    sql "sync"

    qt_sql14 " select * from ${tableName} order by c0 "
    
    // schema change
    sql " ALTER table ${tableName} add column c10 INT DEFAULT '0' "
    try_times=100
    while(true){
        def res = sql " SHOW ALTER TABLE COLUMN WHERE TableName = '${tableName}' ORDER BY CreateTime DESC LIMIT 1 "
        Thread.sleep(1200)
        if(res[0][9].toString() == "FINISHED"){
            break;
        }
        assert(try_times>0)
        try_times--
    }
    
    // test load data without new column
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'partial_columns', 'true'
        set 'columns', 'c0, c1, c2'

        file 'schema_change/load_without_new_column.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(1, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
        }
    }

    sql "sync"

    // check data, new column is filled by default value.
    qt_sql15 " select * from ${tableName} order by c0 "

    // test load data with new column
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'partial_columns', 'true'
        set 'columns', 'c0, c1, c2, c10'

        file 'schema_change/load_with_new_column.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(1, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
        }
    }

    sql "sync"

    // check data, new column is filled by given value.
    qt_sql16 " select * from ${tableName} order by c0 "

    sql """ DROP TABLE IF EXISTS ${tableName} """


    // test delete value column
    tableName = "test_partial_update_schema_change_delete_column"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE ${tableName} (
                `c0` int NULL,
                `c1` int NULL,
                `c2` int NULL,
                `c3` int NULL,
                `c4` int NULL,
                `c5` int NULL,
                `c6` int NULL,
                `c7` int NULL,
                `c8` int NULL,
                `c9` int NULL)
                UNIQUE KEY(`c0`) DISTRIBUTED BY HASH(`c0`) BUCKETS 1
                PROPERTIES(
                    "replication_num" = "1",
                    "enable_unique_key_merge_on_write" = "true")
    """

    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'columns', 'c0, c1, c2, c3, c4, c5, c6, c7, c8, c9'

        file 'schema_change/load.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(1, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
        }
    }

    sql "sync"

    qt_sql17 " select * from ${tableName} order by c0 "
    
    // schema change
    sql " ALTER table ${tableName} DROP COLUMN c8 "
    try_times=100
    while(true){
        def res = sql " SHOW ALTER TABLE COLUMN WHERE TableName = '${tableName}' ORDER BY CreateTime DESC LIMIT 1 "
        Thread.sleep(1200)
        if(res[0][9].toString() == "FINISHED"){
            break;
        }
        assert(try_times>0)
        try_times--
    }

    // test load data without delete column
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'partial_columns', 'true'
        set 'columns', 'c0, c1, c2'

        file 'schema_change/load_with_delete_column.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(1, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
        }
    }

    sql "sync"

    qt_sql18 " select * from ${tableName} order by c0 "
    
    // test load data with delete column
    // todo bug
    // streamLoad {
    //     table "${tableName}"

    //     set 'column_separator', ','
    //     set 'partial_columns', 'true'
    //     set 'columns', 'c0, c1, c8'

    //     file 'schema_change/load_without_delete_column.csv'
    //     time 10000 // limit inflight 10s

    //     check { result, exception, startTime, endTime ->
    //         if (exception != null) {
    //             throw exception
    //         }
    //         // check result, which is fail for loading delete column.
    //         log.info("Stream load result: ${result}".toString())
    //         def json = parseJson(result)
    //         assertEquals("fail", json.Status.toLowerCase())
    //         assertEquals(1, json.NumberTotalRows)
    //         assertEquals(1, json.NumberFilteredRows)
    //         assertEquals(0, json.NumberUnselectedRows)
    //     }
    // }

    sql """ DROP TABLE IF EXISTS ${tableName} """


    // test update value column
    tableName = "test_partial_update_schema_change_update_column"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE ${tableName} (
                `c0` int NULL,
                `c1` int NULL,
                `c2` int NULL,
                `c3` int NULL,
                `c4` int NULL,
                `c5` int NULL,
                `c6` int NULL,
                `c7` int NULL,
                `c8` int NULL,
                `c9` int NULL)
                UNIQUE KEY(`c0`) DISTRIBUTED BY HASH(`c0`) BUCKETS 1
                PROPERTIES(
                    "replication_num" = "1",
                    "enable_unique_key_merge_on_write" = "true")
    """

    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'columns', 'c0, c1, c2, c3, c4, c5, c6, c7, c8, c9'

        file 'schema_change/load.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(1, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
        }
    }

    sql "sync"

    qt_sql19 " select * from ${tableName} order by c0 "
    
    // schema change
    sql " ALTER table ${tableName} MODIFY COLUMN c2 double "
    try_times=100
    while(true){
        def res = sql " SHOW ALTER TABLE COLUMN WHERE TableName = '${tableName}' ORDER BY CreateTime DESC LIMIT 1 "
        Thread.sleep(1200)
        if(res[0][9].toString() == "FINISHED"){
            break;
        }
        assert(try_times>0)
        try_times--
    }

    // test load data with update column
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'partial_columns', 'true'
        set 'columns', 'c0, c1, c2'

        file 'schema_change/load_with_update_column.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(1, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
        }
    }

    sql "sync"

    qt_sql20 " select * from ${tableName} order by c0 "

    sql """ DROP TABLE IF EXISTS ${tableName} """


    // test add key column
    tableName = "test_partial_update_schema_change_add_key_column"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE ${tableName} (
                `c0` int NULL)
                UNIQUE KEY(`c0`) DISTRIBUTED BY HASH(`c0`) BUCKETS 1
                PROPERTIES(
                    "replication_num" = "1",
                    "enable_unique_key_merge_on_write" = "true")
    """

    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'columns', 'c0'

        file 'schema_change/load1.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(1, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
        }
    }

    sql "sync"

    qt_sql21 " select * from ${tableName} order by c0 "
    
    // schema change
    sql " ALTER table ${tableName} ADD COLUMN c1 int key null "
    try_times=100
    while(true){
        def res = sql " SHOW ALTER TABLE COLUMN WHERE TableName = '${tableName}' ORDER BY CreateTime DESC LIMIT 1 "
        Thread.sleep(1200)
        if(res[0][9].toString() == "FINISHED"){
            break;
        }
        assert(try_times>0)
        try_times--
    }
    sql " ALTER table ${tableName} ADD COLUMN c2 int null "
    try_times=100
    while(true){
        def res = sql " SHOW ALTER TABLE COLUMN WHERE TableName = '${tableName}' ORDER BY CreateTime DESC LIMIT 1 "
        Thread.sleep(1200)
        if(res[0][9].toString() == "FINISHED"){
            break;
        }
        assert(try_times>0)
        try_times--
    }

    // test load data with all key column
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'partial_columns', 'true'
        set 'columns', 'c0, c1'

        file 'schema_change/load_with_key_column.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertEquals(1, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
        }
    }

    sql """ DROP TABLE IF EXISTS ${tableName} """


    // test create index
    tableName = "test_partial_update_schema_change_create_index"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE ${tableName} (
                `c0` int NULL,
                `c1` int NULL,
                `c2` int NULL,
                `c3` int NULL,
                `c4` int NULL,
                `c5` int NULL,
                `c6` int NULL,
                `c7` int NULL,
                `c8` int NULL,
                `c9` int NULL)
                UNIQUE KEY(`c0`) DISTRIBUTED BY HASH(`c0`) BUCKETS 1
                PROPERTIES(
                    "replication_num" = "1",
                    "enable_unique_key_merge_on_write" = "true")
    """

    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'columns', 'c0, c1, c2, c3, c4, c5, c6, c7, c8, c9'

        file 'schema_change/load.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(1, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
        }
    }

    sql "sync"

    qt_sql23 " select * from ${tableName} order by c0 "
    
    sql " CREATE INDEX test ON ${tableName} (c1) USING BITMAP "
    try_times=100
    while(true){
        def res = sql " SHOW ALTER TABLE COLUMN WHERE TableName = '${tableName}' ORDER BY CreateTime DESC LIMIT 1 "
        Thread.sleep(1200)
        if(res[0][9].toString() == "FINISHED"){
            break;
        }
        assert(try_times>0)
        try_times--
    }

    //test load data with create index
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'partial_columns', 'true'
        set 'columns', 'c0, c1, c2'

        file 'schema_change/load_with_create_index.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(1, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
        }
    }

    sql "sync"

    qt_sql24 " select * from ${tableName} order by c0 "

    sql """ DROP TABLE IF EXISTS ${tableName} """

    // test change properties
    tableName = "test_partial_update_schema_change_properties"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE ${tableName} (
                `c0` int NULL,
                `c1` int NULL,
                `c2` int NULL,
                `c3` int NULL,
                `c4` int NULL,
                `c5` int NULL,
                `c6` int NULL,
                `c7` int NULL,
                `c8` int NULL,
                `c9` int NULL)
                UNIQUE KEY(`c0`) DISTRIBUTED BY HASH(`c0`) BUCKETS 1
                PROPERTIES(
                    "replication_num" = "1",
                    "enable_unique_key_merge_on_write" = "true")
    """

    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'columns', 'c0, c1, c2, c3, c4, c5, c6, c7, c8, c9'        

        file 'schema_change/load.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(1, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
        }
    }

    sql "sync"

    qt_sql25 " select * from ${tableName} order by c0 "
    
    if (!isCloudMode()) {
        sql " ALTER TABLE ${tableName} set ('in_memory' = 'false') "
    }

    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'partial_columns', 'true'
        set 'columns', 'c0, c1, c2'

        file 'schema_change/load_with_change_properties.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(1, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
        }
    }

    sql "sync"

    qt_sql26 " select * from ${tableName} order by c0 "

    sql """ DROP TABLE IF EXISTS ${tableName} """
}
