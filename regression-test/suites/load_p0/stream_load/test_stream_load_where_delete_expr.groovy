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

suite("test_stream_load_where_delete_expr", "p0") {
    // define a sql table
    def tableName = "test_stream_load_where_expr_csv"

   
    def uniqueSql = """CREATE TABLE ${tableName}(
                user_id BIGINT NOT NULL COMMENT "用户 ID",
                name VARCHAR(20) COMMENT "用户姓名",
                age INT COMMENT "用户年龄"
            )
                UNIQUE KEY(user_id)
                DISTRIBUTED BY HASH(user_id) BUCKETS 10
            PROPERTIES
            (
                "replication_num" = "1"
            );"""
    def dupSql = """CREATE TABLE ${tableName}(
                user_id BIGINT NOT NULL COMMENT "用户 ID",
                name VARCHAR(20) COMMENT "用户姓名",
                age INT COMMENT "用户年龄"
            )
                DUPLICATE KEY(user_id)
                DISTRIBUTED BY HASH(user_id) BUCKETS 10
            PROPERTIES
            (
                "replication_num" = "1"
            );"""
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql uniqueSql
    streamLoad {
        table "${tableName}"
        set 'columns', 'user_id, name, age'
        set 'column_separator', ','
        set 'where', 'age>=35'

        file 'streamload_2.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(10, json.NumberTotalRows)
            assertEquals(4, json.NumberLoadedRows)
        }
    }


    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql uniqueSql

    streamLoad {
        table "${tableName}"
        set 'columns', 'user_id, name, age'
        set 'column_separator', ','
        set 'where', 'age>=35 or name="Olivia"'

        file 'streamload_2.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(10, json.NumberTotalRows)
            assertEquals(5, json.NumberLoadedRows)
        }
    }


    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql uniqueSql

    streamLoad {
        table "${tableName}"
        set 'columns', 'user_id, name, age'
        set 'column_separator', ','

        file 'streamload_1.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(10, json.NumberTotalRows)
            assertEquals(10, json.NumberLoadedRows)
        }
    }


    streamLoad {
        table "${tableName}"
        set 'columns', 'user_id, name, age'
        set 'column_separator', ','
        set 'merge_type', 'DELETE'
        file 'streamload_3.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertEquals(10, json.NumberTotalRows)
            assertEquals(9, json.NumberLoadedRows)
        }
    }

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql dupSql
    streamLoad {
        table "${tableName}"
        set 'columns', 'user_id, name, age'
        set 'column_separator', ','
        set 'where', 'age>=35'

        file 'streamload_2.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(10, json.NumberTotalRows)
            assertEquals(4, json.NumberLoadedRows)
        }
    }


    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql dupSql

    streamLoad {
        table "${tableName}"
        set 'columns', 'user_id, name, age'
        set 'column_separator', ','
        set 'where', 'age>=35 or name="Olivia"'

        file 'streamload_2.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(10, json.NumberTotalRows)
            assertEquals(5, json.NumberLoadedRows)
        }
    }
    

}

