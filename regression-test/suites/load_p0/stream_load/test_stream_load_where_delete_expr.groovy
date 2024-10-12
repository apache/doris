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
                age INT COMMENT "用户年龄",
                name VARCHAR(20) COMMENT "用户姓名"
            )
                UNIQUE KEY(user_id)
                DISTRIBUTED BY HASH(user_id) BUCKETS 10
            PROPERTIES
            (
                "replication_num" = "1"
            );"""

    def uniquePartitionSql = """CREATE TABLE ${tableName}(
                user_id BIGINT NOT NULL COMMENT "用户 ID",
                age INT COMMENT "用户年龄",
                name VARCHAR(20) COMMENT "用户姓名"
                
            )
            ENGINE=OLAP
            UNIQUE KEY(user_id,age)
            PARTITION BY RANGE(`age`)
            (
                PARTITION `p1` VALUES LESS THAN ("35"),
                PARTITION `p2` VALUES LESS THAN ("65")
            )
             DISTRIBUTED BY HASH(user_id) BUCKETS 10
            PROPERTIES
            (
                "replication_num" = "1"
            );"""

    def uniquePartitionSql2 = """CREATE TABLE ${tableName}(
                user_id BIGINT NOT NULL COMMENT "用户 ID",
                age INT COMMENT "用户年龄"
                
            )
            ENGINE=OLAP
            UNIQUE KEY(user_id,age)
            PARTITION BY RANGE(`age`)
            (
                PARTITION `p1` VALUES LESS THAN ("35"),
                PARTITION `p2` VALUES LESS THAN ("65")
            )
             DISTRIBUTED BY HASH(user_id) BUCKETS 10
            PROPERTIES
            (
                "replication_num" = "1"
            );"""


    def dupPartitionSql = """CREATE TABLE ${tableName}(
                user_id BIGINT NOT NULL COMMENT "用户 ID",
                age INT COMMENT "用户年龄",
                name VARCHAR(20) COMMENT "用户姓名"
            )
            ENGINE=OLAP
            DUPLICATE KEY(user_id,age)
            PARTITION BY RANGE(`age`)
            (
                PARTITION `p1` VALUES LESS THAN ("35"),
                PARTITION `p2` VALUES LESS THAN ("65")
            )
                DISTRIBUTED BY HASH(user_id) BUCKETS 10
            PROPERTIES
            (
                "replication_num" = "1"
            );"""
    def dupSql = """CREATE TABLE ${tableName}(
                user_id BIGINT NOT NULL COMMENT "用户 ID",
                age INT COMMENT "用户年龄",
                name VARCHAR(20) COMMENT "用户姓名"
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
        set 'columns', 'user_id, age'
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
        set 'columns', 'user_id, age'
        set 'column_separator', ','
        set 'where', 'age>=35 or user_id=1'

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
        set 'columns', 'user_id, age'
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
        set 'columns', 'user_id, age'
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
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(10, json.NumberTotalRows)
            assertEquals(10, json.NumberLoadedRows)
        }
    }

    /**
     *  dupSql 
     */

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql dupSql
    streamLoad {
        table "${tableName}"
        set 'columns', 'user_id, age'
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
        set 'columns', 'user_id, age'
        set 'column_separator', ','
        set 'where', 'age>=35 or user_id=1'

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

    /**
     *  uniquePartition Table structure with three fields, importing data with two fields
     */

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql uniquePartitionSql
    streamLoad {
        table "${tableName}"
        set 'columns', 'user_id, age'
        set 'column_separator', ','
        set 'column_separator', ','
        set 'partition_columns', 'false'
        set 'max_filter_ratio', '0.1'
        set 'where', 'age>=35 and name=null'

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
            assertEquals(0, json.NumberLoadedRows)
        }
    }


    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql uniquePartitionSql
    streamLoad {
        table "${tableName}"
        set 'columns', 'user_id, age'
        set 'column_separator', ','
        set 'column_separator', ','
        set 'partition_columns', 'false'
        set 'max_filter_ratio', '0.1'
        set 'where', 'age>=35 and name!=null'

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
            assertEquals(0, json.NumberLoadedRows)
        }
    }



    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql uniquePartitionSql
    streamLoad {
        table "${tableName}"
        set 'columns', 'user_id, age'
        set 'column_separator', ','
        set 'column_separator', ','
        set 'partition_columns', 'false'
        set 'max_filter_ratio', '0.1'
        set 'where', 'age>=35 and name!="wangwu"'

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
            assertEquals(0, json.NumberLoadedRows)
        }
    }



    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql uniquePartitionSql
    streamLoad {
        table "${tableName}"
        set 'columns', 'user_id, age'
        set 'column_separator', ','
        set 'column_separator', ','
        set 'partition_columns', 'false'
        set 'max_filter_ratio', '0.1'
        set 'where', 'age>=35 and name="wangwu"'

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
            assertEquals(0, json.NumberLoadedRows)
        }
    }


    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql uniquePartitionSql
    streamLoad {
        table "${tableName}"
        set 'columns', 'user_id, age'
        set 'column_separator', ','
        set 'column_separator', ','
        set 'partition_columns', 'false'
        set 'max_filter_ratio', '0.1'
        set 'where', 'age>=35 or name=null'

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
    sql uniquePartitionSql
    streamLoad {
        table "${tableName}"
        set 'columns', 'user_id, age'
        set 'column_separator', ','
        set 'column_separator', ','
        set 'partition_columns', 'false'
        set 'max_filter_ratio', '0.1'
        set 'where', 'age>=35 or name!=null'

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
    sql uniquePartitionSql
    streamLoad {
        table "${tableName}"
        set 'columns', 'user_id, age'
        set 'column_separator', ','
        set 'column_separator', ','
        set 'partition_columns', 'false'
        set 'max_filter_ratio', '0.1'
        set 'where', 'age>=35 or name!="wangwu"'

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
    sql uniquePartitionSql
    streamLoad {
        table "${tableName}"
        set 'columns', 'user_id, age'
        set 'column_separator', ','
        set 'column_separator', ','
        set 'partition_columns', 'false'
        set 'max_filter_ratio', '0.1'
        set 'where', 'age>=35 or name="wangwu"'

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
    sql uniquePartitionSql
    streamLoad {
        table "${tableName}"
        set 'columns', 'user_id, age'
        set 'column_separator', ','
        set 'column_separator', ','
        set 'partition_columns', 'false'
        set 'max_filter_ratio', '0.1'
        set 'where', 'name=null'

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
            assertEquals(0, json.NumberLoadedRows)
        }
    }


    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql uniquePartitionSql
    streamLoad {
        table "${tableName}"
        set 'columns', 'user_id, age'
        set 'column_separator', ','
        set 'column_separator', ','
        set 'partition_columns', 'false'
        set 'max_filter_ratio', '0.1'
        set 'where', 'name!=null'

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
            assertEquals(0, json.NumberLoadedRows)
        }
    }



    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql uniquePartitionSql
    streamLoad {
        table "${tableName}"
        set 'column_separator', ','
        set 'column_separator', ','
        set 'partition_columns', 'false'
        set 'max_filter_ratio', '0.1'
        set 'where', 'name!=null'
        file 'streamload_2.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertEquals(10, json.NumberTotalRows)
            assertEquals(0, json.NumberLoadedRows)
        }
    }



    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql uniquePartitionSql
    streamLoad {
        table "${tableName}"
        set 'column_separator', ','
        set 'column_separator', ','
        set 'partition_columns', 'false'
        set 'max_filter_ratio', '0.1'
        set 'where', 'age>=35'
        file 'streamload_2.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertEquals(10, json.NumberTotalRows)
            assertEquals(0, json.NumberLoadedRows)
        }
    }
    
    //delete
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql uniquePartitionSql

    streamLoad {
        table "${tableName}"
        set 'columns', 'user_id, age'
        set 'column_separator', ','
        set 'max_filter_ratio', '0.1'
        set 'partition_columns', 'false'
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
            assertEquals(9, json.NumberLoadedRows)
        }
    }


    streamLoad {
        table "${tableName}"
        set 'columns', 'user_id, age'
        set 'column_separator', ','
        set 'merge_type', 'DELETE'
        set 'partition_columns', 'false'
        file 'streamload_3.csv'
        set 'max_filter_ratio', '0.1'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(10, json.NumberTotalRows)
            assertEquals(9, json.NumberLoadedRows)
        }
    }


    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql uniquePartitionSql

    streamLoad {
        table "${tableName}"
        set 'columns', 'user_id, age'
        set 'column_separator', ','
        set 'max_filter_ratio', '0.1'
        set 'partition_columns', 'false'
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
            assertEquals(9, json.NumberLoadedRows)
        }
    }


    streamLoad {
        table "${tableName}"
        set 'column_separator', ','
        set 'merge_type', 'DELETE'
        set 'partition_columns', 'false'
        file 'streamload_3.csv'
        set 'max_filter_ratio', '0.1'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertEquals(10, json.NumberTotalRows)
            assertEquals(0, json.NumberLoadedRows)
        }
    }



    /**
     *  dupPartition Table structure with three fields, importing data with two fields
     */
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql dupPartitionSql
    streamLoad {
        table "${tableName}"
        set 'columns', 'user_id, age'
        set 'column_separator', ','
        set 'where', 'age>=35'
        set 'partitions', 'p1, p2'
        set 'max_filter_ratio', '0.1'
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
    sql dupPartitionSql

    streamLoad {
        table "${tableName}"
        set 'columns', 'user_id, age'
        set 'column_separator', ','
        set 'where', 'age>=35 or user_id=1'
        set 'partitions', 'p1, p2'
        set 'max_filter_ratio', '0.1'
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



    /**
     *  uniquePartition Table structure with two fields, importing data with two fields
     */

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql uniquePartitionSql2
    streamLoad {
        table "${tableName}"
        set 'column_separator', ','
        set 'column_separator', ','
        set 'partition_columns', 'true'
        set 'max_filter_ratio', '0.1'
        set 'where', 'age>=35 and name!=null'

        file 'streamload_2.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertEquals(0, json.NumberTotalRows)
            assertEquals(0, json.NumberLoadedRows)
        }
    }


    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql uniquePartitionSql2
    streamLoad {
        table "${tableName}"
        set 'column_separator', ','
        set 'column_separator', ','
        set 'partition_columns', 'true'
        set 'max_filter_ratio', '0.1'
        set 'where', 'age>=35 and name!=null'

        file 'streamload_2.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertEquals(0, json.NumberTotalRows)
            assertEquals(0, json.NumberLoadedRows)
        }
    }



    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql uniquePartitionSql2
    streamLoad {
        table "${tableName}"
        set 'column_separator', ','
        set 'column_separator', ','
        set 'partition_columns', 'true'
        set 'max_filter_ratio', '0.1'
        set 'where', 'age>=35 and name!="wangwu"'

        file 'streamload_2.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertEquals(0, json.NumberTotalRows)
            assertEquals(0, json.NumberLoadedRows)
        }
    }



    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql uniquePartitionSql2
    streamLoad {
        table "${tableName}"
        set 'column_separator', ','
        set 'column_separator', ','
        set 'partition_columns', 'true'
        set 'max_filter_ratio', '0.1'
        set 'where', 'age>=35 and name="wangwu"'

        file 'streamload_2.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertEquals(0, json.NumberTotalRows)
            assertEquals(0, json.NumberLoadedRows)
        }
    }


    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql uniquePartitionSql2
    streamLoad {
        table "${tableName}"
        set 'column_separator', ','
        set 'column_separator', ','
        set 'partition_columns', 'true'
        set 'max_filter_ratio', '0.1'
        set 'where', 'age>=35 or name=null'

        file 'streamload_2.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertEquals(0, json.NumberTotalRows)
            assertEquals(0, json.NumberLoadedRows)
        }
    }


    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql uniquePartitionSql2
    streamLoad {
        table "${tableName}"
        set 'column_separator', ','
        set 'column_separator', ','
        set 'partition_columns', 'true'
        set 'max_filter_ratio', '0.1'
        set 'where', 'age>=35 or name!=null'

        file 'streamload_2.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertEquals(0, json.NumberTotalRows)
            assertEquals(0, json.NumberLoadedRows)
        }
    }


    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql uniquePartitionSql2
    streamLoad {
        table "${tableName}"
        set 'column_separator', ','
        set 'column_separator', ','
        set 'partition_columns', 'true'
        set 'max_filter_ratio', '0.1'
        set 'where', 'age>=35 or name!="wangwu"'

        file 'streamload_2.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertEquals(0, json.NumberTotalRows)
            assertEquals(0, json.NumberLoadedRows)
        }
    }

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql uniquePartitionSql2
    streamLoad {
        table "${tableName}"
        set 'column_separator', ','
        set 'column_separator', ','
        set 'partition_columns', 'true'
        set 'max_filter_ratio', '0.1'
        set 'where', 'age>=35 or name="wangwu"'

        file 'streamload_2.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertEquals(0, json.NumberTotalRows)
            assertEquals(0, json.NumberLoadedRows)
        }
    }

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql uniquePartitionSql2
    streamLoad {
        table "${tableName}"
        set 'column_separator', ','
        set 'column_separator', ','
        set 'partition_columns', 'true'
        set 'max_filter_ratio', '0.1'
        set 'where', 'name=null'

        file 'streamload_2.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertEquals(0, json.NumberTotalRows)
            assertEquals(0, json.NumberLoadedRows)
        }
    }


    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql uniquePartitionSql2
    streamLoad {
        table "${tableName}"
        set 'column_separator', ','
        set 'column_separator', ','
        set 'partition_columns', 'true'
        set 'max_filter_ratio', '0.1'
        set 'where', 'name!=null'

        file 'streamload_2.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertEquals(0, json.NumberTotalRows)
            assertEquals(0, json.NumberLoadedRows)
        }
    }



    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql uniquePartitionSql2
    streamLoad {
        table "${tableName}"
        set 'column_separator', ','
        set 'column_separator', ','
        set 'partition_columns', 'true'
        set 'max_filter_ratio', '0.1'
        set 'where', 'name!=null'
        file 'streamload_2.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertEquals(0, json.NumberTotalRows)
            assertEquals(0, json.NumberLoadedRows)
        }
    }



    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql uniquePartitionSql2
    streamLoad {
        table "${tableName}"
        set 'column_separator', ','
        set 'column_separator', ','
        set 'partition_columns', 'true'
        set 'max_filter_ratio', '0.1'
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

    //delete
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql uniquePartitionSql2

    streamLoad {
        table "${tableName}"
        set 'column_separator', ','
        set 'max_filter_ratio', '0.1'
        set 'partition_columns', 'true'
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
            assertEquals(9, json.NumberLoadedRows)
        }
    }


    streamLoad {
        table "${tableName}"
        set 'column_separator', ','
        set 'merge_type', 'DELETE'
        set 'partition_columns', 'true'
        file 'streamload_3.csv'
        set 'max_filter_ratio', '0.1'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(10, json.NumberTotalRows)
            assertEquals(9, json.NumberLoadedRows)
        }
    }


    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql uniquePartitionSql2

    streamLoad {
        table "${tableName}"
        set 'column_separator', ','
        set 'max_filter_ratio', '0.1'
        set 'partition_columns', 'true'
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
            assertEquals(9, json.NumberLoadedRows)
        }
    }


    streamLoad {
        table "${tableName}"
        set 'column_separator', ','
        set 'merge_type', 'DELETE'
        set 'partition_columns', 'true'
        file 'streamload_3.csv'
        set 'max_filter_ratio', '0.1'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(10, json.NumberTotalRows)
            assertEquals(9, json.NumberLoadedRows)
        }
    }




}

