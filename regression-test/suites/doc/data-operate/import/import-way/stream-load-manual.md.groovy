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

suite("test_stream_load_doc_case", "p0") {
    def tableName = "test_stream_load_doc_case"

    // case 1: csv format
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE ${tableName}(
            user_id            BIGINT       NOT NULL COMMENT "用户 ID",
            name               VARCHAR(20)           COMMENT "用户姓名",
            age                INT                   COMMENT "用户年龄"
        )
        UNIQUE KEY(user_id)
        DISTRIBUTED BY HASH(user_id) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "function_column.sequence_col" = 'age'
        );
    """
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'columns', 'user_id,name,age'

        file 'streamload_example.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
        }
    }
    qt_sql1 "SELECT * FROM ${tableName} order by user_id"

    // case 2: json format
    sql """ truncate table ${tableName} """
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'columns', 'user_id,name,age'
        set 'format', 'json'
        set 'jsonpaths', '[\"$.userid\", \"$.username\", \"$.userage\"]'
        set 'strip_outer_array', 'true'

        file 'streamload_example.json'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
        }
    }
    qt_sql2 "SELECT * FROM ${tableName} order by user_id"

    //case 3: timout
    sql """ truncate table ${tableName} """
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'columns', 'user_id,name,age'

        file 'streamload_example.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
        }
    }
    qt_sql3 "SELECT * FROM ${tableName} order by user_id"

    //case4: max_filter_ratio
    sql """ truncate table ${tableName} """
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'columns', 'user_id,name,age'
        set 'max_filter_ratio', '0.4'

        file 'streamload_example.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
        }
    }
    qt_sql4 "SELECT * FROM ${tableName} order by user_id"

    // case5: where
    sql """ truncate table ${tableName} """
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'columns', 'user_id,name,age'
        set 'where', 'age>=35'

        file 'streamload_example.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
        }
    }
    qt_sql5 "SELECT * FROM ${tableName} order by user_id"

    //case6: csv_with_names
    sql """ truncate table ${tableName} """
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'columns', 'user_id,name,age'
        set 'format', 'csv_with_names'

        file 'streamload_example.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
        }
    }
    qt_sql6 "SELECT * FROM ${tableName} order by user_id"

    //case7: merge type
    sql """ truncate table ${tableName} """
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'columns', 'user_id,name,age'

        file 'streamload_example.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
        }
    }
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'columns', 'user_id,name,age'
        set 'merge_type', 'delete'

        file 'test_merge_type.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
        }
    }
    qt_sql7 "SELECT * FROM ${tableName} order by user_id"

    sql """ truncate table ${tableName} """
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'columns', 'user_id,name,age'

        file 'streamload_example.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
        }
    }
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'columns', 'user_id,name,age'
        set 'merge_type', 'merge'
        set 'delete', 'user_id=1'

        file 'test_merge_type.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
        }
    }
    qt_sql7 "SELECT * FROM ${tableName} order by user_id"

    //case 8: function_column.sequence_col
    sql """ truncate table ${tableName} """
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'columns', 'user_id,name,age'

        file 'streamload_example.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
        }
    }
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'columns', 'user_id,name,age'
        set 'merge_type', 'delete'
        set 'function_column.sequence_col', 'age'

        file 'test_seq.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
        }
    }
    qt_sql8 "SELECT * FROM ${tableName} order by user_id"

    // case 9: enclose escape
    def tableName1 = "test_stream_load_doc_case1"
    sql """ DROP TABLE IF EXISTS ${tableName1} """
    sql """
        CREATE TABLE ${tableName1}(
            username           VARCHAR(20)      NOT NULL,
            age                INT,
            address            VARCHAR(50)
        )
        DUPLICATE KEY(username)
        DISTRIBUTED BY HASH(username) BUCKETS 10
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """
    streamLoad {
        table "${tableName1}"

        set 'column_separator', ','
        set 'enclose', '\''
        set 'escape', '\\'

        file 'test_enclose_and_escape.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
        }
    }
    qt_sql9 "SELECT * FROM ${tableName1} order by username"

    // case 10: DEFAULT CURRENT_TIMESTAMP
    def tableName2 = "test_stream_load_doc_case2"
    sql """ DROP TABLE IF EXISTS ${tableName2} """
    sql """
        CREATE TABLE ${tableName2}(
            `id` bigint(30) NOT NULL,
            `order_code` varchar(30) DEFAULT NULL COMMENT '',
            `create_time` datetimev2(3) DEFAULT CURRENT_TIMESTAMP
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """
    streamLoad {
        table "${tableName2}"

        set 'column_separator', ','
        set 'colunms', 'id, order_code, create_time=CURRENT_TIMESTAMP()'

        file 'test_default.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
        }
    }
    qt_sql10 "SELECT * FROM ${tableName2} order by id"

    //case 11: array
    def tableName3 = "test_stream_load_doc_case3"
    sql """ DROP TABLE IF EXISTS ${tableName3} """
    sql """
        CREATE TABLE ${tableName3}(
            typ_id     BIGINT          NOT NULL COMMENT "ID",
            name       VARCHAR(20)     NULL     COMMENT "名称",
            arr        ARRAY<int(10)>  NULL     COMMENT "数组"
        )
        DUPLICATE KEY(typ_id)
        DISTRIBUTED BY HASH(typ_id) BUCKETS 10
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """
    streamLoad {
        table "${tableName3}"

        set 'column_separator', '|'

        file 'test_array.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
        }
    }
    qt_sql11 "SELECT * FROM ${tableName3} order by typ_id"

    //case 12: map
    def tableName4 = "test_stream_load_doc_case4"
    sql """ DROP TABLE IF EXISTS ${tableName4} """
    sql """
        CREATE TABLE ${tableName4}(
            user_id            BIGINT       NOT NULL COMMENT "ID",
            namemap            Map<STRING, INT>  NULL     COMMENT "名称"
        )
        DUPLICATE KEY(user_id)
        DISTRIBUTED BY HASH(user_id) BUCKETS 10
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """
    streamLoad {
        table "${tableName4}"

        set 'format', 'json'
        set 'strip_outer_array', 'true'

        file 'test_map.json'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
        }
    }
    qt_sql12 "SELECT * FROM ${tableName4} order by user_id"

    //case 13:bitmap
    def tableName5 = "test_stream_load_doc_case5"
    sql """ DROP TABLE IF EXISTS ${tableName5} """
    sql """
        CREATE TABLE ${tableName5}(
            typ_id     BIGINT                NULL   COMMENT "ID",
            hou        VARCHAR(10)           NULL   COMMENT "one",
            arr        BITMAP  BITMAP_UNION  NOT NULL   COMMENT "two"
        )
        AGGREGATE KEY(typ_id,hou)
        DISTRIBUTED BY HASH(typ_id,hou) BUCKETS 10
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """
    streamLoad {
        table "${tableName5}"

        set 'columns', 'typ_id,hou,arr,arr=to_bitmap(arr)'
        set 'column_separator', '|'

        file 'test_bitmap.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
        }
    }
    qt_sql13 "SELECT * FROM ${tableName5} order by typ_id"

    //case14: hll
    def tableName6 = "test_stream_load_doc_case6"
    sql """ DROP TABLE IF EXISTS ${tableName6} """
    sql """
        CREATE TABLE ${tableName6}(
            typ_id           BIGINT          NULL   COMMENT "ID",
            typ_name         VARCHAR(10)     NULL   COMMENT "NAME",
            pv               hll hll_union   NOT NULL   COMMENT "hll"
        )
        AGGREGATE KEY(typ_id,typ_name)
        DISTRIBUTED BY HASH(typ_id) BUCKETS 10
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """
    streamLoad {
        table "${tableName6}"

        set 'columns', 'typ_id,typ_name,pv=hll_hash(typ_id)'
        set 'column_separator', '|'

        file 'test_hll.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
        }
    }
    qt_sql14 "SELECT * FROM ${tableName6} order by typ_id"
}