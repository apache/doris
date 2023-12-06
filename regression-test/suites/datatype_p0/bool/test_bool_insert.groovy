
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

suite("test_bool_insert") {
    sql """ DROP TABLE IF EXISTS test_bool_insert """

    sql """
        CREATE TABLE test_bool_insert (
          `id` int,
          `commiter` boolean
        ) ENGINE=OLAP
        UNIQUE KEY (`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        """

    sql "insert into test_bool_insert values(1, 'true')"
    sql "insert into test_bool_insert values(2, 'false')"
    sql "insert into test_bool_insert values(3, true)"
    sql "insert into test_bool_insert values(4, false)"
    sql "insert into test_bool_insert values(5, '1')"
    sql "insert into test_bool_insert values(6, '0')"
    sql "insert into test_bool_insert values(7, 1)"
    sql "insert into test_bool_insert values(8, 0)"

    // wrong value
    test {
        sql "insert into test_bool_insert values(9, 333)"
        exception("java.sql.SQLException: errCode = 2, detailMessage = (172.21.16.12)[INVALID_ARGUMENT]Wrong expression! bool only be true|false or 0|1")
    }

    qt_sql_insert "select * from test_bool_insert order by id"

    sql " truncate table test_bool_insert "

    // test stream load
    streamLoad {
        table "test_bool_insert"
        set 'column_separator', '|'
        file 'bool.csv'
        set 'strict_mode', 'true'
        time 60000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals(5, json.NumberTotalRows)
            assertEquals(4, json.NumberLoadedRows)
        }
    }

    sql """sync"""
    qt_sql_load """ SELECT * FROM test_bool_insert ORDER BY id; """

    // without strict mode
    streamLoad {
        table "test_bool_insert"
        set 'column_separator', '|'
        file 'bool.csv'
        set 'strict_mode', 'false'
        time 60000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals(5, json.NumberTotalRows)
            assertEquals(5, json.NumberLoadedRows)
        }
    }

    sql """sync"""
    qt_sql_load1 """ SELECT * FROM test_bool_insert ORDER BY id; """
}
