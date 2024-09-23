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

suite("test_unique_table_sequence") {
    for (def enable_nereids_planner : [false, true]) {
        def tableName = "test_uniq_sequence"
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
              `k1` int NULL,
              `v1` tinyint NULL,
              `v2` int,
              `v3` int,
              `v4` int
            ) ENGINE=OLAP
            UNIQUE KEY(k1)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 3
            PROPERTIES (
            "function_column.sequence_type" = "int",
            "replication_allocation" = "tag.location.default: 1"
            );
        """
        sql """ set enable_nereids_planner=${enable_nereids_planner}; """
        sql "set enable_fallback_to_original_planner=false; "
        // test streamload with seq col
        streamLoad {
            table "${tableName}"

            set 'column_separator', ','
            set 'columns', 'k1,v1,v2,v3,v4'
            set 'function_column.sequence_col', 'v2'

            file 'unique_key_data1.csv'
            time 10000 // limit inflight 10s

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(4, json.NumberTotalRows)
                assertEquals(4, json.NumberLoadedRows)
                assertEquals(0, json.NumberFilteredRows)
                assertEquals(0, json.NumberUnselectedRows)
            }
        }
        sql "sync"
        order_qt_all "SELECT * from ${tableName}"

        // test update data, using streamload with seq col
        streamLoad {
            table "${tableName}"

            set 'column_separator', ','
            set 'columns', 'k1,v1,v2,v3,v4'
            set 'function_column.sequence_col', 'v2'

            file 'unique_key_data2.csv'
            time 10000 // limit inflight 10s

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(4, json.NumberTotalRows)
                assertEquals(4, json.NumberLoadedRows)
                assertEquals(0, json.NumberFilteredRows)
                assertEquals(0, json.NumberUnselectedRows)
            }
        }
        sql "sync"

        qt_count "SELECT COUNT(*) from ${tableName}"

        order_qt_part "SELECT k1, v1, v2 from ${tableName}"

        order_qt_all "SELECT * from ${tableName}"

        // test update on table with seq col
        sql "UPDATE ${tableName} SET v1 = 10 WHERE k1 = 1"

        sql "UPDATE ${tableName} SET v2 = 14 WHERE k1 = 2"

        sql "UPDATE ${tableName} SET v2 = 11 WHERE k1 = 3"

        sql "sync"

        qt_count "SELECT COUNT(*) from ${tableName}"

        order_qt_part "SELECT k1, v1, v2 from ${tableName}"

        order_qt_all "SELECT * from ${tableName}"

        // test insert into without column list
        test {
            sql "INSERT INTO ${tableName} values(15, 8, 19, 20, 21)"
            exception "Table ${tableName} has sequence column, need to specify the sequence column"
        }

        // test insert into with column list
        test {
            sql "INSERT INTO ${tableName} (k1, v1, v2, v3, v4) values(15, 8, 19, 20, 21)"
            exception "Table ${tableName} has sequence column, need to specify the sequence column"
        }

        // correct way of insert into with seq col
        sql "INSERT INTO ${tableName} (k1, v1, v2, v3, v4, __DORIS_SEQUENCE_COL__) values(15, 8, 19, 20, 21, 3)"

        sql "INSERT INTO ${tableName} (k1, v1, v2, v3, v4, __DORIS_SEQUENCE_COL__) values(15, 9, 18, 21, 22, 2)"

        sql "SET show_hidden_columns=true"

        sql "sync"

        qt_count "SELECT COUNT(*) from ${tableName}"

        order_qt_part "SELECT k1, v1, v2 from ${tableName}"

        order_qt_all "SELECT * from ${tableName}"

        sql "DROP TABLE ${tableName}"

        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
              `k1` int NULL,
              `v1` tinyint NULL,
              `v2` int,
              `v3` int,
              `or` int
            ) ENGINE=OLAP
            UNIQUE KEY(k1)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 3
            PROPERTIES (
            "function_column.sequence_type" = "int",
            "replication_allocation" = "tag.location.default: 1"
            );
        """

        // test insert into without column list, in begin/commit
        sql "begin;"
        test {
            sql "INSERT INTO ${tableName} values(15, 8, 19, 20, 21)"
            exception "Table ${tableName} has sequence column, need to specify the sequence column"
        }
        sql "commit;"

        // test insert into with column list, in begin/commit
        sql "begin;"
        test {
            sql "INSERT INTO ${tableName} (k1, v1, v2, v3, `or`) values(15, 8, 19, 20, 21)"
            exception "Table ${tableName} has sequence column, need to specify the sequence column"
        }
        sql "commit;"

        sql """set enable_nereids_planner=true"""

        sql "begin;"
        sql "insert into ${tableName} (k1, v1, v2, v3, `or`, __doris_sequence_col__) values (1,1,1,1,1,1),(2,2,2,2,2,2),(3,3,3,3,3,3);"
        sql "commit;"

        qt_1 "select * from ${tableName} order by k1;"

        sql "begin;"
        sql "insert into ${tableName} (k1, v1, v2, v3, `or`, __DORIS_SEQUENCE_COL__) values (2,20,20,20,20,20);"
        sql "commit;"

        qt_2 "select * from ${tableName} order by k1;"

        sql "begin;"
        sql "insert into ${tableName} (k1, v1, v2, v3, `or`, __DORIS_SEQUENCE_COL__) values (3,30,30,30,30,1);"
        sql "commit;"

        qt_3 "select * from ${tableName} order by k1"

        sql "SET show_hidden_columns=false"

        sql "DROP TABLE ${tableName}"
    }
}

