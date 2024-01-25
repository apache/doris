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

suite("test_unique_table_new_sequence") {
    for (def enable_fall_back : [false, true]) {
        def tableName = "test_uniq_new_sequence"
        sql """ DROP TABLE IF EXISTS ${tableName} """
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
        "enable_unique_key_merge_on_write" = "true",
        "function_column.sequence_col" = "v2",
        "replication_allocation" = "tag.location.default: 1",
        "light_schema_change" = "true"
        );
        """
        sql "set enable_fallback_to_original_planner=${enable_fall_back}"
        // test streamload with seq col
        streamLoad {
            table "${tableName}"

            set 'column_separator', ','
            set 'columns', 'k1,v1,v2,v3,v4'

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

        sql "UPDATE ${tableName} SET v1 = 10 WHERE k1 = 1"

        sql "UPDATE ${tableName} SET v2 = 14 WHERE k1 = 2"

        sql "UPDATE ${tableName} SET v2 = 11 WHERE k1 = 3"

        sql "sync"

        qt_count "SELECT COUNT(*) from ${tableName}"

        order_qt_part "SELECT k1, v1, v2 from ${tableName}"

        order_qt_all "SELECT * from ${tableName}"

        // test insert into with column list, which not contains the seq mapping column v2
        test {
            sql "INSERT INTO ${tableName} (k1, v1, v3, v4) values(15, 8, 20, 21)"
            exception "Table ${tableName} has sequence column, need to specify the sequence column"
        }

        // test insert into without column list
        sql "INSERT INTO ${tableName} values(15, 8, 19, 20, 21)"

        // test insert into with column list
        sql "INSERT INTO ${tableName} (k1, v1, v2, v3, v4) values(15, 9, 18, 21, 22)"

        sql "SET show_hidden_columns=true"

        sql "sync"

        qt_count "SELECT COUNT(*) from ${tableName}"

        order_qt_part "SELECT k1, v1, v2 from ${tableName}"

        order_qt_all "SELECT * from ${tableName}"

        qt_desc "desc ${tableName}"

        sql "DROP TABLE ${tableName}"

        sql """ DROP TABLE IF EXISTS ${tableName} """
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
        "enable_unique_key_merge_on_write" = "false",
        "function_column.sequence_col" = "v4",
        "replication_allocation" = "tag.location.default: 1",
        "light_schema_change" = "true"
        );
        """

        // test insert into with column list, which not contains the seq mapping column v4
        // in begin/commit
        sql "begin;"
        test {
            sql "INSERT INTO ${tableName} (k1, v1, v2, v3) values(1,1,1,1)"
            exception "Table ${tableName} has sequence column, need to specify the sequence column"
        }
        sql "commit;"

        // test insert into without column list, in begin/commit
        sql "begin;"
        sql "insert into ${tableName} values (1,1,1,1,1),(2,2,2,2,2),(3,3,3,3,3);"
        sql "commit;"

        qt_1 "select * from ${tableName} order by k1;"

        sql "begin;"
        sql "insert into ${tableName} (k1, v1, v2, v3, v4) values (2,20,20,20,20);"
        sql "commit;"

        qt_2 "select * from ${tableName} order by k1;"

        sql "begin;"
        sql "insert into ${tableName} (k1, v1, v2, v3, v4) values (3,30,30,30,1);"
        sql "commit;"

        qt_3 "select * from ${tableName} order by k1"

        qt_desc "desc ${tableName}"

        sql "SET show_hidden_columns=false"

        sql "DROP TABLE ${tableName}"
    }
}

