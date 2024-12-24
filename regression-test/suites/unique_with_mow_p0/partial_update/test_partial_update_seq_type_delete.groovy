
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

suite("test_primary_key_partial_update_seq_type_delete", "p0") {

    String db = context.config.getDbNameByFile(context.file)
    sql "select 1;" // to create database

    for (def use_row_store : [false, true]) {
        logger.info("current params: use_row_store: ${use_row_store}")

        connect( context.config.jdbcUser, context.config.jdbcPassword, context.config.jdbcUrl) {
            sql "use ${db};"

            def tableName = "test_primary_key_partial_update_seq_type_delete"

            // create table
            sql """ DROP TABLE IF EXISTS ${tableName} """
            sql """
                    CREATE TABLE ${tableName} (
                        `id` int(11) NOT NULL COMMENT "用户 ID",
                        `name` varchar(65533) DEFAULT "unknown" COMMENT "用户姓名",
                        `score` int(11) NOT NULL COMMENT "用户得分",
                        `test` int(11) NULL COMMENT "null test",
                        `dft` int(11) DEFAULT "4321",
                        `update_time` date NULL)
                    UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
                    PROPERTIES(
                        "replication_num" = "1",
                        "enable_unique_key_merge_on_write" = "true",
                        "function_column.sequence_type" = "int",
                        "store_row_column" = "${use_row_store}"); """
            // insert 2 lines
            sql """
                insert into ${tableName}
                    (id, name, score, test, dft, update_time, __DORIS_SEQUENCE_COL__)
                values
                    (2, "doris2", 2000, 223, 1, '2023-01-01', 1),
                    (1, "doris", 1000, 123, 1, '2023-01-01', 1)
            """

            sql "sync"

            qt_select_default """
                select * from ${tableName} order by id;
            """

            // no sequence column header, stream load should fail
            streamLoad {
                table "${tableName}"

                set 'column_separator', ','
                set 'format', 'csv'
                set 'partial_columns', 'true'
                set 'columns', 'id,score,test,update_time'
                set 'merge_type', 'DELETE'

                file 'basic_with_test.csv'
                time 10000 // limit inflight 10s

                check { result, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    log.info("Stream load result: ${result}".toString())
                    def json = parseJson(result)
                    assertEquals("fail", json.Status.toLowerCase())
                    assertTrue(json.Message.contains('need to specify the sequence column'))
                }
            }

            sql "sync"

            // both partial_columns and sequence column header, stream load should success
            streamLoad {
                table "${tableName}"

                set 'column_separator', ','
                set 'format', 'csv'
                set 'partial_columns', 'true'
                set 'columns', 'id,score,test,update_time'
                set 'function_column.sequence_col', 'score'
                set 'merge_type', 'DELETE'

                file 'basic_with_test.csv'
                time 10000 // limit inflight 10s
            }

            sql "sync"

            qt_partial_update_with_seq_score """
                select * from ${tableName} order by id;
            """

            sql "SET show_hidden_columns=true"

            sql "sync"

            qt_partial_update_with_seq_score_hidden """
                select * from ${tableName} order by id;
            """

            // use test as sequence column
            streamLoad {
                table "${tableName}"

                set 'column_separator', ','
                set 'format', 'csv'
                set 'partial_columns', 'true'
                set 'columns', 'id,score,test,update_time'
                set 'function_column.sequence_col', 'test'
                set 'merge_type', 'DELETE'

                file 'basic_with_test.csv'
                time 10000 // limit inflight 10s
            }

            sql "SET show_hidden_columns=false"

            sql "sync"

            qt_partial_update_with_seq_test """
                select * from ${tableName} order by id;
            """

            sql "SET show_hidden_columns=true"

            sql "sync"

            qt_partial_update_with_seq_test_hidden """
                select * from ${tableName} order by id;
            """

            // no partial update header, stream load should success,
            // but the missing columns will be filled with default values
            streamLoad {
                table "${tableName}"

                set 'column_separator', ','
                set 'format', 'csv'
                set 'columns', 'id,score,test,update_time'
                set 'function_column.sequence_col', 'score'
                set 'merge_type', 'DELETE'

                file 'basic_with_test2.csv'
                time 10000 // limit inflight 10s
            }

            sql "sync"

            qt_select_no_partial_update_score """
                select * from ${tableName} order by id;
            """

            // no partial update header, stream load should success,
            // but the missing columns will be filled with default values
            streamLoad {
                table "${tableName}"

                set 'column_separator', ','
                set 'format', 'csv'
                set 'columns', 'id,score,test,update_time'
                set 'function_column.sequence_col', 'test'
                set 'merge_type', 'DELETE'

                file 'basic_with_test2.csv'
                time 10000 // limit inflight 10s
            }

            sql "sync"

            qt_select_no_partial_update_test """
                select * from ${tableName} order by id;
            """

            // drop table
            sql """ DROP TABLE IF EXISTS ${tableName} """
        }
    }
}
