
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

suite("test_primary_key_partial_update_complex_type_schema_change", "p0") {

    String db = context.config.getDbNameByFile(context.file)
    sql "select 1;" // to create database

    for (def use_row_store : [false, true]) {
        logger.info("current params: use_row_store: ${use_row_store}")

        connect(user = context.config.jdbcUser, password = context.config.jdbcPassword, url = context.config.jdbcUrl) {
            sql "use ${db};"
            def tableName = "test_primary_key_partial_update_complex_type_schema_change"
            // create table
            sql """ DROP TABLE IF EXISTS ${tableName} """
            sql """ CREATE TABLE ${tableName} (
                        `id` int(11) NOT NULL COMMENT "用户 ID",
                        `c_varchar` varchar(65533) NULL COMMENT "用户姓名")
                        UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
                        PROPERTIES("replication_num" = "1", "enable_unique_key_merge_on_write" = "true",
                        "store_row_column" = "${use_row_store}"); """

            // insert 2 lines
            sql """
                insert into ${tableName} values(2, "doris2")
            """

            sql """
                insert into ${tableName} values(1, "doris1")
            """

            // add new jsonb column
            sql """
                ALTER TABLE ${tableName} ADD COLUMN c_jsonb JSONB NULL;
            """

            sql """
                insert into ${tableName} values(2, "doris3", '{"jsonk3": 333, "jsonk4": 444}')
            """

            // update varchar column
            streamLoad {
                table "${tableName}"

                set 'partial_columns', 'true'
                set 'columns', 'id,c_varchar'

                file 'complex_type/varchar.tsv'
                time 10000 // limit inflight 10s
            }

            sql "sync"

            qt_update_varchar"""
                select * from ${tableName} order by id;
            """

            // update jsonb column, update 2 rows, add 1 new row
            streamLoad {
                table "${tableName}"

                set 'partial_columns', 'true'
                set 'columns', 'id,c_jsonb'

                file 'complex_type/jsonb.tsv'
                time 10000 // limit inflight 10s
            }

            sql "sync"

            qt_update_jsonb"""
                select * from ${tableName} order by id;
            """

            // add new array column
            sql """
                ALTER TABLE ${tableName} ADD COLUMN c_array ARRAY<INT> NULL;
            """

            sql """
                insert into ${tableName} values(2, "doris3", '{"jsonk3": 333, "jsonk4": 444}', [300, 400])
            """

            // update varchar column
            streamLoad {
                table "${tableName}"

                set 'partial_columns', 'true'
                set 'columns', 'id,c_varchar'

                file 'complex_type/varchar.tsv'
                time 10000 // limit inflight 10s
            }

            sql "sync"

            qt_update_varchar"""
                select * from ${tableName} order by id;
            """

            // update array column, update 2 rows, add 1 new row
            streamLoad {
                table "${tableName}"

                set 'partial_columns', 'true'
                set 'columns', 'id,c_array'

                file 'complex_type/array.tsv'
                time 10000 // limit inflight 10s
            }

            sql "sync"

            qt_update_array"""
                select * from ${tableName} order by id;
            """

            // add new struct column
            sql """
                ALTER TABLE ${tableName} ADD COLUMN c_struct STRUCT<a:INT, b:INT> NULL;
            """

            sql """
                insert into ${tableName} values(2, "doris3", '{"jsonk3": 333, "jsonk4": 444}', [300, 400], {3, 4})
            """

            // update varchar column
            streamLoad {
                table "${tableName}"

                set 'partial_columns', 'true'
                set 'columns', 'id,c_varchar'

                file 'complex_type/varchar.tsv'
                time 10000 // limit inflight 10s
            }

            sql "sync"

            qt_update_varchar"""
                select * from ${tableName} order by id;
            """

            // update struct column, update 2 rows, add 1 new row
            streamLoad {
                table "${tableName}"

                set 'partial_columns', 'true'
                set 'columns', 'id,c_struct'

                file 'complex_type/struct.tsv'
                time 10000 // limit inflight 10s
            }

            sql "sync"

            qt_update_struct"""
                select * from ${tableName} order by id;
            """

            // drop table
            sql """ DROP TABLE IF EXISTS ${tableName} """
        }
    }
}
