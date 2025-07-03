
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

suite("test_primary_key_partial_update_with_update_stmt", "p0") {

    String db = context.config.getDbNameByFile(context.file)
    sql "select 1;" // to create database

    for (def use_row_store : [false, true]) {
        logger.info("current params: use_row_store: ${use_row_store}")

        connect( context.config.jdbcUser, context.config.jdbcPassword, context.config.jdbcUrl) {
            sql "use ${db};"

            def tableName = "test_primary_key_partial_update_with_update_stmt"
            def tableNameJoinA = "test_primary_key_partial_update_with_update_stmt_join_a"
            def tableNameJoinB = "test_primary_key_partial_update_with_update_stmt_join_b"

            // create table
            sql """ DROP TABLE IF EXISTS ${tableName} """
            sql """ DROP TABLE IF EXISTS ${tableNameJoinA} """
            sql """ DROP TABLE IF EXISTS ${tableNameJoinB} """
            sql """
                    CREATE TABLE ${tableName} (
                        `id` int(11) NOT NULL COMMENT "用户 ID",
                        `name` varchar(65533) NOT NULL COMMENT "用户姓名",
                        `score` int(11) NOT NULL COMMENT "用户得分",
                        `test` int(11) NULL COMMENT "null test",
                        `dft` int(11) DEFAULT "4321")
                        UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
                        PROPERTIES("replication_num" = "1", "enable_unique_key_merge_on_write" = "true",
                        "store_row_column" = "${use_row_store}"); """
            // insert 2 lines
            sql """
                insert into ${tableName} values(2, "doris2", 2000, 223, 1)
            """

            sql """
                insert into ${tableName} values(1, "doris", 1000, 123, 1)
            """

            // case 1: partially update normally
            sql """
                update ${tableName} set score = 4000 where id = 1
            """

            sql "sync"

            qt_select_default """
                select * from ${tableName} order by id;
            """

            // case 2: partially update non-exist key
            def result1 = sql """
                update ${tableName} set score = 2000 where id = 3
            """
            assertTrue(result1.size() == 1)
            assertTrue(result1[0].size() == 1)
            assertTrue(result1[0][0] == 0, "Query OK, 0 rows affected")

            sql "sync"

            // create two table for join
            sql """CREATE TABLE ${tableNameJoinA} (
                        `id` int(11) NOT NULL COMMENT "用户 ID",
                        `name` varchar(65533) NOT NULL COMMENT "用户姓名")
                        UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
                        PROPERTIES("replication_num" = "1", "enable_unique_key_merge_on_write" = "true",
                        "store_row_column" = "${use_row_store}"); """
 
            sql """CREATE TABLE ${tableNameJoinB} (
                        `id` int(11) NOT NULL COMMENT "用户 ID",
                        `score` int(11) NOT NULL COMMENT "用户得分")
                        UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
                        PROPERTIES("replication_num" = "1", "enable_unique_key_merge_on_write" = "true",
                        "store_row_column" = "${use_row_store}"); """

            // case 3: non-exsit key with join
            sql """
                insert into ${tableNameJoinA} values(4, "doris6")
            """
            sql """
                insert into ${tableNameJoinB} values(4, 4000)
            """
            def result2 = sql """
                update ${tableName} set ${tableName}.score = ${tableNameJoinB}.score, ${tableName}.name = ${tableNameJoinA}.name
                from ${tableNameJoinA} inner join ${tableNameJoinB} on ${tableNameJoinA}.id = ${tableNameJoinB}.id
                where ${tableName}.id = ${tableNameJoinA}.id
            """
            assertTrue(result2.size() == 1)
            assertTrue(result2[0].size() == 1)
            assertTrue(result2[0][0] == 0, "Query OK, 0 rows affected")

            sql "sync"

            // case 4: partially update normally with join
            sql """
                insert into ${tableNameJoinA} values(2, "doris4")
            """

            sql """
                insert into ${tableNameJoinA} values(1, "doris3")
            """

            sql """
                insert into ${tableNameJoinB} values(2, 8000)
            """

            sql """
                insert into ${tableNameJoinB} values(3, 7000)
            """

            sql """
                update ${tableName} set ${tableName}.score = ${tableNameJoinB}.score, ${tableName}.name = ${tableNameJoinA}.name
                from ${tableNameJoinA} inner join ${tableNameJoinB} on ${tableNameJoinA}.id = ${tableNameJoinB}.id
                where ${tableName}.id = ${tableNameJoinA}.id
            """

            sql "sync"

            qt_select_join """
                select * from ${tableName} order by id;
            """


            sql """ DROP TABLE IF EXISTS ${tableName} """
            sql """ DROP TABLE IF EXISTS ${tableNameJoinA} """
            sql """ DROP TABLE IF EXISTS ${tableNameJoinB} """
        }
    }
}
