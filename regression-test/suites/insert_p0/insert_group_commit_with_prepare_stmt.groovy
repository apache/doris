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

import com.mysql.cj.jdbc.StatementImpl

suite("insert_group_commit_with_prepare_stmt") {
    def user = context.config.jdbcUser
    def password = context.config.jdbcPassword
    def realDb = "regression_test_insert_p0"
    def table = realDb + ".insert_group_commit_with_prepare_stmt"

    sql "CREATE DATABASE IF NOT EXISTS ${realDb}"

    def getRowCount = { expectedRowCount ->
        def retry = 0
        while (retry < 30) {
            sleep(4000)
            def rowCount = sql "select count(*) from ${table}"
            logger.info("rowCount: " + rowCount + ", retry: " + retry)
            if (rowCount[0][0] >= expectedRowCount) {
                break
            }
            retry++
        }
    }

    // id, name, score
    def insert_prepared = { stmt, k1, k2, k3 ->
        stmt.setInt(1, k1)
        stmt.setString(2, k2)
        if (k3 == null) {
            stmt.setNull(3, java.sql.Types.INTEGER)
        } else {
            stmt.setInt(3, k3)
        }
        stmt.addBatch()
    }

    // name, id, delete_sign
    def insert_prepared_partial = { stmt, k1, k2, k3 ->
        stmt.setObject(1, k1)
        stmt.setObject(2, k2)
        stmt.setObject(3, k3)
        stmt.addBatch()
    }

    // name, id
    def insert_prepared_partial_dup = { stmt, k1, k2 ->
        stmt.setString(1, k1)
        stmt.setInt(2, k2)
        stmt.addBatch()
    }

    def group_commit_insert = { stmt, expected_row_count ->
        def result = stmt.executeBatch()
        logger.info("insert result: " + result)
        def serverInfo = (((StatementImpl) stmt).results).getServerInfo()
        logger.info("result server info: " + serverInfo)
        if (result != expected_row_count) {
            logger.warn("insert result: " + result + ", expected_row_count: " + expected_row_count)
        }
        // assertEquals(result, expected_row_count)
        assertTrue(serverInfo.contains("'status':'PREPARE'"))
        assertTrue(serverInfo.contains("'label':'group_commit_"))
    }

    def url = getServerPrepareJdbcUrl(context.config.jdbcUrl, realDb)
    logger.info("url: " + url)

    def result1 = connect(user=user, password=password, url=url) {
        try {
            // create table
            sql """ drop table if exists ${table}; """

            sql """
            CREATE TABLE ${table} (
                `id` int(11) NOT NULL,
                `name` varchar(50) NULL,
                `score` int(11) NULL default "-1"
            ) ENGINE=OLAP
            UNIQUE KEY(`id`, `name`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            );
            """

            sql """ set enable_insert_group_commit = true; """

            // 1. insert into
            def insert_stmt = prepareStatement """ INSERT INTO ${table} VALUES(?, ?, ?) """
            assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, insert_stmt.class)

            insert_prepared insert_stmt, 1, "a", 10
            group_commit_insert insert_stmt, 1

            insert_prepared insert_stmt, 2, null, 20
            insert_prepared insert_stmt, 3, "c", null
            insert_prepared insert_stmt, 4, "d", 40
            group_commit_insert insert_stmt, 3

            insert_prepared insert_stmt, 5, "e", null
            insert_prepared insert_stmt, 6, "f", 40
            group_commit_insert insert_stmt, 2

            getRowCount(6)
            qt_sql """ select * from ${table} order by id asc; """

            // 2. insert into partial columns
            insert_stmt = prepareStatement """ INSERT INTO ${table}(name, id, __DORIS_DELETE_SIGN__) VALUES(?, ?, ?) """
            assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, insert_stmt.class)

            insert_prepared_partial insert_stmt, 'a', 1, 1
            group_commit_insert insert_stmt, 1

            insert_prepared_partial insert_stmt, 'e', 7, 0
            insert_prepared_partial insert_stmt, null, 8, 0
            group_commit_insert insert_stmt, 2

            getRowCount(7)
            qt_sql """ select * from ${table} order by id, name, score asc; """

        } finally {
            // try_sql("DROP TABLE ${table}")
        }
    }

    table = "test_prepared_stmt_duplicate"
    result1 = connect(user=user, password=password, url=url) {
        try {
            // create table
            sql """ drop table if exists ${table}; """

            sql """
            CREATE TABLE ${table} (
                `id` int(11) NOT NULL,
                `name` varchar(50) NULL,
                `score` int(11) NULL default "-1"
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`, `name`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            );
            """

            sql """ set enable_insert_group_commit = true; """

            // 1. insert into
            def insert_stmt = prepareStatement """ INSERT INTO ${table} VALUES(?, ?, ?) """
            assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, insert_stmt.class)

            insert_prepared insert_stmt, 1, "a", 10
            group_commit_insert insert_stmt, 1

            insert_prepared insert_stmt, 2, null, 20
            insert_prepared insert_stmt, 3, "c", null
            insert_prepared insert_stmt, 4, "d", 40
            group_commit_insert insert_stmt, 3

            insert_prepared insert_stmt, 5, "e", null
            insert_prepared insert_stmt, 6, "f", 40
            group_commit_insert insert_stmt, 2

            getRowCount(6)
            qt_sql """ select * from ${table} order by id asc; """

            // 2. insert into partial columns
            insert_stmt = prepareStatement """ INSERT INTO ${table}(name, id) VALUES(?, ?) """
            assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, insert_stmt.class)

            insert_prepared_partial_dup insert_stmt, 'a', 1
            group_commit_insert insert_stmt, 1

            insert_prepared_partial_dup insert_stmt, 'e', 7
            insert_prepared_partial_dup insert_stmt, null, 8
            group_commit_insert insert_stmt, 2

            getRowCount(9)
            qt_sql """ select * from ${table} order by id, name, score asc; """

        } finally {
            // try_sql("DROP TABLE ${table}")
        }
    }
}
