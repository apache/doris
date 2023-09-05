
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

// The cases is copied from https://github.com/trinodb/trino/tree/master
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases
// and modified by Doris.
suite("prepare_insert") {
    def user = context.config.jdbcUser
    def password = context.config.jdbcPassword
    def realDb = "regression_test_insert_p0"
    def tableName = realDb + ".prepare_insert"

    sql "CREATE DATABASE IF NOT EXISTS ${realDb}"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE ${tableName} (
            `id` int(11) NOT NULL,
            `name` varchar(50) NULL,
            `score` int(11) NULL DEFAULT "-1"
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`, `name`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    // Parse url
    String jdbcUrl = context.config.jdbcUrl
    String urlWithoutSchema = jdbcUrl.substring(jdbcUrl.indexOf("://") + 3)
    def sql_ip = urlWithoutSchema.substring(0, urlWithoutSchema.indexOf(":"))
    def sql_port
    if (urlWithoutSchema.indexOf("/") >= 0) {
        // e.g: jdbc:mysql://locahost:8080/?a=b
        sql_port = urlWithoutSchema.substring(urlWithoutSchema.indexOf(":") + 1, urlWithoutSchema.indexOf("/"))
    } else {
        // e.g: jdbc:mysql://locahost:8080
        sql_port = urlWithoutSchema.substring(urlWithoutSchema.indexOf(":") + 1)
    }
    // set server side prepared statement url
    def url = "jdbc:mysql://" + sql_ip + ":" + sql_port + "/" + realDb + "?&useServerPrepStmts=true"

    def result1 = connect(user = user, password = password, url = url) {
        def stmt = prepareStatement "insert into ${tableName} values(?, ?, ?)"
        assertEquals(stmt.class, com.mysql.cj.jdbc.ServerPreparedStatement)
        stmt.setInt(1, 1)
        stmt.setString(2, "a")
        stmt.setInt(3, 90)
        def result = stmt.execute()
        logger.info("result: ${result}")
        stmt.setInt(1, 2)
        stmt.setString(2, "ab")
        stmt.setInt(3, 91)
        result = stmt.execute()
        logger.info("result: ${result}")

        stmt.setInt(1, 3)
        stmt.setString(2, "abc")
        stmt.setInt(3, 92)
        stmt.addBatch()
        stmt.setInt(1, 4)
        stmt.setString(2, "abcd")
        stmt.setInt(3, 93)
        stmt.addBatch()
        result = stmt.executeBatch()
        logger.info("result: ${result}")
        assertEquals(result.size(), 2)
        assertEquals(result[0], 1)
        assertEquals(result[1], 1)

        stmt.close()
    }

    // insert with label
    def label = "insert_" + System.currentTimeMillis()
    result1 = connect(user = user, password = password, url = url) {
        def stmt = prepareStatement "insert into ${tableName} with label ${label} values(?, ?, ?)"
        assertEquals(stmt.class, com.mysql.cj.jdbc.ClientPreparedStatement)
        stmt.setInt(1, 5)
        stmt.setString(2, "a5")
        stmt.setInt(3, 94)
        def result = stmt.execute()
        logger.info("result: ${result}")

        stmt.close()
    }

    url += "&rewriteBatchedStatements=true"
    result1 = connect(user = user, password = password, url = url) {
        def stmt = prepareStatement "insert into ${tableName} values(?, ?, ?)"
        assertEquals(stmt.class, com.mysql.cj.jdbc.ServerPreparedStatement)
        stmt.setInt(1, 10)
        stmt.setString(2, "a")
        stmt.setInt(3, 90)
        def result = stmt.execute()
        logger.info("result: ${result}")
        stmt.setInt(1, 20)
        stmt.setString(2, "ab")
        stmt.setInt(3, 91)
        result = stmt.execute()
        logger.info("result: ${result}")

        stmt.setInt(1, 30)
        stmt.setString(2, "abc")
        stmt.setInt(3, 92)
        stmt.addBatch()
        stmt.setInt(1, 40)
        stmt.setString(2, "abcd")
        stmt.setInt(3, 93)
        stmt.addBatch()
        result = stmt.executeBatch()
        logger.info("result: ${result}")
        assertEquals(result.size(), 2)
        // TODO why return -2
        assertEquals(result[0], -2)
        assertEquals(result[1], -2)

        stmt.close()
    }

    qt_sql """ select * from ${tableName} order by id, name, score """
}