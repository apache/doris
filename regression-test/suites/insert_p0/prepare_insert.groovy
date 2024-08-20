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

import com.mysql.cj.ServerPreparedQuery
import com.mysql.cj.jdbc.ConnectionImpl
import com.mysql.cj.jdbc.JdbcStatement
import com.mysql.cj.jdbc.ServerPreparedStatement
import com.mysql.cj.jdbc.StatementImpl

import java.lang.reflect.Field
import java.util.concurrent.CopyOnWriteArrayList

suite("prepare_insert") {
    def user = context.config.jdbcUser
    def password = context.config.jdbcPassword
    def realDb = "regression_test_insert_p0"
    def tableName = realDb + ".prepare_insert"

    sql "CREATE DATABASE IF NOT EXISTS ${realDb}"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE ${tableName} (
            `id` int(11) NULL,
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
    String url = getServerPrepareJdbcUrl(context.config.jdbcUrl, realDb)

    def check_is_master_fe = {
        // check if executed on master fe
        // observer fe will forward the insert statements to master and forward does not support prepare statement
        def fes = sql_return_maparray "show frontends"
        logger.info("frontends: ${fes}")
        def is_master_fe = true
        for (def fe : fes) {
            if (url.contains(fe.Host + ":")) {
                if (fe.IsMaster == "false") {
                    is_master_fe = false
                }
                break
            }
        }
        logger.info("is master fe: ${is_master_fe}")
        return is_master_fe
    }
    def is_master_fe = check_is_master_fe()

    def getServerInfo = { stmt ->
        def serverInfo = (((StatementImpl) stmt).results).getServerInfo()
        logger.info("server info: " + serverInfo)
        return serverInfo
    }

    def getStmtId = { stmt ->
        if (!is_master_fe) {
            return 0
        }
        ConnectionImpl connection = (ConnectionImpl) stmt.getConnection()
        Field field = ConnectionImpl.class.getDeclaredField("openStatements")
        field.setAccessible(true)
        CopyOnWriteArrayList<JdbcStatement> openStatements = (CopyOnWriteArrayList<JdbcStatement>) field.get(
                connection)
        List<Long> serverStatementIds = new ArrayList<Long>()
        for (JdbcStatement openStatement : openStatements) {
            ServerPreparedStatement serverPreparedStatement = (ServerPreparedStatement) openStatement
            ServerPreparedQuery serverPreparedQuery = (ServerPreparedQuery) serverPreparedStatement.getQuery()
            long serverStatementId = serverPreparedQuery.getServerStatementId()
            serverStatementIds.add(serverStatementId)
        }
        logger.info("server statement ids: " + serverStatementIds)
        return serverStatementIds
    }

    def check = { Closure<?> c -> {
            if (is_master_fe) {
                c()
            }
        }
    }

    def result1 = connect(user = user, password = password, url = url) {
        def stmt = prepareStatement "insert into ${tableName} values(?, ?, ?)"
        check {assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, stmt.class)}
        stmt.setInt(1, 1)
        stmt.setString(2, "a")
        stmt.setInt(3, 90)
        def result = stmt.execute()
        logger.info("result: ${result}")
        getServerInfo(stmt)
        def stmtId = getStmtId(stmt)

        stmt.setInt(1, 2)
        stmt.setString(2, "ab")
        stmt.setInt(3, 91)
        result = stmt.execute()
        logger.info("result: ${result}")
        getServerInfo(stmt)
        assertEquals(stmtId, getStmtId(stmt))

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
        getServerInfo(stmt)
        // Even if we write 2 rows as a batch, but the client does not add rewriteBatchedStatements=true in the url,
        // so the insert is executed in fe one by one.
        assertEquals(stmtId, getStmtId(stmt))

        stmt.close()
    }

    // insert with null
    result1 = connect(user = user, password = password, url = url) {
        def stmt = prepareStatement "insert into ${tableName} values(?, ?, ?)"
        check {assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, stmt.class)}
        stmt.setNull(1, java.sql.Types.INTEGER)
        stmt.setNull(2, java.sql.Types.VARCHAR)
        stmt.setNull(3, java.sql.Types.INTEGER)
        def result = stmt.execute()
        logger.info("result: ${result}")
        getServerInfo(stmt)

        stmt.close()
    }

    // insert with label
    def label = "insert_" + System.currentTimeMillis()
    result1 = connect(user = user, password = password, url = url) {
        def stmt = prepareStatement "insert into ${tableName} with label ${label} values(?, ?, ?)"
        assertEquals(com.mysql.cj.jdbc.ClientPreparedStatement, stmt.class)
        stmt.setInt(1, 5)
        stmt.setString(2, "a5")
        stmt.setInt(3, 94)
        def result = stmt.execute()
        logger.info("result: ${result}")
        getServerInfo(stmt)

        stmt.close()
    }

    url += "&rewriteBatchedStatements=true"
    result1 = connect(user = user, password = password, url = url) {
        def stmt = prepareStatement "insert into ${tableName} values(?, ?, ?)"
        check {assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, stmt.class)}
        stmt.setInt(1, 10)
        stmt.setString(2, "a")
        stmt.setInt(3, 90)
        def result = stmt.execute()
        logger.info("result: ${result}")
        getServerInfo(stmt)
        def stmtId = getStmtId(stmt)

        stmt.setInt(1, 20)
        stmt.setString(2, "ab")
        stmt.setInt(3, 91)
        result = stmt.execute()
        logger.info("result: ${result}")
        getServerInfo(stmt)
        assertEquals(stmtId, getStmtId(stmt))

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
        getServerInfo(stmt)
        assertEquals(stmtId, getStmtId(stmt))

        stmt.close()
    }

    url += "&cachePrepStmts=true"
    result1 = connect(user = user, password = password, url = url) {
        def stmt = prepareStatement "insert into ${tableName} values(?, ?, ?)"
        check {assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, stmt.class)}
        stmt.setInt(1, 10)
        stmt.setString(2, "a")
        stmt.setInt(3, 90)
        def result = stmt.execute()
        logger.info("result: ${result}")
        getServerInfo(stmt)
        def stmtId = getStmtId(stmt)

        stmt.setInt(1, 20)
        stmt.setString(2, "ab")
        stmt.setInt(3, 91)
        result = stmt.execute()
        logger.info("result: ${result}")
        getServerInfo(stmt)
        assertEquals(stmtId, getStmtId(stmt))

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
        getServerInfo(stmt)
        def stmtId2 = getStmtId(stmt)
        check {assertEquals(2, stmtId2.size())}

        stmt.close()
    }

    qt_sql """ select * from ${tableName} order by id, name, score """
}
