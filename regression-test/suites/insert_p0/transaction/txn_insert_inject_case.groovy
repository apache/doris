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
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement
import org.apache.doris.regression.util.DebugPoint
import org.apache.doris.regression.util.NodeType

suite("txn_insert_inject_case", "nonConcurrent") {
    // test load fail
    def table = "txn_insert_inject_case"
    for (int j = 0; j < 3; j++) {
        def tableName = table + "_" + j
        sql """ DROP TABLE IF EXISTS $tableName """
        sql """
            create table $tableName (
                k1 int, 
                k2 double,
                k3 varchar(100),
                k4 array<int>,
                k5 array<boolean>
            ) distributed by hash(k1) buckets 1
            properties("replication_num" = "1"); 
        """
    }
    GetDebugPoint().disableDebugPointForAllBEs("FlushToken.submit_flush_error")
    sql """insert into ${table}_1 values(1, 2.2, "abc", [], []), (2, 3.3, "xyz", [1], [1, 0]), (null, null, null, [null], [null, 0])  """
    sql """insert into ${table}_2 values(3, 2.2, "abc", [], []), (4, 3.3, "xyz", [1], [1, 0]), (null, null, null, [null], [null, 0])  """

    def ipList = [:]
    def portList = [:]
    (ipList, portList) = GetDebugPoint().getBEHostAndHTTPPort()
    logger.info("be ips: ${ipList}, ports: ${portList}")

    def enableDebugPoint = { ->
        ipList.each { beid, ip ->
            DebugPoint.enableDebugPoint(ip, portList[beid] as int, NodeType.BE, "FlushToken.submit_flush_error")
        }
    }

    def disableDebugPoint = { ->
        ipList.each { beid, ip ->
            DebugPoint.disableDebugPoint(ip, portList[beid] as int, NodeType.BE, "FlushToken.submit_flush_error")
        }
    }

    try {
        enableDebugPoint()
        sql """ begin """
        try {
            sql """ insert into ${table}_0 select * from ${table}_1; """
            assertTrue(false, "insert should fail")
        } catch (Exception e) {
            logger.info("1" + e.getMessage())
            assertTrue(e.getMessage().contains("dbug_be_memtable_submit_flush_error"))
        }
        try {
            sql """ insert into ${table}_0 select * from ${table}_1; """
            assertTrue(false, "insert should fail")
        } catch (Exception e) {
            logger.info("2" + e.getMessage())
            assertTrue(e.getMessage().contains("dbug_be_memtable_submit_flush_error"))
        }

        disableDebugPoint()
        sql """ insert into ${table}_0 select * from ${table}_1; """

        enableDebugPoint()
        try {
            sql """ insert into ${table}_0 select * from ${table}_1; """
            assertTrue(false, "insert should fail")
        } catch (Exception e) {
            logger.info("4" + e.getMessage())
            assertTrue(e.getMessage().contains("dbug_be_memtable_submit_flush_error"))
        }

        disableDebugPoint()
        sql """ insert into ${table}_0 select * from ${table}_1; """
        sql """ commit"""
    } catch (Exception e) {
        logger.error("failed", e)
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("FlushToken.submit_flush_error")
    }
    sql "sync"
    order_qt_select1 """select * from ${table}_0"""

    if (isCloudMode()) {
        return
    }

    sql """ truncate table ${table}_0 """

    // 1. publish timeout
    def commit_timeout_second_value = getFeConfig("commit_timeout_second")
    logger.info("commit_timeout_second_value: ${commit_timeout_second_value}")
    def backendId_to_params = get_be_param("pending_data_expire_time_sec")
    try {
        setFeConfig('commit_timeout_second', '2')
        // test be report tablet and expire txns and fe handle it
        set_be_param.call("pending_data_expire_time_sec", "1")
        GetDebugPoint().enableDebugPointForAllFEs('PublishVersionDaemon.stop_publish')

        sql """ begin; """
        sql """ insert into ${table}_0 select * from ${table}_1; """
        sql """ insert into ${table}_0 select * from ${table}_2; """
        sql """ insert into ${table}_0 select * from ${table}_1; """
        sql """ insert into ${table}_0 select * from ${table}_2; """
        try {
            // master fe will commit successfully, but publish will stop
            sql """ commit; """
        } catch (Exception e) {
            // observer fe will get this exception
            logger.info("commit failed", e)
            assertTrue(e.getMessage().contains("transaction commit successfully, BUT data will be visible later."))
        }

        def result = sql "SELECT COUNT(*) FROM ${table}_0"
        rowCount = result[0][0]
        assertEquals(0, rowCount)
        // sleep(10000)
    } finally {
        setFeConfig('commit_timeout_second', commit_timeout_second_value)
        set_original_be_param("pending_data_expire_time_sec", backendId_to_params)
        GetDebugPoint().disableDebugPointForAllFEs('PublishVersionDaemon.stop_publish')

        def rowCount = 0
        for (int i = 0; i < 600; i++) {
            def result = sql "SELECT COUNT(*) FROM ${table}_0"
            logger.info("select result: ${result}")
            rowCount = result[0][0]
            if (rowCount == 12) {
                break
            }
            sleep(100)
        }
        assertEquals(12, rowCount)
    }

    // 2. commit failed
    sql """ truncate table ${table}_0 """
    def dbName = "regression_test_insert_p0_transaction"
    def url = getServerPrepareJdbcUrl(context.config.jdbcUrl, dbName).replace("&useServerPrepStmts=true", "") + "&useLocalSessionState=true"
    logger.info("url: ${url}")
    def get_txn_id_from_server_info = { serverInfo ->
        logger.info("result server info: " + serverInfo)
        int index = serverInfo.indexOf("txnId")
        int index2 = serverInfo.indexOf("'}", index)
        String txnStr = serverInfo.substring(index + 8, index2)
        logger.info("txnId: " + txnStr)
        return Long.parseLong(txnStr)
    }
    GetDebugPoint().enableDebugPointForAllFEs('DatabaseTransactionMgr.commitTransaction.failed')
    long txn_id = 0
    try (Connection conn = DriverManager.getConnection(url, context.config.jdbcUser, context.config.jdbcPassword);
        Statement statement = conn.createStatement()) {
        statement.execute("begin");
        statement.execute("insert into ${table}_0 select * from ${table}_1;")
        txn_id = get_txn_id_from_server_info((((StatementImpl) statement).results).getServerInfo())
        statement.execute("insert into ${table}_0 select * from ${table}_2;")
        try {
            statement.execute("commit")
            assertTrue(false, "commit should fail")
        } catch (Exception e) {
            logger.info("commit failed " + e.getMessage())
        }
    } finally {
        GetDebugPoint().disableDebugPointForAllFEs('DatabaseTransactionMgr.commitTransaction.failed')
    }
    assertNotEquals(txn_id, 0)
    def txn_info = sql_return_maparray """ show transaction where id = ${txn_id} """
    logger.info("txn_info: ${txn_info}")
    assertEquals(1, txn_info.size())
    assertEquals("ABORTED", txn_info[0].get("TransactionStatus"))
    assertTrue(txn_info[0].get("Reason").contains("DebugPoint: DatabaseTransactionMgr.commitTransaction.failed"))

    // 3. one txn publish failed
    def insert_visible_timeout = sql """show variables where variable_name = 'insert_visible_timeout_ms';"""
    logger.info("insert_visible_timeout: ${insert_visible_timeout}")
    sql """ truncate table ${table}_0 """
    txn_id = 0
    try (Connection conn = DriverManager.getConnection(url, context.config.jdbcUser, context.config.jdbcPassword);
         Statement statement = conn.createStatement()) {
        statement.execute("ADMIN SET FRONTEND CONFIG ('commit_timeout_second' = '2');")
        statement.execute("begin")
        statement.execute("insert into ${table}_0 select * from ${table}_1;")
        txn_id = get_txn_id_from_server_info((((StatementImpl) statement).results).getServerInfo())
        GetDebugPoint().enableDebugPointForAllFEs('PublishVersionDaemon.genPublishTask.failed', [txnId:txn_id])
        statement.execute("insert into ${table}_0 select * from ${table}_2;")
        statement.execute("commit")

        sql "set insert_visible_timeout_ms = 2000"
        sql """insert into ${table}_0 values(100, 2.2, "abc", [], [])"""
        sql """insert into ${table}_1 values(101, 2.2, "abc", [], [])"""
        sql """insert into ${table}_2 values(102, 2.2, "abc", [], [])"""
        order_qt_select2 """select * from ${table}_0"""
        order_qt_select3 """select * from ${table}_1"""
        order_qt_select4 """select * from ${table}_2"""
    } catch (Exception e) {
        logger.info("failed", e)
        assertTrue(false, "should not reach here")
    } finally {
        setFeConfig('commit_timeout_second', commit_timeout_second_value)
        sql "set insert_visible_timeout_ms = ${insert_visible_timeout[0][1]}"
        GetDebugPoint().disableDebugPointForAllFEs('PublishVersionDaemon.genPublishTask.failed')
        def rowCount = 0
        for (int i = 0; i < 200; i++) {
            def result = sql "select count(*) from ${table}_0"
            logger.info("rowCount: " + result + ", retry: " + i)
            rowCount =  result[0][0]
            if (rowCount >= 7) {
                break
            }
            sleep(100)
        }
        assertEquals(7, rowCount)
    }
}
