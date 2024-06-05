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

suite("txn_insert_inject_case", "nonConcurrent") {
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
    sql """insert into ${table}_1 values(1, 2.2, "abc", [], []), (2, 3.3, "xyz", [1], [1, 0]), (null, null, null, [null], [null, 0])  """
    sql """insert into ${table}_2 values(3, 2.2, "abc", [], []), (4, 3.3, "xyz", [1], [1, 0]), (null, null, null, [null], [null, 0])  """

    // 1. publish timeout
    def backendId_to_params = get_be_param("pending_data_expire_time_sec")
    try {
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

        sleep(10000)
    } finally {
        set_original_be_param("pending_data_expire_time_sec", backendId_to_params)
        GetDebugPoint().disableDebugPointForAllFEs('PublishVersionDaemon.stop_publish')

        def rowCount = 0
        for (int i = 0; i < 30; i++) {
            def result = sql "SELECT COUNT(*) FROM ${table}_0"
            logger.info("select result: ${result}")
            rowCount = result[0][0]
            if (rowCount == 12) {
                break
            }
            sleep(2000)
        }
        assertEquals(12, rowCount)
    }

    // 2. commit failed
    def dbName = "regression_test_insert_p0"
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
            logger.error("commit failed", e);
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
}
