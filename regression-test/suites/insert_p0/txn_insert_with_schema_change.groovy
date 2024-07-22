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

import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

suite("txn_insert_with_schema_change") {
    def table = "txn_insert_with_schema_change"
    def dbName = "regression_test_insert_p0"
    def url = getServerPrepareJdbcUrl(context.config.jdbcUrl, dbName).replace("&useServerPrepStmts=true", "") + "&useLocalSessionState=true"
    logger.info("url: ${url}")
    List<String> errors = new ArrayList<>()
    CountDownLatch insertLatch = new CountDownLatch(1)
    CountDownLatch schemaChangeLatch = new CountDownLatch(1)

    for (int j = 0; j < 5; j++) {
        def tableName = table + "_" + j
        sql """ DROP TABLE IF EXISTS $tableName """
        sql """
            create table $tableName (
                `ID` int(11) NOT NULL,
                `NAME` varchar(100) NULL,
                `score` int(11) NULL
            ) ENGINE=OLAP
            duplicate KEY(`id`) 
            distributed by hash(id) buckets 1
            properties("replication_num" = "1"); 
        """
    }
    sql """ insert into ${table}_0 values(0, '20', 10) """
    sql """ insert into ${table}_1 values(0, '20', 10) """
    sql """ insert into ${table}_2 values(0, '20', 10) """
    sql """ insert into ${table}_3 values(1, '21', 11), (2, '22', 12) """
    sql """ insert into ${table}_4 values(3, '23', 13), (4, '24', 14), (5, '25', 15) """

    def getAlterTableState = { tName, job_state ->
        waitForSchemaChangeDone {
            sql """ SHOW ALTER TABLE COLUMN WHERE tablename='${tName}' ORDER BY createtime DESC LIMIT 1 """
            time 600
        }
    }

    def txnInsert = { sqls ->
        try (Connection conn = DriverManager.getConnection(url, context.config.jdbcUser, context.config.jdbcPassword);
             Statement statement = conn.createStatement()) {
            logger.info("execute sql: begin")
            statement.execute("begin")
            logger.info("execute sql: ${sqls[0]}")
            statement.execute(sqls[0])

            schemaChangeLatch.countDown()
            insertLatch.await(5, TimeUnit.MINUTES)

            logger.info("execute sql: ${sqls[1]}")
            statement.execute(sqls[1])
            logger.info("execute sql: commit")
            statement.execute("commit")
        } catch (Throwable e) {
            logger.error("txn insert failed", e)
            errors.add("txn insert failed " + e.getMessage())
        }
    }

    def schemaChange = { sql, tName, job_state ->
        try (Connection conn = DriverManager.getConnection(url, context.config.jdbcUser, context.config.jdbcPassword);
             Statement statement = conn.createStatement()) {
            schemaChangeLatch.await(5, TimeUnit.MINUTES)
            logger.info("execute sql: ${sql}")
            statement.execute(sql)
            if (job_state != null) {
                getAlterTableState(tName, job_state)
            }
            insertLatch.countDown()
        } catch (Throwable e) {
            logger.error("schema change failed", e)
            errors.add("schema change failed " + e.getMessage())
        }
    }

    def sqls = [
            ["insert into ${table}_0(id, name, score) select * from ${table}_3;",
             "insert into ${table}_0(id, name, score) select * from ${table}_4;"],
            ["delete from ${table}_1 where id = 0 or id = 3;",
             "insert into ${table}_1(id, name, score) select * from ${table}_4;"],
            /*["insert into ${table}_2(id, name, score) select * from ${table}_4;",
             "delete from ${table}_2 where id = 0 or id = 3;"]*/
    ]

    for (int i = 0; i < sqls.size(); i++) {
        def insert_sqls = sqls[i]
        logger.info("insert sqls: ${insert_sqls}")
        // 1. do light weight schema change: add column
        if (true) {
            insertLatch = new CountDownLatch(1)
            schemaChangeLatch = new CountDownLatch(1)
            Thread insert_thread = new Thread(() -> txnInsert(insert_sqls))
            Thread schema_change_thread = new Thread(() -> schemaChange("alter table ${table}_${i} ADD column age int after name;", "${table}_${i}", null))
            insert_thread.start()
            schema_change_thread.start()
            insert_thread.join()
            schema_change_thread.join()

            logger.info("errors: " + errors)
            assertEquals(0, errors.size())
            order_qt_select1 """select id, name, score from ${table}_${i} """
            getAlterTableState("${table}_${i}", "FINISHED")
            order_qt_select2 """select id, name, score from ${table}_${i} """
        }

        // 2. do hard weight schema change: change order
        if (true) {
            insertLatch = new CountDownLatch(1)
            schemaChangeLatch = new CountDownLatch(1)
            Thread insert_thread = new Thread(() -> txnInsert(insert_sqls))
            Thread schema_change_thread = new Thread(() -> schemaChange("alter table ${table}_${i} order by (id, score, age, name);", "${table}_${i}", "WAITING_TXN"))
            insert_thread.start()
            schema_change_thread.start()
            insert_thread.join()
            schema_change_thread.join()

            logger.info("errors: " + errors)
            assertEquals(0, errors.size())
            order_qt_select3 """select id, name, score from ${table}_${i} """
            getAlterTableState("${table}_${i}", "FINISHED")
            order_qt_select4 """select id, name, score from ${table}_${i} """
        }

        // 3. do hard weight schema change: change type
        if (true) {
            insertLatch = new CountDownLatch(1)
            schemaChangeLatch = new CountDownLatch(1)
            Thread insert_thread = new Thread(() -> txnInsert(insert_sqls))
            Thread schema_change_thread = new Thread(() -> schemaChange("alter table ${table}_${i} MODIFY column name int(11)", "${table}_${i}", "WAITING_TXN"))
            insert_thread.start()
            schema_change_thread.start()
            insert_thread.join()
            schema_change_thread.join()

            logger.info("errors: " + errors)
            assertEquals(0, errors.size())
            order_qt_select5 """select id, name, score from ${table}_${i} """
            getAlterTableState("${table}_${i}", "FINISHED")
            order_qt_select6 """select id, name, score from ${table}_${i} """
        }
        check_table_version_continuous(dbName, table + "_" + i)
    }
}
