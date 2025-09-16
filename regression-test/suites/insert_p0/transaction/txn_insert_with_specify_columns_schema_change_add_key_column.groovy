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

suite("txn_insert_with_specify_columns_schema_change_add_key_column", "nonConcurrent") {
    if(!isCloudMode()) {
       def table = "txn_insert_with_specify_columns_schema_change_add_key_column"

       def dbName = "regression_test_insert_p0_transaction"
       def url = getServerPrepareJdbcUrl(context.config.jdbcUrl, dbName).replace("&useServerPrepStmts=true", "") + "&useLocalSessionState=true"
       logger.info("url: ${url}")
       List<String> errors = new ArrayList<>()
       CountDownLatch insertLatch = new CountDownLatch(1)
       CountDownLatch insertLatch2 = new CountDownLatch(1)

       sql """ DROP TABLE IF EXISTS $table force """
       sql """
           create table $table (
               c1 INT NULL,
               c2 INT NULL,
               c3 INT NULL
           ) ENGINE=OLAP
           UNIQUE KEY(c1)
           DISTRIBUTED BY HASH(c1) BUCKETS 1
           PROPERTIES (
       "replication_num" = "1"); 
       """
       sql """ insert into ${table} (c3, c2, c1) values (3, 2, 1) """

       def getAlterTableState = { job_state ->
           def retry = 0
           sql "use ${dbName};"
           while (true) {
               sleep(2000)
               def state = sql " show alter table column where tablename = '${table}' order by CreateTime desc limit 1"
               logger.info("alter table state: ${state}")
               if (state.size() > 0 && state[0][9] == job_state) {
                   return
               }
               retry++
               if (retry >= 10) {
                   break
               }
           }
           assertTrue(false, "alter table job state is ${last_state}, not ${job_state} after retry ${retry} times")
       }

       def txnInsert = {
           try (Connection conn = DriverManager.getConnection(url, context.config.jdbcUser, context.config.jdbcPassword);
                Statement statement = conn.createStatement()) {
               try {
                   qt_select_desc1 """desc $table"""

                   insertLatch.await(2, TimeUnit.MINUTES)

                   statement.execute("begin")
                   statement.execute("insert into ${table} (c3, c2, c1) values (33, 22, 11),(333, 222, 111);")

                   insertLatch2.await(2, TimeUnit.MINUTES)
                   qt_select_desc2 """desc $table"""
                   statement.execute("insert into ${table} (c3, c2, c1) values(3333, 2222, 1111);")
                   statement.execute("insert into ${table} (c3, c2, c1) values(33333, 22222, 11111),(333333, 222222, 111111);")
                   statement.execute("commit")
               } catch (Exception e) {
                   logger.info("txn insert failed", e)
                   assertTrue(e.getMessage().contains("There are schema changes in one transaction, you can commit this transaction with formal data or rollback this whole transaction."))
                   statement.execute("rollback")
               }
           }
       }

       def schemaChange = { sql ->
           try (Connection conn = DriverManager.getConnection(url, context.config.jdbcUser, context.config.jdbcPassword);
               Statement statement = conn.createStatement()) {
               statement.execute(sql)
               getAlterTableState("RUNNING")
               insertLatch.countDown()
               getAlterTableState("FINISHED")
               insertLatch2.countDown()
           } catch (Throwable e) {
               logger.error("schema change failed", e)
               errors.add("schema change failed " + e.getMessage())
           }
       }

       GetDebugPoint().clearDebugPointsForAllBEs()
       GetDebugPoint().clearDebugPointsForAllFEs()
       try {
           GetDebugPoint().enableDebugPointForAllBEs("SchemaChangeJob._do_process_alter_tablet.sleep")
           Thread schema_change_thread = new Thread(() -> schemaChange("alter table ${table} add column new_col int key after c1;"))
           Thread insert_thread = new Thread(() -> txnInsert())
           schema_change_thread.start()
           insert_thread.start()
           schema_change_thread.join()
           insert_thread.join()

           logger.info("errors: " + errors)
           assertEquals(0, errors.size())
           getAlterTableState("FINISHED")
           order_qt_select1 """select * from ${table} order by c1, c2, c3"""
       } catch (Exception e) {
           logger.info("failed: " + e.getMessage())
           assertTrue(false)
       } finally {
           GetDebugPoint().clearDebugPointsForAllBEs()
       }
    }
}
