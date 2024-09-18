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

// schema change and modify replica num
suite("txn_insert_with_schema_change") {
    def tableName = "txn_insert_with_schema_change"
    def dbName = "regression_test_insert_p2"
    def url = getServerPrepareJdbcUrl(context.config.jdbcUrl, dbName).replace("&useServerPrepStmts=true", "") + "&useLocalSessionState=true"
    logger.info("url: ${url}")

    for (int i = 0; i < 5; i++) {
        def table_name = "${tableName}_${i}"
        sql """ drop table if exists ${table_name} """
        // load sf1 lineitem table
        sql """
            CREATE TABLE IF NOT EXISTS ${table_name} (
                L_ORDERKEY    INTEGER NOT NULL,
                L_PARTKEY     INTEGER NOT NULL,
                L_SUPPKEY     INTEGER NOT NULL,
                L_LINENUMBER  INTEGER NOT NULL,
                L_QUANTITY    DECIMAL(15,2) NOT NULL,
                L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,
                L_DISCOUNT    DECIMAL(15,2) NOT NULL,
                L_TAX         DECIMAL(15,2) NOT NULL,
                L_RETURNFLAG  CHAR(1) NOT NULL,
                L_LINESTATUS  CHAR(1) NOT NULL,
                L_SHIPDATE    DATE NOT NULL,
                L_COMMITDATE  DATE NOT NULL,
                L_RECEIPTDATE DATE NOT NULL,
                L_SHIPINSTRUCT CHAR(25) NOT NULL,
                L_SHIPMODE     CHAR(10) NOT NULL,
                L_COMMENT      VARCHAR(44) NOT NULL
            )
            DUPLICATE KEY(L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER)
            DISTRIBUTED BY HASH(L_ORDERKEY) BUCKETS 3
            PROPERTIES (
                "enable_mow_light_delete" = "true",
                "replication_num" = "1"
            )
        """

        if (i < 4) {
            continue
        }

        for (def file_name : ["lineitem.csv.split00.gz", "lineitem.csv.split01.gz"]) {
            streamLoad {
                table table_name
                set 'column_separator', '|'
                set 'compress_type', 'GZ'
                file """${getS3Url()}/regression/tpch/sf1/${file_name}"""
                time 10000 // limit inflight 10s
                check { result, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    log.info("Stream load result: ${result}".toString())
                    def json = parseJson(result)
                    assertEquals("success", json.Status.toLowerCase())
                    assertEquals(json.NumberTotalRows, json.NumberLoadedRows)
                    assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
                }
            }
        }
    }
    sql """ sync """
    for (int i = 0; i < 4; i++) {
        sql """ insert into ${tableName}_${i} select * from ${tableName}_4 """
        sql """ insert into ${tableName}_${i} select * from ${tableName}_4 """
    }
    sql """ sync """

    List<String> errors = new ArrayList<>()
    CountDownLatch insertLatch = new CountDownLatch(1)
    CountDownLatch schemaChangeLatch = new CountDownLatch(1)

    def getAlterTableState = { tName, job_state ->
        def retry = 0
        sql "use ${dbName};"
        def last_state = ""
        while (true) {
            sleep(4000)
            def state = sql """ show alter table column where tablename = "${tName}" order by CreateTime desc limit 1"""
            logger.info("alter table state: ${state}")
            last_state = state[0][9]
            if (state.size() > 0 && last_state == job_state) {
                return
            }
            retry++
            if (retry >= 60 || last_state == "FINISHED" || last_state == "CANCELLED") {
                break
            }
        }
        assertTrue(false, "alter table job state is ${last_state}, not ${job_state} after retry ${retry} times")
    }

    // sqls size is 2
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
            statement.execute("sync")
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
            ["insert into ${tableName}_0(L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT) select * from ${tableName}_4;",
             "insert into ${tableName}_0(L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT) select * from ${tableName}_3;"],
            ["delete from ${tableName}_1 where L_ORDERKEY < 50000;",
             "insert into ${tableName}_1(L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT) select * from ${tableName}_3;"],
            /*["insert into ${tableName}_2(L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT) select * from ${tableName}_3;",
             "delete from ${tableName}_2 where L_ORDERKEY < 50000;"]*/
    ]
    def expected_row_count = [
            [6001215 * 5, 6001215 * 8, 6001215 * 11, 6001215 * 14, 6001215 * 17],
            [23904476, 35806522, 47708568, 47708568 + 11902046, 47708568 + 11902046 * 2],
            [23904476, 35806522, 47708568, 47708568 + 11902046, 47708568 + 11902046 * 2]
    ]

    def check_row_count = { table_name, row_count ->
        def result = sql """ select count() from ${table_name} """
        logger.info("result: ${result}, expected: ${row_count}")
        assertEquals(row_count, result[0][0])
    }

    for (int i = 0; i < sqls.size(); i++) {
        def insert_sqls = sqls[i]
        logger.info("insert sqls: ${insert_sqls}")

        // 1. do light weight schema change: add column
        if (true) {
            insertLatch = new CountDownLatch(1)
            schemaChangeLatch = new CountDownLatch(1)
            Thread insert_thread = new Thread(() -> txnInsert(insert_sqls))
            Thread schema_change_thread = new Thread(() -> schemaChange("alter table ${tableName}_${i} ADD column L_TMP VARCHAR(100) after L_SHIPMODE;", "${tableName}_${i}", null))
            insert_thread.start()
            schema_change_thread.start()
            insert_thread.join()
            schema_change_thread.join()

            logger.info("errors: " + errors)
            assertEquals(0, errors.size())
            check_row_count("${tableName}_${i}", expected_row_count[i][0])
            getAlterTableState("${tableName}_${i}", "FINISHED")
            check_row_count("${tableName}_${i}", expected_row_count[i][0])
        }

        // 2. do hard weight schema change: change order
        if (true) {
            insertLatch = new CountDownLatch(1)
            schemaChangeLatch = new CountDownLatch(1)
            Thread insert_thread = new Thread(() -> txnInsert(insert_sqls))
            Thread schema_change_thread = new Thread(() -> schemaChange("alter table ${tableName}_${i} order by (L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT, L_TMP);", "${tableName}_${i}", "WAITING_TXN"))
            insert_thread.start()
            schema_change_thread.start()
            insert_thread.join()
            schema_change_thread.join()

            logger.info("errors: " + errors)
            assertEquals(0, errors.size())
            check_row_count("${tableName}_${i}", expected_row_count[i][1])
            getAlterTableState("${tableName}_${i}", "FINISHED")
            check_row_count("${tableName}_${i}", expected_row_count[i][1])
        }

        // 3. do hard weight schema change: change type
        if (true) {
            insertLatch = new CountDownLatch(1)
            schemaChangeLatch = new CountDownLatch(1)
            Thread insert_thread = new Thread(() -> txnInsert(insert_sqls))
            Thread schema_change_thread = new Thread(() -> schemaChange("alter table ${tableName}_${i} MODIFY column L_TAX VARCHAR(100)", "${tableName}_${i}", "WAITING_TXN"))
            insert_thread.start()
            schema_change_thread.start()
            insert_thread.join()
            schema_change_thread.join()

            logger.info("errors: " + errors)
            assertEquals(0, errors.size())
            check_row_count("${tableName}_${i}", expected_row_count[i][2])
            getAlterTableState("${tableName}_${i}", "FINISHED")
            check_row_count("${tableName}_${i}", expected_row_count[i][2])
        }

        // 4. change replica num from 1 to 3, 3 to 1
        def backends = sql "show backends"
        logger.info("backends: ${backends}")
        if (backends.size() >= 3) {
            insertLatch = new CountDownLatch(1)
            schemaChangeLatch = new CountDownLatch(1)
            Thread insert_thread = new Thread(() -> txnInsert(insert_sqls))
            Thread schema_change_thread = new Thread(() -> schemaChange("ALTER TABLE ${tableName}_${i} SET ( \"default.replication_allocation\" = \"tag.location.default: 3\" ) ", null))
            insert_thread.start()
            schema_change_thread.start()
            insert_thread.join()
            schema_change_thread.join()

            logger.info("errors: " + errors)
            assertEquals(0, errors.size())
            check_row_count("${tableName}_${i}", expected_row_count[i][3])
            getAlterTableState("${tableName}_${i}", "FINISHED")
            check_row_count("${tableName}_${i}", expected_row_count[i][3])
            check_table_version_continuous(dbName, tableName + "_" + i)
        }

        // 5. change replica num from 3 to 1
        if (backends.size() >= 3) {
            insertLatch = new CountDownLatch(1)
            schemaChangeLatch = new CountDownLatch(1)
            Thread insert_thread = new Thread(() -> txnInsert(insert_sqls))
            Thread schema_change_thread = new Thread(() -> schemaChange("ALTER TABLE ${tableName}_${i} SET ( \"default.replication_allocation\" = \"tag.location.default: 1\" ) ", null))
            insert_thread.start()
            schema_change_thread.start()
            insert_thread.join()
            schema_change_thread.join()

            logger.info("errors: " + errors)
            assertEquals(0, errors.size())
            check_row_count("${tableName}_${i}", expected_row_count[i][4])
            getAlterTableState("${tableName}_${i}", "FINISHED")
            check_row_count("${tableName}_${i}", expected_row_count[i][4])
        }
        check_table_version_continuous(dbName, tableName + "_" + i)
    }
}
