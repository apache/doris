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
import java.util.concurrent.TimeUnit
import java.util.concurrent.CompletableFuture

// test update and delete command
suite("txn_insert_concurrent_insert_ud") {
    def tableName = "txn_insert_concurrent_insert_ud"
    List<String> errors = new ArrayList<>()

    for (int i = 0; i < 3; i++) {
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
            UNIQUE KEY(L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER)
            DISTRIBUTED BY HASH(L_ORDERKEY) BUCKETS 3
            PROPERTIES (
                "replication_num" = "1"
            )
        """

        if (i < 2) {
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

    def dbName = "regression_test_insert_p2"
    def url = getServerPrepareJdbcUrl(context.config.jdbcUrl, dbName).replace("&useServerPrepStmts=true", "") + "&useLocalSessionState=true"
    logger.info("url: ${url}")

    /**
     * one txn will execute the following sqls in random order:
     * 1. insert into ${tableName}_0 select * from ${tableName}_2; (6001215 rows)
     * 2. insert into ${tableName}_0 select * from ${tableName}_2 L_ORDERKEY < 2000000; (2000495 rows)
     * 3. insert into ${tableName}_1 select * from ${tableName}_2 where L_ORDERKEY < 3000000; (2999666 rows)
     * 4. update ${tableName}_0 set L_QUANTITY = L_QUANTITY + 10 where L_ORDERKEY < 1000000;
     " 5. update ${tableName}_0 set ${tableName}_0.L_QUANTITY = 100 where ${tableName}_0.L_ORDERKEY in (select L_ORDERKEY from ${tableName}_1 where L_ORDERKEY >= 2000000 and L_ORDERKEY < 3000000);
     " 6. delete from ${tableName}_0 where ${tableName}_0.L_ORDERKEY in (select L_ORDERKEY from ${tableName}_1 where L_ORDERKEY >= 2000000);
     " 7. delete from ${tableName}_0 where ${tableName}_0.L_ORDERKEY in (select L_ORDERKEY from ${tableName}_2 where L_ORDERKEY >= 3000000 and L_ORDERKEY < 4000000);
     *
     * suppose there is 'threadNum' concurrency, the rows of each table should be:
     * t0: insert from t1: unknown, depend on the commit order
     *     insert from t2: 6001215
     *     update from t1: [2000000, ) -> + 10, maybe overwrite by insert
     *     update from t2: [3000000, 4000000) -> 100, maybe overwrite by insert
     *     delete from t1: the first sub txn, will insert again
     *     delete from t2: the first sub txn, will insert again
     * t1: from t2: 2999666
     */
    def sqls = [
            "insert into ${tableName}_0 select * from ${tableName}_2 where L_ORDERKEY < 1000000;",
            "insert into ${tableName}_0 select * from ${tableName}_2 where L_ORDERKEY >= 1000000 and L_ORDERKEY < 2000000;",
            "insert into ${tableName}_0 select * from ${tableName}_2 where L_ORDERKEY >= 2000000 and L_ORDERKEY < 3000000;",
            "insert into ${tableName}_0 select * from ${tableName}_2 where L_ORDERKEY >= 3000000 and L_ORDERKEY < 4000000;",
            "insert into ${tableName}_0 select * from ${tableName}_2 where L_ORDERKEY >= 4000000 and L_ORDERKEY < 5000000;",
            "insert into ${tableName}_0 select * from ${tableName}_2 where L_ORDERKEY >= 5000000 and L_ORDERKEY < 6000000;",
            "insert into ${tableName}_0 select * from ${tableName}_2 where L_ORDERKEY >= 6000000;",
            "insert into ${tableName}_1 select * from ${tableName}_2 where L_ORDERKEY < 1000000;",
            "insert into ${tableName}_1 select * from ${tableName}_2 where L_ORDERKEY >= 1000000 and L_ORDERKEY < 2000000;",
            "insert into ${tableName}_1 select * from ${tableName}_2 where L_ORDERKEY >= 2000000 and L_ORDERKEY < 3000000;",
            "insert into ${tableName}_0 select * from ${tableName}_1 where L_ORDERKEY < 1000000;",
            "insert into ${tableName}_0 select * from ${tableName}_1 where L_ORDERKEY >= 1000000 and L_ORDERKEY < 2000000;",
            "update ${tableName}_0 set L_QUANTITY = L_QUANTITY + 10 where L_ORDERKEY < 1000000;",
            "update ${tableName}_0 set ${tableName}_0.L_QUANTITY = 100 where ${tableName}_0.L_ORDERKEY in (select L_ORDERKEY from ${tableName}_1 where L_ORDERKEY >= 2000000 and L_ORDERKEY < 3000000);",
            // "delete from ${tableName}_0 where ${tableName}_0.L_ORDERKEY in (select L_ORDERKEY from ${tableName}_1 where L_ORDERKEY >= 2000000);",
            // "delete from ${tableName}_0 where ${tableName}_0.L_ORDERKEY in (select L_ORDERKEY from ${tableName}_2 where L_ORDERKEY >= 3000000 and L_ORDERKEY < 4000000);",
    ]
    def txn_insert = { ->
        try (Connection conn = DriverManager.getConnection(url, context.config.jdbcUser, context.config.jdbcPassword);
             Statement stmt = conn.createStatement()) {
            // begin
            def pre_sqls = ["begin",
                "delete from ${tableName}_0 where ${tableName}_0.L_ORDERKEY in (select L_ORDERKEY from ${tableName}_1 where L_ORDERKEY >= 2000000);",
                "delete from ${tableName}_0 where ${tableName}_0.L_ORDERKEY in (select L_ORDERKEY from ${tableName}_2 where L_ORDERKEY >= 3000000 and L_ORDERKEY < 4000000);"
            ]
            for (def pre_sql : pre_sqls) {
                logger.info("execute sql: " + pre_sql)
                stmt.execute(pre_sql)
            }
            // insert
            List<Integer> list = new ArrayList()
            for (int i = 0; i < sqls.size(); i++) {
                list.add(i)
            }
            Collections.shuffle(list)
            logger.info("sql random order: " + list)
            for (def i : list) {
                def sql = sqls.get(i)
                logger.info("execute sql: " + sql)
                stmt.execute(sql)
            }
            // commit
            logger.info("execute sql: commit")
            stmt.execute("commit")
            logger.info("finish txn insert")
        } catch (Throwable e) {
            logger.error("txn insert failed", e)
            errors.add("txn insert failed " + e.getMessage())
        }
    }

    List<CompletableFuture<Void>> futures = new ArrayList<>()
    def threadNum = 50
    for (int i = 0; i < threadNum; i++) {
        CompletableFuture<Void> future = CompletableFuture.runAsync(txn_insert)
        futures.add(future)
    }
    CompletableFuture<?>[] futuresArray = futures.toArray(new CompletableFuture[0])
    CompletableFuture.allOf(futuresArray).get(30, TimeUnit.MINUTES)
    sql """ sync """

    logger.info("error num: " + errors.size() + ", errors: " + errors)

    def t0_row_count = 6001215 // 2000495 or 5000226
    def result = sql """ select count() from ${tableName}_0 """
    logger.info("${tableName}_0 row count: ${result}, expected >= ${t0_row_count}")

    def t1_row_count = 2999666
    def result2 = sql """ select count() from ${tableName}_1 """
    logger.info("${tableName}_1 row count: ${result2}, expected: ${t1_row_count}")

    def tables = sql """ show tables from $dbName """
    logger.info("tables: $tables")
    for (def table_info : tables) {
        def table_name = table_info[0]
        if (table_name.startsWith(tableName)) {
            check_table_version_continuous(dbName, table_name)
        }
    }

    assertTrue(result[0][0] >= t0_row_count)
    assertEquals(t1_row_count, result2[0][0])
    assertEquals(0, errors.size())
    result = sql """ select L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER,count(*) a from ${tableName}_0 group by L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER having a > 1; """
    assertEquals(0, result.size())
    result = sql """ select L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER,count(*) a from ${tableName}_1 group by L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER having a > 1; """
    assertEquals(0, result.size())
}
