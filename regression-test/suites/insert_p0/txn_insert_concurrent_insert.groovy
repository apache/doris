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

suite("txn_insert_concurrent_insert") {
    def tableName = "txn_insert_concurrent_insert"

    for (int i = 0; i < 3; i++) {
        def table_name = "${tableName}_${i}"
        sql """ drop table if exists ${table_name} """
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
                "replication_num" = "1"
            );
        """

        if (i == 0) {
            continue
        }

        streamLoad {
            table table_name
            set 'column_separator', '|'
            set 'compress_type', 'GZ'
            set 'columns', "l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag,l_linestatus, l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,l_shipmode,l_comment,temp"
            file """${getS3Url()}/regression/tpch/sf0.1/lineitem.tbl.gz"""

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
    sql """ sync """

    def dbName = "regression_test_insert_p0"
    def url = getServerPrepareJdbcUrl(context.config.jdbcUrl, dbName).replace("&useServerPrepStmts=true", "") + "&useLocalSessionState=true"
    logger.info("url: ${url}")

    def sqls = [
            "begin",
            "insert into ${tableName}_0 select * from ${tableName}_1 where L_ORDERKEY < 30000;",
            "insert into ${tableName}_1 select * from ${tableName}_2 where L_ORDERKEY > 500000;",
            "insert into ${tableName}_0 select * from ${tableName}_2 where L_ORDERKEY < 30000;",
            "commit"
    ]
    def txn_insert = { ->
        try (Connection conn = DriverManager.getConnection(url, context.config.jdbcUser, context.config.jdbcPassword);
             Statement stmt = conn.createStatement()) {
            for (def sql : sqls) {
                logger.info(Thread.currentThread().getName() + " execute sql: " + sql)
                stmt.execute(sql)
            }
            logger.info("finish txn insert for " + Thread.currentThread().getName())
        } catch (Throwable e) {
            logger.error("txn insert failed", e)
        }
    }

    List<CompletableFuture<Void>> futures = new ArrayList<>()
    for (int i = 0; i < 20; i++) {
        CompletableFuture<Void> future = CompletableFuture.runAsync(txn_insert)
        futures.add(future)
    }
    CompletableFuture<?>[] futuresArray = futures.toArray(new CompletableFuture[0])
    CompletableFuture.allOf(futuresArray).get(3, TimeUnit.MINUTES)
    sql """ sync """

    def result = sql """ select count() from ${tableName}_0 """
    logger.info("result: ${result}")
    assertEquals(1208360, result[0][0])
    result = sql """ select count() from ${tableName}_1 """
    logger.info("result: ${result}")
    assertEquals(2606192, result[0][0])

    def db_name = "regression_test_insert_p0"
    def tables = sql """ show tables from $db_name """
    logger.info("tables: $tables")
    for (def table_info : tables) {
        def table_name = table_info[0]
        if (table_name.startsWith(tableName)) {
            check_table_version_continuous(db_name, table_name)
        }
    }
}
