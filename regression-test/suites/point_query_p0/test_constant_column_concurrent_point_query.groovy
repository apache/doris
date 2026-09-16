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

import com.mysql.cj.jdbc.ServerPreparedStatement

import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Statement
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

suite("test_constant_column_concurrent_point_query", "p0,nonConcurrent") {
    String dbName = context.config.getDbNameByFile(context.file)
    String serverPrepareUrl = getServerPrepareJdbcUrl(context.config.jdbcUrl, dbName, false) +
            "&connectTimeout=10000&socketTimeout=30000"
    String user = context.config.jdbcUser
    String password = context.config.jdbcPassword

    sql "DROP TABLE IF EXISTS test_constant_column_concurrent_point_query FORCE"
    sql """
        CREATE TABLE test_constant_column_concurrent_point_query (
            k INT NOT NULL,
            payload VARCHAR(64) NOT NULL
        )
        UNIQUE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "true",
            "store_row_column" = "true"
        )
    """

    sql "SET show_hidden_columns = true"
    sql "SET enable_nereids_planner = true"
    sql "SET enable_fallback_to_original_planner = false"

    def expectedRows = { String projection ->
        def rows = sql """
            SELECT ${projection}
            FROM test_constant_column_concurrent_point_query
            ORDER BY k
        """
        def expected = rows.collectEntries { row ->
            [(Integer.parseInt(row[0].toString())): row.collect { value -> value?.toString() }]
        }
        expected.each { key, row ->
            assertTrue(Long.parseLong(row[-1]) > 0, "key=${key} has an invalid version ${row[-1]}")
        }
        return expected
    }

    def runConcurrentPointQueries = { String phase, String projection, Map expected ->
        int threadCount = 8
        int iterations = 12
        List<Integer> keys = expected.keySet().toList().sort()
        def errors = new ConcurrentLinkedQueue<String>()
        def ready = new CountDownLatch(threadCount)
        def start = new CountDownLatch(1)
        def threads = []

        for (int threadId = 0; threadId < threadCount; ++threadId) {
            int workerId = threadId
            threads.add(Thread.startDaemon {
                try (Connection connection = DriverManager.getConnection(serverPrepareUrl, user, password);
                     Statement sessionStatement = connection.createStatement()) {
                    sessionStatement.execute("SET show_hidden_columns = true")
                    sessionStatement.execute("SET enable_nereids_planner = true")
                    sessionStatement.execute("SET enable_fallback_to_original_planner = false")

                    String query = "SELECT /*+ SET_VAR(enable_nereids_planner=true) */ ${projection} " +
                            "FROM test_constant_column_concurrent_point_query WHERE k = ?"
                    try (PreparedStatement statement = connection.prepareStatement(query)) {
                        if (statement.class != ServerPreparedStatement) {
                            throw new AssertionError("expected a server prepared statement, got ${statement.class}")
                        }

                        ready.countDown()
                        if (!start.await(30, TimeUnit.SECONDS)) {
                            throw new AssertionError("timed out waiting to start phase ${phase}")
                        }

                        for (int iteration = 0; iteration < iterations; ++iteration) {
                            int key = keys[(workerId + iteration) % keys.size()]
                            statement.setInt(1, key)
                            try (ResultSet resultSet = statement.executeQuery()) {
                                if (!resultSet.next()) {
                                    throw new AssertionError("phase ${phase}: key ${key} returned no row")
                                }
                                int columnCount = resultSet.metaData.columnCount
                                def actual = new ArrayList<String>(columnCount)
                                for (int column = 1; column <= columnCount; ++column) {
                                    actual.add(resultSet.getObject(column)?.toString())
                                }
                                if (resultSet.next()) {
                                    throw new AssertionError("phase ${phase}: key ${key} returned multiple rows")
                                }
                                if (actual != expected[key]) {
                                    throw new AssertionError(
                                            "phase ${phase}: key ${key}, expected ${expected[key]}, actual ${actual}")
                                }
                            }
                        }
                    }
                } catch (Throwable t) {
                    ready.countDown()
                    errors.add("worker ${workerId}: ${t.class.simpleName}: ${t.message}")
                }
            })
        }

        boolean allWorkersReady = ready.await(30, TimeUnit.SECONDS)
        start.countDown()
        long finishDeadline = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(2)
        threads.each { thread ->
            long remainingMillis = finishDeadline - System.currentTimeMillis()
            if (remainingMillis > 0) {
                thread.join(remainingMillis)
            }
            if (thread.isAlive()) {
                errors.add("${thread.name} did not finish")
                thread.interrupt()
            }
        }
        if (!allWorkersReady) {
            errors.add("not all workers connected within 30 seconds")
        }
        assertTrue(errors.isEmpty(), "phase ${phase} failed:\n${errors.toList().join('\n')}")
    }

    // Case 1: warm the row-store point-query path before the schema changes and record the physical
    // rowset version expected for each key.
    sql """
        INSERT INTO test_constant_column_concurrent_point_query VALUES
            (1, 'old-1'),
            (2, 'old-2')
    """
    sql "SYNC"

    explain {
        sql """
            SELECT k, payload, __DORIS_VERSION_COL__
            FROM test_constant_column_concurrent_point_query
            WHERE k = 1
        """
        contains "SHORT-CIRCUIT"
    }
    def beforeAdd = expectedRows("k, payload, __DORIS_VERSION_COL__")
    runConcurrentPointQueries("before_add", "k, payload, __DORIS_VERSION_COL__", beforeAdd)
    order_qt_concurrent_point_before_add """
        SELECT k, payload
        FROM test_constant_column_concurrent_point_query
        ORDER BY k
    """

    // Case 2: keys 1 and 2 read c_default from a ConstantColumnIterator; keys 3 and 4 read a physical
    // column. Every prepared point query also reads the synthesized rowset version.
    sql """
        ALTER TABLE test_constant_column_concurrent_point_query
        ADD COLUMN c_default INT NOT NULL DEFAULT '10'
    """
    waitForSchemaChangeDone({
        sql """
            SHOW ALTER TABLE COLUMN
            WHERE TableName = 'test_constant_column_concurrent_point_query'
            ORDER BY CreateTime DESC LIMIT 1
        """
        time 600
    })
    sql """
        INSERT INTO test_constant_column_concurrent_point_query VALUES
            (3, 'new-3', 30),
            (4, 'new-4', 40)
    """
    sql "SYNC"

    explain {
        sql """
            SELECT k, payload, c_default, __DORIS_VERSION_COL__
            FROM test_constant_column_concurrent_point_query
            WHERE k = 1
        """
        contains "SHORT-CIRCUIT"
    }
    def afterAdd = expectedRows("k, payload, c_default, __DORIS_VERSION_COL__")
    assertEquals('10', afterAdd[1][2])
    assertEquals('10', afterAdd[2][2])
    assertEquals('30', afterAdd[3][2])
    assertEquals('40', afterAdd[4][2])
    runConcurrentPointQueries("after_add", "k, payload, c_default, __DORIS_VERSION_COL__", afterAdd)
    order_qt_concurrent_point_after_add """
        SELECT k, payload, c_default
        FROM test_constant_column_concurrent_point_query
        ORDER BY k
    """

    // Case 3: a same-name column with a different type/default must not reuse the dropped INT column UID.
    sql "ALTER TABLE test_constant_column_concurrent_point_query DROP COLUMN c_default"
    waitForSchemaChangeDone({
        sql """
            SHOW ALTER TABLE COLUMN
            WHERE TableName = 'test_constant_column_concurrent_point_query'
            ORDER BY CreateTime DESC LIMIT 1
        """
        time 600
    })
    sql """
        ALTER TABLE test_constant_column_concurrent_point_query
        ADD COLUMN c_default VARCHAR(32) NOT NULL DEFAULT 'fresh'
    """
    waitForSchemaChangeDone({
        sql """
            SHOW ALTER TABLE COLUMN
            WHERE TableName = 'test_constant_column_concurrent_point_query'
            ORDER BY CreateTime DESC LIMIT 1
        """
        time 600
    })
    sql """
        INSERT INTO test_constant_column_concurrent_point_query VALUES
            (5, 'new-5', 'physical')
    """
    sql "SYNC"

    explain {
        sql """
            SELECT k, payload, c_default, __DORIS_VERSION_COL__
            FROM test_constant_column_concurrent_point_query
            WHERE k = 3
        """
        contains "SHORT-CIRCUIT"
    }
    def afterReAdd = expectedRows("k, payload, c_default, __DORIS_VERSION_COL__")
    assertEquals('fresh', afterReAdd[1][2])
    assertEquals('fresh', afterReAdd[2][2])
    assertEquals('fresh', afterReAdd[3][2])
    assertEquals('fresh', afterReAdd[4][2])
    assertEquals('physical', afterReAdd[5][2])
    runConcurrentPointQueries("after_readd", "k, payload, c_default, __DORIS_VERSION_COL__", afterReAdd)
    order_qt_concurrent_point_after_readd """
        SELECT k, payload, c_default
        FROM test_constant_column_concurrent_point_query
        ORDER BY k
    """

    sql "SET show_hidden_columns = false"
}
