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

import groovy.json.JsonSlurper
import org.apache.doris.regression.suite.ConnectionInfo

import java.nio.file.Files
import java.sql.DriverManager
import java.sql.SQLException
import java.util.concurrent.TimeUnit

suite("test_remote_doris_private_rpc_tls", "p0,nonConcurrent") {
    def logRoot = new File(System.getProperty("DORIS_HOME"), "regression-test/log").toPath()
    Files.createDirectories(logRoot)
    def logs = Files.createTempDirectory(logRoot, "private-rpc-tls-").toFile()
    def helper = new File(context.file.parentFile, "private_rpc_tls_fixture.py")
    // Keep lifecycle and port-allocation checks in the default regression entry point as well.
    def fixtureTests = new ProcessBuilder("python3", "-m", "unittest", "discover",
            "-s", helper.parent, "-p", "test_private_rpc_tls_fixture.py", "-v")
            .redirectErrorStream(true).redirectOutput(new File(logs, "fixture-tests.log")).start()
    try {
        assertTrue(fixtureTests.waitFor(30, TimeUnit.SECONDS), "Private RPC fixture tests timed out; see ${logs}")
        assertEquals(0, fixtureTests.exitValue(), "Private RPC fixture tests failed; see ${logs}")
    } finally {
        fixtureTests.destroyForcibly()
    }
    def process = new ProcessBuilder("python3", helper.absolutePath,
            "--package-home", getConf("privateRpcTlsPackageHome", System.getProperty("DORIS_HOME")),
            "--log-dir", logs.absolutePath).redirectError(new File(logs, "fixture.log")).start()
    def reader = process.inputStream.newReader("UTF-8")
    try {
        // Provision unconditionally: a default P0 run must execute the SQL rather than return success.
        long readyDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(300)
        while (!reader.ready() && process.isAlive() && System.nanoTime() < readyDeadline) {
            Thread.sleep(100)
        }
        assertTrue(reader.ready(), "Private RPC TLS fixture did not start; see ${logs}")
        def endpoints = new JsonSlurper().parseText(reader.readLine())

        def withFixtureCluster = { endpoint, Closure action ->
            String url = "jdbc:mysql://${endpoint.host}:${endpoint.ports.fe.query_port}/" +
                    "?useLocalSessionState=true&useSSL=false&connectTimeout=1000&socketTimeout=30000"
            def previous = context.threadLocalConn.get()
            try {
                // Public SQL stays plaintext independently of the parent run's client TLS settings.
                DriverManager.getConnection(url, "root", "").withCloseable { connection ->
                    context.threadLocalConn.set(new ConnectionInfo(conn: connection, username: "root", password: ""))
                    action.call()
                }
            } finally {
                if (previous == null) {
                    context.threadLocalConn.remove()
                } else {
                    context.threadLocalConn.set(previous)
                }
            }
        }
        def waitReady = { Closure condition ->
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120)
            while (true) {
                try {
                    if (condition.call()) {
                        break
                    }
                } catch (SQLException unavailable) {
                    if (System.nanoTime() >= deadline) {
                        throw unavailable
                    }
                }
                assertTrue(System.nanoTime() < deadline, "Private RPC test cluster did not become ready; see ${logs}")
                Thread.sleep(500)
            }
        }
        [endpoints.local, endpoints.remote].each { endpoint ->
            waitReady {
                withFixtureCluster(endpoint) { sql "SELECT 1" }
                true
            }
            waitReady {
                withFixtureCluster(endpoint) {
                    sql_return_maparray("SHOW BACKENDS").any { it.Alive.toString().equalsIgnoreCase("true") }
                }
            }
        }

        String host = endpoints.remote.host
        String httpPort = endpoints.remote.ports.fe.http_port
        String thriftPort = endpoints.remote.ports.fe.rpc_port
        String arrowPort = endpoints.remote.ports.fe.arrow_flight_sql_port
        String user = "root"
        String password = ""
        withFixtureCluster(endpoints.local) {
            withFixtureCluster(endpoints.remote) {
                sql "CREATE DATABASE IF NOT EXISTS private_rpc_exchange_test"
                sql "DROP TABLE IF EXISTS private_rpc_exchange_test.remote_data"
                sql """CREATE TABLE private_rpc_exchange_test.remote_data (id INT, value INT)
                       DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                       PROPERTIES ("replication_num" = "1")"""
                sql "INSERT INTO private_rpc_exchange_test.remote_data VALUES (1, 100), (2, 200), (3, 300)"
            }
            sql "CREATE DATABASE IF NOT EXISTS private_rpc_exchange_test"
            sql "DROP TABLE IF EXISTS private_rpc_exchange_test.local_data"
            sql """CREATE TABLE private_rpc_exchange_test.local_data (id INT, value INT)
                   DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                   PROPERTIES ("replication_num" = "1")"""
            sql "INSERT INTO private_rpc_exchange_test.local_data VALUES (1, 10), (2, 20), (4, 40)"
            sql "SET enable_sql_cache = false"
            sql "SET disable_join_reorder = true"

            ["private_rpc_remote_direct": "false", "private_rpc_remote_flight": "true"].each { catalog, flight ->
                sql "DROP CATALOG IF EXISTS ${catalog}"
                sql """CREATE CATALOG ${catalog} PROPERTIES (
                        "type" = "doris",
                        "fe_http_hosts" = "http://${host}:${httpPort}",
                        "fe_thrift_hosts" = "${host}:${thriftPort}",
                        "fe_arrow_hosts" = "${host}:${arrowPort}",
                        "user" = "${user}", "password" = "${password}",
                        "use_arrow_flight" = "${flight}")"""
            }

            qt_remote_only """SELECT id, value FROM private_rpc_remote_flight.private_rpc_exchange_test.remote_data
                              ORDER BY id"""
            qt_local_only "SELECT id, value FROM private_rpc_exchange_test.local_data ORDER BY id"

            test {
                sql "SELECT id, value FROM private_rpc_remote_direct.private_rpc_exchange_test.remote_data ORDER BY id"
                exception "use_arrow_flight=true"
            }

            // Direct OLAP mode would require incompatible BE-to-BE exchanges in either direction.
            ["broadcast", "shuffle"].each { distribution ->
                test {
                    sql """SELECT l.id, l.value, r.value FROM private_rpc_exchange_test.local_data l
                           JOIN [${distribution}] private_rpc_remote_direct.private_rpc_exchange_test.remote_data r
                           ON l.id = r.id ORDER BY l.id"""
                    exception "use_arrow_flight=true"
                }
                test {
                    sql """SELECT r.id, l.value, r.value
                           FROM private_rpc_remote_direct.private_rpc_exchange_test.remote_data r
                           JOIN [${distribution}] private_rpc_exchange_test.local_data l
                           ON r.id = l.id ORDER BY r.id"""
                    exception "use_arrow_flight=true"
                }
            }

            // Arrow Flight fetches external results into local workers; all query exchanges keep local TLS.
            qt_flight_broadcast """SELECT l.id, l.value, r.value FROM private_rpc_exchange_test.local_data l
                                  JOIN [broadcast] private_rpc_remote_flight.private_rpc_exchange_test.remote_data r
                                  ON l.id = r.id ORDER BY l.id"""
            qt_flight_broadcast_reverse """SELECT r.id, l.value, r.value
                                          FROM private_rpc_remote_flight.private_rpc_exchange_test.remote_data r
                                          JOIN [broadcast] private_rpc_exchange_test.local_data l
                                          ON r.id = l.id ORDER BY r.id"""
            qt_flight_shuffle """SELECT l.id, l.value, r.value FROM private_rpc_exchange_test.local_data l
                                JOIN [shuffle] private_rpc_remote_flight.private_rpc_exchange_test.remote_data r
                                ON l.id = r.id ORDER BY l.id"""
            qt_flight_shuffle_reverse """SELECT r.id, l.value, r.value
                                        FROM private_rpc_remote_flight.private_rpc_exchange_test.remote_data r
                                        JOIN [shuffle] private_rpc_exchange_test.local_data l
                                        ON r.id = l.id ORDER BY r.id"""
        }
    } finally {
        reader.close()
        process.outputStream.close()
        if (!process.waitFor(45, TimeUnit.SECONDS)) {
            process.destroy()
            assertTrue(process.waitFor(15, TimeUnit.SECONDS), "Private RPC fixture cleanup timed out; see ${logs}")
        }
        assertEquals(0, process.exitValue(), "Private RPC fixture failed; see ${logs}")
    }
}
