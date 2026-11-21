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

import java.sql.DriverManager
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

suite("test_mysql_kill_query_reuse") {
    def connection = context.getConnection()
    def connectionId = sql("SELECT connection_id()")[0][0].toString()
    def url = connection.getMetaData().getURL()
    def pool = Executors.newSingleThreadExecutor()
    try {
        // Repeated cancellation must not leak the previous executor's state to the next command.
        (1..2).each { attempt ->
            def marker = "mysql_kill_query_reuse_${attempt}"
            def cancel = pool.submit({
                DriverManager.getConnection(url, context.config.jdbcUser, context.config.jdbcPassword)
                        .withCloseable { admin ->
                    admin.createStatement().withCloseable { statement ->
                        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20)
                        while (System.nanoTime() < deadline) {
                            boolean running = false
                            statement.executeQuery("SHOW FULL PROCESSLIST").withCloseable { rows ->
                                while (rows.next()) {
                                    if (rows.getString("Id") == connectionId &&
                                            rows.getString("Info")?.contains(marker) &&
                                            rows.getString("QueryId")?.contains("-") &&
                                            rows.getInt("Time") >= 1) {
                                        running = true
                                    }
                                }
                            }
                            if (running) {
                                statement.execute("KILL QUERY ${connectionId}")
                                return
                            }
                            Thread.sleep(50)
                        }
                        throw new IllegalStateException("query did not start: ${marker}")
                    }
                }
            } as java.util.concurrent.Callable)
            test {
                sql "SELECT sleep(30 + number) FROM numbers('number'='1') /* ${marker} */"
                exception "cancel"
            }
            cancel.get(25, TimeUnit.SECONDS)
            // A reconnect would also allow SELECT 42, so verify the server connection identity.
            assertEquals(connectionId, sql("SELECT connection_id()")[0][0].toString())
            qt_reused "SELECT 42"
        }
    } finally {
        pool.shutdownNow()
    }
}
