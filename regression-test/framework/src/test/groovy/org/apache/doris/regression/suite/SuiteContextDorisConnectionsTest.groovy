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

package org.apache.doris.regression.suite

import org.apache.doris.regression.Config
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

import java.lang.reflect.InvocationHandler
import java.lang.reflect.Method
import java.lang.reflect.Proxy
import java.nio.file.Path
import java.sql.Connection
import java.util.concurrent.CountDownLatch

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertFalse
import static org.junit.jupiter.api.Assertions.assertTrue

// A Doris connection a suite's thread-local accessors open stays open as long as the thread that opened it runs:
// every accessor first closes the connections of the threads that have finished, and the end of the suite closes
// those left of finished threads and hands those of threads still running to the strays, which any suite's next
// statement closes once their thread has finished.
class SuiteContextDorisConnectionsTest {
    @TempDir
    Path tempDir

    // The names of the connections closed, each "<accessor> of <the thread that opened it>".
    private final List<String> closed = Collections.synchronizedList([])

    // A Connection that only knows how to close, and records its name when it does.
    private Connection connection(String accessor) {
        String name = "${accessor} of ${Thread.currentThread().name}".toString()
        return (Connection) Proxy.newProxyInstance(Connection.classLoader, [Connection] as Class[],
                { Object proxy, Method method, Object[] args ->
                    switch (method.name) {
                        case "close": closed.add(name); return null
                        case "hashCode": return System.identityHashCode(proxy)
                        case "equals": return proxy.is(args[0])
                        case "toString": return name
                        default: throw new UnsupportedOperationException(method.name)
                    }
                } as InvocationHandler)
    }

    // A config whose Arrow Flight SQL connections are fakes.
    private static class FakeConnectionsConfig extends Config {
        Closure<Connection> open

        @Override
        Connection getConnectionByArrowFlightSqlDbName(String dbName) {
            return open.call("arrow flight sql connection")
        }
    }

    // A suite context whose MySQL connections, to any frontend and to the master, are fakes.
    private static class FakeConnectionsSuiteContext extends SuiteContext {
        private final Closure<Connection> open

        FakeConnectionsSuiteContext(File file, String suiteName, ScriptContext scriptContext, Config config,
                                    Closure<Connection> open) {
            super(file, suiteName, "p0", scriptContext, new SuiteCluster(suiteName, config), null, null, config)
            this.open = open
        }

        @Override
        Connection getConnectionByDbName(String dbName) {
            return open.call("connection")
        }

        @Override
        Connection getMasterConnectionByDbName(String dbName) {
            return open.call("master connection")
        }
    }

    private SuiteContext newSuiteContext(String suiteName) {
        File suiteDir = tempDir.resolve("suites").toFile()
        suiteDir.mkdirs()
        File suiteFile = new File(suiteDir, "${suiteName}.groovy")
        assertTrue(suiteFile.createNewFile())
        FakeConnectionsConfig config = new FakeConnectionsConfig(open: this.&connection)
        config.suitePath = suiteDir.toString()
        config.dataPath = tempDir.resolve("data").toString()
        config.realDataPath = tempDir.resolve("real-data").toString()
        config.defaultDb = "regression_test"
        ScriptContext scriptContext = new ScriptContext(suiteFile, null, null, config, Collections.emptyList(), { true })
        return new FakeConnectionsSuiteContext(suiteFile, suiteName, scriptContext, config, this.&connection)
    }

    private static void onAThreadThatEnds(String name, Closure action) {
        Thread thread = new Thread(action as Runnable, name)
        thread.start()
        thread.join()
    }

    @Test
    void everyAccessorClosesTheConnectionsOfTheThreadsThatHaveFinished() {
        SuiteContext context = newSuiteContext("sweeps")
        try {
            onAThreadThatEnds("t1") { context.getConnection() }
            assertEquals([], closed)
            context.getMasterConnection()
            assertEquals(["connection of t1"], closed)

            onAThreadThatEnds("t2") { context.getMasterConnection() }
            context.getArrowFlightSqlConnection()
            assertEquals(["connection of t1", "master connection of t2"], closed)

            onAThreadThatEnds("t3") { context.getArrowFlightSqlConnection() }
            context.getConnection()
            assertEquals(["connection of t1", "master connection of t2", "arrow flight sql connection of t3"], closed)
        } finally {
            context.close()
        }
    }

    @Test
    void theEndOfTheSuiteLeavesTheConnectionOfAThreadStillRunningToTheStrays() {
        SuiteContext context = newSuiteContext("leaves_a_poller_running")
        CountDownLatch opened = new CountDownLatch(1)
        CountDownLatch release = new CountDownLatch(1)
        // Like test_active_queries, which leaves a thread polling a system table after its suite returned.
        Thread poller = new Thread({
            context.getConnection()
            opened.countDown()
            release.await()
        } as Runnable, "poller")
        poller.start()
        opened.await()
        onAThreadThatEnds("finished") { context.getConnection() }

        // The suite ends: the connection of the thread that finished is closed, the poller keeps its own ...
        context.close()
        assertEquals(["connection of finished"], closed)

        // ... until it has finished, when the next statement of any suite closes it.
        release.countDown()
        poller.join()
        SuiteContext nextSuite = newSuiteContext("next_suite")
        try {
            assertFalse(closed.contains("connection of poller"))
            nextSuite.getConnection()
            assertTrue(closed.contains("connection of poller"))
        } finally {
            nextSuite.close()
        }
    }
}
