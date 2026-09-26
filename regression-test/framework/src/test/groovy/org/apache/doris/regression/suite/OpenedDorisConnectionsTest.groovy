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

import org.junit.jupiter.api.Test

import java.lang.reflect.InvocationHandler
import java.lang.reflect.Method
import java.lang.reflect.Proxy
import java.sql.Connection
import java.util.concurrent.CountDownLatch

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertFalse
import static org.junit.jupiter.api.Assertions.assertTrue

class OpenedDorisConnectionsTest {
    // A Connection that only knows how to close, and records under which name it did.
    private static Connection connection(List<String> closed, String name) {
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

    private static Thread finishedThread() {
        Thread thread = new Thread({ })
        thread.start()
        thread.join()
        return thread
    }

    private static Thread runningUntil(CountDownLatch release) {
        Thread thread = new Thread({ release.await() })
        thread.start()
        return thread
    }

    @Test
    void closesAConnectionOnceTheThreadThatOpenedItHasFinished() {
        List<String> closed = []
        OpenedDorisConnections table = new OpenedDorisConnections("test")
        CountDownLatch release = new CountDownLatch(1)
        Thread running = runningUntil(release)
        assertTrue(table.add(connection(closed, "of the running thread"), running))
        assertTrue(table.add(connection(closed, "of the finished thread"), finishedThread()))

        assertEquals(1, table.closeThoseOfFinishedThreads())
        assertEquals(["of the finished thread"], closed)
        assertEquals(1, table.size())

        release.countDown()
        running.join()
        assertEquals(1, table.closeThoseOfFinishedThreads())
        assertEquals(["of the finished thread", "of the running thread"], closed)
        assertEquals(0, table.size())
        assertEquals(0, table.closeThoseOfFinishedThreads())
    }

    @Test
    void drainTakesEverythingOutAndRefusesWhatComesLater() {
        List<String> closed = []
        OpenedDorisConnections table = new OpenedDorisConnections("test")
        Connection before = connection(closed, "before the drain")
        assertTrue(table.add(before, Thread.currentThread()))

        Map<Connection, Thread> drained = table.drain()
        assertEquals([before] as Set, drained.keySet())
        assertEquals(Thread.currentThread(), drained.get(before))
        assertEquals(0, table.size())
        // Draining hands the connections over, it closes nothing itself.
        assertEquals([], closed)

        assertFalse(table.add(connection(closed, "after the drain"), Thread.currentThread()))
        assertEquals(0, table.size())
    }

    @Test
    void removeForgetsAConnectionWithoutClosingIt() {
        List<String> closed = []
        OpenedDorisConnections table = new OpenedDorisConnections("test")
        Connection conn = connection(closed, "forgotten")
        assertTrue(table.add(conn, finishedThread()))

        table.remove(conn)
        assertEquals(0, table.size())
        assertEquals(0, table.closeThoseOfFinishedThreads())
        assertEquals([], closed)
    }

    @Test
    void closeAllClosesTheConnectionsOfRunningThreadsToo() {
        List<String> closed = []
        OpenedDorisConnections table = new OpenedDorisConnections("test")
        CountDownLatch release = new CountDownLatch(1)
        Thread running = runningUntil(release)
        try {
            assertTrue(table.add(connection(closed, "of the running thread"), running))
            assertTrue(table.add(connection(closed, "of the finished thread"), finishedThread()))

            table.closeAll()
            assertEquals(["of the finished thread", "of the running thread"] as Set, closed as Set)
            assertEquals(0, table.size())
        } finally {
            release.countDown()
            running.join()
        }
    }

    @Test
    void aConnectionThatFailsToCloseDoesNotStopTheOthers() {
        List<String> closed = []
        OpenedDorisConnections table = new OpenedDorisConnections("test")
        Connection failing = (Connection) Proxy.newProxyInstance(Connection.classLoader, [Connection] as Class[],
                { Object proxy, Method method, Object[] args ->
                    switch (method.name) {
                        case "close": throw new IllegalStateException("already gone")
                        case "hashCode": return System.identityHashCode(proxy)
                        case "equals": return proxy.is(args[0])
                        default: return null
                    }
                } as InvocationHandler)
        assertTrue(table.add(failing, finishedThread()))
        assertTrue(table.add(connection(closed, "closable"), finishedThread()))

        assertEquals(2, table.closeThoseOfFinishedThreads())
        assertEquals(["closable"], closed)
        assertEquals(0, table.size())
    }
}
