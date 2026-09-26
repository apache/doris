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

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

import java.sql.Connection
import java.util.concurrent.ConcurrentHashMap

/**
 * Doris connections, each with the thread that opened it, kept open exactly as long as that thread runs.
 *
 * A suite's connections are ThreadLocal to the thread that opened them (SuiteContext.getConnection and its
 * siblings), and only the suite thread and the threads Suite.thread() runs end with closeThreadLocal(): a
 * thread the suite created itself (a Thread, an Executors pool) takes its connection to the grave, and
 * before this table it stayed open on the frontend until the client JVM garbage-collected it - seconds or
 * minutes later, at the JVM's whim. Every statement now closes the connections of the threads that have
 * finished (closeThoseOfFinishedThreads), so a suite that starts a thread per step holds at most the
 * connections of the threads still running.
 *
 * Each suite has a table of its own, drained when the suite is over. A thread the suite left running past
 * its end - test_active_queries polls a system table for five minutes after its suite returned - keeps its
 * connection: the suite's drain hands it to STRAY, the table shared by all suites, where the next statement
 * of any suite closes it once the thread has finished, and the end of the run closes whatever is left.
 */
@Slf4j
@CompileStatic
class OpenedDorisConnections {
    static final OpenedDorisConnections STRAY = new OpenedDorisConnections("threads that outlived their suite")

    private final String owner
    private final Map<Connection, Thread> connections = new ConcurrentHashMap<>()
    private boolean drained = false

    OpenedDorisConnections(String owner) {
        this.owner = owner
    }

    // Records conn as opened by thread. False once drain() ran: the caller has to keep it elsewhere.
    // Serialized with drain(), so a registration racing the drain is either drained or refused, never
    // left in a table nobody reads again.
    synchronized boolean add(Connection conn, Thread thread = Thread.currentThread()) {
        if (drained) {
            return false
        }
        connections.put(conn, thread)
        return true
    }

    // Forgets conn: the accessor that opened it closes it itself.
    void remove(Connection conn) {
        connections.remove(conn)
    }

    // Closes the connections whose thread has finished. Returns how many.
    int closeThoseOfFinishedThreads() {
        int closed = 0
        for (Map.Entry<Connection, Thread> entry : connections.entrySet()) {
            if (!entry.value.isAlive() && connections.remove(entry.key, entry.value)) {
                closeQuietly(entry.key, "connection of finished thread ${entry.value.name}".toString())
                closed++
            }
        }
        if (closed > 0) {
            log.info("Closed ${closed} connection(s) of finished threads (${owner})".toString())
        }
        return closed
    }

    // Takes everything out, and refuses registrations from then on.
    synchronized Map<Connection, Thread> drain() {
        drained = true
        Map<Connection, Thread> all = new HashMap<>(connections)
        connections.clear()
        return all
    }

    // Closes everything, whether its thread has finished or not: for the end of the run.
    void closeAll() {
        List<String> running = []
        for (Map.Entry<Connection, Thread> entry : connections.entrySet()) {
            if (connections.remove(entry.key, entry.value)) {
                if (entry.value.isAlive()) {
                    running.add(entry.value.name)
                }
                closeQuietly(entry.key, "connection of thread ${entry.value.name}".toString())
            }
        }
        if (!running.isEmpty()) {
            log.warn("Closed the connections of thread(s) ${running} still running at the end of the run "
                    + "(${owner})".toString())
        }
    }

    int size() {
        return connections.size()
    }

    static void closeQuietly(Connection conn, String what) {
        try {
            conn.close()
        } catch (Throwable t) {
            log.warn("Close ${what} failed".toString(), t)
        }
    }
}
