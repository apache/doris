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

package org.apache.doris.httpv2.websql;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class WebSqlSession {
    private final String id;
    private final String owner;
    private final long createdAtMillis;
    private final ReentrantLock executionLock = new ReentrantLock(true);
    private final AtomicInteger queued = new AtomicInteger();
    private volatile long lastAccessMillis;
    private volatile Connection connection;
    private volatile Statement activeStatement;
    private volatile boolean closed;

    WebSqlSession(String id, String owner, Connection connection, long nowMillis) {
        this.id = id;
        this.owner = owner;
        this.connection = connection;
        this.createdAtMillis = nowMillis;
        this.lastAccessMillis = nowMillis;
    }

    boolean tryEnter(int maxQueued, long waitMillis) throws InterruptedException {
        int position = queued.incrementAndGet();
        if (position > Math.max(1, maxQueued)) {
            queued.decrementAndGet();
            return false;
        }
        boolean acquired = false;
        try {
            acquired = executionLock.tryLock(waitMillis, TimeUnit.MILLISECONDS);
            return acquired;
        } finally {
            queued.decrementAndGet();
        }
    }

    boolean tryEnterForCleanup() {
        return executionLock.tryLock();
    }

    void leave(long nowMillis) {
        lastAccessMillis = nowMillis;
        executionLock.unlock();
    }

    void touch(long nowMillis) {
        lastAccessMillis = nowMillis;
    }

    void leaveWithoutTouch() {
        executionLock.unlock();
    }

    void setActiveStatement(Statement statement) {
        activeStatement = statement;
    }

    boolean cancel() throws SQLException {
        Statement statement = activeStatement;
        if (statement == null) {
            return false;
        }
        statement.cancel();
        return true;
    }

    void replaceConnection(Connection replacement) throws SQLException {
        Connection previous = connection;
        if (previous != null) {
            previous.close();
        }
        connection = replacement;
    }

    void closeConnection() throws SQLException {
        if (closed) {
            return;
        }
        closed = true;
        Statement statement = activeStatement;
        if (statement != null) {
            try {
                statement.cancel();
            } catch (SQLException ignored) {
                // Closing the connection below remains the final cleanup path.
            }
        }
        connection.close();
    }

    public String getId() {
        return id;
    }

    public String getOwner() {
        return owner;
    }

    public long getCreatedAtMillis() {
        return createdAtMillis;
    }

    public long getLastAccessMillis() {
        return lastAccessMillis;
    }

    Connection getConnection() {
        return connection;
    }

    boolean isClosed() {
        return closed;
    }
}
