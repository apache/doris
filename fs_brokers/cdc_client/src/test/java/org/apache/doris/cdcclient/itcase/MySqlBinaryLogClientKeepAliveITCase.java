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

package org.apache.doris.cdcclient.itcase;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Testcontainers
class MySqlBinaryLogClientKeepAliveITCase {

    private static final String ROOT_USER = "root";
    private static final String ROOT_PASSWORD = "123456";
    private static final String TABLE = "keepalive_replay";
    private static final int ROW_COUNT = 500;

    @Container
    static final MySQLContainer<?> MYSQL =
            new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))
                    .withDatabaseName("cdc_test")
                    .withUsername("cdc")
                    .withPassword(ROOT_PASSWORD)
                    .withEnv("MYSQL_ROOT_PASSWORD", ROOT_PASSWORD);

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void keepAliveReconnectReplaysIncompleteNonGtidTransaction() throws Exception {
        BinlogPosition startPosition;
        try (Connection connection = rootConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS " + TABLE);
            statement.execute("CREATE TABLE " + TABLE + " (id INT PRIMARY KEY)");
            startPosition = currentBinlogPosition(statement);
        }

        BinaryLogClient client =
                new BinaryLogClient(
                        MYSQL.getHost(),
                        MYSQL.getMappedPort(MySQLContainer.MYSQL_PORT),
                        ROOT_USER,
                        ROOT_PASSWORD);
        client.setBinlogFilename(startPosition.filename);
        client.setBinlogPosition(startPosition.position);
        client.setHeartbeatInterval(100L);
        client.setKeepAliveInterval(300L);
        client.setConnectTimeout(3_000L);

        Set<Integer> receivedIds = ConcurrentHashMap.newKeySet();
        AtomicBoolean interruptFirstRowsEvent = new AtomicBoolean(true);
        AtomicInteger connectionCount = new AtomicInteger();
        AtomicReference<Throwable> listenerFailure = new AtomicReference<>();
        CountDownLatch firstRowsEventInterrupted = new CountDownLatch(1);
        CountDownLatch reconnected = new CountDownLatch(1);
        CountDownLatch allRowsReceived = new CountDownLatch(1);

        client.registerLifecycleListener(
                new BinaryLogClient.AbstractLifecycleListener() {
                    @Override
                    public void onConnect(BinaryLogClient connectedClient) {
                        if (connectionCount.incrementAndGet() > 1) {
                            reconnected.countDown();
                        }
                    }
                });
        client.registerEventListener(
                event -> {
                    if (!(event.getData() instanceof WriteRowsEventData)) {
                        return;
                    }
                    List<Serializable[]> rows =
                            ((WriteRowsEventData) event.getData()).getRows();
                    if (interruptFirstRowsEvent.compareAndSet(true, false)) {
                        if (rows.size() <= 20) {
                            listenerFailure.set(
                                    new AssertionError(
                                            "Expected one multi-row event, but received "
                                                    + rows.size()
                                                    + " rows"));
                            firstRowsEventInterrupted.countDown();
                            return;
                        }
                        addRows(receivedIds, rows.subList(0, 20));
                        firstRowsEventInterrupted.countDown();

                        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                        while (client.isConnected() && System.nanoTime() < deadline) {
                            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                        }
                        if (client.isConnected()) {
                            listenerFailure.set(
                                    new AssertionError(
                                            "Keepalive did not disconnect the blocked listener"));
                        }
                        return;
                    }

                    addRows(receivedIds, rows);
                    if (receivedIds.size() >= ROW_COUNT) {
                        allRowsReceived.countDown();
                    }
                });

        try {
            client.connect(5_000L);
            try (Connection connection = rootConnection();
                    Statement statement = connection.createStatement()) {
                statement.execute(insertRowsSql());
            }

            assertThat(firstRowsEventInterrupted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(listenerFailure.get()).isNull();
            assertThat(reconnected.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(allRowsReceived.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(listenerFailure.get()).isNull();
            assertThat(connectionCount.get()).isGreaterThanOrEqualTo(2);
            assertThat(receivedIds).containsExactlyInAnyOrderElementsOf(expectedIds());
        } finally {
            client.disconnect();
        }
    }

    private static void addRows(Set<Integer> receivedIds, List<Serializable[]> rows) {
        for (Serializable[] row : rows) {
            receivedIds.add(((Number) row[0]).intValue());
        }
    }

    private static List<Integer> expectedIds() {
        return IntStream.rangeClosed(1, ROW_COUNT).boxed().collect(Collectors.toList());
    }

    private static String insertRowsSql() {
        List<String> values = new ArrayList<>(ROW_COUNT);
        for (int id = 1; id <= ROW_COUNT; id++) {
            values.add("(" + id + ")");
        }
        return "INSERT INTO " + TABLE + " VALUES " + String.join(",", values);
    }

    private static BinlogPosition currentBinlogPosition(Statement statement) throws Exception {
        try (ResultSet resultSet = statement.executeQuery("SHOW MASTER STATUS")) {
            assertThat(resultSet.next()).isTrue();
            return new BinlogPosition(resultSet.getString("File"), resultSet.getLong("Position"));
        }
    }

    private static Connection rootConnection() throws Exception {
        String url =
                "jdbc:mysql://"
                        + MYSQL.getHost()
                        + ":"
                        + MYSQL.getMappedPort(MySQLContainer.MYSQL_PORT)
                        + "/"
                        + MYSQL.getDatabaseName()
                        + "?serverTimezone=UTC";
        return DriverManager.getConnection(url, ROOT_USER, ROOT_PASSWORD);
    }

    private static final class BinlogPosition {
        private final String filename;
        private final long position;

        private BinlogPosition(String filename, long position) {
            this.filename = filename;
            this.position = position;
        }
    }
}
