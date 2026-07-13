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

import static java.util.concurrent.TimeUnit.SECONDS;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;

@Testcontainers
abstract class OceanBaseTestBase {

    protected static final int OCEANBASE_CDC_PORT = 2883;
    protected static final String USER = "root@test";
    protected static final String PASSWORD = "123456";

    @Container
    static final GenericContainer<?> OCEANBASE =
            new GenericContainer<>(
                            DockerImageName.parse("quay.io/oceanbase/obbinlog-ce:4.2.5-test"))
                    .withExposedPorts(2881, OCEANBASE_CDC_PORT)
                    .withStartupTimeout(Duration.ofMinutes(6))
                    .waitingFor(
                            new LogMessageWaitStrategy()
                                    .withRegEx(".*OBBinlog is ready!.*")
                                    .withTimes(1)
                                    .withStartupTimeout(Duration.ofMinutes(6)));

    @BeforeAll
    static void waitUntilJdbcEndpointIsReady() {
        Awaitility.await()
                .atMost(60, SECONDS)
                .pollInterval(1, SECONDS)
                .ignoreExceptions()
                .until(
                        () -> {
                            try (Connection connection = connection("")) {
                                return connection.isValid(1);
                            }
                        });
    }

    protected static Connection connection(String database) throws Exception {
        String url =
                "jdbc:mysql://"
                        + OCEANBASE.getHost()
                        + ":"
                        + OCEANBASE.getMappedPort(OCEANBASE_CDC_PORT)
                        + "/"
                        + database
                        + "?serverTimezone=UTC";
        return DriverManager.getConnection(url, USER, PASSWORD);
    }

    protected void createDatabase(String database, String... statements) throws Exception {
        try (Connection connection = connection("");
                Statement statement = connection.createStatement()) {
            statement.execute("DROP DATABASE IF EXISTS " + database);
            statement.execute("CREATE DATABASE " + database);
            statement.execute("USE " + database);
            for (String sql : statements) {
                statement.execute(sql);
            }
        }
    }

    protected void dropDatabase(String database) throws Exception {
        try (Connection connection = connection("");
                Statement statement = connection.createStatement()) {
            statement.execute("DROP DATABASE IF EXISTS " + database);
        }
    }

    protected CdcClientWriteHarness oceanBaseHarness(
            String jobId,
            String database,
            String includeTables,
            String offset,
            MockDorisServer mock) {
        return CdcClientWriteHarness.oceanbase(
                jobId,
                OCEANBASE.getHost(),
                OCEANBASE.getMappedPort(OCEANBASE_CDC_PORT),
                USER,
                PASSWORD,
                database,
                includeTables,
                offset,
                "doris_target_db",
                mock);
    }
}
