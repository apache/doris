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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;

abstract class OceanBaseTestBase {

    protected static final int OCEANBASE_CDC_PORT = 2883;
    protected static final String USER = "root@test";
    protected static final String PASSWORD = "123456";

    private static final String EXTERNAL_HOST = System.getProperty("oceanbase.host");
    private static final int EXTERNAL_CDC_PORT =
            Integer.getInteger("oceanbase.cdc.port", OCEANBASE_CDC_PORT);

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
    static void initializeOceanBase() {
        if (!useExternalOceanBase()) {
            OCEANBASE.start();
        }

        Awaitility.await()
                .atMost(60, SECONDS)
                .pollInterval(1, SECONDS)
                .ignoreExceptions()
                .until(
                        () -> {
                            try (Connection connection = connection("");
                                    Statement statement = connection.createStatement();
                                    ResultSet resultSet =
                                            statement.executeQuery("SHOW MASTER STATUS")) {
                                return connection.isValid(1)
                                        && resultSet.next()
                                        && !resultSet.getString("File").isEmpty()
                                        && resultSet.getLong("Position") >= 4;
                            }
                        });
    }

    @AfterAll
    static void stopOceanBase() {
        if (!useExternalOceanBase()) {
            OCEANBASE.stop();
        }
    }

    protected static Connection connection(String database) throws Exception {
        String url =
                "jdbc:mysql://"
                        + oceanBaseHost()
                        + ":"
                        + oceanBaseCdcPort()
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
                oceanBaseHost(),
                oceanBaseCdcPort(),
                USER,
                PASSWORD,
                database,
                includeTables,
                offset,
                "doris_target_db",
                mock);
    }

    private static boolean useExternalOceanBase() {
        return EXTERNAL_HOST != null;
    }

    private static String oceanBaseHost() {
        return useExternalOceanBase() ? EXTERNAL_HOST : OCEANBASE.getHost();
    }

    private static int oceanBaseCdcPort() {
        return useExternalOceanBase()
                ? EXTERNAL_CDC_PORT
                : OCEANBASE.getMappedPort(OCEANBASE_CDC_PORT);
    }
}
