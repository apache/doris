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

import org.apache.doris.cdcclient.common.Env;
import org.apache.doris.cdcclient.itcase.CdcClientReadHarness.SnapshotResult;
import org.apache.doris.job.cdc.split.SnapshotSplit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Verify that the cdc_client read path works when {@code lower_case_table_names=1}.
 *
 * <p>MySQL 8.0 pre-initializes the data directory during image build, so simply setting
 * the option in a conf.d override is not enough — MySQL will refuse to start because the
 * data directory was initialized with {@code lower_case_table_names=0}.  The container
 * command includes a pre-start step that removes the data directory so MySQL re-initializes
 * it with the new setting.
 *
 * <p>In this mode every table name is stored and returned in lowercase. The include_tables
 * and snapshot-table name must use the lowercase form even when the DDL uses uppercase.
 */
@Testcontainers
class MySqlCaseInsensitiveITCase {

    private static final String USER = "cdc";
    private static final String PASSWORD = "123456";
    private static final String ROOT_USER = "root";
    private static final String ROOT_PASSWORD = "123456";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final AtomicLong JOB_ID_SEQ = new AtomicLong(510_000);

    @Container
    static final MySQLContainer<?> MYSQL =
            new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))
                    .withConfigurationOverride("docker/server-case-insensitive")
                    .withDatabaseName("cdc_test")
                    .withUsername(USER)
                    .withPassword(PASSWORD)
                    .withEnv("MYSQL_ROOT_PASSWORD", ROOT_PASSWORD)
                    .withCommand(
                            "bash",
                            "-c",
                            "rm -rf /var/lib/mysql/*"
                                    + " && exec /usr/local/bin/docker-entrypoint.sh mysqld");

    private String jobId;
    private String database;

    @BeforeEach
    void setUp() throws Exception {
        jobId = String.valueOf(JOB_ID_SEQ.incrementAndGet());
        database = "case_db_" + jobId;
        try (Connection conn = rootConnection("");
                Statement st = conn.createStatement()) {
            st.execute("CREATE DATABASE " + database);
            st.execute("USE " + database);
            st.execute(
                    "CREATE TABLE T_Users (id INT NOT NULL, UserName VARCHAR(50), PRIMARY KEY (id))");
            st.execute("INSERT INTO T_Users VALUES (1,'Alice'), (2,'Bob'), (3,'Carol')");
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        Env.getCurrentEnv().close(jobId);
        try (Connection conn = rootConnection("");
                Statement st = conn.createStatement()) {
            st.execute("DROP DATABASE IF EXISTS " + database);
        }
    }

    @Test
    void readsSnapshotThenBinlogWithLowercaseConfig() throws Exception {
        // Table name is stored in lowercase by MySQL; include_tables must match.
        try (CdcClientReadHarness harness =
                CdcClientReadHarness.mysql(
                        jobId,
                        MYSQL.getHost(),
                        MYSQL.getMappedPort(MySQLContainer.MYSQL_PORT),
                        USER,
                        PASSWORD,
                        database,
                        "t_users",
                        "initial")) {

            List<SnapshotSplit> splits = harness.fetchAllSnapshotSplits("t_users");
            SnapshotResult snapshot = harness.readSnapshot(splits);

            Map<Integer, String> names = namesById(snapshot.records());
            assertThat(names).containsOnlyKeys(1, 2, 3);
            assertThat(names.get(1)).isEqualTo("Alice");
            assertThat(names.get(2)).isEqualTo("Bob");
            assertThat(names.get(3)).isEqualTo("Carol");

            insert("t_users", 4, "Dave");

            List<String> binlog =
                    harness.readBinlogUntil(snapshot, splits, 1, Duration.ofSeconds(60));
            assertThat(binlog).hasSize(1);
            JsonNode row = MAPPER.readTree(binlog.get(0));
            assertThat(row.get("id").asInt()).isEqualTo(4);
            assertThat(row.get("UserName").asText()).isEqualTo("Dave");
            assertThat(row.get("__DORIS_DELETE_SIGN__").asInt()).isZero();
        }
    }

    private Map<Integer, String> namesById(List<String> records) throws Exception {
        Map<Integer, String> result = new HashMap<>();
        for (String record : records) {
            JsonNode node = MAPPER.readTree(record);
            result.put(node.get("id").asInt(), node.get("UserName").asText());
        }
        return result;
    }

    private void insert(String table, int id, String name) throws Exception {
        try (Connection conn = rootConnection(database);
                Statement st = conn.createStatement()) {
            st.execute(String.format("INSERT INTO %s VALUES (%d, '%s')", table, id, name));
        }
    }

    private Connection rootConnection(String db) throws Exception {
        String url =
                "jdbc:mysql://"
                        + MYSQL.getHost()
                        + ":"
                        + MYSQL.getMappedPort(MySQLContainer.MYSQL_PORT)
                        + "/"
                        + db;
        return DriverManager.getConnection(url, ROOT_USER, ROOT_PASSWORD);
    }
}
