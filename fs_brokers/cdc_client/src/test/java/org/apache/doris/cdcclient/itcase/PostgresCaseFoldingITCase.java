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
import org.testcontainers.containers.PostgreSQLContainer;
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
 * Verify that the cdc_client read path handles PostgreSQL's identifier case-folding correctly.
 *
 * <p>PostgreSQL folds unquoted identifiers to lowercase (per SQL standard). Quoted identifiers
 * preserve the declared case. The cdc_client must use the stored name in its config to match
 * what Debezium returns — which is what PG actually stores.
 */
@Testcontainers
class PostgresCaseFoldingITCase {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final AtomicLong JOB_ID_SEQ = new AtomicLong(520_000);

    @Container
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(
                            DockerImageName.parse("postgres:14")
                                    .asCompatibleSubstituteFor("postgres"))
                    .withCommand("postgres", "-c", "wal_level=logical");

    private String jobId;

    @BeforeEach
    void setUp() {
        jobId = String.valueOf(JOB_ID_SEQ.incrementAndGet());
    }

    @AfterEach
    void tearDown() {
        Env.getCurrentEnv().close(jobId);
    }

    @Test
    void unquotedTableNameIsFoldedToLowercase() throws Exception {
        try (Connection conn = connect();
                Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS T_Users");
            // Unquoted identifier: PG folds to lowercase t_users.
            st.execute(
                    "CREATE TABLE T_Users (id INT PRIMARY KEY, name VARCHAR(50))");
            st.execute("INSERT INTO T_Users VALUES (1,'Alice'), (2,'Bob')");
        }

        try (CdcClientReadHarness harness =
                CdcClientReadHarness.postgres(
                        jobId,
                        POSTGRES.getHost(),
                        POSTGRES.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT),
                        POSTGRES.getUsername(),
                        POSTGRES.getPassword(),
                        POSTGRES.getDatabaseName(),
                        "public",
                        "t_users",
                        "initial")) {

            List<SnapshotSplit> splits = harness.fetchAllSnapshotSplits("t_users");
            SnapshotResult snapshot = harness.readSnapshot(splits);

            Map<Integer, String> names = namesById(snapshot.records());
            assertThat(names).containsOnlyKeys(1, 2);
            assertThat(names.get(1)).isEqualTo("Alice");
            assertThat(names.get(2)).isEqualTo("Bob");

            try (Connection conn = connect();
                    Statement st = conn.createStatement()) {
                st.execute("INSERT INTO T_Users VALUES (3,'Carol')");
            }

            List<String> binlog =
                    harness.readBinlogUntil(snapshot, splits, 1, Duration.ofSeconds(60));
            assertThat(binlog).hasSize(1);
            JsonNode row = MAPPER.readTree(binlog.get(0));
            assertThat(row.get("id").asInt()).isEqualTo(3);
            assertThat(row.get("name").asText()).isEqualTo("Carol");
        } finally {
            try (Connection conn = connect();
                    Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS t_users");
            }
        }
    }

    @Test
    void quotedTableNamePreservesGivenCase() throws Exception {
        try (Connection conn = connect();
                Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS \"T_Users\"");
            // Quoted identifier: PG preserves T_Users exactly.
            st.execute(
                    "CREATE TABLE \"T_Users\" (id INT PRIMARY KEY, name VARCHAR(50))");
            st.execute("INSERT INTO \"T_Users\" VALUES (1,'Alice'), (2,'Bob')");
        }

        try (CdcClientReadHarness harness =
                CdcClientReadHarness.postgres(
                        jobId,
                        POSTGRES.getHost(),
                        POSTGRES.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT),
                        POSTGRES.getUsername(),
                        POSTGRES.getPassword(),
                        POSTGRES.getDatabaseName(),
                        "public",
                        "T_Users",
                        "initial")) {

            List<SnapshotSplit> splits = harness.fetchAllSnapshotSplits("T_Users");
            SnapshotResult snapshot = harness.readSnapshot(splits);

            Map<Integer, String> names = namesById(snapshot.records());
            assertThat(names).containsOnlyKeys(1, 2);
            assertThat(names.get(1)).isEqualTo("Alice");
            assertThat(names.get(2)).isEqualTo("Bob");

            try (Connection conn = connect();
                    Statement st = conn.createStatement()) {
                st.execute("INSERT INTO \"T_Users\" VALUES (3,'Carol')");
            }

            List<String> binlog =
                    harness.readBinlogUntil(snapshot, splits, 1, Duration.ofSeconds(60));
            assertThat(binlog).hasSize(1);
            JsonNode row = MAPPER.readTree(binlog.get(0));
            assertThat(row.get("id").asInt()).isEqualTo(3);
            assertThat(row.get("name").asText()).isEqualTo("Carol");
        } finally {
            try (Connection conn = connect();
                    Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS \"T_Users\"");
            }
        }
    }

    private Map<Integer, String> namesById(List<String> records) throws Exception {
        Map<Integer, String> result = new HashMap<>();
        for (String record : records) {
            JsonNode node = MAPPER.readTree(record);
            result.put(node.get("id").asInt(), node.get("name").asText());
        }
        return result;
    }

    private Connection connect() throws Exception {
        return DriverManager.getConnection(
                POSTGRES.getJdbcUrl(),
                POSTGRES.getUsername(),
                POSTGRES.getPassword());
    }
}
