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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.doris.cdcclient.common.Constants;
import org.apache.doris.cdcclient.common.Env;
import org.apache.doris.job.cdc.split.SnapshotSplit;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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

@Testcontainers
class MySqlVectorITCase {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final AtomicLong JOB_IDS = new AtomicLong(960_000);

    @Container
    static final MySQLContainer<?> MYSQL =
            new MySQLContainer<>(DockerImageName.parse("mysql:9.7.4"))
                    .withConfigurationOverride("docker/server-with-binlog")
                    .withCommand("--binlog-row-metadata=FULL")
                    .withDatabaseName("cdc_test")
                    .withUsername("cdc")
                    .withPassword("123456")
                    .withEnv("MYSQL_ROOT_PASSWORD", "123456");

    @ParameterizedTest
    @ValueSource(strings = {"PLAIN", "VIRTUAL", "STORED"})
    @Timeout(120)
    void snapshotAndDmlEmitNumericVectors(String storage) throws Exception {
        String jobId = String.valueOf(JOB_IDS.incrementAndGet());
        String database = "vector_db_" + jobId;
        boolean plain = storage.equals("PLAIN");
        String vectorColumn = "v1 VECTOR(4)";
        if (!plain) {
            vectorColumn += " GENERATED ALWAYS AS (TO_VECTOR(info ->> '$.a1')) " + storage;
        }
        try (Connection connection =
                        DriverManager.getConnection(
                                "jdbc:mysql://"
                                        + MYSQL.getHost()
                                        + ":"
                                        + MYSQL.getMappedPort(3306)
                                        + "/?allowPublicKeyRetrieval=true&useSSL=false",
                                "root",
                                "123456");
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE DATABASE " + database);
            statement.execute("USE " + database);
            statement.execute(
                    "CREATE TABLE t_vector (id INT PRIMARY KEY, info JSON, "
                            + vectorColumn
                            + ", marker INT, v2 VECTOR(2), tail VARCHAR(20))");
            insertVector(statement, 1, plain);
            statement.execute("INSERT INTO t_vector(id, marker, tail) VALUES (2, 42, 'tail')");

            try (MockDorisServer mock = new MockDorisServer();
                    CdcClientWriteHarness harness =
                            CdcClientWriteHarness.mysql(
                                    jobId,
                                    MYSQL.getHost(),
                                    MYSQL.getMappedPort(3306),
                                    "root",
                                    "123456",
                                    database,
                                    "t_vector",
                                    "initial",
                                    "doris_target_db",
                                    mock)) {
                List<SnapshotSplit> splits = harness.fetchAllSnapshotSplits("t_vector");
                harness.writeSnapshot(splits);
                Map<Integer, JsonNode> snapshot = indexById(harness.loadedRecords());
                assertThat(snapshot).containsOnlyKeys(1, 2);
                assertRow(snapshot.get(1), "[1.25,-2.5,0,4]", "[0.5,-0.5]", 0);
                assertRow(snapshot.get(2), null, null, 0);

                insertVector(statement, 3, plain);
                // A physical vector must update even when its JSON companion does not change.
                statement.execute(
                        plain
                                ? "UPDATE t_vector SET v1=TO_VECTOR('[5,-6.25,7.5,8]') WHERE id=1"
                                : "UPDATE t_vector SET info='{\"a1\":[5,-6.25,7.5,8]}' WHERE id=1");
                statement.execute("DELETE FROM t_vector WHERE id=2");
                statement.execute("INSERT INTO t_vector(id, marker, tail) VALUES (4, 42, 'tail')");
                Map<Integer, JsonNode> delta =
                        indexById(harness.writeBinlogUntil(splits, 4, Duration.ofSeconds(90)));
                assertThat(delta).containsOnlyKeys(1, 2, 3, 4);
                assertRow(delta.get(1), "[5,-6.25,7.5,8]", "[0.5,-0.5]", 0);
                assertRow(delta.get(2), null, null, 1);
                assertRow(delta.get(3), "[1.25,-2.5,0,4]", "[0.5,-0.5]", 0);
                assertRow(delta.get(4), null, null, 0);
            } finally {
                Env.getCurrentEnv().close(jobId);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"PLAIN", "VIRTUAL", "STORED"})
    @Timeout(180)
    void addedVectorColumnEmitsDdlAndSubsequentDml(String storage) throws Exception {
        String jobId = String.valueOf(JOB_IDS.incrementAndGet());
        String database = "vector_add_db_" + jobId;
        boolean plain = storage.equals("PLAIN");
        String vectorColumn = "vector VECTOR(4)";
        if (!plain) {
            vectorColumn += " GENERATED ALWAYS AS (TO_VECTOR(info ->> '$.a1')) " + storage;
        }
        try (Connection connection =
                        DriverManager.getConnection(
                                "jdbc:mysql://"
                                        + MYSQL.getHost()
                                        + ":"
                                        + MYSQL.getMappedPort(3306)
                                        + "/?allowPublicKeyRetrieval=true&useSSL=false",
                                "root",
                                "123456");
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE DATABASE " + database);
            statement.execute("USE " + database);
            // Keep both the table and added column names unquoted to exercise identifier parsing.
            statement.execute(
                    "CREATE TABLE vector (id INT PRIMARY KEY, info JSON, marker INT, tail VARCHAR(20))");
            statement.execute(
                    "INSERT INTO vector VALUES (1, '{\"a1\":[1.25,-2.5,0,4]}', 42, 'tail'),"
                            + " (2, NULL, 42, 'tail')");

            try (MockDorisServer mock = new MockDorisServer();
                    CdcClientWriteHarness harness =
                            CdcClientWriteHarness.mysql(
                                    jobId,
                                    MYSQL.getHost(),
                                    MYSQL.getMappedPort(3306),
                                    "root",
                                    "123456",
                                    database,
                                    "vector",
                                    "initial",
                                    "doris_target_db",
                                    mock)) {
                List<SnapshotSplit> splits = harness.fetchAllSnapshotSplits("vector");
                harness.writeSnapshot(splits);
                Map<Integer, JsonNode> snapshot = indexById(harness.loadedRecords());
                assertThat(snapshot).containsOnlyKeys(1, 2);
                assertThat(snapshot.get(1).has("vector")).isFalse();
                assertThat(snapshot.get(2).has("vector")).isFalse();
                // Commit the old schema before ALTER so the test exercises incremental DDL handling.
                harness.enterBinlog(splits);
                String schemaBeforeAdd = harness.committedTableSchemas();
                assertThat(schemaBeforeAdd).isNotNull();

                statement.execute("ALTER TABLE vector ADD COLUMN " + vectorColumn + " AFTER info");
                statement.execute(
                        "INSERT INTO vector(id, info, marker, tail"
                                + (plain ? ", vector" : "")
                                + ") VALUES (3, '{\"a1\":[1.25,-2.5,0,4]}', 42, 'tail'"
                                + (plain ? ", TO_VECTOR('[1.25,-2.5,0,4]')" : "")
                                + ")");
                statement.execute(
                        plain
                                ? "UPDATE vector SET vector=TO_VECTOR('[5,-6.25,7.5,8]') WHERE id=1"
                                : "UPDATE vector SET info='{\"a1\":[5,-6.25,7.5,8]}' WHERE id=1");
                statement.execute("DELETE FROM vector WHERE id=2");
                statement.execute("INSERT INTO vector(id, marker, tail) VALUES (4, 42, 'tail')");

                Map<Integer, JsonNode> delta =
                        indexById(harness.continueBinlog(4, Duration.ofSeconds(90)));
                assertThat(harness.executedDdls())
                        .containsExactly(
                                "ALTER TABLE `doris_target_db`.`vector` ADD COLUMN `vector` ARRAY<FLOAT>");
                assertThat(harness.committedTableSchemas()).isNotNull().isNotEqualTo(schemaBeforeAdd);
                assertThat(delta).containsOnlyKeys(1, 2, 3, 4);
                assertVector(delta.get(1).get("vector"), "[5,-6.25,7.5,8]");
                assertVector(delta.get(2).get("vector"), null);
                assertVector(delta.get(3).get("vector"), "[1.25,-2.5,0,4]");
                assertVector(delta.get(4).get("vector"), null);
                for (JsonNode row : delta.values()) {
                    assertThat(row.get("marker").asInt()).isEqualTo(42);
                    assertThat(row.get("tail").asText()).isEqualTo("tail");
                    assertThat(row.get(Constants.DORIS_DELETE_SIGN).asInt())
                            .isEqualTo(row.get("id").asInt() == 2 ? 1 : 0);
                }

                // A rebuilt reader must keep the committed schema and consume the next change.
                harness.rebuildReaderOnNextWrite();
                statement.execute(
                        plain
                                ? "UPDATE vector SET vector=TO_VECTOR('[9,10,11,12]') WHERE id=1"
                                : "UPDATE vector SET info='{\"a1\":[9,10,11,12]}' WHERE id=1");
                Map<Integer, JsonNode> resumed =
                        indexById(harness.continueBinlog(1, Duration.ofSeconds(60)));
                assertThat(resumed).containsOnlyKeys(1);
                assertVector(resumed.get(1).get("vector"), "[9,10,11,12]");
                assertThat(resumed.get(1).get("marker").asInt()).isEqualTo(42);
                assertThat(resumed.get(1).get("tail").asText()).isEqualTo("tail");
                assertThat(resumed.get(1).get(Constants.DORIS_DELETE_SIGN).asInt()).isZero();
                assertThat(harness.executedDdls()).hasSize(1);
            } finally {
                Env.getCurrentEnv().close(jobId);
            }
        }
    }

    private void insertVector(Statement statement, int id, boolean plain) throws Exception {
        statement.execute(
                "INSERT INTO t_vector(id, info, marker, v2, tail"
                        + (plain ? ", v1" : "")
                        + ") VALUES ("
                        + id
                        + ", '{\"a1\":[1.25,-2.5,0,4]}', 42, TO_VECTOR('[0.5,-0.5]'), 'tail'"
                        + (plain ? ", TO_VECTOR('[1.25,-2.5,0,4]')" : "")
                        + ")");
    }

    private Map<Integer, JsonNode> indexById(List<String> records) throws Exception {
        Map<Integer, JsonNode> result = new HashMap<>();
        for (String record : records) {
            JsonNode row = MAPPER.readTree(record);
            result.put(row.get("id").asInt(), row);
        }
        return result;
    }

    private void assertRow(JsonNode row, String v1, String v2, int deleted) throws Exception {
        assertThat(row.get("marker").asInt()).isEqualTo(42);
        assertThat(row.get("tail").asText()).isEqualTo("tail");
        assertThat(row.get(Constants.DORIS_DELETE_SIGN).asInt()).isEqualTo(deleted);
        assertVector(row.get("v1"), v1);
        assertVector(row.get("v2"), v2);
    }

    private void assertVector(JsonNode actual, String expected) throws Exception {
        if (expected == null) {
            assertThat(actual.isNull()).isTrue();
            return;
        }
        JsonNode values = MAPPER.readTree(expected);
        assertThat(actual.isArray()).isTrue();
        assertThat(actual.size()).isEqualTo(values.size());
        for (int i = 0; i < values.size(); i++) {
            assertThat(actual.get(i).isNumber()).isTrue();
            assertThat(actual.get(i).asDouble()).isEqualTo(values.get(i).asDouble());
        }
    }
}
