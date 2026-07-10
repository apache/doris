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

import org.apache.doris.cdcclient.common.Constants;
import org.apache.doris.cdcclient.common.Env;
import org.apache.doris.job.cdc.split.SnapshotSplit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.PostgreSQLContainer;
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
 * Version compatibility smoke test for the PostgreSQL releases not exercised by the default ITCases
 * (which already cover 14, the supported lower bound): 15, 16 and 17. Runs the core read chain —
 * snapshot full-load then incremental insert/update/delete — to confirm the from-to
 * {@code writeRecords} path and the pgoutput slot/publication setup work across newer servers.
 */
class PostgresVersionSmokeITCase {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final AtomicLong JOB_ID_SEQ = new AtomicLong(570_000);

    @ParameterizedTest
    @ValueSource(strings = {"postgres:15", "postgres:16", "postgres:17"})
    void snapshotAndDmlWorkAcrossVersions(String image) throws Exception {
        String jobId = String.valueOf(JOB_ID_SEQ.incrementAndGet());
        try (PostgreSQLContainer<?> postgres =
                new PostgreSQLContainer<>(
                                DockerImageName.parse(image).asCompatibleSubstituteFor("postgres"))
                        .withCommand("postgres", "-c", "wal_level=logical")) {
            postgres.start();

            try (Connection conn = connect(postgres);
                    Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS t_user");
                st.execute(
                        "CREATE TABLE t_user (id INT PRIMARY KEY, name VARCHAR(50),"
                                + " age INT, c_decimal DECIMAL(10,2))");
                st.execute("ALTER TABLE t_user REPLICA IDENTITY FULL");
                st.execute("INSERT INTO t_user VALUES (1,'alice',30,12.34), (2,'bob',25,56.78)");
            }

            try (MockDorisServer mock = new MockDorisServer();
                    CdcClientWriteHarness harness =
                            CdcClientWriteHarness.postgres(
                                    jobId,
                                    postgres.getHost(),
                                    postgres.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT),
                                    postgres.getUsername(),
                                    postgres.getPassword(),
                                    postgres.getDatabaseName(),
                                    "public",
                                    "t_user",
                                    "initial",
                                    "doris_target_db",
                                    mock)) {

                List<SnapshotSplit> splits = harness.fetchAllSnapshotSplits("t_user");
                harness.writeSnapshot(splits);

                // snapshot full-load
                Map<Integer, JsonNode> snap = indexById(harness.loadedRecords());
                assertThat(snap).containsKeys(1, 2);
                assertThat(snap.get(1).get("name").asText()).isEqualTo("alice");
                assertThat(snap.get(1).get("age").asInt()).isEqualTo(30);
                assertThat(snap.get(1).get("c_decimal").decimalValue().toPlainString())
                        .isEqualTo("12.34");

                // incremental insert/update/delete
                try (Connection conn = connect(postgres);
                        Statement st = conn.createStatement()) {
                    st.execute("INSERT INTO t_user VALUES (3,'carol',40,99.99)");
                    st.execute("UPDATE t_user SET age = 31 WHERE id = 1");
                    st.execute("DELETE FROM t_user WHERE id = 2");
                }

                List<String> binlog = harness.writeBinlogUntil(splits, 3, Duration.ofSeconds(90));
                Map<Integer, JsonNode> byId = indexById(binlog);
                assertThat(byId).containsKeys(1, 2, 3);
                assertThat(byId.get(3).get("name").asText()).isEqualTo("carol");
                assertThat(byId.get(3).get(Constants.DORIS_DELETE_SIGN).asInt()).isZero();
                assertThat(byId.get(1).get("age").asInt()).isEqualTo(31);
                assertThat(byId.get(2).get(Constants.DORIS_DELETE_SIGN).asInt()).isEqualTo(1);
            } finally {
                Env.getCurrentEnv().close(jobId);
            }
        }
    }

    private Map<Integer, JsonNode> indexById(List<String> records) throws Exception {
        Map<Integer, JsonNode> result = new HashMap<>();
        for (String record : records) {
            JsonNode node = MAPPER.readTree(record);
            result.put(node.get("id").asInt(), node);
        }
        return result;
    }

    private Connection connect(PostgreSQLContainer<?> postgres) throws Exception {
        return DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
    }
}
