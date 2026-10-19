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
 * Exercises the from-to {@code writeRecords} path for PostgreSQL incremental (WAL) DML: insert /
 * update / delete after the snapshot must be stream-loaded with the correct values and delete sign.
 */
@Testcontainers
class PostgresWriteDmlITCase {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final AtomicLong JOB_ID_SEQ = new AtomicLong(510_000);

    @Container
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:14"))
                    .withCommand("postgres", "-c", "wal_level=logical");

    private String jobId;

    @BeforeEach
    void setUp() throws Exception {
        jobId = String.valueOf(JOB_ID_SEQ.incrementAndGet());
        try (Connection conn = connect();
                Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS t_user");
            st.execute("CREATE TABLE t_user (id INT PRIMARY KEY, name VARCHAR(50))");
            // emit full old image for updates/deletes
            st.execute("ALTER TABLE t_user REPLICA IDENTITY FULL");
            st.execute("INSERT INTO t_user VALUES (1,'alice'), (2,'bob'), (3,'carol')");
        }
    }

    @AfterEach
    void tearDown() {
        Env.getCurrentEnv().close(jobId);
    }

    @Test
    void walDmlStreamLoadsInsertUpdateDelete() throws Exception {
        try (MockDorisServer mock = new MockDorisServer();
                CdcClientWriteHarness harness =
                        CdcClientWriteHarness.postgres(
                                jobId,
                                POSTGRES.getHost(),
                                POSTGRES.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT),
                                POSTGRES.getUsername(),
                                POSTGRES.getPassword(),
                                POSTGRES.getDatabaseName(),
                                "public",
                                "t_user",
                                "initial",
                                "doris_target_db",
                                mock)) {

            List<SnapshotSplit> splits = harness.fetchAllSnapshotSplits("t_user");
            harness.writeSnapshot(splits);

            try (Connection conn = connect();
                    Statement st = conn.createStatement()) {
                st.execute("INSERT INTO t_user VALUES (4, 'dave')");
                st.execute("UPDATE t_user SET name = 'alice2' WHERE id = 1");
                st.execute("DELETE FROM t_user WHERE id = 2");
            }

            List<String> binlog = harness.writeBinlogUntil(splits, 3, Duration.ofSeconds(90));

            Map<Integer, JsonNode> byId = indexById(binlog);
            assertThat(byId).containsKeys(1, 2, 4);
            // insert
            assertThat(byId.get(4).get("name").asText()).isEqualTo("dave");
            assertThat(byId.get(4).get(Constants.DORIS_DELETE_SIGN).asInt()).isZero();
            // update carries the new value
            assertThat(byId.get(1).get("name").asText()).isEqualTo("alice2");
            assertThat(byId.get(1).get(Constants.DORIS_DELETE_SIGN).asInt()).isZero();
            // delete carries the delete sign
            assertThat(byId.get(2).get(Constants.DORIS_DELETE_SIGN).asInt()).isEqualTo(1);
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

    private Connection connect() throws Exception {
        return DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    }
}
