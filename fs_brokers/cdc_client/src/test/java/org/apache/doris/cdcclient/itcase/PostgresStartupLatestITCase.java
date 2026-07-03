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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Verifies the {@code latest} startup mode for PostgreSQL: the job skips the snapshot entirely and
 * reads only WAL changes that happen after it starts. Pre-existing rows must never be stream-loaded.
 */
@Testcontainers
class PostgresStartupLatestITCase {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final AtomicLong JOB_ID_SEQ = new AtomicLong(540_000);

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
            // pre-existing rows that latest mode must NOT read
            st.execute("INSERT INTO t_user VALUES (1,'alice'), (2,'bob')");
        }
    }

    @AfterEach
    void tearDown() {
        Env.getCurrentEnv().close(jobId);
    }

    @Test
    void latestSkipsSnapshotAndReadsOnlyNewChanges() throws Exception {
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
                                "latest",
                                "doris_target_db",
                                mock)) {

            // Resolve the latest WAL position now; rows 1 and 2 are already behind it.
            harness.enterBinlogFromStartupMode();

            insert("INSERT INTO t_user VALUES (3,'carol')");
            List<Integer> first = ids(harness.continueBinlog(1, Duration.ofSeconds(90)));
            assertThat(first).containsExactly(3);

            insert("INSERT INTO t_user VALUES (4,'dave')");
            List<Integer> second = ids(harness.continueBinlog(1, Duration.ofSeconds(90)));
            assertThat(second).containsExactly(4);

            // The whole run must never have touched the pre-existing snapshot rows.
            List<Integer> all = ids(harness.loadedRecords());
            assertThat(all).doesNotContain(1, 2);
            assertThat(all).containsExactlyInAnyOrder(3, 4);
        }
    }

    private List<Integer> ids(List<String> records) throws Exception {
        List<Integer> result = new ArrayList<>();
        for (String record : records) {
            JsonNode node = MAPPER.readTree(record);
            result.add(node.get("id").asInt());
        }
        return result;
    }

    private void insert(String sql) throws Exception {
        try (Connection conn = connect();
                Statement st = conn.createStatement()) {
            st.execute(sql);
        }
    }

    private Connection connect() throws Exception {
        return DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    }
}
