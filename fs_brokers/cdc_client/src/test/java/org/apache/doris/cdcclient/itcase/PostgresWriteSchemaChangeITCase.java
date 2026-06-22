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
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Exercises the from-to {@code writeRecords} path's schema-change handling for PostgreSQL: an ADD
 * COLUMN after the snapshot is detected from the WAL, the generated DDL is executed against the
 * (mock) FE, and the row carrying the new column is stream-loaded. This is behavior the TVF path
 * does not cover.
 */
@Testcontainers
class PostgresWriteSchemaChangeITCase {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final AtomicLong JOB_ID_SEQ = new AtomicLong(500_000);

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
            st.execute("INSERT INTO t_user VALUES (1,'alice'), (2,'bob')");
        }
    }

    @AfterEach
    void tearDown() {
        Env.getCurrentEnv().close(jobId);
    }

    @Test
    void addColumnIsDetectedExecutedAndDataStreamLoaded() throws Exception {
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
            // establish the pre-ALTER schema baseline before changing the table
            harness.enterBinlog(splits);

            try (Connection conn = connect();
                    Statement st = conn.createStatement()) {
                st.execute("ALTER TABLE t_user ADD COLUMN age INT");
                st.execute("INSERT INTO t_user (id, name, age) VALUES (3, 'carol', 30)");
            }

            List<String> binlog = harness.continueBinlog(1, Duration.ofSeconds(90));

            // the ADD COLUMN DDL was generated and executed against the (mock) FE
            assertThat(harness.loadedRecords()).isNotEmpty();
            assertThat(harness.executedDdls())
                    .anySatisfy(
                            ddl -> {
                                String upper = ddl.toUpperCase();
                                assertThat(upper).contains("ADD COLUMN");
                                assertThat(upper).contains("AGE");
                            });

            // the row carrying the new column was stream-loaded
            assertThat(binlog).isNotEmpty();
            JsonNode row = MAPPER.readTree(binlog.get(binlog.size() - 1));
            assertThat(row.get("id").asInt()).isEqualTo(3);
            assertThat(row.get("age").asInt()).isEqualTo(30);
        }
    }

    private Connection connect() throws Exception {
        return DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    }
}
