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
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Verifies the from-to {@code writeRecords} path deserializes PostgreSQL-specific column types
 * (array / uuid / jsonb / numeric / boolean) correctly through the snapshot phase.
 */
@Testcontainers
class PostgresWriteTypesITCase {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final AtomicLong JOB_ID_SEQ = new AtomicLong(600_000);

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
            st.execute("DROP TABLE IF EXISTS t_types");
            st.execute(
                    "CREATE TABLE t_types ("
                            + "id INT PRIMARY KEY,"
                            + "tags TEXT[],"
                            + "nums INT[],"
                            + "uid UUID,"
                            + "info JSONB,"
                            + "price NUMERIC(10,2),"
                            + "active BOOLEAN)");
            st.execute(
                    "INSERT INTO t_types VALUES ("
                            + "1,"
                            + "ARRAY['x','y'],"
                            + "ARRAY[1,2,3],"
                            + "'11111111-1111-1111-1111-111111111111',"
                            + "'{\"a\":1,\"b\":[2,3]}',"
                            + "12.34,"
                            + "true)");
        }
    }

    @AfterEach
    void tearDown() {
        Env.getCurrentEnv().close(jobId);
    }

    @Test
    void snapshotDeserializesPgTypes() throws Exception {
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
                                "t_types",
                                "initial",
                                "doris_target_db",
                                mock)) {

            List<SnapshotSplit> splits = harness.fetchAllSnapshotSplits("t_types");
            harness.writeSnapshot(splits);

            assertThat(harness.loadedRecords()).hasSize(1);
            JsonNode row = MAPPER.readTree(harness.loadedRecords().get(0));

            assertThat(row.get("id").asInt()).isEqualTo(1);
            // text[] / int[] -> JSON arrays
            assertThat(row.get("tags").isArray()).isTrue();
            assertThat(row.get("tags").get(0).asText()).isEqualTo("x");
            assertThat(row.get("tags").get(1).asText()).isEqualTo("y");
            assertThat(row.get("nums").get(2).asInt()).isEqualTo(3);
            // uuid -> string
            assertThat(row.get("uid").asText())
                    .isEqualTo("11111111-1111-1111-1111-111111111111");
            // numeric -> number
            assertThat(row.get("price").decimalValue().toPlainString()).isEqualTo("12.34");
            // boolean
            assertThat(row.get("active").asBoolean()).isTrue();
        }
    }

    private Connection connect() throws Exception {
        return DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    }
}
