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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Guards that the MySQL TIME out-of-range fix (DebeziumJsonDeserializer.convertToTime) does not
 * regress PostgreSQL time handling. PG time values share the same MicroTime path: in-range values
 * must stay byte-for-byte on the unchanged LocalTime branch, and the PG-legal boundary '24:00:00'
 * is now formatted as text instead of falling back to the raw long.
 */
@Testcontainers
class PostgresTimeRangeITCase {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final AtomicLong JOB_ID_SEQ = new AtomicLong(585_000);

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
            st.execute("DROP TABLE IF EXISTS t_time");
            st.execute("CREATE TABLE t_time (id INT PRIMARY KEY, t_col TIME(6))");
            st.execute("ALTER TABLE t_time REPLICA IDENTITY FULL");
            // id 1: ordinary in-range value -- the fix must leave it byte-for-byte unchanged.
            // id 2: PG-legal upper boundary 24:00:00 -- a raw-long fallback before the fix.
            st.execute("INSERT INTO t_time VALUES (1,'12:34:56.123456'), (2,'24:00:00')");
        }
    }

    @AfterEach
    void tearDown() {
        Env.getCurrentEnv().close(jobId);
    }

    @Test
    void pgTimeUnaffectedByMysqlTimeFix() throws Exception {
        try (CdcClientReadHarness harness =
                CdcClientReadHarness.postgres(
                        jobId,
                        POSTGRES.getHost(),
                        POSTGRES.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT),
                        POSTGRES.getUsername(),
                        POSTGRES.getPassword(),
                        POSTGRES.getDatabaseName(),
                        "public",
                        "t_time",
                        "initial")) {

            List<SnapshotSplit> splits = harness.fetchAllSnapshotSplits("t_time");
            CdcClientReadHarness.SnapshotResult snapshot = harness.readSnapshot(splits);
            Map<Integer, JsonNode> snap = indexById(snapshot.records());

            // in-range ordinary value is unchanged (the PG common case)
            assertThat(snap.get(1).get("t_col").asText()).isEqualTo("12:34:56.123456");
            // PG-legal 24:00:00 is now formatted instead of emitted as a raw long
            assertThat(snap.get(2).get("t_col").asText()).isEqualTo("24:00:00");
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
