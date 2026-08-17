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
 * End-to-end coverage of MySQL TIME full range [-838:59:59, 838:59:59] across the snapshot and
 * binlog phases. Out-of-range values (negative or >=24h) must be emitted as ±HH:MM:SS[.ffffff]
 * text rather than the raw long literal they fell back to before the convertToTime fix.
 */
@Testcontainers
class MySqlTimeRangeITCase {

    private static final String ROOT_USER = "root";
    private static final String ROOT_PASSWORD = "123456";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final AtomicLong JOB_ID_SEQ = new AtomicLong(810_000);

    @Container
    static final MySQLContainer<?> MYSQL =
            new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))
                    .withDatabaseName("cdc_test")
                    .withUsername("cdc")
                    .withPassword("123456")
                    .withEnv("MYSQL_ROOT_PASSWORD", ROOT_PASSWORD);

    private String jobId;
    private String database;

    @BeforeEach
    void setUp() throws Exception {
        jobId = String.valueOf(JOB_ID_SEQ.incrementAndGet());
        database = "time_db_" + jobId;
        try (Connection conn = rootConnection("");
                Statement st = conn.createStatement()) {
            st.execute("CREATE DATABASE " + database);
            st.execute("USE " + database);
            st.execute(
                    "CREATE TABLE t_time (id INT NOT NULL, t_col TIME(6), PRIMARY KEY (id))");
            // snapshot rows: in-range, negative lower bound, positive upper bound.
            // MySQL TIME bounds are exactly +/-838:59:59 (no fractional part allowed at the bound).
            st.execute(
                    "INSERT INTO t_time VALUES "
                            + "(1,'12:34:56.123456'), (2,'-838:59:59'), (3,'838:59:59')");
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
    void timeFullRangeInBothPhases() throws Exception {
        try (CdcClientReadHarness harness =
                CdcClientReadHarness.mysql(
                        jobId,
                        MYSQL.getHost(),
                        MYSQL.getMappedPort(MySQLContainer.MYSQL_PORT),
                        ROOT_USER,
                        ROOT_PASSWORD,
                        database,
                        "t_time",
                        "initial")) {

            List<SnapshotSplit> splits = harness.fetchAllSnapshotSplits("t_time");
            SnapshotResult snapshot = harness.readSnapshot(splits);
            Map<Integer, JsonNode> snap = indexById(snapshot.records());

            // snapshot phase: in-range keeps LocalTime text, out-of-range formats as ±HH:MM:SS
            assertThat(snap.get(1).get("t_col").asText()).isEqualTo("12:34:56.123456");
            assertThat(snap.get(2).get("t_col").asText()).isEqualTo("-838:59:59");
            assertThat(snap.get(3).get("t_col").asText()).isEqualTo("838:59:59");

            // binlog phase: negative >24h, and midnight (in-range) stay correct
            try (Connection conn = rootConnection(database);
                    Statement st = conn.createStatement()) {
                st.execute("INSERT INTO t_time VALUES (101,'-100:00:00.500000'), (102,'00:00:00')");
            }
            List<String> binlog = harness.readBinlogUntil(snapshot, splits, 2, Duration.ofSeconds(60));
            Map<Integer, JsonNode> bin = indexById(binlog);
            assertThat(bin.get(101).get("t_col").asText()).isEqualTo("-100:00:00.5");
            assertThat(bin.get(102).get("t_col").asText()).isEqualTo("00:00");
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
