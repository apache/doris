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
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Verifies MySQL temporal values round-trip through the from-to {@code writeRecords} path: a
 * TIMESTAMP and a DATETIME both keep their inserted wall-clock regardless of the configured server
 * timezone.
 *
 * <p>For TIMESTAMP this is non-trivial: Debezium reads it using the connection's server timezone
 * (producing a different UTC instant per zone — e.g. 04:00Z under Asia/Shanghai vs 10:00Z under
 * Europe/Berlin for the same stored 12:00), and the deserializer converts it back using the same
 * server timezone, so the two offsets cancel and the materialized wall-clock is stable. If those two
 * zones ever diverged (e.g. the deserializer's zone were lost and defaulted), the output would shift
 * and these exact-string assertions would fail — which is the property under test. DATETIME is
 * timezone-free to begin with.
 */
@Testcontainers
class MySqlTimezoneITCase {

    private static final String ROOT_USER = "root";
    private static final String ROOT_PASSWORD = "123456";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final AtomicLong JOB_ID_SEQ = new AtomicLong(910_000);

    /** The inserted wall-clock that must survive end to end under any server timezone. */
    private static final String EXPECTED_WALL_CLOCK = "2023-06-15 12:00:00.0";

    @Container
    static final MySQLContainer<?> MYSQL =
            new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))
                    .withDatabaseName("cdc_test")
                    .withUsername("cdc")
                    .withPassword("123456")
                    .withEnv("MYSQL_ROOT_PASSWORD", ROOT_PASSWORD)
                    // Fix the server's session timezone so the TIMESTAMP literal maps to a known UTC
                    // instant; the reader's own serverTimezone is what we vary per job.
                    .withEnv("TZ", "UTC");

    private final java.util.List<String> jobIds = new java.util.ArrayList<>();

    @AfterEach
    void tearDown() throws Exception {
        for (String id : jobIds) {
            Env.getCurrentEnv().close(id);
        }
    }

    @Test
    void temporalValuesRoundTripToInsertedWallClockAcrossServerTimezones() throws Exception {
        // '2023-06-15 12:00:00' inserted at session tz UTC, read back under two different reader
        // server timezones.
        JsonNode shanghai = MAPPER.readTree(readTimestampColumns("Asia/Shanghai"));
        JsonNode berlin = MAPPER.readTree(readTimestampColumns("Europe/Berlin"));

        // TIMESTAMP: read-zone and deserialize-zone cancel out, so the inserted wall-clock is
        // preserved identically under both server timezones. A broken cancellation would shift one
        // or both of these off 12:00:00.
        assertThat(shanghai.get("c_timestamp").asText()).isEqualTo(EXPECTED_WALL_CLOCK);
        assertThat(berlin.get("c_timestamp").asText()).isEqualTo(EXPECTED_WALL_CLOCK);

        // DATETIME: timezone-free wall-clock, unaffected by the server timezone.
        assertThat(shanghai.get("c_datetime").asText()).isEqualTo(EXPECTED_WALL_CLOCK);
        assertThat(berlin.get("c_datetime").asText()).isEqualTo(EXPECTED_WALL_CLOCK);
    }

    private String readTimestampColumns(String serverTimezone) throws Exception {
        String jobId = String.valueOf(JOB_ID_SEQ.incrementAndGet());
        jobIds.add(jobId);
        String database = "tz_db_" + jobId;
        try (Connection conn = rootConnection("");
                Statement st = conn.createStatement()) {
            st.execute("CREATE DATABASE " + database);
            st.execute("USE " + database);
            st.execute("SET time_zone = '+00:00'");
            st.execute(
                    "CREATE TABLE t_tz ("
                            + "id INT NOT NULL,"
                            + "c_timestamp TIMESTAMP,"
                            + "c_datetime DATETIME,"
                            + "PRIMARY KEY (id))");
            st.execute(
                    "INSERT INTO t_tz VALUES "
                            + "(1, '2023-06-15 12:00:00', '2023-06-15 12:00:00')");
        }
        try (MockDorisServer mock = new MockDorisServer();
                CdcClientWriteHarness harness =
                        CdcClientWriteHarness.mysql(
                                        jobId,
                                        MYSQL.getHost(),
                                        MYSQL.getMappedPort(MySQLContainer.MYSQL_PORT),
                                        ROOT_USER,
                                        ROOT_PASSWORD,
                                        database,
                                        "t_tz",
                                        "initial",
                                        "doris_target_db",
                                        mock)
                                .withServerTimezone(serverTimezone)) {

            List<SnapshotSplit> splits = harness.fetchAllSnapshotSplits("t_tz");
            harness.writeSnapshot(splits);
            assertThat(harness.loadedRecords()).hasSize(1);
            return harness.loadedRecords().get(0);
        } finally {
            try (Connection conn = rootConnection("");
                    Statement st = conn.createStatement()) {
                st.execute("DROP DATABASE IF EXISTS " + database);
            }
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
