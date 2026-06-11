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
 * Verifies the from-to {@code writeRecords} path applies the configured server timezone when
 * deserializing MySQL temporal types: a TIMESTAMP (absolute instant, stored in UTC) read under two
 * different server timezones denotes the same instant, while a DATETIME (wall-clock, timezone-free)
 * reads identically regardless of the server timezone.
 */
@Testcontainers
class MySqlTimezoneITCase {

    private static final String ROOT_USER = "root";
    private static final String ROOT_PASSWORD = "123456";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final AtomicLong JOB_ID_SEQ = new AtomicLong(910_000);

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
    void timestampDenotesSameInstantAcrossServerTimezones() throws Exception {
        // '2023-06-15 12:00:00' inserted at session tz UTC -> stored instant 2023-06-15T12:00:00Z.
        String shanghai = readTimestampColumns("Asia/Shanghai");
        String berlin = readTimestampColumns("Europe/Berlin");

        JsonNode sh = MAPPER.readTree(shanghai);
        JsonNode be = MAPPER.readTree(berlin);

        // DATETIME is wall-clock: identical text regardless of the reader's server timezone.
        assertThat(sh.get("c_datetime").asText()).isEqualTo(be.get("c_datetime").asText());

        // TIMESTAMP is an absolute instant: both readings denote the same moment in time.
        java.time.Instant shInstant = parseInstant(sh.get("c_timestamp").asText());
        java.time.Instant beInstant = parseInstant(be.get("c_timestamp").asText());
        assertThat(shInstant).isEqualTo(beInstant);
        assertThat(shInstant).isEqualTo(java.time.Instant.parse("2023-06-15T12:00:00Z"));
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

    /** Accept both an ISO instant ("...Z"/offset) and a plain "yyyy-MM-dd HH:mm:ss" (taken as UTC). */
    private java.time.Instant parseInstant(String value) {
        String v = value.trim();
        try {
            return java.time.Instant.parse(v.contains(" ") ? v.replace(' ', 'T') + "Z" : v);
        } catch (Exception e) {
            return java.time.OffsetDateTime.parse(v).toInstant();
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
