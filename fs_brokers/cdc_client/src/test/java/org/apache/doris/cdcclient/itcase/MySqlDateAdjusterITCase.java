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
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Investigates MySQL date/year handling across the snapshot and binlog phases: whether genuinely
 * ancient 4-digit years are preserved, whether 2-digit-input years stay completed, and whether the
 * YEAR type is affected — for both phases.
 */
@Testcontainers
class MySqlDateAdjusterITCase {

    private static final String ROOT_USER = "root";
    private static final String ROOT_PASSWORD = "123456";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final AtomicLong JOB_ID_SEQ = new AtomicLong(800_000);

    @Container
    static final MySQLContainer<?> MYSQL =
            new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))
                    .withConfigurationOverride("docker/server-allow-ancient-date-time")
                    .withDatabaseName("cdc_test")
                    .withUsername("cdc")
                    .withPassword("123456")
                    .withEnv("MYSQL_ROOT_PASSWORD", ROOT_PASSWORD);

    private String jobId;
    private String database;

    @BeforeEach
    void setUp() throws Exception {
        jobId = String.valueOf(JOB_ID_SEQ.incrementAndGet());
        database = "date_db_" + jobId;
        try (Connection conn = rootConnection("");
                Statement st = conn.createStatement()) {
            st.execute("CREATE DATABASE " + database);
            st.execute("USE " + database);
            st.execute(
                    "CREATE TABLE t_dates ("
                            + "id INT NOT NULL,"
                            + "d_anc DATE,"      // ancient, 4-digit input
                            + "d_2d DATE,"       // 2-digit input -> MySQL completes
                            + "d_norm DATE,"     // normal
                            + "dt_anc DATETIME," // ancient datetime
                            + "yr YEAR,"
                            + "PRIMARY KEY (id))");
            // row read in the snapshot phase
            st.execute(
                    "INSERT INTO t_dates VALUES "
                            + "(1, '0099-01-01', '99-01-01', '2020-07-17', '0099-01-01 00:00:00', 1999)");
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
    void ancientYearsPreservedInBothPhases() throws Exception {
        try (CdcClientReadHarness harness =
                CdcClientReadHarness.mysql(
                        jobId,
                        MYSQL.getHost(),
                        MYSQL.getMappedPort(MySQLContainer.MYSQL_PORT),
                        ROOT_USER,
                        ROOT_PASSWORD,
                        database,
                        "t_dates",
                        "initial")) {

            List<SnapshotSplit> splits = harness.fetchAllSnapshotSplits("t_dates");
            SnapshotResult snapshot = harness.readSnapshot(splits);
            JsonNode snap = MAPPER.readTree(snapshot.records().get(0));
            System.out.println("[ADJUSTER] snapshot row = " + snap);

            // ancient row in the binlog phase
            insertAncient(2);
            List<String> binlog = harness.readBinlogUntil(snapshot, splits, 1, Duration.ofSeconds(60));
            JsonNode bin = MAPPER.readTree(binlog.get(0));
            System.out.println("[ADJUSTER] binlog row   = " + bin);

            // snapshot phase: faithful ancient years; 2-digit stays completed; normal unchanged
            assertThat(snap.get("d_anc").asText()).isEqualTo("0099-01-01");
            assertThat(snap.get("d_2d").asText()).isEqualTo("1999-01-01");
            assertThat(snap.get("d_norm").asText()).isEqualTo("2020-07-17");
            assertThat(snap.get("dt_anc").asText()).startsWith("0099-01-01 00:00:00");
            assertThat(snap.get("yr").asInt()).isEqualTo(1999);

            // binlog phase: faithful ancient years
            assertThat(bin.get("d_anc").asText()).isEqualTo("0017-08-12");
            assertThat(bin.get("dt_anc").asText()).startsWith("0017-08-12 00:00:00");
        }
    }

    private void insertAncient(int id) throws Exception {
        try (Connection conn = rootConnection(database);
                Statement st = conn.createStatement()) {
            st.execute(
                    String.format(
                            "INSERT INTO t_dates VALUES (%d, '0017-08-12', '70-01-01', '2021-01-01',"
                                    + " '0017-08-12 00:00:00', 2000)",
                            id));
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
