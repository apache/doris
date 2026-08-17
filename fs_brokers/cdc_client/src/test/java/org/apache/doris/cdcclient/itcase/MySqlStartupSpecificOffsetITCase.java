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
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Verifies the specific-offset startup mode for MySQL: given a recorded binlog file/position, the
 * job replays from exactly that point forward — capturing changes made after the recorded position
 * (even ones that happened before the job started) while never re-reading anything before it.
 */
@Testcontainers
class MySqlStartupSpecificOffsetITCase {

    private static final String ROOT_USER = "root";
    private static final String ROOT_PASSWORD = "123456";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final AtomicLong JOB_ID_SEQ = new AtomicLong(970_000);

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
        database = "specoff_db_" + jobId;
        try (Connection conn = rootConnection("");
                Statement st = conn.createStatement()) {
            st.execute("CREATE DATABASE " + database);
            st.execute("USE " + database);
            st.execute("CREATE TABLE t_user (id INT PRIMARY KEY, name VARCHAR(50))");
            st.execute("INSERT INTO t_user VALUES (1,'alice'), (2,'bob')");
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
    void specificOffsetReplaysFromRecordedPositionForward() throws Exception {
        // Record the binlog position right after the pre-existing rows.
        String[] pos = currentBinlogPosition();
        String offset = String.format("{\"file\":\"%s\",\"pos\":\"%s\"}", pos[0], pos[1]);

        // This change happens after the recorded offset but before the job starts — it must be read.
        insert("INSERT INTO t_user VALUES (3,'carol')");

        try (MockDorisServer mock = new MockDorisServer();
                CdcClientWriteHarness harness =
                        CdcClientWriteHarness.mysql(
                                jobId,
                                MYSQL.getHost(),
                                MYSQL.getMappedPort(MySQLContainer.MYSQL_PORT),
                                ROOT_USER,
                                ROOT_PASSWORD,
                                database,
                                "t_user",
                                offset,
                                "doris_target_db",
                                mock)) {

            // First window replays from the recorded position, so row 3 (already in the binlog)
            // is read straight away rather than waited on as a fresh change.
            List<Integer> first = ids(harness.readBinlogFromStartupMode(1, Duration.ofSeconds(90)));
            assertThat(first).containsExactly(3);

            insert("INSERT INTO t_user VALUES (4,'dave')");
            List<Integer> second = ids(harness.continueBinlog(1, Duration.ofSeconds(90)));
            assertThat(second).containsExactly(4);

            // Replays from the recorded position forward: rows 3 and 4 only, never 1 or 2.
            List<Integer> all = ids(harness.loadedRecords());
            assertThat(all).doesNotContain(1, 2);
            assertThat(all).containsExactlyInAnyOrder(3, 4);
        }
    }

    private String[] currentBinlogPosition() throws Exception {
        try (Connection conn = rootConnection(database);
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SHOW MASTER STATUS")) {
            if (!rs.next()) {
                throw new IllegalStateException("SHOW MASTER STATUS returned no row");
            }
            return new String[] {rs.getString("File"), rs.getString("Position")};
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
        try (Connection conn = rootConnection(database);
                Statement st = conn.createStatement()) {
            st.execute(sql);
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
