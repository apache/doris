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
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Verifies the {@code latest} startup mode for MySQL: the job skips the snapshot entirely and reads
 * only binlog changes that happen after it starts. Pre-existing rows must never be stream-loaded.
 */
@Testcontainers
class MySqlStartupLatestITCase {

    private static final String ROOT_USER = "root";
    private static final String ROOT_PASSWORD = "123456";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final AtomicLong JOB_ID_SEQ = new AtomicLong(960_000);

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
        database = "latest_db_" + jobId;
        try (Connection conn = rootConnection("");
                Statement st = conn.createStatement()) {
            st.execute("CREATE DATABASE " + database);
            st.execute("USE " + database);
            st.execute("CREATE TABLE t_user (id INT PRIMARY KEY, name VARCHAR(50))");
            // pre-existing rows that latest mode must NOT read
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
    void latestSkipsSnapshotAndReadsOnlyNewChanges() throws Exception {
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
                                "latest",
                                "doris_target_db",
                                mock)) {

            // Resolve the latest binlog position now; rows 1 and 2 are already behind it.
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
