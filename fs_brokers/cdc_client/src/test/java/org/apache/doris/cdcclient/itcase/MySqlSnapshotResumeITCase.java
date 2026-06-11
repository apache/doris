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
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simulates a snapshot interrupted partway and resumed: with a small chunk size the table splits
 * into multiple chunks, and the from-to {@code writeRecords} path reads them as two separate windows
 * (a first batch, then the remaining batch from the persisted chunk boundaries). The union of both
 * windows must cover every row exactly once — no chunk re-read, no row skipped across the gap.
 */
@Testcontainers
class MySqlSnapshotResumeITCase {

    private static final String ROOT_USER = "root";
    private static final String ROOT_PASSWORD = "123456";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final AtomicLong JOB_ID_SEQ = new AtomicLong(950_000);
    private static final int ROW_COUNT = 20;

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
        database = "resume_snap_db_" + jobId;
        try (Connection conn = rootConnection("");
                Statement st = conn.createStatement()) {
            st.execute("CREATE DATABASE " + database);
            st.execute("USE " + database);
            st.execute("CREATE TABLE t_user (id INT NOT NULL, name VARCHAR(50), PRIMARY KEY (id))");
            StringBuilder values = new StringBuilder();
            for (int i = 1; i <= ROW_COUNT; i++) {
                if (i > 1) {
                    values.append(',');
                }
                values.append("(").append(i).append(",'name-").append(i).append("')");
            }
            st.execute("INSERT INTO t_user VALUES " + values);
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
    void resumedSnapshotReadsEveryChunkExactlyOnce() throws Exception {
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
                                        "initial",
                                        "doris_target_db",
                                        mock)
                                .withSplitSize(3)) {

            List<SnapshotSplit> splits = harness.fetchAllSnapshotSplits("t_user");
            // a small chunk size must yield several chunks, otherwise there is nothing to resume
            assertThat(splits.size()).isGreaterThan(1);

            // snapshot_parallelism=1: FE issues one chunk per read window, mirrored here
            int cut = splits.size() / 2;
            // first window: read the leading chunks (snapshot interrupted after these)
            for (int i = 0; i < cut; i++) {
                harness.writeSnapshot(Collections.singletonList(splits.get(i)));
            }
            int afterFirst = harness.loadedRecords().size();
            assertThat(afterFirst).isPositive();

            // resume: read the remaining chunks from their persisted boundaries
            for (int i = cut; i < splits.size(); i++) {
                harness.writeSnapshot(Collections.singletonList(splits.get(i)));
            }

            List<Integer> ids = ids(harness.loadedRecords());
            // every row read exactly once across the two windows
            assertThat(ids).hasSize(ROW_COUNT);
            assertThat(ids).doesNotHaveDuplicates();
            List<Integer> expected = new ArrayList<>();
            for (int i = 1; i <= ROW_COUNT; i++) {
                expected.add(i);
            }
            assertThat(ids).containsExactlyInAnyOrderElementsOf(expected);
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
