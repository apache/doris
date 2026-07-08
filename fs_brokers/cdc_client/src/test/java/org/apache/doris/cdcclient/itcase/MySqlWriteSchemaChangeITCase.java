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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Exercises MySQL schema-change recovery in the from-to write path: a task can execute Doris DDL
 * and then fail before commitOffset, so the next task must replay the old binlog from FE's old
 * schema baseline and tolerate the already-applied DDL.
 */
@Testcontainers
class MySqlWriteSchemaChangeITCase {

    private static final String ROOT_USER = "root";
    private static final String ROOT_PASSWORD = "123456";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final AtomicLong JOB_ID_SEQ = new AtomicLong(1_000_000);

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
        database = "mysql_sc_db_" + jobId;
        try (Connection conn = rootConnection("");
                Statement st = conn.createStatement()) {
            st.execute("CREATE DATABASE " + database);
            st.execute("USE " + database);
            st.execute("CREATE TABLE t_user (id INT NOT NULL, name VARCHAR(50), PRIMARY KEY (id))");
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
    void replaysOldBinlogWithOldSchemaAfterCommitOffsetFailure() throws Exception {
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
                                mock)) {

            List<SnapshotSplit> splits = harness.fetchAllSnapshotSplits("t_user");
            harness.writeSnapshot(splits);
            harness.enterBinlog(splits);
            String committedOffsetBeforeSchemaChange = harness.committedOffset();
            String tableSchemasBeforeSchemaChange = harness.committedTableSchemas();
            assertThat(tableSchemasBeforeSchemaChange).doesNotContain("city");

            try (Connection conn = rootConnection(database);
                    Statement st = conn.createStatement()) {
                st.execute("ALTER TABLE t_user ADD COLUMN city VARCHAR(50)");
                st.execute("INSERT INTO t_user (id, name, city) VALUES (3, 'carol', 'beijing')");
            }

            mock.failCommitOffset();
            assertThatThrownBy(() -> harness.continueBinlog(1, Duration.ofSeconds(90)))
                    .isInstanceOf(Exception.class);
            assertThat(mock.failedCommitOffsetCount()).isPositive();
            assertThat(harness.committedOffset()).isEqualTo(committedOffsetBeforeSchemaChange);
            assertThat(harness.committedTableSchemas()).isEqualTo(tableSchemasBeforeSchemaChange);

            mock.allowCommitOffset();
            harness.rebuildReaderOnNextWrite();
            List<String> retriedRecords = harness.continueBinlog(1, Duration.ofSeconds(90));

            assertThat(mock.executedDdls()).hasSize(2);
            assertThat(mock.executedDdls().get(0)).contains("ADD COLUMN").contains("city");
            assertThat(mock.executedDdls().get(1)).contains("ADD COLUMN").contains("city");
            assertThat(mock.ddlResponses().get(1)).contains("Can not add column which already exists");
            assertThat(retriedRecords).isNotEmpty();
            JsonNode row = MAPPER.readTree(retriedRecords.get(retriedRecords.size() - 1));
            assertThat(row.get("id").asInt()).isEqualTo(3);
            assertThat(row.get("city").asText()).isEqualTo("beijing");
            assertThat(harness.committedOffset()).isNotEqualTo(committedOffsetBeforeSchemaChange);
            assertThat(harness.committedTableSchemas()).contains("city");
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
