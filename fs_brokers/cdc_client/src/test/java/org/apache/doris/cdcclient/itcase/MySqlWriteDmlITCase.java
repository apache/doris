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

import org.apache.doris.cdcclient.common.Constants;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Exercises the from-to {@code writeRecords} path for incremental (binlog) DML: insert / update /
 * delete after the snapshot must be stream-loaded with the correct values and delete sign.
 */
@Testcontainers
class MySqlWriteDmlITCase {

    private static final String ROOT_USER = "root";
    private static final String ROOT_PASSWORD = "123456";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final AtomicLong JOB_ID_SEQ = new AtomicLong(400_000);

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
        database = "dml_db_" + jobId;
        try (Connection conn = rootConnection("");
                Statement st = conn.createStatement()) {
            st.execute("CREATE DATABASE " + database);
            st.execute("USE " + database);
            st.execute("CREATE TABLE t_user (id INT NOT NULL, name VARCHAR(50), PRIMARY KEY (id))");
            st.execute("INSERT INTO t_user VALUES (1,'alice'), (2,'bob'), (3,'carol')");
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
    void binlogDmlStreamLoadsInsertUpdateDelete() throws Exception {
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

            // snapshot first (establishes the high-watermark to resume binlog from)
            List<SnapshotSplit> splits = harness.fetchAllSnapshotSplits("t_user");
            harness.writeSnapshot(splits);

            // three DML ops after the snapshot
            try (Connection conn = rootConnection(database);
                    Statement st = conn.createStatement()) {
                st.execute("INSERT INTO t_user VALUES (4, 'dave')");
                st.execute("UPDATE t_user SET name = 'alice2' WHERE id = 1");
                st.execute("DELETE FROM t_user WHERE id = 2");
            }

            List<String> binlog = harness.writeBinlogUntil(splits, 3, Duration.ofSeconds(60));

            Map<Integer, JsonNode> byId = indexById(binlog);
            assertThat(byId).containsKeys(1, 2, 4);
            // insert
            assertThat(byId.get(4).get("name").asText()).isEqualTo("dave");
            assertThat(byId.get(4).get(Constants.DORIS_DELETE_SIGN).asInt()).isZero();
            // update carries the new value
            assertThat(byId.get(1).get("name").asText()).isEqualTo("alice2");
            assertThat(byId.get(1).get(Constants.DORIS_DELETE_SIGN).asInt()).isZero();
            // delete carries the delete sign
            assertThat(byId.get(2).get(Constants.DORIS_DELETE_SIGN).asInt()).isEqualTo(1);
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
