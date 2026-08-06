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
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Verifies the from-to {@code writeRecords} path decodes per-column MySQL charsets correctly on
 * <em>both</em> read paths: a row carrying utf8mb4 (CJK + emoji), GBK (CJK) and latin1 (accented)
 * columns must round-trip to the exact original strings through the JDBC snapshot scan and again
 * through binlog incremental decoding (a separate code path that can mis-handle charsets
 * independently of the snapshot).
 */
@Testcontainers
class MySqlCharsetITCase {

    private static final String ROOT_USER = "root";
    private static final String ROOT_PASSWORD = "123456";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final AtomicLong JOB_ID_SEQ = new AtomicLong(980_000);

    private static final String UTF8MB4_VALUE = "测试数据😀";
    private static final String GBK_VALUE = "另一个测试数据";
    private static final String LATIN1_VALUE = "ÀÆÉ Üæû";

    // Distinct values inserted during the binlog phase.
    private static final String UTF8MB4_INCR = "增量数据🚀";
    private static final String GBK_INCR = "增量中文";
    private static final String LATIN1_INCR = "Çédille Ñ";

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
        database = "charset_db_" + jobId;
        try (Connection conn = rootConnection("");
                Statement st = conn.createStatement()) {
            st.execute("CREATE DATABASE " + database);
            st.execute("USE " + database);
            st.execute(
                    "CREATE TABLE t_charset ("
                            + "id INT NOT NULL,"
                            + "c_utf8mb4 VARCHAR(50) CHARACTER SET utf8mb4,"
                            + "c_gbk VARCHAR(50) CHARACTER SET gbk,"
                            + "c_latin1 VARCHAR(50) CHARACTER SET latin1,"
                            + "PRIMARY KEY (id))");
            insertRow(conn, 1, UTF8MB4_VALUE, GBK_VALUE, LATIN1_VALUE);
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
    void snapshotAndBinlogDecodePerColumnCharsets() throws Exception {
        try (MockDorisServer mock = new MockDorisServer();
                CdcClientWriteHarness harness =
                        CdcClientWriteHarness.mysql(
                                jobId,
                                MYSQL.getHost(),
                                MYSQL.getMappedPort(MySQLContainer.MYSQL_PORT),
                                ROOT_USER,
                                ROOT_PASSWORD,
                                database,
                                "t_charset",
                                "initial",
                                "doris_target_db",
                                mock)) {

            List<SnapshotSplit> splits = harness.fetchAllSnapshotSplits("t_charset");
            harness.writeSnapshot(splits);

            // snapshot path (JDBC scan)
            assertThat(harness.loadedRecords()).hasSize(1);
            JsonNode snap = MAPPER.readTree(harness.loadedRecords().get(0));
            assertThat(snap.get("c_utf8mb4").asText()).isEqualTo(UTF8MB4_VALUE);
            assertThat(snap.get("c_gbk").asText()).isEqualTo(GBK_VALUE);
            assertThat(snap.get("c_latin1").asText()).isEqualTo(LATIN1_VALUE);

            // binlog path (event decoding) — a distinct row inserted after the snapshot
            try (Connection conn = rootConnection(database)) {
                insertRow(conn, 2, UTF8MB4_INCR, GBK_INCR, LATIN1_INCR);
            }
            List<String> binlog = harness.writeBinlogUntil(splits, 1, Duration.ofSeconds(90));
            assertThat(binlog).hasSize(1);
            JsonNode incr = MAPPER.readTree(binlog.get(0));
            assertThat(incr.get("id").asInt()).isEqualTo(2);
            assertThat(incr.get("c_utf8mb4").asText()).isEqualTo(UTF8MB4_INCR);
            assertThat(incr.get("c_gbk").asText()).isEqualTo(GBK_INCR);
            assertThat(incr.get("c_latin1").asText()).isEqualTo(LATIN1_INCR);
        }
    }

    private void insertRow(Connection conn, int id, String utf8mb4, String gbk, String latin1)
            throws Exception {
        try (PreparedStatement ps =
                conn.prepareStatement("INSERT INTO t_charset VALUES (?, ?, ?, ?)")) {
            ps.setInt(1, id);
            ps.setString(2, utf8mb4);
            ps.setString(3, gbk);
            ps.setString(4, latin1);
            ps.execute();
        }
    }

    private Connection rootConnection(String db) throws Exception {
        String url =
                "jdbc:mysql://"
                        + MYSQL.getHost()
                        + ":"
                        + MYSQL.getMappedPort(MySQLContainer.MYSQL_PORT)
                        + "/"
                        + db
                        + "?characterEncoding=UTF-8";
        return DriverManager.getConnection(url, ROOT_USER, ROOT_PASSWORD);
    }
}
