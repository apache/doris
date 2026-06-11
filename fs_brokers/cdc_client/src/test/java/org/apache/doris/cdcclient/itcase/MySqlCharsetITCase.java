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
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Verifies the from-to {@code writeRecords} path decodes per-column MySQL charsets correctly: a
 * single row carrying utf8mb4 (CJK + emoji), GBK (CJK) and latin1 (accented) columns must round-trip
 * to the exact original strings in the deserialized JSON.
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
            try (java.sql.PreparedStatement ps =
                    conn.prepareStatement("INSERT INTO t_charset VALUES (1, ?, ?, ?)")) {
                ps.setString(1, UTF8MB4_VALUE);
                ps.setString(2, GBK_VALUE);
                ps.setString(3, LATIN1_VALUE);
                ps.execute();
            }
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
    void snapshotDecodesPerColumnCharsets() throws Exception {
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

            assertThat(harness.loadedRecords()).hasSize(1);
            JsonNode row = MAPPER.readTree(harness.loadedRecords().get(0));

            assertThat(row.get("c_utf8mb4").asText()).isEqualTo(UTF8MB4_VALUE);
            assertThat(row.get("c_gbk").asText()).isEqualTo(GBK_VALUE);
            assertThat(row.get("c_latin1").asText()).isEqualTo(LATIN1_VALUE);
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
