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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Regression test for PR #66559 — CDC SSL connection ordering fix.
 *
 * <p>Before the fix, {@code initializeEffectiveOffset()} was called <em>before</em> the SSL
 * properties were applied to the Debezium JDBC connection, causing a connection failure on MySQL
 * servers that enforce {@code require_secure_transport=ON}. The fix reorders SSL configuration
 * to happen before the offset resolution JDBC call.
 *
 * <p>This test guards the correct ordering by:
 * <ul>
 *   <li>Starting a MySQL container that enforces TLS via {@code require_secure_transport=ON}.</li>
 *   <li>Using the CDC harness with {@code .withSslMode("require")} which must succeed now that
 *       SSL is configured before offset resolution.</li>
 *   <li>Verifying data flows correctly through the binlog pipeline.</li>
 *   <li>Using a separate non-TLS container to confirm the fix does not break the default path.</li>
 * </ul>
 */
@Testcontainers
class MySqlStartupSslITCase {

    private static final String ROOT_USER = "root";
    private static final String ROOT_PASSWORD = "123456";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final AtomicLong JOB_ID_SEQ = new AtomicLong(990_000);

    /** Container that will enforce TLS after startup (see {@link #enableSecureTransport}). */
    @Container
    static final MySQLContainer<?> MYSQL_SSL =
            new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))
                    .withDatabaseName("cdc_test")
                    .withUsername("cdc")
                    .withPassword("123456")
                    .withEnv("MYSQL_ROOT_PASSWORD", ROOT_PASSWORD);

    /** Separate container WITHOUT TLS enforcement — used for the non-SSL control test (D6). */
    @Container
    static final MySQLContainer<?> MYSQL_PLAIN =
            new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))
                    .withDatabaseName("cdc_test")
                    .withUsername("cdc")
                    .withPassword("123456")
                    .withEnv("MYSQL_ROOT_PASSWORD", ROOT_PASSWORD);

    private String jobId;
    private String database;

    /**
     * Enable {@code require_secure_transport=ON} AFTER container startup so the Testcontainers
     * readiness probe (which connects without TLS) is not broken.
     */
    @BeforeAll
    static void enableSecureTransport() throws Exception {
        // Use execInContainer with the mysql CLI so we do not need a TLS JDBC connection yet.
        MYSQL_SSL.execInContainer(
                "mysql",
                "-uroot",
                "-p" + ROOT_PASSWORD,
                "-e",
                "SET GLOBAL require_secure_transport=ON;");
    }

    @BeforeEach
    void setUp() throws Exception {
        jobId = String.valueOf(JOB_ID_SEQ.incrementAndGet());
        database = "ssl_db_" + jobId;
        // Use execInContainer for DDL because the server now requires TLS.
        execSql(MYSQL_SSL,
                "CREATE DATABASE " + database + ";",
                "USE " + database + ";",
                "CREATE TABLE t_user (id INT PRIMARY KEY, name VARCHAR(50));",
                "INSERT INTO t_user VALUES (1,'alice'), (2,'bob');");
    }

    @AfterEach
    void tearDown() throws Exception {
        Env.getCurrentEnv().close(jobId);
        execSql(MYSQL_SSL, "DROP DATABASE IF EXISTS " + database + ";");
    }

    // -------------------------------------------------------------------------
    // offset=latest reaches initializeEffectiveOffset(), which is the call that
    // opened the un-encrypted JDBC connection before PR #66559.
    //
    // Only 'latest' is covered here. 'earliest' and the timestamp mode go through
    // the same initializeEffectiveOffset() path, so they add no extra coverage of
    // the ordering bug, and they need different assertions because they replay the
    // rows written during setUp() instead of skipping them.
    // -------------------------------------------------------------------------

    @Test
    void latestStartupModeSucceedsWithSecureTransport() throws Exception {
        try (MockDorisServer mock = new MockDorisServer();
                CdcClientWriteHarness harness =
                        CdcClientWriteHarness.mysql(
                                jobId,
                                MYSQL_SSL.getHost(),
                                MYSQL_SSL.getMappedPort(MySQLContainer.MYSQL_PORT),
                                ROOT_USER,
                                ROOT_PASSWORD,
                                database,
                                "t_user",
                                "latest",
                                "doris_target_db",
                                mock)
                        .withSslMode("require")) {

            // Resolves the latest binlog position over TLS. Before PR #66559 this threw,
            // because the SSL properties were attached to the config factory only after
            // this call had already opened its JDBC connection.
            harness.enterBinlogFromStartupMode();

            execSql(MYSQL_SSL,
                    "USE " + database + ";",
                    "INSERT INTO t_user VALUES (3,'carol');");

            List<Integer> streamed = ids(harness.continueBinlog(1, Duration.ofSeconds(90)));
            assertThat(streamed).containsExactly(3);

            // offset=latest must not replay the rows that existed before the job started.
            assertThat(ids(harness.loadedRecords())).doesNotContain(1, 2);
        }
    }

    // -------------------------------------------------------------------------
    // Control: non-TLS path still works (uses separate MYSQL_PLAIN container — D6 fix)
    // -------------------------------------------------------------------------

    @Test
    void nonSslPathStillWorksWithoutSecureTransport() throws Exception {
        String plainDb = "plain_db_" + jobId;
        try (Connection conn = plainRootConnection("");
                Statement st = conn.createStatement()) {
            st.execute("CREATE DATABASE " + plainDb);
            st.execute("USE " + plainDb);
            st.execute("CREATE TABLE t_user (id INT PRIMARY KEY, name VARCHAR(50))");
            st.execute("INSERT INTO t_user VALUES (1,'alice')");
        }

        try (MockDorisServer mock = new MockDorisServer();
                CdcClientWriteHarness harness =
                        CdcClientWriteHarness.mysql(
                                jobId,
                                MYSQL_PLAIN.getHost(),
                                MYSQL_PLAIN.getMappedPort(MySQLContainer.MYSQL_PORT),
                                ROOT_USER,
                                ROOT_PASSWORD,
                                plainDb,
                                "t_user",
                                "latest",
                                "doris_target_db",
                                mock)) {

            // No .withSslMode() — default (no TLS) path must still work.
            harness.enterBinlogFromStartupMode();

            try (Connection conn = plainRootConnection(plainDb);
                    Statement st = conn.createStatement()) {
                st.execute("INSERT INTO t_user VALUES (2,'bob')");
            }

            List<String> rows = harness.continueBinlog(1, Duration.ofSeconds(90));
            assertThat(ids(rows)).containsExactly(2);
        } finally {
            try (Connection conn = plainRootConnection("");
                    Statement st = conn.createStatement()) {
                st.execute("DROP DATABASE IF EXISTS " + plainDb);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Sanity: plaintext connection IS rejected by the TLS-enforcing container
    // -------------------------------------------------------------------------

    @Test
    void plaintextConnectionRejectedBySecureTransportServer() {
        // Attempt a plaintext JDBC connection to the TLS-enforcing container.
        String url = "jdbc:mysql://"
                + MYSQL_SSL.getHost()
                + ":"
                + MYSQL_SSL.getMappedPort(MySQLContainer.MYSQL_PORT)
                + "/cdc_test"
                + "?sslMode=DISABLED&allowPublicKeyRetrieval=true";
        assertThatThrownBy(() -> DriverManager.getConnection(url, ROOT_USER, ROOT_PASSWORD))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("secure transport");
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Execute SQL statements via {@code execInContainer} using the mysql CLI. This avoids needing
     * a TLS-capable JDBC connection for the SSL-enforcing container.
     */
    private static void execSql(MySQLContainer<?> container, String... statements)
            throws Exception {
        StringBuilder sb = new StringBuilder();
        for (String stmt : statements) {
            sb.append(stmt);
            if (!stmt.endsWith(";")) {
                sb.append(";");
            }
        }
        org.testcontainers.containers.Container.ExecResult result = container.execInContainer(
                "mysql",
                "-uroot",
                "-p" + ROOT_PASSWORD,
                "-e",
                sb.toString());
        if (result.getExitCode() != 0) {
            throw new RuntimeException(
                    "mysql exec failed (exit " + result.getExitCode() + "): " + result.getStderr());
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

    /** Plain JDBC connection to the non-TLS container (MYSQL_PLAIN). */
    private Connection plainRootConnection(String db) throws Exception {
        String url = "jdbc:mysql://"
                + MYSQL_PLAIN.getHost()
                + ":"
                + MYSQL_PLAIN.getMappedPort(MySQLContainer.MYSQL_PORT)
                + "/"
                + db;
        return DriverManager.getConnection(url, ROOT_USER, ROOT_PASSWORD);
    }
}
