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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.MySQLContainer;
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
 * Version compatibility smoke test for the MySQL releases not exercised by the default ITCases
 * (which already cover 8.0): 5.7 and the 8.4 LTS. Runs the core read chain — snapshot full-load then
 * incremental insert/update/delete — to confirm the from-to {@code writeRecords} path works across
 * versions whose defaults (charset, sql_mode, binlog-off on 5.7) differ from 8.0.
 */
class MySqlVersionSmokeITCase {

    private static final String ROOT_USER = "root";
    private static final String ROOT_PASSWORD = "123456";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final AtomicLong JOB_ID_SEQ = new AtomicLong(920_000);

    @ParameterizedTest
    @ValueSource(strings = {"mysql:5.7", "mysql:8.4"})
    void snapshotAndDmlWorkAcrossVersions(String image) throws Exception {
        MySQLContainer<?> mysql =
                new MySQLContainer<>(
                                DockerImageName.parse(image).asCompatibleSubstituteFor("mysql"))
                        .withDatabaseName("cdc_test")
                        .withUsername("cdc")
                        .withPassword("123456")
                        .withEnv("MYSQL_ROOT_PASSWORD", ROOT_PASSWORD);
        // 5.7 ships with binlog disabled; 8.x enables row-based binlog by default.
        if (image.startsWith("mysql:5.7")) {
            mysql.withConfigurationOverride("docker/server-with-binlog");
        }

        String jobId = String.valueOf(JOB_ID_SEQ.incrementAndGet());
        String database = "smoke_db_" + jobId;
        try (MySQLContainer<?> container = mysql) {
            container.start();
            try (Connection conn = rootConnection(container, "");
                    Statement st = conn.createStatement()) {
                st.execute("CREATE DATABASE " + database);
                st.execute("USE " + database);
                st.execute(
                        "CREATE TABLE t_user ("
                                + "id INT NOT NULL,"
                                + "name VARCHAR(50),"
                                + "age INT,"
                                + "c_decimal DECIMAL(10,2),"
                                + "PRIMARY KEY (id))");
                st.execute("INSERT INTO t_user VALUES (1,'alice',30,12.34), (2,'bob',25,56.78)");
            }

            try (MockDorisServer mock = new MockDorisServer();
                    CdcClientWriteHarness harness =
                            CdcClientWriteHarness.mysql(
                                    jobId,
                                    container.getHost(),
                                    container.getMappedPort(MySQLContainer.MYSQL_PORT),
                                    ROOT_USER,
                                    ROOT_PASSWORD,
                                    database,
                                    "t_user",
                                    "initial",
                                    "doris_target_db",
                                    mock)) {

                List<SnapshotSplit> splits = harness.fetchAllSnapshotSplits("t_user");
                harness.writeSnapshot(splits);

                // snapshot full-load
                Map<Integer, JsonNode> snap = indexById(harness.loadedRecords());
                assertThat(snap).containsKeys(1, 2);
                assertThat(snap.get(1).get("name").asText()).isEqualTo("alice");
                assertThat(snap.get(1).get("age").asInt()).isEqualTo(30);
                assertThat(snap.get(1).get("c_decimal").decimalValue().toPlainString())
                        .isEqualTo("12.34");

                // incremental insert/update/delete
                try (Connection conn = rootConnection(container, database);
                        Statement st = conn.createStatement()) {
                    st.execute("INSERT INTO t_user VALUES (3,'carol',40,99.99)");
                    st.execute("UPDATE t_user SET age = 31 WHERE id = 1");
                    st.execute("DELETE FROM t_user WHERE id = 2");
                }

                List<String> binlog = harness.writeBinlogUntil(splits, 3, Duration.ofSeconds(90));
                Map<Integer, JsonNode> byId = indexById(binlog);
                assertThat(byId).containsKeys(1, 2, 3);
                assertThat(byId.get(3).get("name").asText()).isEqualTo("carol");
                assertThat(byId.get(3).get(Constants.DORIS_DELETE_SIGN).asInt()).isZero();
                assertThat(byId.get(1).get("age").asInt()).isEqualTo(31);
                assertThat(byId.get(2).get(Constants.DORIS_DELETE_SIGN).asInt()).isEqualTo(1);
            } finally {
                Env.getCurrentEnv().close(jobId);
            }
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

    private Connection rootConnection(MySQLContainer<?> container, String db) throws Exception {
        String url =
                "jdbc:mysql://"
                        + container.getHost()
                        + ":"
                        + container.getMappedPort(MySQLContainer.MYSQL_PORT)
                        + "/"
                        + db
                        + "?allowPublicKeyRetrieval=true&useSSL=false";
        return DriverManager.getConnection(url, ROOT_USER, ROOT_PASSWORD);
    }
}
