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
 * Verifies the from-to {@code writeRecords} path deserializes the common MySQL column families
 * (signed/unsigned integers, decimal, floating point, string, enum/set, bit, json) correctly
 * through the snapshot phase. Date/time/year types are covered by {@link MySqlDateAdjusterITCase}.
 */
@Testcontainers
class MySqlWriteTypesITCase {

    private static final String ROOT_USER = "root";
    private static final String ROOT_PASSWORD = "123456";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final AtomicLong JOB_ID_SEQ = new AtomicLong(900_000);

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
        database = "types_db_" + jobId;
        try (Connection conn = rootConnection("");
                Statement st = conn.createStatement()) {
            st.execute("CREATE DATABASE " + database);
            st.execute("USE " + database);
            st.execute(
                    "CREATE TABLE t_types ("
                            + "id INT NOT NULL,"
                            + "c_tinyint TINYINT,"
                            + "c_smallint SMALLINT,"
                            + "c_bigint BIGINT,"
                            + "c_bigint_unsigned BIGINT UNSIGNED,"
                            + "c_decimal DECIMAL(10,2),"
                            + "c_float FLOAT,"
                            + "c_double DOUBLE,"
                            + "c_bool BOOLEAN,"
                            + "c_varchar VARCHAR(50),"
                            + "c_char CHAR(5),"
                            + "c_text TEXT,"
                            + "c_enum ENUM('a','b','c'),"
                            + "c_set SET('x','y','z'),"
                            + "c_bit BIT(8),"
                            + "c_json JSON,"
                            + "PRIMARY KEY (id))");
            st.execute(
                    "INSERT INTO t_types VALUES ("
                            + "1,"
                            + "-5,"
                            + "300,"
                            + "9223372036854775807,"
                            + "18446744073709551615,"
                            + "12.34,"
                            + "1.5,"
                            + "3.14159,"
                            + "true,"
                            + "'hello',"
                            + "'abc',"
                            + "'a long text value',"
                            + "'b',"
                            + "'x,z',"
                            + "b'00000101',"
                            + "'{\"k\": 1, \"arr\": [2, 3]}')");
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
    void snapshotDeserializesMySqlTypes() throws Exception {
        try (MockDorisServer mock = new MockDorisServer();
                CdcClientWriteHarness harness =
                        CdcClientWriteHarness.mysql(
                                jobId,
                                MYSQL.getHost(),
                                MYSQL.getMappedPort(MySQLContainer.MYSQL_PORT),
                                ROOT_USER,
                                ROOT_PASSWORD,
                                database,
                                "t_types",
                                "initial",
                                "doris_target_db",
                                mock)) {

            List<SnapshotSplit> splits = harness.fetchAllSnapshotSplits("t_types");
            harness.writeSnapshot(splits);

            assertThat(harness.loadedRecords()).hasSize(1);
            JsonNode row = MAPPER.readTree(harness.loadedRecords().get(0));

            assertThat(row.get("id").asInt()).isEqualTo(1);
            assertThat(row.get("c_tinyint").asInt()).isEqualTo(-5);
            assertThat(row.get("c_smallint").asInt()).isEqualTo(300);
            assertThat(row.get("c_bigint").asLong()).isEqualTo(9223372036854775807L);
            // bigint unsigned beyond Long range -> exact decimal string
            assertThat(row.get("c_bigint_unsigned").asText()).isEqualTo("18446744073709551615");
            assertThat(row.get("c_decimal").decimalValue().toPlainString()).isEqualTo("12.34");
            assertThat(row.get("c_float").asDouble()).isEqualTo(1.5);
            assertThat(row.get("c_double").asDouble()).isEqualTo(3.14159);
            assertThat(row.get("c_bool").asInt()).isEqualTo(1);
            assertThat(row.get("c_varchar").asText()).isEqualTo("hello");
            assertThat(row.get("c_char").asText()).isEqualTo("abc");
            assertThat(row.get("c_text").asText()).isEqualTo("a long text value");
            assertThat(row.get("c_enum").asText()).isEqualTo("b");
            assertThat(row.get("c_set").asText()).isEqualTo("x,z");
            assertThat(row.get("c_json").asText()).contains("\"k\"");
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
