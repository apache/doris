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
import org.apache.doris.cdcclient.itcase.CdcClientReadHarness.SnapshotResult;
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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Guards that the snapshot phase (JDBC) and the binlog phase (Debezium decoding) — two different
 * converter paths — deserialize the same MySQL value identically. The identical value is inserted
 * once before capture (snapshot, id 1) and once after (binlog, id 2); every column must match
 * across phases. Any difference is a real bug (the MySQL TIME out-of-range issue was one instance).
 *
 * <p>JSON is compared by parsed value, not text: MySQL returns snapshot JSON with key reordering and
 * spaces ({@code {"a": [2, 3], "k": 1}}) while the binlog path emits compact JSON
 * ({@code {"a":[2,3],"k":1}}) — same value, different text, so a semantic comparison is used.
 */
@Testcontainers
class MySqlTypeConsistencyITCase {

    private static final String ROOT_USER = "root";
    private static final String ROOT_PASSWORD = "123456";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final AtomicLong JOB_ID_SEQ = new AtomicLong(821_000);

    // Identical column tuple used for both the snapshot row (id 1) and the binlog row (id 2).
    private static final String VALUES =
            "200,"                              // c_tinyint_u TINYINT UNSIGNED
                    + "4000000000,"             // c_int_u INT UNSIGNED
                    + "18446744073709551615,"   // c_bigint_u BIGINT UNSIGNED
                    + "12345678901234.567890,"  // c_decimal DECIMAL(20,6)
                    + "1.5,"                    // c_float FLOAT
                    + "3.141592653589793,"      // c_double DOUBLE
                    + "1,"                      // c_bool TINYINT(1)
                    + "'b',"                    // c_enum
                    + "'x,z',"                  // c_set
                    + "b'10100101',"            // c_bit BIT(8)
                    + "'{\"k\": 1, \"a\": [2, 3]}'," // c_json
                    + "'2023-08-15 12:34:56.123456',"  // c_datetime DATETIME(6)
                    + "'2023-08-15 12:34:56.123456',"  // c_timestamp TIMESTAMP(6)
                    + "'2023-08-15',"           // c_date DATE
                    + "'12:34:56.123456',"      // c_time TIME(6)
                    + "2023,"                   // c_year YEAR
                    + "0x0102030405,"           // c_varbinary VARBINARY(8)
                    + "'abcde'";                // c_char CHAR(5)

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
        database = "typescan_db_" + jobId;
        try (Connection conn = rootConnection("");
                Statement st = conn.createStatement()) {
            st.execute("CREATE DATABASE " + database);
            st.execute("USE " + database);
            st.execute(
                    "CREATE TABLE t_scan ("
                            + "id INT NOT NULL,"
                            + "c_tinyint_u TINYINT UNSIGNED,"
                            + "c_int_u INT UNSIGNED,"
                            + "c_bigint_u BIGINT UNSIGNED,"
                            + "c_decimal DECIMAL(20,6),"
                            + "c_float FLOAT,"
                            + "c_double DOUBLE,"
                            + "c_bool TINYINT(1),"
                            + "c_enum ENUM('a','b','c'),"
                            + "c_set SET('x','y','z'),"
                            + "c_bit BIT(8),"
                            + "c_json JSON,"
                            + "c_datetime DATETIME(6),"
                            + "c_timestamp TIMESTAMP(6) NULL,"
                            + "c_date DATE,"
                            + "c_time TIME(6),"
                            + "c_year YEAR,"
                            + "c_varbinary VARBINARY(8),"
                            + "c_char CHAR(5),"
                            + "PRIMARY KEY (id))");
            st.execute("INSERT INTO t_scan VALUES (1," + VALUES + ")");
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
    void snapshotAndBinlogDeserializeIdentically() throws Exception {
        try (CdcClientReadHarness harness =
                CdcClientReadHarness.mysql(
                        jobId,
                        MYSQL.getHost(),
                        MYSQL.getMappedPort(MySQLContainer.MYSQL_PORT),
                        ROOT_USER,
                        ROOT_PASSWORD,
                        database,
                        "t_scan",
                        "initial")) {

            List<SnapshotSplit> splits = harness.fetchAllSnapshotSplits("t_scan");
            SnapshotResult snapshot = harness.readSnapshot(splits);
            JsonNode snap = MAPPER.readTree(snapshot.records().get(0));

            // identical value inserted after capture starts -> binlog path
            try (Connection conn = rootConnection(database);
                    Statement st = conn.createStatement()) {
                st.execute("INSERT INTO t_scan VALUES (2," + VALUES + ")");
            }
            List<String> binlog = harness.readBinlogUntil(snapshot, splits, 1, Duration.ofSeconds(60));
            JsonNode bin = MAPPER.readTree(binlog.get(0));

            List<String> mismatches = new ArrayList<>();
            Iterator<String> fields = snap.fieldNames();
            while (fields.hasNext()) {
                String col = fields.next();
                if (col.equals("id") || col.startsWith("__DORIS")) {
                    continue;
                }
                String snapVal = snap.get(col).asText();
                JsonNode binNode = bin.get(col);
                String binVal = binNode == null ? "<missing>" : binNode.asText();
                if (!equalOrJsonEquivalent(snapVal, binVal)) {
                    mismatches.add(col + ": snapshot=[" + snapVal + "] binlog=[" + binVal + "]");
                }
            }
            mismatches.forEach(m -> System.out.println("[TYPE SCAN][MISMATCH] " + m));
            assertThat(mismatches).as("snapshot vs binlog per-column mismatches").isEmpty();
        }
    }

    // Text equality, falling back to parsed-JSON equality so JSON columns that differ only in
    // whitespace/key-order are treated as equal while a genuinely different value still fails.
    private boolean equalOrJsonEquivalent(String a, String b) {
        if (a.equals(b)) {
            return true;
        }
        try {
            return MAPPER.readTree(a).equals(MAPPER.readTree(b));
        } catch (Exception e) {
            return false;
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
