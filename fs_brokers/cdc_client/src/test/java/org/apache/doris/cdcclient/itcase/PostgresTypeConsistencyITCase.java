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
import org.testcontainers.containers.PostgreSQLContainer;
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
 * PostgreSQL counterpart of {@link MySqlTypeConsistencyITCase}: the same value inserted before
 * capture (snapshot, JDBC path) and after (binlog, logical-decoding path) must deserialize
 * identically per column. JSON/JSONB are compared by parsed value to tolerate whitespace/key-order
 * differences while still catching a genuinely different value.
 */
@Testcontainers
class PostgresTypeConsistencyITCase {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final AtomicLong JOB_ID_SEQ = new AtomicLong(595_000);

    // Identical column tuple for both the snapshot row (id 1) and the binlog row (id 2).
    private static final String VALUES =
            "12345678901234.567890,"            // c_numeric NUMERIC(20,6)
                    + "1.5,"                    // c_real REAL
                    + "3.141592653589793,"      // c_double DOUBLE PRECISION
                    + "true,"                   // c_bool BOOLEAN
                    + "'hello',"                // c_text TEXT
                    + "'world',"                // c_varchar VARCHAR(20)
                    + "'abcde',"                // c_char CHAR(5)
                    + "'{\"k\": 1, \"a\": [2, 3]}',"  // c_json JSON
                    + "'{\"k\": 1, \"a\": [2, 3]}',"  // c_jsonb JSONB
                    + "B'10100101',"            // c_bit BIT(8)
                    + "B'101',"                 // c_varbit VARBIT(16)
                    + "'\\x0102030405',"        // c_bytea BYTEA
                    + "'11111111-1111-1111-1111-111111111111',"  // c_uuid UUID
                    + "'2023-08-15',"           // c_date DATE
                    + "'12:34:56.123456',"      // c_time TIME(6)
                    + "'12:34:56.123456+08',"   // c_timetz TIMETZ
                    + "'2023-08-15 12:34:56.123456',"     // c_timestamp TIMESTAMP(6)
                    + "'2023-08-15 12:34:56.123456+08',"  // c_timestamptz TIMESTAMPTZ
                    + "'1 day 02:03:04',"       // c_interval INTERVAL
                    + "'192.168.0.1',"          // c_inet INET
                    + "'{1,2,3}',"              // c_int_arr INT[]
                    + "'{a,b}',"                // c_text_arr TEXT[]
                    + "'12.34'";                // c_money MONEY

    @Container
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:14"))
                    .withCommand("postgres", "-c", "wal_level=logical");

    private String jobId;
    private String table;

    @BeforeEach
    void setUp() throws Exception {
        jobId = String.valueOf(JOB_ID_SEQ.incrementAndGet());
        table = "t_scan_" + jobId;
        try (Connection conn = connect();
                Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS " + table);
            st.execute(
                    "CREATE TABLE " + table + " ("
                            + "id INT PRIMARY KEY,"
                            + "c_numeric NUMERIC(20,6),"
                            + "c_real REAL,"
                            + "c_double DOUBLE PRECISION,"
                            + "c_bool BOOLEAN,"
                            + "c_text TEXT,"
                            + "c_varchar VARCHAR(20),"
                            + "c_char CHAR(5),"
                            + "c_json JSON,"
                            + "c_jsonb JSONB,"
                            + "c_bit BIT(8),"
                            + "c_varbit VARBIT(16),"
                            + "c_bytea BYTEA,"
                            + "c_uuid UUID,"
                            + "c_date DATE,"
                            + "c_time TIME(6),"
                            + "c_timetz TIMETZ,"
                            + "c_timestamp TIMESTAMP(6),"
                            + "c_timestamptz TIMESTAMPTZ,"
                            + "c_interval INTERVAL,"
                            + "c_inet INET,"
                            + "c_int_arr INT[],"
                            + "c_text_arr TEXT[],"
                            + "c_money MONEY)");
            st.execute("ALTER TABLE " + table + " REPLICA IDENTITY FULL");
            st.execute("INSERT INTO " + table + " VALUES (1," + VALUES + ")");
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        Env.getCurrentEnv().close(jobId);
        try (Connection conn = connect();
                Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test
    void snapshotAndBinlogDeserializeIdentically() throws Exception {
        try (CdcClientReadHarness harness =
                CdcClientReadHarness.postgres(
                        jobId,
                        POSTGRES.getHost(),
                        POSTGRES.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT),
                        POSTGRES.getUsername(),
                        POSTGRES.getPassword(),
                        POSTGRES.getDatabaseName(),
                        "public",
                        table,
                        "initial")) {

            List<SnapshotSplit> splits = harness.fetchAllSnapshotSplits(table);
            CdcClientReadHarness.SnapshotResult snapshot = harness.readSnapshot(splits);
            JsonNode snap = MAPPER.readTree(snapshot.records().get(0));

            try (Connection conn = connect();
                    Statement st = conn.createStatement()) {
                st.execute("INSERT INTO " + table + " VALUES (2," + VALUES + ")");
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
                JsonNode snapNode = snap.get(col);
                JsonNode binNode = bin.get(col);
                if (!columnsMatch(col, snapNode, binNode)) {
                    mismatches.add(col + ": snapshot=[" + snapNode + "] binlog=[" + binNode + "]");
                }
            }
            mismatches.forEach(m -> System.out.println("[PG TYPE SCAN][MISMATCH] " + m));
            assertThat(mismatches).as("snapshot vs binlog per-column mismatches").isEmpty();
        }
    }

    // Compare the parsed nodes directly so container columns (arrays such as c_int_arr/c_text_arr)
    // compare by content rather than collapsing to "" via JsonNode.asText(). The whitespace/key-order
    // tolerance is limited to the JSON/JSONB columns: a JSON value carried as a string can differ in
    // spacing/key order between the snapshot (JDBC) and binlog paths, so those are compared by parsed
    // value; every other column must match exactly so a real representation difference is never masked.
    private boolean columnsMatch(String col, JsonNode a, JsonNode b) {
        if (a == null || b == null) {
            return false;
        }
        if (a.equals(b)) {
            return true;
        }
        if (col.equals("c_json") || col.equals("c_jsonb")) {
            try {
                return MAPPER.readTree(a.asText()).equals(MAPPER.readTree(b.asText()));
            } catch (Exception e) {
                return false;
            }
        }
        return false;
    }

    private Connection connect() throws Exception {
        return DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    }
}
