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

import org.apache.doris.cdcclient.source.reader.mysql.MySqlSourceReader;
import org.apache.doris.job.cdc.DataSourceConfigKeys;

import com.mysql.cj.jdbc.result.ResultSetImpl;
import com.mysql.cj.protocol.a.result.ResultsetRowsCursor;
import com.mysql.cj.protocol.a.result.ResultsetRowsStatic;
import com.mysql.cj.protocol.a.result.ResultsetRowsStreaming;

import org.apache.flink.cdc.connectors.mysql.debezium.DebeziumUtils;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.utils.StatementUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;

import io.debezium.connector.mysql.MySqlConnection;

@Testcontainers
class MySqlSnapshotStreamingITCase {
    @Container
    static final MySQLContainer<?> MYSQL =
            new MySQLContainer<>("mysql:8.0")
                    .withDatabaseName("cdc_test")
                    .withUsername("cdc")
                    .withPassword("123456");

    @ParameterizedTest
    @ValueSource(strings = {"", "useCursorFetch=true", "useServerPrepStmts=true", "defaultFetchSize=4096"})
    void snapshotStreamsRowsAndReleasesConnection(String jdbcOptions) throws Exception {
        String jdbcUrl = MYSQL.getJdbcUrl() + "?" + jdbcOptions;
        try (Connection connection =
                        DriverManager.getConnection(
                                jdbcUrl, MYSQL.getUsername(), MYSQL.getPassword());
                Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS t_snapshot_streaming");
            statement.execute(
                    "CREATE TABLE t_snapshot_streaming (id INT PRIMARY KEY, payload LONGBLOB)");
            for (int id = 0; id < 16; id++) {
                statement.execute(
                        "INSERT INTO t_snapshot_streaming VALUES ("
                                + id
                                + ", REPEAT('x', 262144))");
            }
        }

        Method method =
                MySqlSourceReader.class.getDeclaredMethod(
                        "generateMySqlConfig", Map.class, String.class, int.class);
        method.setAccessible(true);
        MySqlSourceConfig config =
                (MySqlSourceConfig)
                        method.invoke(
                                new MySqlSourceReader(),
                                Map.of(
                                        DataSourceConfigKeys.JDBC_URL,
                                        jdbcUrl,
                                        DataSourceConfigKeys.USER,
                                        MYSQL.getUsername(),
                                        DataSourceConfigKeys.PASSWORD,
                                        MYSQL.getPassword(),
                                        DataSourceConfigKeys.DATABASE,
                                        MYSQL.getDatabaseName(),
                                        DataSourceConfigKeys.TABLE,
                                        "t_snapshot_streaming",
                                        DataSourceConfigKeys.OFFSET,
                                        "initial"),
                                "snapshot-streaming",
                                0);

        try (MySqlConnection jdbc = DebeziumUtils.createMySqlConnection(config)) {
            // Use the same statement builder as snapshot splits, including an unbounded split.
            for (int fetchSize : new int[] {1024, config.getFetchSize()}) {
                for (boolean closeEarly : new boolean[] {true, false}) {
                    try (PreparedStatement statement =
                                    StatementUtils.readTableSplitDataStatement(
                                            jdbc,
                                            "SELECT * FROM cdc_test.t_snapshot_streaming ORDER BY"
                                                + " id",
                                            true,
                                            true,
                                            null,
                                            null,
                                            1,
                                            fetchSize);
                            ResultSet rows = statement.executeQuery()) {
                        assertThat(statement.getFetchSize()).isEqualTo(fetchSize);
                        assertThat(rows.getType()).isEqualTo(ResultSet.TYPE_FORWARD_ONLY);
                        assertThat(rows.getConcurrency()).isEqualTo(ResultSet.CONCUR_READ_ONLY);
                        var driverRows = rows.unwrap(ResultSetImpl.class).getRows();
                        if (fetchSize == Integer.MIN_VALUE) {
                            assertThat(driverRows).isInstanceOf(ResultsetRowsStreaming.class);
                        } else if (jdbcOptions.equals("useCursorFetch=true")) {
                            assertThat(driverRows).isInstanceOf(ResultsetRowsCursor.class);
                        } else {
                            assertThat(driverRows).isInstanceOf(ResultsetRowsStatic.class);
                            // All rows are already buffered before the first ResultSet.next().
                            assertThat(driverRows.size()).isEqualTo(16);
                        }
                        int count = 0;
                        while (rows.next()) {
                            assertThat(rows.getInt(1)).isEqualTo(count);
                            assertThat(rows.getBytes(2)).hasSize(262144);
                            count++;
                            if (closeEarly) {
                                break;
                            }
                        }
                        assertThat(count).isEqualTo(closeEarly ? 1 : 16);
                    }
                    try (Statement statement = jdbc.connection().createStatement();
                            ResultSet result = statement.executeQuery("SELECT 1")) {
                        assertThat(result.next()).isTrue();
                        assertThat(result.getInt(1)).isEqualTo(1);
                    }
                }
            }
        }
    }
}
