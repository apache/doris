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

import java.sql.DriverManager
import java.sql.Types
import java.time.LocalDateTime

suite("test_flight_int96_timestamp_range", "arrow_flight_sql,external,hive,tvf,external_docker") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableHiveTest"))) {
        return
    }
    def host = context.config.otherConfigs.get("externalEnvIp")
    def port = context.config.otherConfigs.get("hive2HdfsPort")
    // External clusters can override the configured Flight port; use the running FE's endpoint.
    def frontend = jdbc_sql_return_maparray("SHOW FRONTENDS").find {
        it.IsMaster.toString().toBoolean() && it.Alive.toString().toBoolean()
    }
    assertNotNull(frontend)
    assertTrue(frontend.ArrowFlightSqlPort.toString().toInteger() > 0)
    def flightUrl = "jdbc:arrow-flight-sql://${frontend.Host}:${frontend.ArrowFlightSqlPort}" +
            "/catalog=${context.dbName}?useServerPrepStmts=false&useSSL=false&useEncryption=false"
    Class.forName("org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver")
    def query = { String file ->
        """SELECT * FROM HDFS(
           "uri" = "hdfs://${host}:${port}/user/doris/tvf_data/test_hdfs_parquet/group4/${file}",
           "hadoop.username" = "doris", "format" = "parquet") LIMIT 10"""
    }
    // Keep the configured Flight identity without the TLS-dependent connect() wrapper.
    DriverManager.getConnection(flightUrl, context.config.otherConfigs.get("extArrowFlightSqlUser"),
            context.config.otherConfigs.get("extArrowFlightSqlPassword")).withCloseable { flight ->
        flight.createStatement().withCloseable { statement ->
            // Nanosecond fractions in these fixtures truncate to valid DATETIME(6) values.
            for (def file : ["part-00000-570d8e52-652d-4892-8bdc-7fa5466ffa69.c000.snappy.parquet",
                              "part-00000-b945dfb5-9982-4f86-b903-dabef99caba1.c000.snappy.parquet",
                              "part-00000-721700d2-26d7-42a3-a8f9-b6601628ccd4.c000.snappy.parquet",
                              "part-00000-afeef968-a917-4d51-a652-e5a4214df453.c000.snappy.parquet"]) {
                logger.info("Read INT96 Flight fixture: ${file}")
                statement.executeQuery(query(file)).withCloseable { rows ->
                    assertTrue(rows.next())
                    def metadata = rows.getMetaData()
                    for (int column = 1; column <= metadata.getColumnCount(); ++column) {
                        if (metadata.getColumnType(column) == Types.TIMESTAMP) {
                            assertNotNull(rows.getObject(column, LocalDateTime.class))
                        }
                    }
                    assertEquals(LocalDateTime.parse("9999-12-31T23:59:59.999999"),
                            rows.getObject(metadata.getColumnCount(), LocalDateTime.class))
                    assertFalse(rows.next())
                }
            }
        }
        // Consume the result on the same discovered endpoint so zero-year values must fail over Flight.
        try {
            flight.createStatement().withCloseable { statement ->
                statement.executeQuery(query("int96_timestamps_nanos_outside_day_range.parquet"))
                        .withCloseable { rows ->
                    while (rows.next()) {
                        for (int column = 1; column <= rows.getMetaData().getColumnCount(); ++column) {
                            rows.getObject(column)
                        }
                    }
                }
            }
            assertTrue(false, "Expected an out-of-range Flight timestamp error")
        } catch (Exception error) {
            assertTrue(error.toString().contains("outside the supported 0001-9999 range"), error.toString())
        }
    }
}
