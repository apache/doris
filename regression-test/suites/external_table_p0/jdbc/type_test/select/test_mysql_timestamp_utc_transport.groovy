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

suite("test_mysql_timestamp_utc_transport", "p0,external") {
    if (context.config.otherConfigs.get("enableJdbcTest") != "true") {
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    String port = context.config.otherConfigs.get("mysql_57_port")
    String bucket = getS3BucketName()
    String s3_endpoint = getS3Endpoint()
    def originalZone = sql("SELECT @@time_zone")[0][0]
    def originalCastPushdown = sql("SHOW VARIABLES LIKE 'enable_jdbc_cast_predicate_push_down'")[0][1]
    try {
        for (def driver : [["modern", "mysql-connector-j-8.4.0.jar", "com.mysql.cj.jdbc.Driver"],
                            ["legacy", "mysql-connector-java-5.1.49.jar", "com.mysql.jdbc.Driver"]]) {
            for (def prepared : [false, true]) {
                String catalog = "mysql_timestamp_utc_${driver[0]}_${prepared}"
                sql "DROP CATALOG IF EXISTS ${catalog}"
                sql """CREATE CATALOG ${catalog} PROPERTIES (
                    "type"="jdbc", "user"="root", "password"="123456",
                    "jdbc_url"="jdbc:mysql://${host}:${port}/doris_test?useSSL=false&zeroDateTimeBehavior=convertToNull&useInformationSchema=true&useTimezone=true&serverTimezone=Asia/Shanghai&useServerPrepStmts=${prepared}",
                    "driver_url"="https://${bucket}.${s3_endpoint}/regression/jdbc_driver/${driver[1]}",
                    "driver_class"="${driver[2]}"
                )"""
                def executeRemote = { statement ->
                    sql("CALL EXECUTE_STMT('${catalog}', '" + statement.replace("'", "''") + "')")
                }
                executeRemote("DROP TABLE IF EXISTS timestamp_utc_transport")
                executeRemote("CREATE TABLE timestamp_utc_transport " +
                        "(id INT, event_time TIMESTAMP(6) NULL, wall_text VARCHAR(32))")
                // FROM_UNIXTIME also handles a SYSTEM session zone without requiring MySQL timezone tables.
                executeRemote("INSERT INTO timestamp_utc_transport VALUES " +
                        "(1, FROM_UNIXTIME(1577923200.123456), " +
                        "'2020-01-02 04:00:00'), " +
                        "(2, FROM_UNIXTIME(1577944800.000001), " +
                        "'2020-01-02 04:00:00'), " +
                        "(3, NULL, NULL)")
                executeRemote("DROP TABLE IF EXISTS zero_timestamp_transport")
                executeRemote("CREATE TABLE zero_timestamp_transport (id INT, event_time TIMESTAMP(6) NULL)")
                // IGNORE creates a real zero TIMESTAMP even when the fixture enables strict SQL mode.
                executeRemote("INSERT IGNORE INTO zero_timestamp_transport VALUES " +
                        "(1, '0000-00-00 00:00:00'), (2, NULL)")
                "qt_zero_${driver[0]}_${prepared}" "SELECT id, CAST(event_time AS STRING) " +
                        "FROM ${catalog}.doris_test.zero_timestamp_transport ORDER BY id"
                def suffixes = ["; -- trailing comment", "; /* trailing comment */"]
                suffixes.eachWithIndex { suffix, index ->
                    "qt_zero_query_${driver[0]}_${prepared}_${index}" """SELECT id, CAST(event_time AS STRING)
                        FROM query("catalog"="${catalog}",
                        "query"="SELECT id, event_time FROM zero_timestamp_transport${suffix}")
                        ORDER BY id"""
                }
                for (def zone : ["UTC", "Asia/Shanghai"]) {
                    sql "SET time_zone='${zone}'"
                    String tag = "${driver[0]}_${prepared}_${zone.replace('/', '_')}"
                    "order_qt_instant_${tag}" """SELECT id, CAST(event_time AS STRING),
                        unix_timestamp(event_time) FROM ${catalog}.doris_test.timestamp_utc_transport"""
                    // Query metadata retains fractional precision even with the legacy driver.
                    "order_qt_query_instant_${tag}" """SELECT id, CAST(event_time AS STRING),
                        unix_timestamp(event_time) FROM query("catalog"="${catalog}",
                        "query"="SELECT id, event_time FROM timestamp_utc_transport")"""
                    String comparison = """SELECT id FROM ${catalog}.doris_test.timestamp_utc_transport
                        WHERE CAST(event_time AS DATETIMEV2(6)) > CAST(wall_text AS DATETIMEV2(6))
                        ORDER BY id LIMIT 2"""
                    sql "SET enable_jdbc_cast_predicate_push_down=false"
                    def localRows = sql(comparison)
                    sql "SET enable_jdbc_cast_predicate_push_down=true"
                    // Remote filtering must not discard rows that satisfy the session-zone casts.
                    assertEquals(localRows, sql(comparison))
                    "qt_cast_columns_${tag}" comparison
                }
            }
        }
    } finally {
        sql "SET time_zone='${originalZone}'"
        sql "SET enable_jdbc_cast_predicate_push_down=${originalCastPushdown}"
    }
}
