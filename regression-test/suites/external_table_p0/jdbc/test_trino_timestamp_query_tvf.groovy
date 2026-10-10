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

suite("test_trino_timestamp_query_tvf", "p0,external") {
    if (context.config.otherConfigs.get("enableTrinoJdbcTest") != "true") {
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    String port = context.config.otherConfigs.get("trino_jdbc_port")
    String driver = context.config.otherConfigs.get("trinoJdbcDriverUrl") ?:
            "https://repo.maven.apache.org/maven2/io/trino/trino-jdbc/435/trino-jdbc-435.jar"
    String catalog = "trino_timestamp_query_tvf"
    sql "DROP CATALOG IF EXISTS ${catalog}"
    sql """CREATE CATALOG ${catalog} PROPERTIES (
        "type"="jdbc", "user"="test", "password"="",
        "jdbc_url"="jdbc:trino://${host}:${port}",
        "driver_url"="${driver}", "driver_class"="io.trino.jdbc.TrinoDriver")"""
    def originalZone = sql("SELECT @@time_zone")[0][0]
    try {
        def utcEpoch = null
        for (def zone : ["UTC", "Asia/Shanghai"]) {
            sql "SET time_zone='${zone}'"
            for (def cte : [false, true]) {
                String value = "SELECT TIMESTAMP '2020-01-02 08:00:00.123456 +08:00' AS ts"
                String body = cte ? "WITH q AS (${value}) SELECT ts FROM q" : value
                String remote = "WITH SESSION query_max_execution_time='2h' ${body}"
                def epoch = sql("""SELECT unix_timestamp(ts)
                    FROM query("catalog"="${catalog}", "query"="${remote}")""")[0][0]
                if (utcEpoch == null) {
                    utcEpoch = epoch
                } else {
                    assertEquals(utcEpoch, epoch)
                }
                // Session properties must remain at statement scope when the instant projection is added.
                "qt_session_${zone.replace('/', '_')}_${cte}" """SELECT CAST(ts AS STRING), unix_timestamp(ts)
                    FROM query("catalog"="${catalog}", "query"="${remote}")"""
            }
        }
    } finally {
        sql "SET time_zone='${originalZone}'"
    }
}
