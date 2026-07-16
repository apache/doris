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

suite("test_clickhouse_jdbc_v2", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String clickhousePort = context.config.otherConfigs.get("clickhouse_22_port")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String driverUrl = "https://repo.maven.apache.org/maven2/com/clickhouse/clickhouse-jdbc/0.9.8/clickhouse-jdbc-0.9.8-all.jar"

        sql """ drop catalog if exists clickhouse_v2_schema """
        sql """ create catalog clickhouse_v2_schema properties(
                    "type"="jdbc",
                    "user"="default",
                    "password"="123456",
                    "jdbc_url" = "jdbc:clickhouse://${externalEnvIp}:${clickhousePort}/doris_test?jdbc_schema_term=schema",
                    "driver_url" = "${driverUrl}",
                    "driver_class" = "com.clickhouse.jdbc.Driver"
        );"""

        order_qt_clickhouse_v2_schema """ select count(*) from clickhouse_v2_schema.doris_test.type """
        order_qt_clickhouse_v2_distributed """ select count(*) from clickhouse_v2_schema.doris_test.distributed_type """
        order_qt_clickhouse_v2_materialized_view """ select count(*) from clickhouse_v2_schema.doris_test.materialized_view_type """
        sql """ drop catalog clickhouse_v2_schema """
    }
}
