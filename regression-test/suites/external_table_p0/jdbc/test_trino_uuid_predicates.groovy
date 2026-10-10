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

suite("test_trino_uuid_predicates", "p0,external") {
    if (context.config.otherConfigs.get("enableTrinoJdbcTest") != "true") {
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    String port = context.config.otherConfigs.get("trino_jdbc_port")
    String driver = context.config.otherConfigs.get("trinoJdbcDriverUrl") ?:
            "https://repo.maven.apache.org/maven2/io/trino/trino-jdbc/435/trino-jdbc-435.jar"
    sql "DROP CATALOG IF EXISTS trino_uuid_predicates"
    sql """CREATE CATALOG trino_uuid_predicates PROPERTIES (
        "type"="jdbc", "user"="test", "password"="",
        "jdbc_url"="jdbc:trino://${host}:${port}/memory",
        "driver_url"="${driver}", "driver_class"="io.trino.jdbc.TrinoDriver")"""
    def remoteExecute = { String query ->
        sql("CALL EXECUTE_STMT('trino_uuid_predicates', '" + query.replace("'", "''") + "')")
    }
    remoteExecute("DROP TABLE IF EXISTS memory.default.doris_uuid_predicates")
    remoteExecute("CREATE TABLE memory.default.doris_uuid_predicates (id INTEGER, u UUID)")
    remoteExecute("INSERT INTO memory.default.doris_uuid_predicates VALUES " +
            "(1, UUID '00000000-0000-0000-0000-000000000000'), " +
            "(2, UUID '00112233-4455-6677-8899-aabbccddeeff'), " +
            "(3, UUID '80000000-0000-0000-0000-000000000000'), " +
            "(4, UUID 'ffffffff-ffff-ffff-ffff-ffffffffffff'), (5, NULL)")
    // UUID predicates must keep the literal type when rendered as remote Trino SQL.
    qt_equal """SELECT id, u FROM trino_uuid_predicates.`default`.doris_uuid_predicates
        WHERE u = CAST('00112233-4455-6677-8899-aabbccddeeff' AS UUID) ORDER BY id"""
    qt_in """SELECT id, u FROM trino_uuid_predicates.`default`.doris_uuid_predicates
        WHERE u IN (CAST('00000000-0000-0000-0000-000000000000' AS UUID),
                    CAST('ffffffff-ffff-ffff-ffff-ffffffffffff' AS UUID)) ORDER BY id"""
    qt_range """SELECT id, u FROM trino_uuid_predicates.`default`.doris_uuid_predicates
        WHERE u >= CAST('80000000-0000-0000-0000-000000000000' AS UUID) ORDER BY id"""
    qt_between """SELECT id, u FROM trino_uuid_predicates.`default`.doris_uuid_predicates
        WHERE u BETWEEN CAST('00112233-4455-6677-8899-aabbccddeeff' AS UUID)
                    AND CAST('ffffffff-ffff-ffff-ffff-ffffffffffff' AS UUID) ORDER BY id"""
    qt_null """SELECT id, u FROM trino_uuid_predicates.`default`.doris_uuid_predicates
        WHERE u IS NULL ORDER BY id"""
}
