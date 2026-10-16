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

// External checklist: T02/T03, R05/R08/R09, Q01/Q02/Q04, W05/W06, V02.
suite("test_clickhouse_uuid", "p0,external") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableJdbcTest"))) {
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    String port = context.config.otherConfigs.get("clickhouse_22_port")
    String driver = "https://${getS3BucketName()}.${getS3Endpoint()}/regression/jdbc_driver/clickhouse-jdbc-0.7.1-patch1-all.jar"
    sql "SET enable_sql_cache = false"
    sql "SET enable_query_cache = false"
    sql "DROP CATALOG IF EXISTS clickhouse_uuid"
    sql """CREATE CATALOG clickhouse_uuid PROPERTIES(
            'type'='jdbc','user'='default','password'='123456',
            'jdbc_url'='jdbc:clickhouse://${host}:${port}/doris_test',
            'driver_url'='${driver}','driver_class'='com.clickhouse.jdbc.ClickHouseDriver')"""
    sql """CALL execute_stmt('clickhouse_uuid', 'DROP TABLE IF EXISTS doris_test.uuid_external')"""
    sql """CALL execute_stmt('clickhouse_uuid', 'CREATE TABLE doris_test.uuid_external
            (id Int32,u Nullable(UUID),v Nullable(UUID),a Array(Nullable(UUID))) ENGINE=MergeTree ORDER BY id')"""
    // Values are constructed by ClickHouse, independently of Doris serialization.
    String values = "['00000000-0000-0000-0000-000000000000','00000000-0000-0000-0000-000000000001'," +
            "'7fffffff-ffff-ffff-ffff-ffffffffffff','80000000-0000-0000-0000-000000000000'," +
            "'ffffffff-ffff-ffff-ffff-ffffffffffff','00112233-4455-6677-8899-aabbccddeeff'," +
            "'00000000-0000-0000-ffff-ffffffffffff','00112233-4455-6677-8899-aabbccddeeff']"
    String insert = """INSERT INTO doris_test.uuid_external SELECT toInt32(number),
            if(number%9=8,NULL,toUUID(arrayElement(${values},number%9+1))) u,
            if(number%9=0,NULL,toUUID(arrayElement(${values},(number+1)%8+1))) v,
            [u,NULL] FROM numbers(8193)"""
    sql """CALL execute_stmt('clickhouse_uuid', '${insert.replace("'", "''")}')"""
    qt_schema "DESC clickhouse_uuid.doris_test.uuid_external"
    qt_values "SELECT * FROM clickhouse_uuid.doris_test.uuid_external WHERE id < 9 ORDER BY id"
    // COUNT(*) alone can avoid fetching UUID data. These aggregates materialize every batch.
    qt_batches """SELECT COUNT(*),COUNT(u),COUNT(DISTINCT u),MIN(u),MAX(u),SUM(SIZE(a)),
                   COUNT(a[1]) FROM clickhouse_uuid.doris_test.uuid_external"""
    qt_group "SELECT u,COUNT(*),SUM(id) FROM clickhouse_uuid.doris_test.uuid_external GROUP BY u ORDER BY u"
    for (boolean fold : [false, true]) {
        sql "SET debug_skip_fold_constant = ${!fold}"
        for (String op : ['<','<=','>','>=','=','!=']) {
            qt_range """SELECT id,u FROM clickhouse_uuid.doris_test.uuid_external
                        WHERE id < 9 AND u ${op} CAST('80000000000000000000000000000000' AS UUID) ORDER BY id"""
            qt_column_range """SELECT id,u,v FROM clickhouse_uuid.doris_test.uuid_external
                               WHERE id < 9 AND u ${op} v ORDER BY id"""
        }
    }
    qt_null "SELECT id FROM clickhouse_uuid.doris_test.uuid_external WHERE id<9 AND u IS NULL ORDER BY id"
    qt_in """SELECT id FROM clickhouse_uuid.doris_test.uuid_external WHERE id<9 AND
             u IN (CAST('00000000000000000000000000000000' AS UUID),CAST('ffffffffffffffffffffffffffffffff' AS UUID)) ORDER BY id"""
    qt_between """SELECT id FROM clickhouse_uuid.doris_test.uuid_external WHERE id<9 AND
                  u BETWEEN CAST('00000000000000000000000000000000' AS UUID)
                  AND CAST('80000000000000000000000000000000' AS UUID) ORDER BY id"""
    explain {
        sql """SELECT id FROM clickhouse_uuid.doris_test.uuid_external
                WHERE u < CAST('80000000000000000000000000000000' AS UUID) LIMIT 1"""
        contains 'QUERY: SELECT'
        notContains 'WHERE ('
        notContains 'LIMIT 1'
    }
    explain {
        sql """SELECT id FROM clickhouse_uuid.doris_test.uuid_external
                WHERE u = CAST('80000000000000000000000000000000' AS UUID)"""
        contains 'WHERE ('
    }
    qt_join """SELECT COUNT(*) FROM clickhouse_uuid.doris_test.uuid_external l
               JOIN clickhouse_uuid.doris_test.uuid_external r ON l.u=r.u WHERE l.id<9 AND r.id<9"""
    qt_window """SELECT id,u,DENSE_RANK() OVER(ORDER BY u) FROM clickhouse_uuid.doris_test.uuid_external
                 WHERE id<9 ORDER BY id"""
    sql "DROP TABLE IF EXISTS clickhouse_uuid_ctas"
    sql """CREATE TABLE clickhouse_uuid_ctas DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES('replication_num'='1') AS SELECT * FROM clickhouse_uuid.doris_test.uuid_external WHERE id<9"""
    qt_ctas_schema "DESC clickhouse_uuid_ctas"
    qt_ctas_values "SELECT * FROM clickhouse_uuid_ctas ORDER BY id"
    sql """CALL execute_stmt('clickhouse_uuid', 'DROP TABLE IF EXISTS doris_test.uuid_sink')"""
    sql """CALL execute_stmt('clickhouse_uuid', 'CREATE TABLE doris_test.uuid_sink
            (id Int32,u Nullable(UUID)) ENGINE=MergeTree ORDER BY id')"""
    sql "INSERT INTO clickhouse_uuid.doris_test.uuid_sink SELECT id,u FROM clickhouse_uuid_ctas"
    qt_write "SELECT * FROM clickhouse_uuid.doris_test.uuid_sink ORDER BY id"
}
