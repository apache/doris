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

// UUID metadata plus OLAP/Arrow materialization, including nested values and NULL batches.
suite("test_remote_doris_uuid", "p0,external") {
    def frontend = (sql "SHOW FRONTENDS")[0]
    String host = frontend[1]
    String user = context.config.jdbcUser
    String password = context.config.jdbcPassword
    sql "SET enable_sql_cache = false"
    sql "SET enable_query_cache = false"
    sql "CREATE DATABASE IF NOT EXISTS remote_doris_uuid_db"
    sql "DROP TABLE IF EXISTS remote_doris_uuid_db.uuid_source"
    sql """CREATE TABLE remote_doris_uuid_db.uuid_source
            (id INT,u UUID,a ARRAY<UUID>,m MAP<UUID,UUID>,s STRUCT<k:UUID>)
            DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')"""
    sql """INSERT INTO remote_doris_uuid_db.uuid_source SELECT id,u,
            IF(id%7=0,NULL,ARRAY(u,NULL)),
            MAP(CAST('00000000000000000000000000000000' AS UUID),u),NAMED_STRUCT('k',u)
            FROM (SELECT CAST(number AS INT) id,CAST(CASE number%4
                  WHEN 0 THEN NULL WHEN 1 THEN '00000000000000000000000000000000'
                  WHEN 2 THEN '00112233445566778899aabbccddeeff'
                  ELSE 'ffffffffffffffffffffffffffffffff' END AS UUID) u
                  FROM numbers('number'='4097')) values_source"""
    for (boolean arrow : [false, true]) {
        sql "DROP CATALOG IF EXISTS remote_doris_uuid"
        sql """CREATE CATALOG remote_doris_uuid PROPERTIES('type'='doris',
                'fe_http_hosts'='http://${host}:${frontend[3]}',
                'fe_arrow_hosts'='${host}:${frontend[6]}',
                'fe_thrift_hosts'='${host}:${frontend[5]}',
                'user'='${user}','password'='${password}','use_arrow_flight'='${arrow}')"""
        for (boolean scannerV2 : [false, true]) {
            sql "SET enable_file_scanner_v2 = ${scannerV2}"
            qt_schema "DESC remote_doris_uuid.remote_doris_uuid_db.uuid_source"
            qt_values "SELECT * FROM remote_doris_uuid.remote_doris_uuid_db.uuid_source WHERE id<9 ORDER BY id"
            qt_batches """SELECT COUNT(*),COUNT(u),COUNT(DISTINCT u),MIN(u),MAX(u),COUNT(a[1]),SUM(SIZE(a))
                          FROM remote_doris_uuid.remote_doris_uuid_db.uuid_source"""
            qt_range """SELECT id,u FROM remote_doris_uuid.remote_doris_uuid_db.uuid_source
                        WHERE id<9 AND u>=CAST('80000000000000000000000000000000' AS UUID) ORDER BY id"""
            qt_groups """SELECT u,COUNT(*) FROM remote_doris_uuid.remote_doris_uuid_db.uuid_source GROUP BY u ORDER BY u"""
            qt_join """SELECT COUNT(*) FROM remote_doris_uuid.remote_doris_uuid_db.uuid_source l
                       JOIN remote_doris_uuid_db.uuid_source r ON l.u=r.u WHERE l.id<9 AND r.id<9"""
            qt_window """SELECT id,u,DENSE_RANK() OVER(ORDER BY u)
                         FROM remote_doris_uuid.remote_doris_uuid_db.uuid_source WHERE id<9 ORDER BY id"""
            sql "REFRESH TABLE remote_doris_uuid.remote_doris_uuid_db.uuid_source"
            qt_refresh "SELECT * FROM remote_doris_uuid.remote_doris_uuid_db.uuid_source WHERE id=4096"
        }
    }
    sql "DROP TABLE IF EXISTS remote_doris_uuid_ctas"
    sql """CREATE TABLE remote_doris_uuid_ctas DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES('replication_num'='1') AS SELECT * FROM remote_doris_uuid.remote_doris_uuid_db.uuid_source WHERE id<9"""
    qt_ctas_schema "DESC remote_doris_uuid_ctas"
    qt_ctas_values "SELECT * FROM remote_doris_uuid_ctas ORDER BY id"
}
