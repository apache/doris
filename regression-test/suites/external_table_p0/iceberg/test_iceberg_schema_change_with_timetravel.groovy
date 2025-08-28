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



suite("iceberg_schema_change_with_timetravel", "p0,external,doris,external_docker,external_docker_doris") {

    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "iceberg_schema_change_with_timetravel"

    sql """drop catalog if exists ${catalog_name}"""
    sql """
    CREATE CATALOG ${catalog_name} PROPERTIES (
        'type'='iceberg',
        'iceberg.catalog.type'='rest',
        'uri' = 'http://${externalEnvIp}:${rest_port}',
        "s3.access_key" = "admin",
        "s3.secret_key" = "password",
        "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
        "s3.region" = "us-east-1"
    );"""

    logger.info("catalog " + catalog_name + " created")
    sql """switch ${catalog_name};"""
    logger.info("switched to catalog " + catalog_name)
    sql """ use test_db;""" 

    def executeTimeTravelingQueries = { String tableName ->
        def snapshots = sql """ select snapshot_id from iceberg_meta("table" = "${catalog_name}.test_db.${tableName}", "query_type" = "snapshots") order by committed_at; """
        def snapshotIds = [
            s0: snapshots.get(0)[0],
            s1: snapshots.get(1)[0],
            s2: snapshots.get(2)[0],
            s3: snapshots.get(3)[0],
            s4: snapshots.get(4)[0]
        ]

        qt_q0 """ desc ${tableName} """
        qt_q1 """ select * from ${tableName} order by c1 """
        qt_q2 """ select * from ${tableName} for version as of ${snapshotIds.s0} order by c1 """
        qt_q3 """ select * from ${tableName} for version as of ${snapshotIds.s1} order by c1 """
        qt_q4 """ select * from ${tableName} for version as of ${snapshotIds.s2} order by c1 """
        qt_q5 """ select * from ${tableName} for version as of ${snapshotIds.s3} order by c1 """
        qt_q6 """ select * from ${tableName} for version as of ${snapshotIds.s4} order by c1 """
    }

    executeTimeTravelingQueries("schema_change_with_time_travel")
    executeTimeTravelingQueries("schema_change_with_time_travel_orc")

}

/*
create table schema_change_with_time_travel (c1 int);
insert into schema_change_with_time_travel values (1);
alter table schema_change_with_time_travel add column c2 int;
insert into schema_change_with_time_travel values (2,3);
alter table schema_change_with_time_travel add column c3 int; 
insert into schema_change_with_time_travel values (4,5,6);
alter table schema_change_with_time_travel drop column c2;
insert into schema_change_with_time_travel values (7,8);
alter table schema_change_with_time_travel add column c2 int;
insert into schema_change_with_time_travel values (9,10,11);
alter table schema_change_with_time_travel add column c4 int;
*/

