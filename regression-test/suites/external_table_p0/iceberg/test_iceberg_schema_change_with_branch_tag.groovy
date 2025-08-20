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



suite("iceberg_schema_change_with_branch_tag", "p0,external,doris,external_docker,external_docker_doris") {

    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "iceberg_schema_change_with_branch_tag"

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
    // init table test_schema_change_with_branch_tag
    sql """ drop table if exists test_schema_change_with_branch_tag; """
    sql """ create table test_schema_change_with_branch_tag (id int); """
    sql """ insert into test_schema_change_with_branch_tag values (1), (2), (3); """
    // create branch and tag
    sql """ alter table test_schema_change_with_branch_tag create branch test_branch; """
    sql """ alter table test_schema_change_with_branch_tag create tag test_tag; """
    // schema change but no insert data, no snaptshot
    sql """ alter table test_schema_change_with_branch_tag add column name string; """

    // this should get latest schema
    qt_desc_schema """ desc test_schema_change_with_branch_tag; """
    qt_select_table """ select * from test_schema_change_with_branch_tag order by id; """

    qt_select_branch """ select * from test_schema_change_with_branch_tag FOR VERSION AS OF 'test_branch' """
    qt_select_tag """ select * from test_schema_change_with_branch_tag FOR VERSION AS OF 'test_tag' """
}
