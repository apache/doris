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

suite("test_iceberg_write_nullable_truncate_negative",
        "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test")
        return
    }

    // This opt-in switch isolates a BE-fatal negative scenario from the shared P0 cluster.
    // Enable it only in a cluster whose BE processes can be restarted after the suite.
    String crashTestEnabled = context.config.otherConfigs.get("enableIcebergCrashTest")
    if (crashTestEnabled == null || !crashTestEnabled.equalsIgnoreCase("true")) {
        logger.info("skip isolated Iceberg crash regression")
        return
    }

    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_iceberg_write_nullable_truncate_negative"
    String dbName = "iceberg_write_nullable_truncate_negative_db"

    sql """drop catalog if exists ${catalogName}"""
    sql """
        create catalog ${catalogName} properties (
            "type" = "iceberg",
            "iceberg.catalog.type" = "rest",
            "uri" = "http://${externalEnvIp}:${restPort}",
            "s3.access_key" = "admin",
            "s3.secret_key" = "password",
            "s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
            "s3.region" = "us-east-1"
        )
    """
    sql """switch ${catalogName}"""
    sql """drop database if exists ${dbName} force"""
    sql """create database ${dbName}"""
    sql """use ${dbName}"""

    sql """
        create table nullable_truncate (
            id int not null,
            zone string
        )
        partition by list (zone) ()
        properties ("format-version" = "2")
    """
    sql """insert into nullable_truncate values (1, 'CN'), (2, null)"""

    // Negative scenario: evolve to a truncate transform whose source remains nullable,
    // then write both non-NULL and NULL partition values through Doris.
    sql """
        alter table nullable_truncate
        add partition key truncate(2, zone) as zone_prefix
    """
    sql """insert into nullable_truncate values (3, 'US-east'), (4, null)"""

    order_qt_nullable_truncate_rows """
        select id, zone from nullable_truncate order by id
    """
}
