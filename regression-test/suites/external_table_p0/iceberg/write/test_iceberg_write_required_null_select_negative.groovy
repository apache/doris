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

suite("test_iceberg_write_required_null_select_negative",
        "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test")
        return
    }

    // This opt-in switch isolates a write that can publish an unreadable Iceberg data file.
    String knownBugTestEnabled = context.config.otherConfigs.get("enableIcebergKnownBugTest")
    if (knownBugTestEnabled == null || !knownBugTestEnabled.equalsIgnoreCase("true")) {
        logger.info("skip isolated Iceberg known-bug regression")
        return
    }

    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_iceberg_write_required_null_select_negative"
    String dbName = "iceberg_write_required_null_select_negative_db"
    String internalDb = "iceberg_write_required_null_select_negative_internal_db"

    sql """drop database if exists internal.${internalDb} force"""
    sql """create database internal.${internalDb}"""
    sql """
        create table internal.${internalDb}.nullable_source (
            id int,
            required_text string
        )
        duplicate key(id)
        distributed by hash(id) buckets 3
        properties ("replication_num" = "1")
    """
    sql """
        insert into internal.${internalDb}.nullable_source values
            (1, 'valid'),
            (2, null)
    """

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
        create table required_select (
            id int not null,
            required_text string not null
        )
        partition by list (bucket(8, id)) ()
        properties ("format-version" = "2")
    """

    // W07-S02: A mixed distributed INSERT SELECT must reject the whole statement atomically.
    test {
        sql """
            insert into required_select
            select id, required_text
            from internal.${internalDb}.nullable_source
        """
        exception "null"
    }
}
