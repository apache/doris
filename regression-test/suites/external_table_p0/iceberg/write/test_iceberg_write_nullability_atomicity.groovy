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

suite("test_iceberg_write_nullability_atomicity",
        "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test")
        return
    }

    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_iceberg_write_nullability_atomicity"
    String dbName = "iceberg_write_nullability_atomicity_db"
    String internalDb = "iceberg_write_nullability_atomicity_internal_db"

    sql """drop database if exists internal.${internalDb} force"""
    sql """create database internal.${internalDb}"""
    sql """drop table if exists internal.${internalDb}.nullable_source"""
    sql """
        create table internal.${internalDb}.nullable_source (
            id int,
            required_text string,
            optional_text string
        )
        duplicate key(id)
        distributed by hash(id) buckets 3
        properties ("replication_num" = "1")
    """
    sql """
        insert into internal.${internalDb}.nullable_source values
            (2, 'valid-select', null),
            (4, 'valid-after-invalid', 'value')
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
            "s3.region" = "us-east-1",
            "meta.cache.iceberg.table.ttl-second" = "0",
            "meta.cache.iceberg.schema.ttl-second" = "0"
        )
    """
    sql """switch ${catalogName}"""
    sql """drop database if exists ${dbName} force"""
    sql """create database ${dbName}"""
    sql """use ${dbName}"""
    sql """set enable_fallback_to_original_planner = false"""

    sql """drop table if exists required_sink"""
    sql """
        create table required_sink (
            id int not null,
            required_text string not null,
            optional_text string
        )
        partition by list (bucket(8, id)) ()
        properties (
            "format-version" = "2",
            "write.format.default" = "parquet"
        )
    """
    sql """insert into required_sink values (1, 'committed', null)"""

    long snapshotsBeforeEvolution = (sql """
        select count(*) from required_sink\$snapshots
    """)[0][0] as long

    // W06-S01: Adding a required field or tightening a nullable field is rejected.
    test {
        sql """alter table required_sink add column new_required int not null"""
        exception "doesn't have a default value"
    }
    test {
        sql """alter table required_sink modify column optional_text string not null"""
        exception "Can not change nullable column optional_text to not null"
    }
    assertEquals(snapshotsBeforeEvolution, (sql """
        select count(*) from required_sink\$snapshots
    """)[0][0] as long)

    // W06-S02: Distributed and VALUES writes preserve nullable fields while required fields are valid.
    sql """
        insert into required_sink
        select id, required_text, optional_text
        from internal.${internalDb}.nullable_source
    """
    sql """insert into required_sink values (5, 'valid-values-retry', null)"""
    assertEquals(snapshotsBeforeEvolution + 2, (sql """
        select count(*) from required_sink\$snapshots
    """)[0][0] as long)
    order_qt_required_after_retry """
        select id, required_text, optional_text
        from required_sink
        order by id
    """

    sql """refresh table ${dbName}.required_sink"""
    spark_iceberg """refresh table demo.${dbName}.required_sink"""
    def sparkRows = spark_iceberg """
        select id, required_text, optional_text
        from demo.${dbName}.required_sink
        order by id
    """
    def dorisRows = sql """
        select id, required_text, optional_text
        from required_sink
        order by id
    """
    assertSparkDorisResultEquals(sparkRows, dorisRows)
}
