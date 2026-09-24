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

suite("test_iceberg_delete_unpartitioned_evolution",
        "p0,external,iceberg,external_docker,external_docker_iceberg") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableIcebergTest"))) {
        logger.info("disable iceberg test")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_iceberg_delete_unpartitioned_evolution"
    String dbName = "iceberg_delete_unpartitioned_evolution_db"
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
    sql """create database if not exists ${dbName}"""
    sql """use ${dbName}"""

    def checkRows = { String table, def expected ->
        sql """refresh table ${dbName}.${table}"""
        spark_iceberg """refresh table demo.${dbName}.${table}"""
        def actual = sql """select record_key, metric from ${table} order by record_key"""
        assertSparkDorisResultEquals(expected, actual)
        assertSparkDorisResultEquals(spark_iceberg("""
            select record_key, metric from demo.${dbName}.${table} order by record_key
        """), actual)
    }

    // REPLACE retains historical spec 0 while writing new files under an unpartitioned spec.
    // Spark's replace transaction exercises the same Iceberg metadata transition as Trino RTAS.
    spark_iceberg """drop table if exists demo.${dbName}.replace_to_unpartitioned"""
    spark_iceberg """
        create table demo.${dbName}.replace_to_unpartitioned (record_key int, metric int)
        using iceberg partitioned by (record_key)
        tblproperties ('format-version' = '2', 'write.format.default' = 'parquet')
    """
    sql """refresh database ${dbName}"""
    sql """insert into replace_to_unpartitioned values (1, 10), (2, 20), (3, 30)"""
    spark_iceberg """refresh table demo.${dbName}.replace_to_unpartitioned"""
    spark_iceberg """
        create or replace table demo.${dbName}.replace_to_unpartitioned using iceberg
        tblproperties ('format-version' = '2', 'write.delete.mode' = 'merge-on-read')
        as select * from demo.${dbName}.replace_to_unpartitioned
    """
    sql """refresh table ${dbName}.replace_to_unpartitioned"""
    def replacedSpecs = sql """select distinct spec_id from replace_to_unpartitioned\$data_files"""
    assertEquals(1, replacedSpecs.size())
    assertTrue((replacedSpecs[0][0] as int) > 0)
    def expectedAfterReplaceDelete = spark_iceberg """
        select record_key, metric from demo.${dbName}.replace_to_unpartitioned
        where record_key <> 1 order by record_key
    """
    sql """delete from replace_to_unpartitioned where record_key = 1"""
    checkRows("replace_to_unpartitioned", expectedAfterReplaceDelete)
    assertEquals(replacedSpecs, sql("""
        select distinct spec_id from replace_to_unpartitioned\$delete_files
    """))
    def expectedAfterRefreshDelete = spark_iceberg """
        select record_key, metric from demo.${dbName}.replace_to_unpartitioned
        where record_key <> 2 order by record_key
    """
    sql """delete from replace_to_unpartitioned where record_key = 2"""
    checkRows("replace_to_unpartitioned", expectedAfterRefreshDelete)

    // Dropping the last partition field leaves both old partitioned and new unpartitioned files live.
    spark_iceberg """drop table if exists demo.${dbName}.mixed_partition_specs"""
    spark_iceberg """
        create table demo.${dbName}.mixed_partition_specs (record_key int, metric int)
        using iceberg partitioned by (record_key)
        tblproperties ('format-version' = '2', 'write.format.default' = 'orc',
                       'write.delete.mode' = 'merge-on-read', 'write.update.mode' = 'merge-on-read')
    """
    spark_iceberg """insert into demo.${dbName}.mixed_partition_specs values (1, 10), (2, 20)"""
    spark_iceberg """alter table demo.${dbName}.mixed_partition_specs drop partition field record_key"""
    spark_iceberg """insert into demo.${dbName}.mixed_partition_specs values (3, 30), (4, 40)"""
    sql """refresh database ${dbName}"""
    def mixedSpecs = sql """select distinct spec_id from mixed_partition_specs\$data_files order by spec_id"""
    assertEquals(2, mixedSpecs.size())
    def expectedAfterMixedDelete = spark_iceberg """
        select record_key, metric from demo.${dbName}.mixed_partition_specs
        where record_key not in (1, 3) order by record_key
    """
    sql """delete from mixed_partition_specs where record_key in (1, 3)"""
    checkRows("mixed_partition_specs", expectedAfterMixedDelete)
    assertEquals(mixedSpecs, sql("""
        select distinct spec_id from mixed_partition_specs\$delete_files order by spec_id
    """))
    def expectedAfterUpdate = spark_iceberg """
        select record_key, metric + 100 from demo.${dbName}.mixed_partition_specs order by record_key
    """
    sql """update mixed_partition_specs set metric = metric + 100"""
    checkRows("mixed_partition_specs", expectedAfterUpdate)
}
