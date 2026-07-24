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

suite("test_iceberg_write_string_transform_metadata",
        "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test")
        return
    }

    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_iceberg_write_string_transform_metadata"
    String dbName = "iceberg_write_string_transform_metadata_db"

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

    sql """drop table if exists string_transform_metadata"""
    sql """
        create table string_transform_metadata (
            id int not null,
            p_identity string,
            p_bucket string,
            p_truncate string not null,
            payload string
        )
        partition by list (p_identity) ()
        properties (
            "format-version" = "2",
            "write.format.default" = "parquet"
        )
    """
    sql """
        alter table string_transform_metadata
        add partition key bucket(8, p_bucket) as p_bucket_8
    """
    sql """
        alter table string_transform_metadata
        add partition key truncate(2, p_truncate) as p_truncate_2
    """

    // WS01-S01: Validate the physical partition values, not only logical row
    // equality. STRING bucket accepts NULL and truncate preserves valid UTF-8.
    sql """
        insert into string_transform_metadata values
            (1, 'ascii', 'bucket-a', 'alphabet', 'ascii'),
            (2, '中文', '桶-中文', '中文甲', 'cjk'),
            (3, 'emoji', '😀-bucket', '😀甲乙', 'emoji'),
            (4, concat('e', unhex('CC81')), 'combining-bucket',
                concat('e', unhex('CC81'), 'x'), 'combining'),
            (5, '', '', '', 'empty'),
            (6, null, null, 'null-bucket', 'nullable-bucket')
    """
    order_qt_string_transform_rows """
        select id, hex(p_identity), hex(p_bucket), hex(p_truncate), payload
        from string_transform_metadata
        order by id
    """
    order_qt_string_transform_physical_partitions """
        select struct_element(`partition`, 'p_identity') as p_identity_partition,
               struct_element(`partition`, 'p_bucket_8') as p_bucket_partition,
               hex(struct_element(`partition`, 'p_truncate_2')) as p_truncate_partition,
               record_count
        from string_transform_metadata\$partitions
        order by p_identity_partition, p_bucket_partition, p_truncate_partition
    """

    spark_iceberg """refresh table demo.${dbName}.string_transform_metadata"""
    def sparkRows = spark_iceberg """
        select id, p_identity, p_bucket, p_truncate, payload
        from demo.${dbName}.string_transform_metadata
        order by id
    """
    def dorisRows = sql """
        select id, p_identity, p_bucket, p_truncate, payload
        from string_transform_metadata
        order by id
    """
    assertSparkDorisResultEquals(sparkRows, dorisRows)

    def sparkPartitions = spark_iceberg """
        select partition.p_identity,
               partition.p_bucket_8,
               hex(partition.p_truncate_2),
               record_count
        from demo.${dbName}.string_transform_metadata.partitions
        order by partition.p_identity, partition.p_bucket_8, hex(partition.p_truncate_2)
    """
    def dorisPartitions = sql """
        select struct_element(`partition`, 'p_identity'),
               struct_element(`partition`, 'p_bucket_8'),
               hex(struct_element(`partition`, 'p_truncate_2')),
               record_count
        from string_transform_metadata\$partitions
        order by struct_element(`partition`, 'p_identity'),
                 struct_element(`partition`, 'p_bucket_8'),
                 hex(struct_element(`partition`, 'p_truncate_2'))
    """
    assertSparkDorisResultEquals(sparkPartitions, dorisPartitions)

    // WS01-S02: Evolve the bucket and truncate widths and verify that both
    // physical specs remain readable by Doris and Spark.
    sql """
        alter table string_transform_metadata
        replace partition key p_bucket_8 with bucket(16, p_bucket) as p_bucket_16
    """
    sql """
        alter table string_transform_metadata
        replace partition key p_truncate_2 with truncate(3, p_truncate) as p_truncate_3
    """
    sql """
        insert into string_transform_metadata values
            (7, 'new', 'bucket-new', '中文甲乙', 'new-cjk'),
            (8, null, null, '😀甲乙丙', 'new-null-bucket')
    """
    order_qt_string_transform_evolved_specs """
        select spec_id, count(*), sum(record_count)
        from string_transform_metadata\$partitions
        group by spec_id
        order by spec_id
    """
    order_qt_string_transform_evolved_rows """
        select id, hex(p_identity), hex(p_bucket), hex(p_truncate), payload
        from string_transform_metadata
        order by id
    """
    spark_iceberg """refresh table demo.${dbName}.string_transform_metadata"""
    sparkRows = spark_iceberg """
        select id, p_identity, p_bucket, p_truncate, payload
        from demo.${dbName}.string_transform_metadata
        order by id
    """
    dorisRows = sql """
        select id, p_identity, p_bucket, p_truncate, payload
        from string_transform_metadata
        order by id
    """
    assertSparkDorisResultEquals(sparkRows, dorisRows)
}
