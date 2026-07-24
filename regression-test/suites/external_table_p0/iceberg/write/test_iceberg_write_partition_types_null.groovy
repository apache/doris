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

suite("test_iceberg_write_partition_types_null",
        "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test")
        return
    }

    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_iceberg_write_partition_types_null"
    String dbName = "iceberg_write_partition_types_null_db"

    def assertSparkMatchesDoris = { String tableName, String projection ->
        sql """refresh table ${dbName}.${tableName}"""
        spark_iceberg """refresh table demo.${dbName}.${tableName}"""
        def sparkRows = spark_iceberg """
            select ${projection}
            from demo.${dbName}.${tableName}
            order by id
        """
        def dorisRows = sql """
            select ${projection}
            from ${tableName}
            order by id
        """
        assertSparkDorisResultEquals(sparkRows, dorisRows)
    }

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

    // W05-S00: BOOLEAN is valid for identity but not for Iceberg's bucket transform.
    // The invalid table must be rejected instead of creating a table that fails on its first write.
    test {
        sql """
            create table invalid_boolean_bucket (
                id int,
                p_bool boolean
            )
            partition by list (bucket(4, p_bool)) ()
        """
        exception "Invalid source type boolean for transform: bucket[4]"
    }

    // W05-S01: STRING supports identity, bucket and truncate together.
    // NULL is routed by the nullable identity source while transform-specific sources stay required.
    sql """drop table if exists string_partitions"""
    sql """
        create table string_partitions (
            id int not null,
            p_string string,
            p_bucket string not null,
            p_truncate string not null,
            payload string
        )
        partition by list (p_string, bucket(8, p_bucket), truncate(2, p_truncate)) ()
        properties (
            "format-version" = "2",
            "write.format.default" = "parquet"
        )
    """
    sql """
        insert into string_partitions values
            (1, 'alpha', 'bucket-a', 'alpha', 'a'),
            (2, 'alphabet', 'bucket-b', 'alphabet', 'ab'),
            (3, '', 'bucket-empty', '', 'empty'),
            (4, '中文', 'bucket-unicode', '中文', 'unicode'),
            (5, null, 'bucket-null-identity', 'null-identity', 'null-string')
    """
    order_qt_string_rows """
        select id, p_string, p_bucket, p_truncate, payload
        from string_partitions
        order by id
    """
    order_qt_string_null_filter """
        select id from string_partitions where p_string is null order by id
    """

    // W05-S02: Replace a STRING bucket transform and keep old/new specs filterable.
    sql """
        alter table string_partitions
        replace partition key bucket(8, p_bucket)
        with bucket(16, p_bucket) as p_string_bucket_16
    """
    sql """
        insert into string_partitions values
            (6, 'beta', 'bucket-new', 'beta', 'new-spec'),
            (7, null, 'bucket-new-null-identity', 'null-identity', 'new-null-string')
    """
    order_qt_string_cross_spec_filter """
        select id from string_partitions
        where p_string is null or p_string like 'alp%'
        order by id
    """
    order_qt_string_partition_specs """
        select spec_id, sum(record_count)
        from string_partitions\$partitions
        group by spec_id
        order by spec_id
    """
    assertSparkMatchesDoris(
            "string_partitions",
            "id, p_string, p_bucket, p_truncate, payload")

    // W05-S03: Integer/BIGINT/DECIMAL bucket or truncate transforms and BOOLEAN identity
    // must all route NULL to valid Iceberg partitions.
    sql """drop table if exists numeric_partitions"""
    sql """
        create table numeric_partitions (
            id int not null,
            p_int int,
            p_bigint bigint,
            p_decimal decimal(12, 2),
            p_bool boolean,
            payload string
        )
        partition by list (
            bucket(4, p_int),
            bucket(8, p_bigint),
            truncate(100, p_bigint),
            bucket(8, p_decimal),
            truncate(10, p_decimal),
            p_bool
        ) ()
        properties (
            "format-version" = "2",
            "write.format.default" = "orc"
        )
    """
    sql """
        insert into numeric_partitions values
            (1, 1, 101, 11.11, true, 'positive'),
            (2, -1, -101, -11.11, false, 'negative'),
            (3, 0, 0, 0.00, null, 'zero-null-bool'),
            (4, null, null, null, null, 'all-null')
    """
    order_qt_numeric_rows """
        select id, p_int, p_bigint, p_decimal, p_bool, payload
        from numeric_partitions
        order by id
    """
    order_qt_numeric_null_filter """
        select id from numeric_partitions
        where p_int is null or p_bigint is null or p_decimal is null or p_bool is null
        order by id
    """
    order_qt_numeric_partitions """
        select spec_id, sum(record_count)
        from numeric_partitions\$partitions
        group by spec_id
        order by spec_id
    """
    assertSparkMatchesDoris(
            "numeric_partitions",
            "id, p_int, p_bigint, p_decimal, p_bool, payload")

    // W05-S04: DATE/DATETIME time transforms accept boundary values and NULL.
    sql """drop table if exists temporal_partitions"""
    sql """
        create table temporal_partitions (
            id int not null,
            p_date_bucket date,
            p_date_year date,
            p_date_month date,
            p_ts_bucket datetime,
            p_ts_day datetime,
            p_ts_hour datetime,
            payload string
        )
        partition by list (
            bucket(8, p_date_bucket),
            year(p_date_year),
            month(p_date_month),
            bucket(8, p_ts_bucket),
            day(p_ts_day),
            hour(p_ts_hour)
        ) ()
        properties (
            "format-version" = "2",
            "write.format.default" = "parquet"
        )
    """
    sql """
        insert into temporal_partitions values
            (1, '1969-12-31', '1969-12-31', '1969-12-31',
                '1969-12-31 23:59:59', '1969-12-31 23:59:59', '1969-12-31 23:59:59',
                'before-epoch'),
            (2, '1970-01-01', '1970-01-01', '1970-01-01',
                '1970-01-01 00:00:00', '1970-01-01 00:00:00', '1970-01-01 00:00:00',
                'epoch'),
            (3, '2024-02-29', '2024-02-29', '2024-02-29',
                '2024-02-29 12:34:56', '2024-02-29 12:34:56', '2024-02-29 12:34:56',
                'leap-day'),
            (4, null, null, null, null, null, null, 'all-null')
    """
    order_qt_temporal_rows """
        select id, p_date_bucket, p_date_year, p_date_month,
               p_ts_bucket, p_ts_day, p_ts_hour, payload
        from temporal_partitions
        order by id
    """
    order_qt_temporal_filters """
        select id from temporal_partitions
        where p_date_bucket is null
           or p_ts_hour < timestamp '1970-01-01 00:00:00'
           or p_date_month = date '2024-02-29'
        order by id
    """
    order_qt_temporal_partitions """
        select spec_id, sum(record_count)
        from temporal_partitions\$partitions
        group by spec_id
        order by spec_id
    """
    assertSparkMatchesDoris(
            "temporal_partitions",
            "id, p_date_bucket, p_date_year, p_date_month, " +
                    "p_ts_bucket, p_ts_day, p_ts_hour, payload")
}
