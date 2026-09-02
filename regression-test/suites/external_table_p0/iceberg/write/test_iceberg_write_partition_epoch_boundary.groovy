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

// Iceberg's time transforms floor towards negative infinity (the reference implementation does
// this in DateTimeUtil.convertDays/convertMicros: for a negative input it evaluates one unit later
// and then subtracts one). Doris used to derive them from datetime_diff(), which rounds towards
// zero, so every value before 1970-01-01 that was not exactly on a unit boundary got the wrong
// partition -- 1969-12-31 23:59:59 landed in the same day and hour partition as 1970-01-01
// 00:00:00. The day ordinal additionally has to be proleptic Gregorian, which Doris's
// MySQL-calendar day number is not for 0000-01-01 .. 0000-02-28.
//
// Spark writes through the Iceberg reference implementation, so writing identical rows from both
// engines and comparing the resulting partition metadata is a direct conformance check.
// See https://github.com/apache/doris/issues/67366
suite("test_iceberg_write_partition_epoch_boundary",
        "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test")
        return
    }

    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_iceberg_write_partition_epoch_boundary"
    String dbName = "iceberg_write_partition_epoch_boundary_db"

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

    // Iceberg rejects redundant partition fields (year(x) and month(x) on the same column), so
    // every transform gets its own column carrying the same value.
    def createTable = { String name ->
        sql """drop table if exists ${name}"""
        sql """
            create table ${name} (
                id int not null,
                p_date_year date,
                p_date_month date,
                p_date_day date,
                p_date_bucket date,
                p_ts_year datetime,
                p_ts_month datetime,
                p_ts_day datetime,
                p_ts_hour datetime
            )
            partition by list (
                year(p_date_year), month(p_date_month), day(p_date_day), bucket(8, p_date_bucket),
                year(p_ts_year), month(p_ts_month), day(p_ts_day), hour(p_ts_hour)
            ) ()
            properties (
                "format-version" = "2",
                "write.format.default" = "parquet"
            )
        """
    }

    // Values chosen to straddle every boundary the transforms care about:
    //   * the proleptic-vs-MySQL calendar window 0000-01-01 .. 0000-02-28,
    //   * the epoch itself, where flooring and truncation diverge,
    //   * a partial pre-epoch year/month/day/hour, and a modern leap day as a control.
    def row = { int id, String d, String ts ->
        return "(${id}, date '${d}', date '${d}', date '${d}', date '${d}', " +
                "timestamp '${ts}', timestamp '${ts}', timestamp '${ts}', timestamp '${ts}')"
    }
    def rows = [
            row(1, '0000-01-01', '0000-01-01 12:34:56'),
            row(2, '0000-02-28', '0000-02-28 00:00:00'),
            row(3, '0000-03-01', '0000-03-01 00:00:00'),
            row(4, '1969-06-15', '1969-06-15 10:00:00'),
            row(5, '1969-12-31', '1969-12-31 23:59:59'),
            row(6, '1970-01-01', '1970-01-01 00:00:00'),
            row(7, '2024-02-29', '2024-02-29 12:34:56')
    ].join(",\n            ")

    // Everything is cast to string: the year transform surfaces as a JDBC YEAR type that
    // getObject() refuses, and comparing text keeps the Doris and Spark shapes identical.
    def fields = ['p_date_year_year', 'p_date_month_month', 'p_date_day_day',
                  'p_date_bucket_bucket', 'p_ts_year_year', 'p_ts_month_month',
                  'p_ts_day_day', 'p_ts_hour_hour']

    def dorisPartitionTuples = { String name ->
        sql """refresh table ${name}"""
        def projection = fields.collect { "cast(struct_element(`partition`, '${it}') as string)" }
                .join(", ")
        return sql("select ${projection} from ${name}\$partitions order by 1, 2, 3, 4, 5, 6, 7, 8")
    }

    def sparkPartitionTuples = { String name ->
        spark_iceberg """refresh table demo.${dbName}.${name}"""
        def projection = fields.collect { "cast(partition.${it} as string)" }.join(", ")
        return spark_iceberg(
                "select ${projection} from demo.${dbName}.${name}.partitions order by 1, 2, 3, 4, 5, 6, 7, 8")
    }

    createTable("epoch_boundary_doris")
    createTable("epoch_boundary_spark")

    sql """insert into epoch_boundary_doris values ${rows}"""
    spark_iceberg """insert into demo.${dbName}.epoch_boundary_spark values ${rows}"""

    // The core conformance assertion: identical source rows must produce identical Iceberg
    // partition tuples no matter which engine wrote them.
    def dorisPartitions = dorisPartitionTuples("epoch_boundary_doris")
    def sparkPartitions = sparkPartitionTuples("epoch_boundary_spark")
    logger.info("doris partitions: ${dorisPartitions}")
    logger.info("spark partitions: ${sparkPartitions}")
    assertEquals(7, dorisPartitions.size())
    assertEquals(sparkPartitions.toString(), dorisPartitions.toString(),
                 "Doris and Spark disagree on Iceberg partition values")

    // Backstop, so a regression cannot hide behind both engines being compared only against each
    // other: 1969-12-31 23:59:59 must floor to hour -1, and seven distinct source rows must not
    // share a partition.
    def hours = dorisPartitions.collect { it[7].toString() }
    assertTrue(hours.contains("-1"),
               "1969-12-31 23:59:59 must floor to hour -1, got: ${hours}")
    assertEquals(7, dorisPartitions.collect { it.toString() }.unique().size(),
                 "distinct timestamps must not share a partition: ${dorisPartitions}")

    // DATE values must survive the round trip through the Doris writer as well. Cast to string:
    // the MySQL JDBC driver cannot materialise a year-zero DATE as java.sql.Date.
    // Only the DATE column is compared here: Doris deliberately restricts Parquet timestamp reads
    // to years 0001-9999 (MIN_DORIS_TIMESTAMP_MICROS in be/src/core/data_type_serde/
    // parquet_timestamp.h), so a year-zero timestamp written by either engine reads back as NULL.
    // The partition values for those same rows are still checked above.
    sql """refresh table epoch_boundary_doris"""
    spark_iceberg """refresh table demo.${dbName}.epoch_boundary_doris"""
    assertSparkDorisResultEquals(
            spark_iceberg("""select cast(id as string), cast(p_date_day as string)
                             from demo.${dbName}.epoch_boundary_doris order by 1"""),
            sql("""select cast(id as string), cast(p_date_day as string)
                   from epoch_boundary_doris order by 1"""))

    sql """drop database if exists ${dbName} force"""
    sql """drop catalog if exists ${catalogName}"""
}
