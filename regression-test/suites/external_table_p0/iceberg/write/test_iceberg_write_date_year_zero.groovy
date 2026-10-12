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

// Partition metadata and Parquet DATE payloads must use the same Gregorian epoch days.
suite("test_iceberg_write_date_year_zero",
        "p0,external,iceberg,external_docker,external_docker_iceberg") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableIcebergTest"))) {
        logger.info("disable iceberg test")
        return
    }
    def env = context.config.otherConfigs
    def catalog = "test_iceberg_write_date_year_zero"
    def database = "iceberg_write_date_year_zero_db"
    sql "drop catalog if exists ${catalog}"
    sql """create catalog ${catalog} properties (
        "type" = "iceberg", "iceberg.catalog.type" = "rest",
        "uri" = "http://${env.get('externalEnvIp')}:${env.get('iceberg_rest_uri_port')}",
        "s3.access_key" = "admin", "s3.secret_key" = "password",
        "s3.endpoint" = "http://${env.get('externalEnvIp')}:${env.get('iceberg_minio_port')}",
        "s3.region" = "us-east-1", "meta.cache.iceberg.table.ttl-second" = "0",
        "meta.cache.iceberg.schema.ttl-second" = "0")"""
    sql "switch ${catalog}"
    sql "drop database if exists ${database} force"
    sql "create database ${database}"
    sql "use ${database}"
    try {
        def dates = ['0000-01-01', '0000-02-28', '0000-03-01', '1969-12-31', '1970-01-01', '2024-01-01']
        def rows = dates.withIndex().collect { d, i -> "(${i}, date '${d}', date '${d}')" }.join(', ')
        rows += ', (6, NULL, NULL)'
        for (def name : ['doris_dates', 'spark_dates']) {
            sql """create table ${name} (id int, d_day date, d_bucket date)
                partition by list (day(d_day), bucket(16, d_bucket)) ()
                properties ("format-version" = "2", "write.format.default" = "parquet")"""
        }
        sql "insert into doris_dates values ${rows}"
        spark_iceberg "insert into demo.${database}.spark_dates values ${rows}"
        for (def name : ['doris_dates', 'spark_dates']) {
            sql "refresh table ${name}"
            spark_iceberg "refresh table demo.${database}.${name}"
            def projection = 'cast(id as string), cast(d_day as string), cast(d_bucket as string)'
            assertSparkDorisResultEquals(
                    spark_iceberg("select ${projection} from demo.${database}.${name} order by id"),
                    sql("select ${projection} from ${name} order by id"))
            dates.eachWithIndex { d, i ->
                // An unfiltered scan alone cannot detect partition metadata contradicting the file.
                for (def field : ['d_day', 'd_bucket']) {
                    def predicate = "${field} = date '${d}'"
                    def actual = spark_iceberg("select id from demo.${database}.${name} where ${predicate}")
                    assertEquals([[i]].toString(), actual.toString())
                    assertSparkDorisResultEquals(actual, sql("select id from ${name} where ${predicate}"))
                }
            }
        }
        def partitionQuery = { name ->
            spark_iceberg("""select cast(partition.d_day_day as string),
                cast(partition.d_bucket_bucket as string)
                from demo.${database}.${name}.partitions order by 1, 2""")
        }
        assertEquals(partitionQuery('spark_dates').toString(), partitionQuery('doris_dates').toString())
    } finally {
        sql "drop database if exists ${database} force"
        sql "switch internal"
        sql "drop catalog if exists ${catalog}"
    }
}
