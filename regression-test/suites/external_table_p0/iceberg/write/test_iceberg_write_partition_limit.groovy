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

suite("test_iceberg_write_partition_limit",
        "p0,external,iceberg,external_docker,external_docker_iceberg,nonConcurrent") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test")
        return
    }
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    sql """drop catalog if exists test_iceberg_write_partition_limit"""
    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    sql """create catalog test_iceberg_write_partition_limit properties (
        "type" = "iceberg",
        "iceberg.catalog.type" = "rest",
        "uri" = "http://${externalEnvIp}:${restPort}",
        "s3.access_key" = "admin",
        "s3.secret_key" = "password",
        "s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
        "s3.region" = "us-east-1"
    )"""
    sql """create database if not exists test_iceberg_write_partition_limit.partition_limit_db"""
    sql """use test_iceberg_write_partition_limit.partition_limit_db"""
    sql """drop table if exists iceberg_write_partition_limit"""
    sql """create table iceberg_write_partition_limit (id int, p int)
        partition by list (p) ()
        properties ("format-version" = "2", "write.format.default" = "parquet")"""

    // The limit belongs to each writer, so route all VALUES rows through one writer.
    sql """set parallel_pipeline_task_num = 1"""
    setBeConfigTemporary(["table_sink_partition_write_max_partition_nums_per_writer": "2"]) {
        // Exactly two partitions are allowed; the next partition must fail with actionable context.
        sql """insert into iceberg_write_partition_limit values (0, 0), (1, 1)"""
        test {
            sql """insert into iceberg_write_partition_limit values (2, 2), (3, 3), (4, 4)"""
            exception "[INVALID_ARGUMENT]Too many open partitions for Iceberg writer: " +
                    "open partitions=2, attempted partitions=3, limit=2."
        }
    }
    // The advertised configuration is effective: the same three-partition write can now succeed.
    setBeConfigTemporary(["table_sink_partition_write_max_partition_nums_per_writer": "3"]) {
        sql """insert into iceberg_write_partition_limit values (2, 2), (3, 3), (4, 4)"""
    }
}
