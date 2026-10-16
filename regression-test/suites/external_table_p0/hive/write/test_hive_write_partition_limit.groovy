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

suite("test_hive_write_partition_limit",
        "p0,external,hive,external_docker,external_docker_hive,nonConcurrent") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable hive test")
        return
    }
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    sql """drop catalog if exists test_hive_write_partition_limit"""
    setHivePrefix("hive2")
    String hmsPort = context.config.otherConfigs.get("hive2HmsPort")
    sql """create catalog test_hive_write_partition_limit properties (
        "type" = "hms",
        "hive.metastore.uris" = "thrift://${externalEnvIp}:${hmsPort}",
        "hadoop.username" = "hive"
    )"""
    hive_docker """drop table if exists hive_write_partition_limit"""
    hive_docker """create table hive_write_partition_limit (id int)
        partitioned by (p int) stored as parquet"""
    sql """use test_hive_write_partition_limit.default"""

    // The limit belongs to each writer, so route all VALUES rows through one writer.
    sql """set parallel_pipeline_task_num = 1"""
    setBeConfigTemporary(["table_sink_partition_write_max_partition_nums_per_writer": "2"]) {
        // Exactly two partitions are allowed; the next partition must fail with actionable context.
        sql """insert into hive_write_partition_limit values (0, 0), (1, 1)"""
        test {
            sql """insert into hive_write_partition_limit values (2, 2), (3, 3), (4, 4)"""
            exception "[INVALID_ARGUMENT]Too many open partitions for Hive writer: " +
                    "open partitions=2, attempted partitions=3, limit=2."
        }
    }
    // The advertised configuration is effective: the same three-partition write can now succeed.
    setBeConfigTemporary(["table_sink_partition_write_max_partition_nums_per_writer": "3"]) {
        sql """insert into hive_write_partition_limit values (2, 2), (3, 3), (4, 4)"""
    }
}
