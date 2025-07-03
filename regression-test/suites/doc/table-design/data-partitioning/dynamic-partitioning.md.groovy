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

import org.junit.jupiter.api.Assertions;

suite("docs/table-design/data-partitioning/dynamic-partitioning.md") {
    try {
        sql "drop table if exists tbl1"
        sql """
        CREATE TABLE tbl1
        (
            k1 DATE
        )
        PARTITION BY RANGE(k1) ()
        DISTRIBUTED BY HASH(k1)
        PROPERTIES
        (
            "dynamic_partition.enable" = "true",
            "dynamic_partition.time_unit" = "DAY",
            "dynamic_partition.start" = "-7",
            "dynamic_partition.end" = "3",
            "dynamic_partition.prefix" = "p",
            "dynamic_partition.buckets" = "32",
            "replication_num" = "1"
        )
        """

        sql "drop table if exists tbl1"
        sql """
        CREATE TABLE tbl1
        (
            k1 DATETIME,
        )
        PARTITION BY RANGE(k1) ()
        DISTRIBUTED BY HASH(k1)
        PROPERTIES
        (
            "dynamic_partition.enable" = "true",
            "dynamic_partition.time_unit" = "WEEK",
            "dynamic_partition.start" = "-2",
            "dynamic_partition.end" = "2",
            "dynamic_partition.prefix" = "p",
            "dynamic_partition.buckets" = "8",
            "replication_num" = "1"
        )
        """

        sql "drop table if exists tbl1"
        sql """
        CREATE TABLE tbl1
        (
            k1 DATE
        )
        PARTITION BY RANGE(k1) ()
        DISTRIBUTED BY HASH(k1)
        PROPERTIES
        (
            "dynamic_partition.enable" = "true",
            "dynamic_partition.time_unit" = "MONTH",
            "dynamic_partition.end" = "2",
            "dynamic_partition.prefix" = "p",
            "dynamic_partition.buckets" = "8",
            "dynamic_partition.start_day_of_month" = "3",
            "replication_num" = "1"
        )
        """

        sql "SHOW DYNAMIC PARTITION TABLES"
        sql """ ADMIN SET FRONTEND CONFIG ("dynamic_partition_enable" = "true") """
        cmd """ curl --location-trusted -u ${context.config.jdbcUser}:${context.config.jdbcPassword} -XGET http://${context.config.feHttpAddress}/api/_set_config?dynamic_partition_enable=true """

        sql """ ADMIN SET FRONTEND CONFIG ("dynamic_partition_check_interval_seconds" = "7200") """
        cmd """ curl --location-trusted -u ${context.config.jdbcUser}:${context.config.jdbcPassword} -XGET http://${context.config.feHttpAddress}/api/_set_config?dynamic_partition_check_interval_seconds=432000 """
    } catch (Throwable t) {
        Assertions.fail("examples in docs/table-design/data-partitioning/dynamic-partitioning.md failed to exec, please fix it", t)
    }
}
