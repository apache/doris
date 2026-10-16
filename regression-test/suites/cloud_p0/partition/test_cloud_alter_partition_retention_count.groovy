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

suite("test_cloud_alter_partition_retention_count", "p0") {
    sql "DROP TABLE IF EXISTS test_cloud_alter_partition_retention_count"
    sql """
        CREATE TABLE test_cloud_alter_partition_retention_count (
            k1 DATETIME NOT NULL
        )
        AUTO PARTITION BY RANGE (date_trunc(k1, 'day')) ()
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """

    sql """
        ALTER TABLE test_cloud_alter_partition_retention_count
        SET ("partition.retention_count" = "3")
    """

    sql "DROP TABLE IF EXISTS test_cloud_alter_partition_retention_count_with_dynamic"
    sql """
        CREATE TABLE test_cloud_alter_partition_retention_count_with_dynamic (
            k1 DATETIME NOT NULL
        )
        AUTO PARTITION BY RANGE (date_trunc(k1, 'day')) ()
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "dynamic_partition.enable" = "true",
            "dynamic_partition.time_unit" = "DAY",
            "dynamic_partition.start" = "-3",
            "dynamic_partition.end" = "3",
            "dynamic_partition.prefix" = "p",
            "dynamic_partition.buckets" = "1",
            "replication_num" = "1"
        )
    """

    test {
        sql """
            ALTER TABLE test_cloud_alter_partition_retention_count_with_dynamic
            SET ("partition.retention_count" = "3")
        """
        exception "Can not use partition.retention_count and dynamic_partition properties at the same time"
    }
}
