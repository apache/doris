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

suite("test_show_create_table_with_storage_policy") {
    def tableName = "test_show_create_table_with_storage_policy"
    def resourceName = "remote_hdfs"
    def storagePolicyName = "ods_ods_invalid_events"

    sql """ DROP TABLE IF EXISTS ${tableName} """

    sql """
        CREATE RESOURCE "${resourceName}" PROPERTIES (
        "type"="hdfs",
        "fs.defaultFS"="fs_host:default_fs_port",
        "hadoop.username"="hive",
        "hadoop.password"="hive",
        "dfs.nameservices" = "my_ha",
        "dfs.ha.namenodes.my_ha" = "my_namenode1, my_namenode2",
        "dfs.namenode.rpc-address.my_ha.my_namenode1" = "nn1_host:rpc_port",
        "dfs.namenode.rpc-address.my_ha.my_namenode2" = "nn2_host:rpc_port",
        "dfs.client.failover.proxy.provider.my_ha" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
        );
    """

    sql """
        CREATE STORAGE POLICY ${storagePolicyName} PROPERTIES (
        "storage_resource" = "${resourceName}",
        "cooldown_ttl" = "300"
        );
    """

    sql """
     CREATE TABLE ${tableName} (
      `insert_date` date NULL COMMENT '对应ts_pretty,取yyyy-MM-dd',
      `err_type` INT NULL COMMENT '错误类型',
      `line` TEXT NULL COMMENT '数据列',
      `received_at` datetime NULL DEFAULT CURRENT_TIMESTAMP COMMENT '接收到数据的事件'
    ) ENGINE=OLAP
    DUPLICATE KEY(`insert_date`, `err_type`)
    COMMENT 'OLAP'
    PARTITION BY RANGE(`insert_date`)
    (PARTITION p20240806 VALUES [('2024-08-06'), ('2024-08-07')),
    PARTITION p20240807 VALUES [('2024-08-07'), ('2024-08-08')),
    PARTITION p20240808 VALUES [('2024-08-08'), ('2024-08-09')),
    PARTITION p20240809 VALUES [('2024-08-09'), ('2024-08-10')),
    PARTITION p20240810 VALUES [('2024-08-10'), ('2024-08-11')),
    PARTITION p20240811 VALUES [('2024-08-11'), ('2024-08-12')),
    PARTITION p20240812 VALUES [('2024-08-12'), ('2024-08-13')),
    PARTITION p20240821 VALUES [('2024-08-21'), ('2024-08-22')))
    DISTRIBUTED BY HASH(`insert_date`, `err_type`) BUCKETS AUTO
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "is_being_synced" = "false",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.time_zone" = "Europe/London",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "7",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.replication_allocation" = "tag.location.default: 1",
    "dynamic_partition.buckets" = "2",
    "dynamic_partition.create_history_partition" = "true",
    "dynamic_partition.history_partition_num" = "8",
    "dynamic_partition.hot_partition_num" = "0",
    "dynamic_partition.reserved_history_periods" = "NULL",
    "dynamic_partition.storage_policy" = "${storagePolicyName}",
    "storage_format" = "V2",
    "light_schema_change" = "true",
    "disable_auto_compaction" = "false",
    "enable_single_replica_compaction" = "false"
    );
    """

    def ret = sql """ SHOW CREATE TABLE ${tableName} """
    String createSql = ret[0][1]

    ret = sql """ SHOW PARTITIONS FROM ${tableName} """
    assertEquals(ret[9][12], storagePolicyName)

    sql """ DROP TABLE IF EXISTS ${tableName} """
    // create table successfully with stmt from show create table
    sql createSql
}
