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

import java.text.SimpleDateFormat;
import java.util.Date;

suite("add_table_policy_by_modify_partition_hdfs") {
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    Date date = new Date(System.currentTimeMillis() + 3600000)
    def cooldownTime = format.format(date)

    sql """DROP TABLE IF EXISTS create_table_partition_hdfs"""
    def create_table_partition_not_have_policy_result = try_sql """
        CREATE TABLE IF NOT EXISTS `create_table_partition_hdfs` (
        `lo_orderkey` bigint(20) NOT NULL COMMENT "",
        `lo_linenumber` bigint(20) NOT NULL COMMENT "",
        `lo_custkey` int(11) NOT NULL COMMENT "",
        `lo_partkey` int(11) NOT NULL COMMENT "",
        `lo_suppkey` int(11) NOT NULL COMMENT "",
        `lo_orderdate` int(11) NOT NULL COMMENT "",
        `lo_orderpriority` varchar(16) NOT NULL COMMENT "",
        `lo_shippriority` int(11) NOT NULL COMMENT "",
        `lo_quantity` bigint(20) NOT NULL COMMENT "",
        `lo_extendedprice` bigint(20) NOT NULL COMMENT "",
        `lo_ordtotalprice` bigint(20) NOT NULL COMMENT "",
        `lo_discount` bigint(20) NOT NULL COMMENT "",
        `lo_revenue` bigint(20) NOT NULL COMMENT "",
        `lo_supplycost` bigint(20) NOT NULL COMMENT "",
        `lo_tax` bigint(20) NOT NULL COMMENT "",
        `lo_commitdate` bigint(20) NOT NULL COMMENT "",
        `lo_shipmode` varchar(11) NOT NULL COMMENT ""
        )
        PARTITION BY RANGE(`lo_orderdate`)
        (PARTITION p1992 VALUES [("-2147483648"), ("19930101")),
        PARTITION p1998 VALUES [("19980101"), ("19990101")) )
        DISTRIBUTED BY HASH(`lo_orderkey`) BUCKETS 4
        PROPERTIES (
        "replication_num" = "1"
        );
    """
    assertEquals(create_table_partition_not_have_policy_result.size(), 1);

    // support
    // 1. ALTER TABLE create_table_partition MODIFY PARTITION p1992 SET("storage_policy"="not_exist_policy");
    // 2. ALTER TABLE create_table_partition MODIFY PARTITION (p1992, p1998) SET("storage_policy"="not_exist_policy");
    // 3. ALTER TABLE create_table_partition MODIFY PARTITION (*) SET("storage_policy"="not_exist_policy");
    def alter_table_partition_use_not_exist_policy_result = try_sql """
        ALTER TABLE create_table_partition_hdfs MODIFY PARTITION p1992 SET("storage_policy"="not_exist_policy");
    """
    // errCode = 2, detailMessage = Resource does not exist. name: not_exist_policy
    assertEquals(alter_table_partition_use_not_exist_policy_result, null);

    def storage_exist = { name ->
        def show_storage_policy = sql """
        SHOW STORAGE POLICY;
        """
        for(iter in show_storage_policy){
            if(name == iter[0]){
                return true;
            }
        }
        return false;
    }

    def create_hdfs_resource = try_sql """
        CREATE RESOURCE IF NOT EXISTS "test_modify_partition_table_use_resource_hdfs"
        PROPERTIES(
            "type"="hdfs",
            "fs.defaultFS"="127.0.0.1:8120",
            "hadoop.username"="hive",
            "hadoop.password"="hive",
            "dfs.nameservices" = "my_ha",
            "dfs.ha.namenodes.my_ha" = "my_namenode1, my_namenode2",
            "dfs.namenode.rpc-address.my_ha.my_namenode1" = "127.0.0.1:10000",
            "dfs.namenode.rpc-address.my_ha.my_namenode2" = "127.0.0.1:10000",
            "dfs.client.failover.proxy.provider" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
        );
    """
    def create_succ_1 = try_sql """
        CREATE STORAGE POLICY IF NOT EXISTS created_create_table_partition_alter_policy_hdfs
        PROPERTIES(
        "storage_resource" = "test_modify_partition_table_use_resource_hdfs",
        "cooldown_datetime" = "$cooldownTime"
        );
    """
    sql """ALTER STORAGE POLICY created_create_table_partition_alter_policy_hdfs PROPERTIES("cooldown_datetime" = "$cooldownTime")"""
    assertEquals(storage_exist.call("created_create_table_partition_alter_policy_hdfs"), true)

    def alter_table_partition_try_again_result = try_sql """
        ALTER TABLE create_table_partition_hdfs MODIFY PARTITION (*) SET("storage_policy"="created_create_table_partition_alter_policy_hdfs");
    """
    // OK
    assertEquals(alter_table_partition_try_again_result.size(), 1);

    try_sql """
    CREATE STORAGE POLICY IF NOT EXISTS tmp2_hdfs
        PROPERTIES(
        "storage_resource" = "test_modify_partition_table_use_resource_hdfs",
        "cooldown_datetime" = "$cooldownTime"
        );
    """

    sql """
    CREATE TABLE create_table_partion_use_created_policy_test_hdfs
    (
    k1 DATE,
    k2 INT,
    V1 VARCHAR(2048) REPLACE
    ) PARTITION BY RANGE (k1) (
    PARTITION p1 VALUES LESS THAN ("2022-01-01") ("storage_policy" = "tmp2_hdfs" ,"replication_num"="1"),
    PARTITION p2 VALUES LESS THAN ("2022-02-01") ("storage_policy" = "tmp2_hdfs" ,"replication_num"="1")
    ) DISTRIBUTED BY HASH(k2) BUCKETS 1 
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "storage_policy" = "created_create_table_partition_alter_policy_hdfs"
    );
    """

    // Test that the partition's specified policy would be covered by the table's policy
    def partitions = sql """
    show partitions from create_table_partion_use_created_policy_test_hdfs
    """

    for (par in partitions) {
        assertTrue(par[12] == "created_create_table_partition_alter_policy_hdfs")
    }

    sql """
    DROP TABLE IF EXISTS create_table_partition_hdfs;
    """
    sql """
    DROP TABLE IF EXISTS create_table_partion_use_created_policy_test_hdfs;
    """
    sql """
    DROP STORAGE POLICY created_create_table_partition_alter_policy_hdfs
    """
    sql """
    DROP STORAGE POLICY tmp2_hdfs
    """
    sql """
    DROP RESOURCE test_modify_partition_table_use_resource_hdfs
    """
}

