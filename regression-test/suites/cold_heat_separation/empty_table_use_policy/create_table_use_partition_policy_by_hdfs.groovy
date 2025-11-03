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

suite("create_table_use_partition_policy_hdfs") {
    def cooldown_ttl = "10"

    def create_table_partition_use_not_create_policy = try_sql """
        CREATE TABLE IF NOT EXISTS create_table_partition_use_not_create_policy_hdfs
        (
            k1 DATE,
            k2 INT,
            V1 VARCHAR(2048) REPLACE
        ) PARTITION BY RANGE (k1) (
            PARTITION p1 VALUES LESS THAN ("2022-01-01") ("storage_policy" = "not_exist_policy_1" ,"replication_num"="1"),
            PARTITION p2 VALUES LESS THAN ("2022-02-01") ("storage_policy" = "not_exist_policy_2" ,"replication_num"="1")
        ) DISTRIBUTED BY HASH(k2) BUCKETS 1;
    """

    // errCode = 2, detailMessage = Storage policy does not exist. name: not_exist_policy_1
    assertEquals(create_table_partition_use_not_create_policy, null)

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

    if (!storage_exist.call("test_create_table_partition_use_policy_1_hdfs")) {
        def create_hdfs_resource = try_sql """
            CREATE RESOURCE IF NOT EXISTS "test_create_table_partition_use_resource_1_hdfs"
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
            CREATE STORAGE POLICY IF NOT EXISTS test_create_table_partition_use_policy_1_hdfs
            PROPERTIES(
            "storage_resource" = "test_create_table_partition_use_resource_1_hdfs",
            "cooldown_ttl" = "$cooldown_ttl"
            );
        """
        assertEquals(storage_exist.call("test_create_table_partition_use_policy_1_hdfs"), true)
    }


    if (!storage_exist.call("test_create_table_partition_use_policy_2_hdfs")) {
        def create_hdfs_resource = try_sql """
            CREATE RESOURCE IF NOT EXISTS "test_create_table_partition_use_resource_2_hdfs"
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
            CREATE STORAGE POLICY IF NOT EXISTS test_create_table_partition_use_policy_2_hdfs
            PROPERTIES(
            "storage_resource" = "test_create_table_partition_use_resource_2_hdfs",
            "cooldown_ttl" = "$cooldown_ttl"
            );
        """
        assertEquals(storage_exist.call("test_create_table_partition_use_policy_2_hdfs"), true)
    }

    // success
    def create_table_partition_use_created_policy = try_sql """
        CREATE TABLE IF NOT EXISTS create_table_partition_use_created_policy_hdfs
        (
            k1 DATE,
            k2 INT,
            V1 VARCHAR(2048) REPLACE
        ) PARTITION BY RANGE (k1) (
            PARTITION p1 VALUES LESS THAN ("2022-01-01") ("storage_policy" = "test_create_table_partition_use_policy_1_hdfs" ,"replication_num"="1"),
            PARTITION p2 VALUES LESS THAN ("2022-02-01") ("storage_policy" = "test_create_table_partition_use_policy_2_hdfs" ,"replication_num"="1")
        ) DISTRIBUTED BY HASH(k2) BUCKETS 1;
    """

    assertEquals(create_table_partition_use_created_policy.size(), 1);

    sql """
    DROP TABLE create_table_partition_use_created_policy_hdfs
    """

    def create_table_partition_use_created_policy_1 = try_sql """
        CREATE TABLE IF NOT EXISTS create_table_partition_use_created_policy_1_hdfs
        (
            k1 DATEV2,
            k2 INT,
            V1 VARCHAR(2048) REPLACE
        ) PARTITION BY RANGE (k1) (
            PARTITION p1 VALUES LESS THAN ("2022-01-01") ("storage_policy" = "test_create_table_partition_use_policy_1_hdfs" ,"replication_num"="1"),
            PARTITION p2 VALUES LESS THAN ("2022-02-01") ("storage_policy" = "test_create_table_partition_use_policy_2_hdfs" ,"replication_num"="1")
        ) DISTRIBUTED BY HASH(k2) BUCKETS 1;
    """

    assertEquals(create_table_partition_use_created_policy_1.size(), 1);

    sql """
    DROP TABLE create_table_partition_use_created_policy_1_hdfs
    """

    def create_table_partition_use_created_policy_2 = try_sql """
        CREATE TABLE IF NOT EXISTS create_table_partition_use_created_policy_2_hdfs
        (
            k1 DATETIMEV2(3),
            k2 INT,
            V1 VARCHAR(2048) REPLACE
        ) PARTITION BY RANGE (k1) (
            PARTITION p1 VALUES LESS THAN ("2022-01-01 00:00:00.111") ("storage_policy" = "test_create_table_partition_use_policy_1_hdfs" ,"replication_num"="1"),
            PARTITION p2 VALUES LESS THAN ("2022-02-01 00:00:00.111") ("storage_policy" = "test_create_table_partition_use_policy_2_hdfs" ,"replication_num"="1")
        ) DISTRIBUTED BY HASH(k2) BUCKETS 1;
    """

    assertEquals(create_table_partition_use_created_policy_2.size(), 1);

    sql """
    DROP TABLE create_table_partition_use_created_policy_2_hdfs;
    """
    sql """
    DROP STORAGE POLICY test_create_table_partition_use_policy_1_hdfs;
    """
    sql """
    DROP STORAGE POLICY test_create_table_partition_use_policy_2_hdfs;
    """
    sql """
    DROP RESOURCE test_create_table_partition_use_resource_1_hdfs
    """
    sql """
    DROP RESOURCE test_create_table_partition_use_resource_2_hdfs
    """
}

