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

suite("create_table_use_policy_hdfs") {
    def cooldown_ttl = "10"

    def create_table_use_not_create_policy = try_sql """
        CREATE TABLE IF NOT EXISTS create_table_use_not_create_policy_hdfs
        (
            k1 BIGINT,
            k2 LARGEINT,
            v1 VARCHAR(2048)
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH (k1) BUCKETS 3
        PROPERTIES(
            "storage_policy" = "not_exist_policy",
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "false"
        );
    """

    // errCode = 2, detailMessage = Storage policy does not exist. name: not_exist_policy
    assertEquals(create_table_use_not_create_policy, null)

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
        CREATE RESOURCE IF NOT EXISTS "test_create_table_use_resource_hdfs"
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
        CREATE STORAGE POLICY IF NOT EXISTS test_create_table_use_policy_hdfs
        PROPERTIES(
        "storage_resource" = "test_create_table_use_resource_hdfs",
        "cooldown_ttl" = "$cooldown_ttl"
        );
    """

    sql """ALTER STORAGE POLICY test_create_table_use_policy_hdfs PROPERTIES("cooldown_ttl" = "$cooldown_ttl")"""

    assertEquals(storage_exist.call("test_create_table_use_policy_hdfs"), true)

    // success
    def create_table_use_created_policy = try_sql """
        CREATE TABLE IF NOT EXISTS create_table_use_created_policy_hdfs
        (
            k1 BIGINT,
            k2 LARGEINT,
            v1 VARCHAR(2048)
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH (k1) BUCKETS 3
        PROPERTIES(
            "storage_policy" = "test_create_table_use_policy_hdfs",
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "false"
        );
    """

    assertEquals(create_table_use_created_policy.size(), 1);

    sql """
    DROP TABLE IF EXISTS create_table_use_created_policy_hdfs
    """
    sql """
    DROP STORAGE POLICY test_create_table_use_policy_hdfs
    """
    sql """
    DROP RESOURCE test_create_table_use_resource_hdfs
    """

}

