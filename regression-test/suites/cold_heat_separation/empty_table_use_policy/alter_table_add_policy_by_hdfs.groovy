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

suite("add_table_policy_by_alter_table_hdfs") {
    def create_table_not_have_policy_result = try_sql """
        CREATE TABLE IF NOT EXISTS create_table_not_have_policy_hdfs
        (
            k1 BIGINT,
            k2 LARGEINT,
            v1 VARCHAR(2048)
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH (k1) BUCKETS 3
        PROPERTIES(
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "false"
        );
    """
    assertEquals(create_table_not_have_policy_result.size(), 1);

    def alter_table_use_not_exist_policy_result = try_sql """
        ALTER TABLE create_table_not_have_policy_hdfs set ("storage_policy" = "policy_not_exist");
    """
    //  errCode = 2, detailMessage = Resource does not exist. name: policy_not_exist
    assertEquals(alter_table_use_not_exist_policy_result, null);

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

    if (!storage_exist.call("created_create_table_alter_policy_hdfs")) {
        def create_hdfs_resource = try_sql """
            CREATE RESOURCE IF NOT EXISTS "test_create_alter_table_use_resource_hdfs"
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
            CREATE STORAGE POLICY IF NOT EXISTS created_create_table_alter_policy_hdfs
            PROPERTIES(
            "storage_resource" = "test_create_alter_table_use_resource_hdfs",
            "cooldown_datetime" = "2025-06-08 00:00:00"
            );
        """
        assertEquals(storage_exist.call("created_create_table_alter_policy_hdfs"), true)
    }

    def alter_table_try_again_result = try_sql """
        ALTER TABLE create_table_not_have_policy_hdfs set ("storage_policy" = "created_create_table_alter_policy_hdfs");
    """
    // OK
    assertEquals(alter_table_try_again_result.size(), 1);

    def alter_table_when_table_has_storage_policy_result = try_sql """
        ALTER TABLE create_table_not_have_policy_hdfs set ("storage_policy" = "created_create_table_alter_policy_hdfs");
    """
    // OK
    assertEquals(alter_table_when_table_has_storage_policy_result.size(), 1);

    if (!storage_exist.call("created_create_table_alter_policy_1_hdfs")) {
        def create_hdfs_resource = try_sql """
            CREATE RESOURCE IF NOT EXISTS "test_create_alter_table_use_resource_1_hdfs"
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
            CREATE STORAGE POLICY IF NOT EXISTS created_create_table_alter_policy_1_hdfs
            PROPERTIES(
            "storage_resource" = "test_create_alter_table_use_resource_1_hdfs",
            "cooldown_datetime" = "2025-06-08 00:00:00"
            );
        """
        assertEquals(storage_exist.call("created_create_table_alter_policy_1_hdfs"), true)
    }

    // current not support change storage policy name
    def cannot_modify_exist_storage_policy_table_result = try_sql """
        ALTER TABLE create_table_not_have_policy_hdfs set ("storage_policy" = "created_create_table_alter_policy_1_hdfs");
    """
    // OK
    assertEquals(cannot_modify_exist_storage_policy_table_result.size(), 1);

    // you can change created_create_table_alter_policy's policy cooldown time, cooldown ttl property,
    // by alter storage policy
    def modify_storage_policy_property_result = try_sql """
        ALTER STORAGE POLICY "created_create_table_alter_policy_1_hdfs" PROPERTIES("cooldown_datetime" = "2026-06-08 00:00:00");
    """
    // change s3 resource, ak、sk by alter resource
    def modify_storage_policy_property_result_1 = try_sql """
        ALTER RESOURCE "test_create_alter_table_use_resource_1_hdfs" PROPERTIES("dfs.nameservices" = "has_been_changed");
    """

    sql """
    DROP TABLE create_table_not_have_policy_hdfs;
    """

    sql """
    DROP STORAGE POLICY created_create_table_alter_policy_1_hdfs;
    """

    sql """
    DROP RESOURCE test_create_alter_table_use_resource_1_hdfs;
    """

}

