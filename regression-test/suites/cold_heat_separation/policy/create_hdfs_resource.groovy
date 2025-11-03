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

suite("create_hdfs_policy") {
    def has_created_1 = sql """
        SHOW RESOURCES WHERE NAME = "crete_policy_1_hdfs";
    """

    // doesn't have name crete_policy_1 resources
    // normal
    if(has_created_1.size() == 0) {
        sql """
        CREATE RESOURCE IF NOT EXISTS "crete_policy_1_hdfs"
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

        def create_sucess = sql """
             SHOW RESOURCES WHERE NAME = "crete_policy_1_hdfs";
        """
        assertEquals(create_sucess.size(), 10)

        def failed_cannot_create_duplicate_name_resources = try_sql """
            CREATE RESOURCE "crete_policy_1_hdfs"
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

        // errCode = 2, detailMessage = Resource(crete_policy_1) already exist
        assertEquals(failed_cannot_create_duplicate_name_resources, null)

        // test if we could create one polycy with the same name but
        // with different properties
        def failed_cannot_create_duplicate_name_resources_with_different_properties = try_sql """
            CREATE RESOURCE "crete_policy_1_hdfs"
            PROPERTIES(
            "type"="hdfs",
            "fs.defaultFS"="127.0.0.1:8120",
            "hadoop.username"="hive1",
            "hadoop.password"="hive1",
            "dfs.nameservices" = "my_ha",
            "dfs.ha.namenodes.my_ha" = "my_namenode1, my_namenode2",
            "dfs.namenode.rpc-address.my_ha.my_namenode1" = "127.0.0.1:10000",
            "dfs.namenode.rpc-address.my_ha.my_namenode2" = "127.0.0.1:10000",
            "dfs.client.failover.proxy.provider" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
           );
        """
        assertEquals(failed_cannot_create_duplicate_name_resources_with_different_properties, null)

        // delete the policy with the same name then try to
        // create policies with different property each time
        def drop_result = try_sql """
            DROP RESOURCE 'crete_policy_1_hdfs'
        """
        assertEquals(drop_result.size(), 1)

        def create_duplicate_name_resources_different_username = try_sql """
            CREATE RESOURCE "crete_policy_1_hdfs"
            PROPERTIES(
            "type"="hdfs",
            "fs.defaultFS"="127.0.0.1:8120",
            "hadoop.username"="hive1",
            "hadoop.password"="hive1",
            "dfs.nameservices" = "my_ha",
            "dfs.ha.namenodes.my_ha" = "my_namenode1, my_namenode2",
            "dfs.namenode.rpc-address.my_ha.my_namenode1" = "127.0.0.1:10000",
            "dfs.namenode.rpc-address.my_ha.my_namenode2" = "127.0.0.1:10000",
            "dfs.client.failover.proxy.provider" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
            );
        """
        assertEquals(create_duplicate_name_resources_different_username.size(), 1)

        drop_result = try_sql """
            DROP RESOURCE 'crete_policy_1_hdfs'
        """

        assertEquals(drop_result.size(), 1)

        def create_duplicate_name_resources_different_dfs_nameservices = try_sql """
        CREATE RESOURCE "crete_policy_1_hdfs"
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
        assertEquals(create_duplicate_name_resources_different_dfs_nameservices.size(), 1)

        drop_result = try_sql """
            DROP RESOURCE 'crete_policy_1_hdfs'
        """
        assertEquals(drop_result.size(), 1)
    }

    // can't create success, due to missing required items
    def has_created_2 = sql """
        SHOW RESOURCES WHERE NAME = "crete_policy_2_hdfs";
    """
    if (has_created_2.size() == 0) {
        def failed_create_1 = try_sql """
        CREATE RESOURCE "crete_policy_2_hdfs"
        PROPERTIES(
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
        // errCode = 2, detailMessage = Resource type can't be null
        assertEquals(failed_create_1, null)
    }

    if (has_created_2.size() == 0) {
        def failed_create_2 = try_sql """
        CREATE RESOURCE "crete_policy_2_hdfs"
        PROPERTIES(
            "type"="hdfs",
            "hadoop.username"="hive",
            "hadoop.password"="hive",
            "dfs.nameservices" = "my_ha",
            "dfs.ha.namenodes.my_ha" = "my_namenode1, my_namenode2",
            "dfs.namenode.rpc-address.my_ha.my_namenode1" = "127.0.0.1:10000",
            "dfs.namenode.rpc-address.my_ha.my_namenode2" = "127.0.0.1:10000",
            "dfs.client.failover.proxy.provider" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
        );
        """
        // errCode = 2, detailMessage = Missing [fs.defaultFS] in properties.
        assertEquals(failed_create_2, [[0]])
    }

    if (has_created_2.size() == 0) {
        def failed_create_2 = try_sql """
        CREATE RESOURCE "crete_policy_2_hdfs"
        PROPERTIES(
            "type"="hdfs",
            "fs.defaultFS"="127.0.0.1:8120",
            "hadoop.password"="hive",
            "dfs.nameservices" = "my_ha",
            "dfs.ha.namenodes.my_ha" = "my_namenode1, my_namenode2",
            "dfs.namenode.rpc-address.my_ha.my_namenode1" = "127.0.0.1:10000",
            "dfs.namenode.rpc-address.my_ha.my_namenode2" = "127.0.0.1:10000",
            "dfs.client.failover.proxy.provider" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
        );
        """
        // errCode = 2, detailMessage = Missing [hadoop.username] in properties.
        assertEquals(failed_create_2, null)
    }

    if (has_created_2.size() == 0) {
        def failed_create_2 = try_sql """
        CREATE RESOURCE "crete_policy_2_hdfs"
        PROPERTIES(
            "type"="hdfs",
            "fs.defaultFS"="127.0.0.1:8120",
            "hadoop.username"="hive",
            "hadoop.password"="hive",
            "dfs.ha.namenodes.my_ha" = "my_namenode1, my_namenode2",
            "dfs.namenode.rpc-address.my_ha.my_namenode1" = "127.0.0.1:10000",
            "dfs.namenode.rpc-address.my_ha.my_namenode2" = "127.0.0.1:10000",
            "dfs.client.failover.proxy.provider" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
        );
        """
        // errCode = 2, detailMessage = Missing [dfs.nameservices] in properties.
        assertEquals(failed_create_2, null)
    }

    if (has_created_2.size() == 0) {
        def failed_create_2 = try_sql """
        CREATE RESOURCE "crete_policy_2_hdfs"
        PROPERTIES(
            "type"="hdfs",
            "fs.defaultFS"="127.0.0.1:8120",
            "hadoop.username"="hive",
            "hadoop.password"="hive",
            "dfs.nameservices" = "my_ha",
            "dfs.namenode.rpc-address.my_ha.my_namenode1" = "127.0.0.1:10000",
            "dfs.namenode.rpc-address.my_ha.my_namenode2" = "127.0.0.1:10000",
            "dfs.client.failover.proxy.provider" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
       );
        """
        // errCode = 2, detailMessage = Missing [dfs.ha.namenodes.my_ha] in properties.
        assertEquals(failed_create_2, null)
    }

    // create policy


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

    if (!storage_exist.call("testPolicy_10_hdfs")) {
        def create_s3_resource = try_sql """
            CREATE RESOURCE "testPolicy_10_resource_hdfs"
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
            CREATE STORAGE POLICY testPolicy_10_hdfs
            PROPERTIES(
            "storage_resource" = "testPolicy_10_resource_hdfs",
            "cooldown_datetime" = "2025-06-08 00:00:00"
            );
        """
        assertEquals(storage_exist.call("testPolicy_10_hdfs"), true)

        sql"""
        DROP STORAGE POLICY testPolicy_10_hdfs;
        """

        sql"""
        DROP RESOURCE testPolicy_10_resource_hdfs;
        """
    }

    if (!storage_exist.call("testPolicy_11_hdfs")) {
        def create_s3_resource = try_sql """
            CREATE RESOURCE "testPolicy_11_resource_hdfs"
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
            CREATE STORAGE POLICY testPolicy_11_hdfs
            PROPERTIES(
            "storage_resource" = "testPolicy_11_resource_hdfs",
            "cooldown_ttl" = "10086"
            );
        """
        assertEquals(storage_exist.call("testPolicy_11_hdfs"), true)

        sql """
        DROP STORAGE POLICY testPolicy_11_hdfs;
        """

        sql """
        DROP RESOURCE "testPolicy_11_resource_hdfs";
        """
    }

    if (!storage_exist.call("testPolicy_12_hdfs")) {
        def create_s3_resource = try_sql """
            CREATE RESOURCE "testPolicy_12_resource_hdfs"
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
            CREATE STORAGE POLICY testPolicy_12_hdfs
            PROPERTIES(
            "storage_resource" = "testPolicy_12_resource_hdfs",
            "cooldown_ttl" = "10086",
            "cooldown_datetime" = "2025-06-08 00:00:00"
            );
        """
        // errCode = 2, detailMessage = cooldown_datetime and cooldown_ttl can't be set together.
        assertEquals(storage_exist.call("testPolicy_12_hdfs"), false)

        sql """
            DROP RESOURCE "testPolicy_12_resource_hdfs"
        """
    }

    if (!storage_exist.call("testPolicy_13_hdfs")) {
        def create_s3_resource = try_sql """
            CREATE RESOURCE "testPolicy_13_resource_hdfs"
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
            CREATE STORAGE POLICY testPolicy_13_hdfs
            PROPERTIES(
            "storage_resource" = "testPolicy_13_resource_hdfs",
            "cooldown_ttl" = "-10086"
            );
        """
        // errCode = 2, detailMessage = cooldown_ttl must >= 0.
        assertEquals(storage_exist.call("testPolicy_13_hdfs"), false)
        sql """
            DROP RESOURCE "testPolicy_13_resource_hdfs"
        """
    }

    if (!storage_exist.call("testPolicy_14_hdfs")) {
        def create_s3_resource = try_sql """
            CREATE RESOURCE "testPolicy_14_resource_hdfs"
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
            CREATE STORAGE POLICY testPolicy_14_hdfs
            PROPERTIES(
            "storage_resource" = "testPolicy_14_resource_hdfs",
            "cooldown_datetime" = "2025-06-08"
            );
        """
        //  errCode = 2, detailMessage = cooldown_datetime format error: 2022-06-08
        assertEquals(storage_exist.call("testPolicy_14_hdfs"), false)
    }

    if (!storage_exist.call("testPolicy_15_hdfs")) {
        def create_succ_1 = try_sql """
            CREATE STORAGE POLICY testPolicy_15_hdfs
            PROPERTIES(
            "storage_resource" = "s3_resource_not_exist_hdfs",
            "cooldown_datetime" = "2025-06-08 00:00:00"
            );
        """
        //  errCode = 2, detailMessage = storage resource doesn't exist: s3_resource_not_exist_hdfs
        assertEquals(storage_exist.call("testPolicy_15_hdfs"), false)
    }

    if (!storage_exist.call("testPolicy_redundant_name_hdfs")) {
        def create_s3_resource = try_sql """
            CREATE RESOURCE "testPolicy_redundant_name_resource_hdfs"
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
        def create_s3_resource_1 = try_sql """
            CREATE RESOURCE "testPolicy_redundant_name_resource_1_hdfs"
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
            CREATE STORAGE POLICY testPolicy_redundant_name_hdfs
            PROPERTIES(
            "storage_resource" = "testPolicy_redundant_name_resource_hdfs",
            "cooldown_ttl" = "10086"
            );
        """
        try {
            sql """
                CREATE STORAGE POLICY testPolicy_redundant_name_hdfs
                PROPERTIES(
                "storage_resource" = "testPolicy_redundant_name_resource_1_hdfs",
                "cooldown_ttl" = "10086"
                );
            """
        } catch (Exception e) {
            log.info(e.getMessage())
            assertTrue(e.getMessage().contains('already create'))
        }
    }
}

