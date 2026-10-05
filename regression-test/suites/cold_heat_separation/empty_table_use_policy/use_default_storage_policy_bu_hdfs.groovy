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

suite("use_default_storage_policy_hdfs") {
    try_sql """
    DROP TABLE use_default_storage_policy_hdfs;
    """
    try_sql """
    DROP STORAGE POLICY default_storage_policy_hdfs
    """
    try_sql """
    DROP RESOURCE default_hdfs_resource_hdfs
    """

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

    if (!storage_exist.call("default_storage_policy_hdfs")) {
        def create_table_use_default_policy_but_not_set_default_policy_result = try_sql """
            CREATE TABLE IF NOT EXISTS use_default_storage_policy_hdfs 
            ( k1 DATE, k2 INT, V1 VARCHAR(2048) REPLACE ) 
            PARTITION BY RANGE (k1) 
            ( 
                PARTITION p1 VALUES LESS THAN ("2022-01-01") ("storage_policy" = "default_storage_policy_hdfs", "replication_num"="1"), 
                PARTITION p2 VALUES LESS THAN ("2022-02-01") ("storage_policy" = "default_storage_policy_hdfs", "replication_num"="1") 
            ) DISTRIBUTED BY HASH(k2) BUCKETS 1;
        """
        // errCode = 2, detailMessage = Use default storage policy, but not give s3 info, please use alter resource to add default storage policy S3 info.
        assertEquals(create_table_use_default_policy_but_not_set_default_policy_result, null);

        def create_hdfs_resource = try_sql """
            CREATE RESOURCE IF NOT EXISTS "default_hdfs_resource_hdfs"
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
            CREATE STORAGE POLICY IF NOT EXISTS default_storage_policy_hdfs PROPERTIES(
                "storage_resource" = "default_hdfs_resource_hdfs",
                "cooldown_ttl" = "1008611"
            );
        """
        assertEquals(storage_exist.call("default_storage_policy_hdfs"), true)
    }

    def create_table_use_default_policy_has_set_default_policy_result = try_sql """
        CREATE TABLE IF NOT EXISTS use_default_storage_policy_hdfs 
        ( k1 DATE, k2 INT, V1 VARCHAR(2048) REPLACE ) 
        PARTITION BY RANGE (k1) 
        ( 
            PARTITION p1 VALUES LESS THAN ("2022-01-01") ("storage_policy" = "default_storage_policy_hdfs", "replication_num"="1"), 
            PARTITION p2 VALUES LESS THAN ("2022-02-01") ("storage_policy" = "default_storage_policy_hdfs", "replication_num"="1") 
        ) DISTRIBUTED BY HASH(k2) BUCKETS 1;
    """
    // success
    assertEquals(create_table_use_default_policy_has_set_default_policy_result.size(), 1)

    // you can change default_storage_policy's policy property, such as ak、sk,
    // so table create_table_not_have_policy will use AWS_ACCESS_KEY = "has_been_changed"
    def modify_storage_policy_property_result_1 = try_sql """
        ALTER RESOURCE "default_hdfs_resource_hdfs" PROPERTIES("AWS_ACCESS_KEY" = "has_been_changed");
    """

    sql """
    DROP TABLE use_default_storage_policy_hdfs;
    """
    sql """
    DROP STORAGE POLICY default_storage_policy_hdfs
    """
    sql """
    DROP RESOURCE default_hdfs_resource_hdfs
    """
}

