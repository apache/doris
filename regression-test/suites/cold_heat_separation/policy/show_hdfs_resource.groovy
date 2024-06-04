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

suite("show_hdfs_policy") {
    def get_storage_policy = { name ->
        def show_storage_policy = sql """
        SHOW STORAGE POLICY;
        """
        for(policy in show_storage_policy){
            if(name == policy[0]){
                return policy;
            }
        }
        return [];
    }

    if (get_storage_policy.call("showPolicy_1_policy_hdfs").isEmpty()){
        def create_s3_resource = try_sql """
            CREATE RESOURCE "showPolicy_1_resource_hdfs"
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
            CREATE STORAGE POLICY showPolicy_1_policy_hdfs
            PROPERTIES(
            "storage_resource" = "showPolicy_1_resource_hdfs",
            "cooldown_datetime" = "2022-06-08 00:00:00"
            );
        """
    }
    def show_result = sql """
        SHOW STORAGE POLICY;
    """

    def storage_policy = get_storage_policy("showPolicy_1_policy_hdfs")
    assertEquals(storage_policy.size(), 7)
    assertEquals(storage_policy[4], "showPolicy_1_resource_hdfs")
    assertEquals(storage_policy[5], "2022-06-08 00:00:00")
    sql """
    DROP STORAGE POLICY showPolicy_1_policy_hdfs;
    """

    sql """
    DROP RESOURCE "showPolicy_1_resource_hdfs";
    """
}

