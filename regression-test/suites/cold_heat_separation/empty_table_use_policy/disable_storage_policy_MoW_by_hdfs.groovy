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

suite("disable_storage_policy_MoW_hdfs"){
    def s3_source_name = "default_s3_source_test"
    def hdfs_source_name = "default_hdfs_source_test"
    def storage_policy_name = "default_storage_policy_test_hdfs"

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

    if(!storage_exist.call("${storage_policy_name}")){
        def create_hdfs_resource = sql """
            CREATE RESOURCE IF NOT EXISTS "${hdfs_source_name}"
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
        def create_storage_policy = sql """
            CREATE STORAGE POLICY IF NOT EXISTS ${storage_policy_name} PROPERTIES(
                "storage_resource" = "${hdfs_source_name}",
                "cooldown_ttl" = "1008611"
            );
        """

        create_hdfs_resource
        create_storage_policy
    }

    def table_name_test_1 = "disable_storage_policy_on_mow1_hdfs"
    def table_name_test_2 = "disable_storage_policy_on_mow2_hdfs"
    def table_name_test_3 = "disable_storage_policy_on_mow3_hdfs"
    //Test case I. Should panic when creates MoW table with storage policy
    test{
        sql """
            CREATE TABLE IF NOT EXISTS ${table_name_test_1} (
                k1 DATE,
                k2 INT,
                V1 VARCHAR(2048) 
            ) ENGINE = OLAP
            UNIQUE KEY(k1,k2)
            PARTITION BY RANGE (k1) 
            ( 
                PARTITION p1 VALUES LESS THAN ("2022-01-01") ("storage_policy" = "${storage_policy_name}", "replication_num"="1"), 
                PARTITION p2 VALUES LESS THAN ("2022-02-01") ("storage_policy" = "${storage_policy_name}", "replication_num"="1") 
            ) DISTRIBUTED BY HASH(k2) BUCKETS 1
            PROPERTIES(
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true"
            )  
        """
        exception "with storage policy(${storage_policy_name})"
    }

    //Test case II. Should panic when sets storage policy to MoW table or its partitions
    sql """
        CREATE TABLE IF NOT EXISTS ${table_name_test_2} (
            id INT,
            v1 INT
        ) ENGINE = OLAP
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES(
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    test {
        sql """
            ALTER TABLE ${table_name_test_2} MODIFY PARTITION(*) SET("storage_policy" = "${storage_policy_name}");
        """
        exception "with storage policy(${storage_policy_name})"
    }

    test {
        sql """
            ALTER TABLE ${table_name_test_2} SET ("storage_policy" = "${storage_policy_name}");
        """
        exception "with storage policy(${storage_policy_name})"
    }

    //Test case III. Should panic when creates MoW table(without partitions) with storage policy
    test {
        sql """
            CREATE TABLE IF NOT EXISTS ${table_name_test_3} (
                k1 INT,
                V1 INT
            ) ENGINE = OLAP
            UNIQUE KEY(k1)
            DISTRIBUTED BY HASH (k1) BUCKETS 1
            PROPERTIES(
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "storage_policy" = "${storage_policy_name}"
            );
        """
        exception "with storage policy(${storage_policy_name})"
    }

    //clean resource
    sql""" DROP TABLE IF EXISTS ${table_name_test_1} """
    sql""" DROP TABLE IF EXISTS ${table_name_test_2} """
    sql""" DROP TABLE IF EXISTS ${table_name_test_3} """
}

