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

suite("alter_hdfs_policy") {

    def check_resource_delete_if_exist = { resource_name ->
        def has_resouce = sql """
            SHOW RESOURCES WHERE NAME = "${resource_name}";
        """
        if (has_resouce.size() > 0) {
            sql """
                DROP RESOURCE '${resource_name}';
            """
        }
    }

    // create resource
    def create_source = { resource_name ->
        sql """
        CREATE RESOURCE IF NOT EXISTS "${resource_name}"
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
    }

    def alter_resource_change_property = { resource_name ->
        // support
        def alter_result_succ_1 = try_sql """
            ALTER RESOURCE "${resource_name}" PROPERTIES("fs.defaultFS" = "1111");
        """

        def alter_result_succ_2 = try_sql """
            ALTER RESOURCE "${resource_name}" PROPERTIES("hadoop.username" = "2222");
        """

        def alter_result_succ_5 = try_sql """
            ALTER RESOURCE "${resource_name}" PROPERTIES("hadoop.password" = "5555");
        """

        def alter_result_succ_6 = try_sql """
            ALTER RESOURCE "${resource_name}" PROPERTIES("dfs.nameservices" = "6666");
        """

        def alter_result_succ_7 = try_sql """
            ALTER RESOURCE "${resource_name}" PROPERTIES("dfs.ha.namenodes.my_ha" = "7777");
        """

        // errCode = 2, detailMessage = current not support modify property : dfs.namenode.rpc-address.my_ha.my_namenode1
        def alter_result_fail_1 = try_sql """
            ALTER RESOURCE "${resource_name}" PROPERTIES("dfs.namenode.rpc-address.my_ha.my_namenode1" = "8888");
        """

        // errCode = 2, detailMessage = current not support modify property : dfs.namenode.rpc-address.my_ha.my_namenode2
        def alter_result_fail_2 = try_sql """
            ALTER RESOURCE "${resource_name}" PROPERTIES("dfs.namenode.rpc-address.my_ha.my_namenode2" = "9999");
        """

        // errCode = 2, detailMessage = current not support modify property : dfs.client.failover.proxy.provider
        def alter_result_fail_3 = try_sql """
            ALTER RESOURCE "${resource_name}" PROPERTIES("dfs.client.failover.proxy.provider" = "10101010");
        """

        // errCode = 2, detailMessage = current not support modify property : type
        def alter_result_fail_5 = try_sql """
            ALTER RESOURCE "${resource_name}" PROPERTIES("type" = "local");
        """
    }

    // todo : this function should check by really situation
    def check_alter_resource_result_no_policy = { resource_name ->
        def show_alter_result = order_sql """
            SHOW RESOURCES WHERE NAME = "${resource_name}";
        """
        logger.info(show_alter_result.toString())
        println show_alter_result.toString()

        // AWS_ACCESS_KEY
        assertEquals(show_alter_result[1][3], "false")
        // AWS_BUCKET
        assertEquals(show_alter_result[2][3], "7777")
        // AWS_MAX_CONNECTIONS
        assertEquals(show_alter_result[3][3], "8888")
        // AWS_REQUEST_TIMEOUT_MS
        assertEquals(show_alter_result[4][3], "9999")
        // AWS_CONNECTION_TIMEOUT_MS
        assertEquals(show_alter_result[5][3], "6666")
        // AWS_ENDPOINT
        assertEquals(show_alter_result[6][3], "1111")
        // AWS_REGION
        assertEquals(show_alter_result[7][3], "5555")
        // s3_rootpath
        assertEquals(show_alter_result[8][3], "2222")
        // AWS_SECRET_KEY
        assertEquals(show_alter_result[9][3], "hdfs")
    }

    // todo : this function should check by really situation
    def check_alter_resource_result_with_policy = { resource_name ->
        def show_alter_result = order_sql """
            SHOW RESOURCES WHERE NAME = "${resource_name}";
        """
        logger.info(show_alter_result.toString())
        println show_alter_result.toString()

        // AWS_ACCESS_KEY
        assertEquals(show_alter_result[1][3], "false")
        // AWS_BUCKET
        assertEquals(show_alter_result[2][3], "7777")
        // AWS_MAX_CONNECTIONS
        assertEquals(show_alter_result[3][3], "8888")
        // AWS_REQUEST_TIMEOUT_MS
        assertEquals(show_alter_result[4][3], "9999")
        // AWS_CONNECTION_TIMEOUT_MS
        assertEquals(show_alter_result[5][3], "6666")
        // AWS_ENDPOINT
        assertEquals(show_alter_result[6][3], "1111")
        // AWS_REGION
        assertEquals(show_alter_result[7][3], "5555")
        // s3_rootpath
        assertEquals(show_alter_result[8][3], "2222")
        // AWS_SECRET_KEY
        assertEquals(show_alter_result[9][3], "hdfs")
    }


    // test when no policy binding to resource
    def no_binding_policy_resource = "no_binding_policy_resource_hdfs"
    check_resource_delete_if_exist(no_binding_policy_resource)
    create_source(no_binding_policy_resource)
    alter_resource_change_property(no_binding_policy_resource)
    check_alter_resource_result_no_policy(no_binding_policy_resource)


    // test when policy binding to resource
    def has_resource_policy_alter = "has_resource_policy_alter_hdfs"
    sql """
    DROP STORAGE POLICY IF EXISTS has_resouce_policy_alter_policy_hdfs
    """
    sql """
    DROP STORAGE POLICY IF EXISTS has_test_policy_to_alter_hdfs
    """
    sql """
    DROP STORAGE POLICY IF EXISTS has_test_policy_to_alter_1_hdfs
    """
    check_resource_delete_if_exist(has_resource_policy_alter)
    create_source(has_resource_policy_alter)
    sql """
        CREATE STORAGE POLICY IF NOT EXISTS has_resouce_policy_alter_policy_hdfs
        PROPERTIES(
            "storage_resource" = "${has_resource_policy_alter}",
            "cooldown_ttl" = "1d"
        )
        """
    alter_resource_change_property(has_resource_policy_alter)
    check_alter_resource_result_with_policy(has_resource_policy_alter)
    sql """
    DROP STORAGE POLICY has_resouce_policy_alter_policy_hdfs
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


    if (!storage_exist.call("has_test_policy_to_alter_hdfs")) {
        def create_succ_1 = try_sql """
            CREATE STORAGE POLICY has_test_policy_to_alter_hdfs
            PROPERTIES( 
            "storage_resource" = "${has_resource_policy_alter}",
            "cooldown_ttl" = "10086"
            );
        """
        assertEquals(storage_exist.call("has_test_policy_to_alter_hdfs"), true)
    }

    // OK
    def alter_result_sql_succ_ttl = try_sql """
        ALTER STORAGE POLICY has_test_policy_to_alter_hdfs PROPERTIES("cooldown_ttl" = "10000");
    """

    // FAIL
    alter_result_sql_succ_ttl = try_sql """
        ALTER STORAGE POLICY has_test_policy_to_alter_hdfs PROPERTIES("cooldown_ttl" = "-10000");
    """

    // OK
    def alter_result_sql_succ_datetime = try_sql """
        ALTER STORAGE POLICY has_test_policy_to_alter_hdfs PROPERTIES("cooldown_datetime" = "2023-06-08 00:00:00");
    """

    // FAILED
    alter_result_sql_succ_datetime = try_sql """
        ALTER STORAGE POLICY has_test_policy_to_alter_hdfs PROPERTIES("cooldown_datetime" = "2023-13-32 00:00:00");
    """

    // FAILED
    def alter_result_sql_failed_storage_resource = try_sql """
        ALTER STORAGE POLICY has_test_policy_to_alter_hdfs PROPERTIES("storage_resource" = "${no_binding_policy_resource}");
    """
    // errCode = 2, detailMessage = not support change storage policy's storage resource, you can change s3 properties by alter resource
    assertEquals(alter_result_sql_failed_storage_resource, null)

    if (!storage_exist.call("has_test_policy_to_alter_1_hdfs")) {
        def create_succ_1 = try_sql """
            CREATE STORAGE POLICY has_test_policy_to_alter_1_hdfs
            PROPERTIES(
            "storage_resource" = "${no_binding_policy_resource}",
            "cooldown_datetime" = "2025-06-08 00:00:00"
            );
        """
        assertEquals(storage_exist.call("has_test_policy_to_alter_1_hdfs"), true)
    }

    // go to check be、fe log about notify alter.
    def alter_result_succ_again = try_sql """
        ALTER RESOURCE "${has_resource_policy_alter}" PROPERTIES("AWS_ACCESS_KEY" = "akakak");
    """

    sql """
    DROP STORAGE POLICY has_test_policy_to_alter_hdfs;
    """

    sql """
    DROP STORAGE POLICY has_test_policy_to_alter_1_hdfs;
    """

    sql """
            DROP RESOURCE '${no_binding_policy_resource}'
        """
}

