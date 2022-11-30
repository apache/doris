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

suite("alter_policy") {
    sql """ADMIN SET FRONTEND CONFIG ("enable_storage_policy" = "true");"""

    def has_resouce_policy_alter = sql """
        SHOW RESOURCES WHERE NAME = "has_resouce_policy_alter";
    """
    if(has_resouce_policy_alter.size() == 0) {
        sql """
        CREATE RESOURCE "has_resouce_policy_alter"
        PROPERTIES(
            "type"="s3",
            "s3_endpoint" = "http://bj.s3.comaaaa",
            "s3_region" = "bj",
            "s3_root_path" = "path/to/rootaaaa",
            "s3_access_key" = "bbba",
            "s3_secret_key" = "aaaa",
            "s3_max_connections" = "50",
            "s3_request_timeout_ms" = "3000",
            "s3_connection_timeout_ms" = "1000",
            "s3_bucket" = "test-bucket"
        );
        """
    }

    // support
    def alter_result_succ_1 = try_sql """
        ALTER RESOURCE "has_resouce_policy_alter" PROPERTIES("s3_max_connections" = "1111");
    """

    def alter_result_succ_2 = try_sql """
        ALTER RESOURCE "has_resouce_policy_alter" PROPERTIES("s3_connection_timeout_ms" = "2222");
    """

    def alter_result_succ_5 = try_sql """
        ALTER RESOURCE "has_resouce_policy_alter" PROPERTIES("s3_secret_key" = "5555");
    """

    def alter_result_succ_6 = try_sql """
        ALTER RESOURCE "has_resouce_policy_alter" PROPERTIES("s3_access_key" = "6666");
    """

    def alter_result_succ_7 = try_sql """
        ALTER RESOURCE "has_resouce_policy_alter" PROPERTIES("s3_request_timeout_ms" = "7777");
    """

    // errCode = 2, detailMessage = current not support modify property : s3_region
    def alter_result_fail_1 = try_sql """
        ALTER RESOURCE "has_resouce_policy_alter" PROPERTIES("s3_region" = "8888");
    """

    // errCode = 2, detailMessage = current not support modify property : s3_bucket
    def alter_result_fail_2 = try_sql """
        ALTER RESOURCE "has_resouce_policy_alter" PROPERTIES("s3_bucket" = "9999");
    """

    // errCode = 2, detailMessage = current not support modify property : s3_root_path
    def alter_result_fail_3 = try_sql """
        ALTER RESOURCE "has_resouce_policy_alter" PROPERTIES("s3_root_path" = "10101010");
    """

    // errCode = 2, detailMessage = current not support modify property : s3_endpoint
    def alter_result_fail_4 = try_sql """
        ALTER RESOURCE "has_resouce_policy_alter" PROPERTIES("s3_endpoint" = "11111111");
    """

    def show_alter_result = order_sql """
        SHOW RESOURCES WHERE NAME = "has_resouce_policy_alter";
    """

    // [[has_resouce_policy_alter, s3, s3_access_key, 6666],
    // [has_resouce_policy_alter, s3, s3_bucket, test-bucket],
    // [has_resouce_policy_alter, s3, s3_connection_timeout_ms, 2222],
    // [has_resouce_policy_alter, s3, s3_endpoint, http://bj.s3.comaaaa],
    // [has_resouce_policy_alter, s3, s3_max_connections, 1111],
    // [has_resouce_policy_alter, s3, s3_region, bj],
    // [has_resouce_policy_alter, s3, s3_request_timeout_ms, 7777],
    // [has_resouce_policy_alter, s3, s3_root_path, path/to/rootaaaa],
    // [has_resouce_policy_alter, s3, s3_secret_key, ******],
    // [has_resouce_policy_alter, s3, type, s3]]
    // s3_access_key
    assertEquals(show_alter_result[0][3], "6666")
    // s3_bucket
    assertEquals(show_alter_result[1][3], "test-bucket")
    // s3_connection_timeout_ms
    assertEquals(show_alter_result[2][3], "2222")
    // s3_endpoint
    assertEquals(show_alter_result[3][3], "http://bj.s3.comaaaa")
    // s3_max_connections
    assertEquals(show_alter_result[4][3], "1111")
    // s3_region
    assertEquals(show_alter_result[5][3], "bj")
    // s3_request_timeout_ms
    assertEquals(show_alter_result[6][3], "7777")
    // s3_rootpath
    assertEquals(show_alter_result[7][3], "path/to/rootaaaa")
    // s3_secret_key
    assertEquals(show_alter_result[8][3], "******")
    // type
    assertEquals(show_alter_result[9][3], "s3")

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


    if (!storage_exist.call("has_test_policy_to_alter")) {
        def create_succ_1 = try_sql """
            CREATE STORAGE POLICY has_test_policy_to_alter
            PROPERTIES(
            "storage_resource" = "has_resouce_policy_alter",
            "cooldown_ttl" = "10086"
            );
        """
        assertEquals(storage_exist.call("has_test_policy_to_alter"), true)
    }

    // OK
    def alter_result_sql_succ_ttl = try_sql """
        ALTER STORAGE POLICY has_test_policy_to_alter PROPERTIES("cooldown_ttl" = "10000");
    """

    // OK
    def alter_result_sql_succ_datetime = try_sql """
        ALTER STORAGE POLICY has_test_policy_to_alter PROPERTIES("cooldown_datetime" = "2023-06-08 00:00:00");
    """

    // FAILED
    def alter_result_sql_failed_storage_resource = try_sql """
        ALTER STORAGE POLICY has_test_policy_to_alter PROPERTIES("storage_resource" = "has_resouce_policy_alter");
    """
    // errCode = 2, detailMessage = not support change storage policy's storage resource, you can change s3 properties by alter resource
    assertEquals(alter_result_sql_failed_storage_resource, null)

    if (!storage_exist.call("has_test_policy_to_alter_1")) {
        def create_succ_1 = try_sql """
            CREATE STORAGE POLICY has_test_policy_to_alter_1
            PROPERTIES(
            "storage_resource" = "has_resouce_policy_alter",
            "cooldown_datetime" = "2025-06-08 00:00:00"
            );
        """
        assertEquals(storage_exist.call("has_test_policy_to_alter_1"), true)
    }

    // go to check be、fe log about notify alter.
    def alter_result_succ_again = try_sql """
        ALTER RESOURCE "has_resouce_policy_alter" PROPERTIES("s3_access_key" = "akakak");
    """
}
