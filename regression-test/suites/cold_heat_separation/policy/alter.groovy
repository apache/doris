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
            "AWS_ENDPOINT" = "bj.s3.comaaaa",
            "AWS_REGION" = "bj",
            "AWS_ROOT_PATH" = "path/to/rootaaaa",
            "AWS_ACCESS_KEY" = "bbba",
            "AWS_SECRET_KEY" = "aaaa",
            "AWS_MAX_CONNECTIONS" = "50",
            "AWS_REQUEST_TIMEOUT_MS" = "3000",
            "AWS_CONNECTION_TIMEOUT_MS" = "1000",
            "AWS_BUCKET" = "test-bucket",
            "s3_validity_check" = "false"
        );
        """

        sql """
        CREATE STORAGE POLICY has_resouce_policy_alter_policy
        PROPERTIES(
            "storage_resource" = "has_resouce_policy_alter",
            "cooldown_ttl" = "1d"
        )
        """
    }

    // support
    def alter_result_succ_1 = try_sql """
        ALTER RESOURCE "has_resouce_policy_alter" PROPERTIES("AWS_MAX_CONNECTIONS" = "1111");
    """

    def alter_result_succ_2 = try_sql """
        ALTER RESOURCE "has_resouce_policy_alter" PROPERTIES("AWS_CONNECTION_TIMEOUT_MS" = "2222");
    """

    def alter_result_succ_5 = try_sql """
        ALTER RESOURCE "has_resouce_policy_alter" PROPERTIES("AWS_SECRET_KEY" = "5555");
    """

    def alter_result_succ_6 = try_sql """
        ALTER RESOURCE "has_resouce_policy_alter" PROPERTIES("AWS_ACCESS_KEY" = "6666");
    """

    def alter_result_succ_7 = try_sql """
        ALTER RESOURCE "has_resouce_policy_alter" PROPERTIES("AWS_REQUEST_TIMEOUT_MS" = "7777");
    """

    // errCode = 2, detailMessage = current not support modify property : AWS_REGION
    def alter_result_fail_1 = try_sql """
        ALTER RESOURCE "has_resouce_policy_alter" PROPERTIES("AWS_REGION" = "8888");
    """

    // errCode = 2, detailMessage = current not support modify property : AWS_BUCKET
    def alter_result_fail_2 = try_sql """
        ALTER RESOURCE "has_resouce_policy_alter" PROPERTIES("AWS_BUCKET" = "9999");
    """

    // errCode = 2, detailMessage = current not support modify property : AWS_ROOT_PATH
    def alter_result_fail_3 = try_sql """
        ALTER RESOURCE "has_resouce_policy_alter" PROPERTIES("AWS_ROOT_PATH" = "10101010");
    """

    // errCode = 2, detailMessage = current not support modify property : AWS_ENDPOINT
    def alter_result_fail_4 = try_sql """
        ALTER RESOURCE "has_resouce_policy_alter" PROPERTIES("AWS_ENDPOINT" = "11111111");
    """

    def show_alter_result = order_sql """
        SHOW RESOURCES WHERE NAME = "has_resouce_policy_alter";
    """

    // [[has_resouce_policy_alter, s3, AWS_ACCESS_KEY, 6666],
    // [has_resouce_policy_alter, s3, AWS_BUCKET, test-bucket],
    // [has_resouce_policy_alter, s3, AWS_CONNECTION_TIMEOUT_MS, 2222],
    // [has_resouce_policy_alter, s3, AWS_ENDPOINT, bj.s3.comaaaa],
    // [has_resouce_policy_alter, s3, AWS_MAX_CONNECTIONS, 1111],
    // [has_resouce_policy_alter, s3, AWS_REGION, bj],
    // [has_resouce_policy_alter, s3, AWS_REQUEST_TIMEOUT_MS, 7777],
    // [has_resouce_policy_alter, s3, AWS_ROOT_PATH, path/to/rootaaaa],
    // [has_resouce_policy_alter, s3, AWS_SECRET_KEY, ******],
    // [has_resouce_policy_alter, s3, type, s3]]
    // AWS_ACCESS_KEY
    assertEquals(show_alter_result[0][3], "6666")
    // AWS_BUCKET
    assertEquals(show_alter_result[1][3], "test-bucket")
    // AWS_CONNECTION_TIMEOUT_MS
    assertEquals(show_alter_result[2][3], "2222")
    // AWS_ENDPOINT
    assertEquals(show_alter_result[3][3], "bj.s3.comaaaa")
    // AWS_MAX_CONNECTIONS
    assertEquals(show_alter_result[4][3], "1111")
    // AWS_REGION
    assertEquals(show_alter_result[5][3], "bj")
    // AWS_REQUEST_TIMEOUT_MS
    assertEquals(show_alter_result[6][3], "7777")
    // s3_rootpath
    assertEquals(show_alter_result[7][3], "path/to/rootaaaa")
    // AWS_SECRET_KEY
    assertEquals(show_alter_result[8][3], "******")
    // type
    assertEquals(show_alter_result[10][3], "s3")

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
        ALTER RESOURCE "has_resouce_policy_alter" PROPERTIES("AWS_ACCESS_KEY" = "akakak");
    """
}
