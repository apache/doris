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

suite("add_table_policy_by_alter_table") {
    def create_table_not_have_policy_result = try_sql """
        CREATE TABLE IF NOT EXISTS create_table_not_have_policy
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
    // storage policy is disabled on mow table
    assertEquals(create_table_not_have_policy_result.size(), 1);

    def alter_table_use_not_exist_policy_result = try_sql """
        ALTER TABLE create_table_not_have_policy set ("storage_policy" = "policy_not_exist");
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

    if (!storage_exist.call("created_create_table_alter_policy")) {
        def create_s3_resource = try_sql """
            CREATE RESOURCE IF NOT EXISTS "test_create_alter_table_use_resource"
            PROPERTIES(
                "type"="s3",
                "AWS_REGION" = "bj",
                "AWS_ENDPOINT" = "bj.s3.comaaaa",
                "AWS_ROOT_PATH" = "path/to/rootaaaa",
                "AWS_SECRET_KEY" = "aaaa",
                "AWS_ACCESS_KEY" = "bbba",
                "AWS_BUCKET" = "test-bucket",
                "s3_validity_check" = "false"
            );
        """
        def create_succ_1 = try_sql """
            CREATE STORAGE POLICY IF NOT EXISTS created_create_table_alter_policy
            PROPERTIES(
            "storage_resource" = "test_create_alter_table_use_resource",
            "cooldown_datetime" = "2025-06-08 00:00:00"
            );
        """
        assertEquals(storage_exist.call("created_create_table_alter_policy"), true)
    }

    def alter_table_try_again_result = try_sql """
        ALTER TABLE create_table_not_have_policy set ("storage_policy" = "created_create_table_alter_policy");
    """
    // OK
    assertEquals(alter_table_try_again_result.size(), 1);

    def alter_table_when_table_has_storage_policy_result = try_sql """
        ALTER TABLE create_table_not_have_policy set ("storage_policy" = "created_create_table_alter_policy");
    """
    // OK
    assertEquals(alter_table_when_table_has_storage_policy_result.size(), 1);

    if (!storage_exist.call("created_create_table_alter_policy_1")) {
        def create_s3_resource = try_sql """
            CREATE RESOURCE IF NOT EXISTS "test_create_alter_table_use_resource_1"
            PROPERTIES(
                "type"="s3",
                "AWS_REGION" = "bj",
                "AWS_ENDPOINT" = "bj.s3.comaaaa",
                "AWS_ROOT_PATH" = "path/to/rootaaaa",
                "AWS_SECRET_KEY" = "aaaa",
                "AWS_ACCESS_KEY" = "bbba",
                "AWS_BUCKET" = "test-bucket",
                "s3_validity_check" = "false"
            );
        """
        def create_succ_1 = try_sql """
            CREATE STORAGE POLICY IF NOT EXISTS created_create_table_alter_policy_1
            PROPERTIES(
            "storage_resource" = "test_create_alter_table_use_resource_1",
            "cooldown_datetime" = "2025-06-08 00:00:00"
            );
        """
        assertEquals(storage_exist.call("created_create_table_alter_policy_1"), true)
    }

    // current not support change storage policy name
    def cannot_modify_exist_storage_policy_table_result = try_sql """
        ALTER TABLE create_table_not_have_policy set ("storage_policy" = "created_create_table_alter_policy_1");
    """
    // OK
    assertEquals(cannot_modify_exist_storage_policy_table_result.size(), 1);

    // you can change created_create_table_alter_policy's policy cooldown time, cooldown ttl property,
    // by alter storage policy
    def modify_storage_policy_property_result = try_sql """
        ALTER STORAGE POLICY "created_create_table_alter_policy_1" PROPERTIES("cooldown_datetime" = "2026-06-08 00:00:00");
    """
    // change s3 resource, ak、sk by alter resource
    def modify_storage_policy_property_result_1 = try_sql """
        ALTER RESOURCE "test_create_alter_table_use_resource_1" PROPERTIES("AWS_ACCESS_KEY" = "has_been_changed");
    """

    sql """
    DROP TABLE create_table_not_have_policy;
    """

    sql """
    DROP STORAGE POLICY created_create_table_alter_policy_1;
    """

    sql """
    DROP RESOURCE test_create_alter_table_use_resource_1;
    """

}
