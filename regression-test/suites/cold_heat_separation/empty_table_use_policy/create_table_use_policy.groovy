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

suite("create_table_use_policy") {
    def cooldown_ttl = "10"

    def create_table_use_not_create_policy = try_sql """
        CREATE TABLE IF NOT EXISTS create_table_use_not_create_policy
        (
            k1 BIGINT,
            k2 LARGEINT,
            v1 VARCHAR(2048)
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH (k1) BUCKETS 3
        PROPERTIES(
            "storage_policy" = "not_exist_policy",
            "replication_num" = "1"
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

    def create_s3_resource = try_sql """
        CREATE RESOURCE IF NOT EXISTS "test_create_table_use_resource"
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
        CREATE STORAGE POLICY IF NOT EXISTS test_create_table_use_policy
        PROPERTIES(
        "storage_resource" = "test_create_table_use_resource",
        "cooldown_ttl" = "$cooldown_ttl"
        );
    """

    sql """ALTER STORAGE POLICY test_create_table_use_policy PROPERTIES("cooldown_ttl" = "$cooldown_ttl")"""

    assertEquals(storage_exist.call("test_create_table_use_policy"), true)

    // success
    def create_table_use_created_policy = try_sql """
        CREATE TABLE IF NOT EXISTS create_table_use_created_policy 
        (
            k1 BIGINT,
            k2 LARGEINT,
            v1 VARCHAR(2048)
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH (k1) BUCKETS 3
        PROPERTIES(
            "storage_policy" = "test_create_table_use_policy",
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "false"
        );
    """
    // storage policy is disabled on mow table
    assertEquals(create_table_use_created_policy.size(), 1);

    sql """
    DROP TABLE IF EXISTS create_table_use_created_policy
    """
    sql """
    DROP STORAGE POLICY test_create_table_use_policy
    """
    sql """
    DROP RESOURCE test_create_table_use_resource
    """

}
