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

suite("drop_policy") {
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

    def resource_not_table_use = "resource_not_table_use"
    def use_policy = "use_policy"

    if (!storage_exist.call(use_policy)) {
        def has_resouce_policy_drop = sql """
            SHOW RESOURCES WHERE NAME = "${resource_not_table_use}";
        """
        if(has_resouce_policy_drop.size() == 0) {
            sql """
            CREATE RESOURCE "${resource_not_table_use}"
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
        }


        def drop_result = try_sql """
            DROP RESOURCE ${resource_not_table_use}
        """
        // can drop, no policy use
        assertEquals(drop_result.size(), 1)

        def resource_table_use = "resource_table_use"
        sql """
        CREATE RESOURCE IF NOT EXISTS "${resource_table_use}"
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


        def create_succ_1 = try_sql """
            CREATE STORAGE POLICY ${use_policy}
            PROPERTIES(
            "storage_resource" = "${resource_table_use}",
            "cooldown_datetime" = "2025-06-08 00:00:00"
            );
        """
        assertEquals(storage_exist.call(use_policy), true)

        def drop_s3_resource_result_1 = try_sql """
            DROP RESOURCE ${resource_table_use}
        """
        // errCode = 2, detailMessage = S3 resource used by policy, can't drop it.
        assertEquals(drop_s3_resource_result_1, null)

        def drop_policy_ret = try_sql """
            DROP STORAGE POLICY ${use_policy}
        """
        // can drop, no table use
        assertEquals(drop_policy_ret.size(), 1)

        def create_succ_2 = try_sql """
            CREATE STORAGE POLICY IF NOT EXISTS drop_policy_test_has_table_binded
            PROPERTIES(
            "storage_resource" = "${resource_table_use}",
            "cooldown_datetime" = "2025-06-08 00:00:00"
            );
        """
        assertEquals(storage_exist.call("drop_policy_test_has_table_binded"), true)

        def create_succ_3 = try_sql """
            CREATE STORAGE POLICY IF NOT EXISTS drop_policy_test_has_table_bind_1
            PROPERTIES(
            "storage_resource" = "${resource_table_use}",
            "cooldown_datetime" = "2025-06-08 00:00:00"
            );
        """
        assertEquals(storage_exist.call("drop_policy_test_has_table_bind_1"), true)

        // success
        def create_table_use_created_policy = try_sql """
            CREATE TABLE IF NOT EXISTS create_table_binding_created_policy
            (
                k1 BIGINT,
                k2 date,
                v1 VARCHAR(2048)
            )
            UNIQUE KEY(k1, k2)
            PARTITION BY RANGE(k2)(
                partition p1 VALUES LESS THAN ("2014-01-01") ("storage_policy" = "drop_policy_test_has_table_bind_1"),
                partition p2 VALUES LESS THAN ("2015-01-01"),
                partition p3 VALUES LESS THAN ("2016-01-01")
            )
            DISTRIBUTED BY HASH (k1) BUCKETS 3
            PROPERTIES(
                "storage_policy" = "drop_policy_test_has_table_binded",
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "false"
            );
        """
        // storage policy is disabled on mow table

        assertEquals(create_table_use_created_policy.size(), 1);

        def drop_policy_fail_ret = try_sql """
            DROP STORAGE POLICY drop_policy_test_has_table_binded
        """
        // fail to drop, there are tables using this policy
        assertEquals(drop_policy_fail_ret, null)

        create_table_use_created_policy = try_sql """
            CREATE TABLE IF NOT EXISTS create_table_binding_created_policy_1
            (
                k1 BIGINT,
                k2 date,
                v1 VARCHAR(2048)
            )
            UNIQUE KEY(k1, k2)
            PARTITION BY RANGE(k2)(
                partition p1 VALUES LESS THAN ("2014-01-01") ("storage_policy" = "drop_policy_test_has_table_bind_1"),
                partition p2 VALUES LESS THAN ("2015-01-01"),
                partition p3 VALUES LESS THAN ("2016-01-01")
            )
            DISTRIBUTED BY HASH (k1) BUCKETS 3
            PROPERTIES(
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "false"
            );
        """
        // storage policy is disabled on mow table

        assertEquals(create_table_use_created_policy.size(), 1);

        drop_policy_fail_ret = try_sql """
            DROP STORAGE POLICY drop_policy_test_has_table_bind_1
        """
        // fail to drop, there are partitions using this policy
        assertEquals(drop_policy_fail_ret, null)

        sql """
        DROP TABLE create_table_binding_created_policy;
        """

        sql """
        DROP TABLE create_table_binding_created_policy_1;
        """

        sql """
        DROP STORAGE POLICY drop_policy_test_has_table_binded;
        """

        sql """
        DROP STORAGE POLICY drop_policy_test_has_table_bind_1;
        """

        sql """
        DROP RESOURCE ${resource_table_use};
        """
    }

}
