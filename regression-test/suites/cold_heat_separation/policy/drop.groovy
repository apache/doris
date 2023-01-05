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
    sql """ADMIN SET FRONTEND CONFIG ("enable_storage_policy" = "true");"""

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

    if (!storage_exist.call("drop_policy_test")) {
        def has_resouce_policy_drop = sql """
            SHOW RESOURCES WHERE NAME = "resouce_policy_drop";
        """
        if(has_resouce_policy_drop.size() == 0) {
            sql """
            CREATE RESOURCE "resouce_policy_drop"
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
            DROP RESOURCE 'resouce_policy_drop'
        """
        // can drop, no policy use
        assertEquals(drop_result.size(), 1)

        sql """
        CREATE RESOURCE "resouce_policy_drop"
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
            CREATE STORAGE POLICY drop_policy_test
            PROPERTIES(
            "storage_resource" = "resouce_policy_drop",
            "cooldown_datetime" = "2022-06-08 00:00:00"
            );
        """
        assertEquals(storage_exist.call("drop_policy_test"), true)

        def drop_s3_resource_result_1 = try_sql """
            DROP RESOURCE 'resouce_policy_drop'
        """
        // errCode = 2, detailMessage = S3 resource used by policy, can't drop it.
        assertEquals(drop_s3_resource_result_1, null)

        def drop_policy_ret = try_sql """
            DROP STORAGE POLICY drop_policy_test
        """
        // can drop, no table use
        assertEquals(drop_policy_ret.size(), 1)
    }

}
