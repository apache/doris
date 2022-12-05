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

        def drop_result = try_sql """
            DROP RESOURCE 'resouce_policy_drop'
        """
        // can drop, no policy use
        assertEquals(drop_result.size(), 1)

        sql """
        CREATE RESOURCE "resouce_policy_drop"
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
        // errCode = 2, detailMessage = current not support drop storage policy.
        assertEquals(drop_policy_ret, null)
    }

}
