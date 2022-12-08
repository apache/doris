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

import java.text.SimpleDateFormat;
import java.util.Date;

suite("create_table_use_policy") {
    sql """ADMIN SET FRONTEND CONFIG ("enable_storage_policy" = "true");"""

    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    Date date = new Date(System.currentTimeMillis() + 3600000)
    def cooldownTime = format.format(date)

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
            "s3_region" = "bj",
            "s3_endpoint" = "http://bj.s3.comaaaa",
            "s3_root_path" = "path/to/rootaaaa",
            "s3_secret_key" = "aaaa",
            "s3_access_key" = "bbba",
            "s3_bucket" = "test-bucket"
        );
    """
    def create_succ_1 = try_sql """
        CREATE STORAGE POLICY IF NOT EXISTS test_create_table_use_policy
        PROPERTIES(
        "storage_resource" = "test_create_table_use_resource",
        "cooldown_datetime" = "$cooldownTime"
        );
    """

    sql """ALTER STORAGE POLICY test_create_table_use_policy PROPERTIES("cooldown_datetime" = "$cooldownTime")"""

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
            "replication_num" = "1"
        );
    """

    assertEquals(create_table_use_created_policy.size(), 1);

    sql """
    DROP TABLE IF EXISTS create_table_use_created_policy
    """

}
