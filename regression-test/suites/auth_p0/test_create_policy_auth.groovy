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

import org.junit.Assert;

suite("test_create_policy_auth","p0,auth") {
    String user = 'test_create_policy_auth_user'
    String pwd = 'C123_567p'
    try_sql("DROP USER ${user}")
    sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""
    sql """grant select_priv on regression_test to ${user}"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        try {
            sql "CREATE ROW POLICY test_create_policy_auth ON test.table1 AS RESTRICTIVE TO test USING (c1 = 'a');"
        } catch (Exception e) {
            log.info(e.getMessage())
            assertTrue(e.getMessage().contains("Admin_priv,Grant_priv"))
        }
        try {
            sql """
                CREATE STORAGE POLICY testPolicy
                PROPERTIES(
                  "storage_resource" = "s3",
                  "cooldown_datetime" = "2022-06-08 00:00:00"
                );
                """
        } catch (Exception e) {
            log.info(e.getMessage())
            assertTrue(e.getMessage().contains("Admin_priv"))
        }
    }
    try_sql("DROP USER ${user}")
}
