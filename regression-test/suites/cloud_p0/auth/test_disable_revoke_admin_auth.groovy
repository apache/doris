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

suite("test_disable_revoke_admin_auth", "cloud_auth") {
    def user = "regression_test_cloud_revoke_admin_user"
    sql """drop user if exists ${user}"""

    sql """create user ${user} identified by 'Cloud12345' default role 'admin'"""

    sql "sync"
    def result

    try {
        result = sql """revoke 'admin' from 'admin'""";
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("Unsupported operation"), e.getMessage())
    }

    try {
        result = connect("${user}", 'Cloud12345', context.config.jdbcUrl) {
             sql """
                revoke 'admin' from 'admin'
             """
        }
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("Unsupported operation"), e.getMessage())
    }

    result = sql """revoke 'admin' from ${user}"""
    assertEquals(result[0][0], 0)

    sql """drop user if exists ${user}"""
}
