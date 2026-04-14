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

suite("test_revoke_role_nereids)") {

    String user1 = 'test_revoke_role_nereids_user1'
    String user2 = 'test_revoke_role_nereids_user2'
    String role1 = 'test_revoke_role_nereids_role1'
    String role2 = 'test_revoke_role_nereids_role2'
    String pwd = 'C123_567p'

    try_sql("DROP USER ${user1}")
    try_sql("DROP USER ${user2}")
    try_sql("DROP role ${role1}")
    try_sql("DROP role ${role2}")
    sql """CREATE USER '${user1}' IDENTIFIED BY '${pwd}'"""
    sql """CREATE USER '${user2}' IDENTIFIED BY '${pwd}'"""
    checkNereidsExecute("grant select_priv on regression_test to ${user1};")
    checkNereidsExecute("grant select_priv on regression_test to ${user2};")

    sql """CREATE ROLE ${role1}"""
    sql """CREATE ROLE ${role2}"""

    sql """ADMIN SET FRONTEND CONFIG ('experimental_enable_workload_group' = 'true');"""
    sql """set experimental_enable_pipeline_engine = true;"""

    // role
    checkNereidsExecute("grant '${role1}', '${role2}' to '${user1}';")
    checkNereidsExecute("revoke '${role1}', '${role2}' from '${user1}'")

    checkNereidsExecute("grant '${role1}', '${role2}' to '${user2}';")
    checkNereidsExecute("revoke '${role1}', '${role2}' from '${user2}'")
}
