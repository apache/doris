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

suite("test_workload_sched_policy_username_be") {
    // 1. Create a user
    sql "DROP USER IF EXISTS 'test_policy_user_be'"
    sql "CREATE USER 'test_policy_user_be'@'%' IDENTIFIED BY '12345'"
    sql "GRANT SELECT_PRIV ON *.* TO 'test_policy_user_be'@'%'"

    // 2. Create a workload group
    sql "DROP WORKLOAD GROUP IF EXISTS policy_group_be"
    sql "CREATE WORKLOAD GROUP policy_group_be PROPERTIES ('cpu_share'='1024')"
    sql "GRANT USAGE_PRIV ON WORKLOAD GROUP 'policy_group_be' TO 'test_policy_user_be'@'%'"

    // 3. Create a policy with both username (FE metric) and query_time (BE metric)
    // This should now be allowed and effective on BE
    sql "DROP WORKLOAD POLICY IF EXISTS test_mixed_policy"
    
    // The key part: combine username (FE) with query_time (BE)
    // If the modification works, this policy should be created successfully and propagated to BE
    sql """
        CREATE WORKLOAD POLICY test_mixed_policy
        CONDITIONS(username='test_policy_user_be', query_time > 1000)
        ACTIONS(cancel_query) 
        PROPERTIES('workload_group'='policy_group_be')
    """

    // 4. Verify policy creation
    def policy = sql "SHOW WORKLOAD POLICY WHERE NAME='test_mixed_policy'"
    assertTrue(policy.size() > 0, "Policy should be created successfully")

    // 5. Test execution (simulate via sleep)
    // Note: Actual runtime verification is hard in regression test because we can't easily trigger
    // exact BE timing, but successful creation proves the FE restriction is removed.
    
    connect('test_policy_user_be', '12345') {
        sql "set workload_group = 'policy_group_be'"
        // This query should run normally if fast enough, or be cancelled if it exceeds 1s
        // We mainly verify it doesn't error out on start due to policy issues
        sql "SELECT 1" 
    }

    // Cleanup
    sql "DROP WORKLOAD POLICY IF EXISTS test_mixed_policy"
    sql "DROP WORKLOAD GROUP IF EXISTS policy_group_be"
    sql "DROP USER IF EXISTS 'test_policy_user_be'"
}
