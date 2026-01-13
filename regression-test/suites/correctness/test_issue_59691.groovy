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

suite("test_issue_59691") {
    // ==========================
    // 1. Setup
    // ==========================
    sql "DROP TABLE IF EXISTS t_issue_59691"
    
    sql """
        CREATE TABLE t_issue_59691 (
            id INT,
            name VARCHAR(10)
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """
    
    sql "INSERT INTO t_issue_59691 VALUES (1, 'test');"

    // ==========================
    // 2. Run Tests
    // ==========================

    // -- Case 1: Fake nested comment (The reported issue) --
    // If the matching is greedy, '*/' will be skipped, causing a syntax error.
    // The fix ensures it stops at the first '*/'.
    qt_case1 """
        select 1 
        /* comment /* fake_nested */ 
        as res;
    """

    // -- Case 2: Compact interference --
    // Verifies that the parser handles tight comment boundaries correctly.
    // Adjusted to 'select 1 ...' to ensure valid SQL execution.
    qt_case2 """
        select 1 /*/**/*/ ;
    """

    // -- Case 3: Multi-line comment --
    // Verifies that the comment logic does not consume the subsequent 'AND' condition.
    qt_case3 """
        select * from t_issue_59691
        where id = 1
        /* line 1
           /* line 2 (fake inner)
        */
        and name = 'test';
    """

    // -- Case 4: Comment with symbols --
    // Verifies that math symbols inside comments are ignored.
    qt_case4 """
        /* 3 * 4 / 5 */ select 1;
    """

    // -- Case 5: Mixed comment styles --
    // This is the critical test for Greedy vs Lazy matching.
    // Before fix (Greedy): The parser sees the whole line as one comment -> Empty query.
    // After fix (Lazy): It stops after 'part 2 */', then executes 'select 1'.
    qt_case5 """
        /* part 1 /* part 2 */ select 1; /* part 3 */
    """

    // ==========================
    // 3. Cleanup
    // ==========================
    sql "DROP TABLE IF EXISTS t_issue_59691"
}
