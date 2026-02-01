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

suite("test_pythonudaf_inline_simple") {
    // Simplest Python UDAF test using inline mode
    
    def runtime_version = "3.8.10"
    
    try {
        // Create test table
        sql """ DROP TABLE IF EXISTS udaf_inline_test """
        sql """
        CREATE TABLE udaf_inline_test (
            id INT,
            val INT,
            cat VARCHAR(10)
        ) DISTRIBUTED BY HASH(id) PROPERTIES("replication_num" = "1");
        """
        
        // Insert simple data
        sql """ INSERT INTO udaf_inline_test VALUES
                (1, 10, 'A'),
                (2, 20, 'A'),
                (3, 30, 'B'),
                (4, 40, 'B'),
                (5, 50, 'C');
            """
        
        qt_data """ SELECT * FROM udaf_inline_test ORDER BY id; """

        // Create inline UDAF - Sum
        sql """ DROP FUNCTION IF EXISTS inline_sum(INT); """
        sql """
        CREATE AGGREGATE FUNCTION inline_sum(INT) 
        RETURNS BIGINT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "MySum",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
class MySum:
    def __init__(self):
        self.total = 0
    
    def accumulate(self, value):
        if value is not None:
            self.total += value
    
    def merge(self, other_state):
        if other_state is not None:
            self.total += other_state
    
    def finish(self):
        return self.total
    
    @property
    def aggregate_state(self):
        return self.total
\$\$;
        """

        // Test 1: Sum all values
        qt_sum_all """ SELECT inline_sum(val) as result FROM udaf_inline_test; """
        
        // Test 2: Sum with GROUP BY
        qt_sum_group """ SELECT cat, inline_sum(val) as sum_result 
                         FROM udaf_inline_test 
                         GROUP BY cat 
                         ORDER BY cat; """
        
        // Test 3: Compare with native SUM
        qt_compare """ SELECT cat, 
                              inline_sum(val) as py_sum,
                              sum(val) as native_sum
                       FROM udaf_inline_test 
                       GROUP BY cat 
                       ORDER BY cat; """

    } finally {
        try_sql("DROP FUNCTION IF EXISTS inline_sum(INT);")
        try_sql("DROP TABLE IF EXISTS udaf_inline_test")
    }
}
