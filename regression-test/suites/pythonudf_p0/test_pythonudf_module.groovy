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

suite("test_pythonudf_module") {
    def pyPath = """${context.file.parent}/udf_scripts/python_udf_module_test.zip"""
    scp_udf_file_to_all_be(pyPath)
    def runtime_version = "3.8.10"
    log.info("Python Zip path: ${pyPath}".toString())
    try {
        sql """ DROP FUNCTION IF EXISTS python_udf_ltv_score(BIGINT, BIGINT, DOUBLE); """
        sql """
        CREATE FUNCTION python_udf_ltv_score(BIGINT, BIGINT, DOUBLE) 
        RETURNS DOUBLE 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file"="file://${pyPath}",
            "symbol" = "python_udf_module_test.main.safe_ltv",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """
        sql """ DROP TABLE IF EXISTS user_behavior_test; """
        sql """
        CREATE TABLE user_behavior_test (
            user_id BIGINT,
            days_since_last_action BIGINT,
            total_actions BIGINT,
            total_spend DOUBLE
        ) ENGINE=OLAP 
        DUPLICATE KEY(user_id)
        DISTRIBUTED BY HASH(user_id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        sql """
        INSERT INTO user_behavior_test VALUES
        (1001, 5,   10,  500.0),  
        (1002, 40,  1,   20.0),   
        (1003, 15,  5,   300.0),  
        (1004, -1,  3,   100.0),  
        (1005, NULL, 2,  200.0),  
        (1006, 7,   NULL, 150.0), 
        (1007, 30,  0,   NULL),   
        (1008, 0,   100, 5000.0),
        (1009, 100, 2,   10.0),   
        (1010, 8,   8,   800.0);  
        """

        qt_select """ SELECT 
            user_id,
            days_since_last_action,
            total_actions,
            total_spend,
            python_udf_ltv_score(days_since_last_action, total_actions, total_spend) AS ltv_score
        FROM user_behavior_test
        ORDER BY user_id; """
    } finally {
        try_sql("DROP FUNCTION IF EXISTS python_udf_ltv_score(BIGINT, BIGINT, DOUBLE);")
        try_sql("DROP TABLE IF EXISTS user_behavior_test;")
    }
}
