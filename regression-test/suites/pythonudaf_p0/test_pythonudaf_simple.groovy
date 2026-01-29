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

suite("test_pythonudaf_simple") {
    def pyPath = """${context.file.parent}/udaf_scripts/pyudaf.zip"""
    scp_udf_file_to_all_be(pyPath)
    def runtime_version = "3.8.10"
    log.info("Python zip path: ${pyPath}".toString())
    
    def tableName = "test_pythonudaf_simple"
    
    try {
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `id`       INT NOT NULL COMMENT "ID",
            `value`    INT NOT NULL COMMENT "Value",
            `category` VARCHAR(10) NOT NULL COMMENT "Category"
            )
            DISTRIBUTED BY HASH(id) PROPERTIES("replication_num" = "1");
        """
        
        // Insert simple test data
        sql """ INSERT INTO ${tableName} VALUES
                (1, 10, 'A'),
                (2, 20, 'A'),
                (3, 30, 'B'),
                (4, 40, 'B'),
                (5, 50, 'C');
            """
        
        qt_select_data """ SELECT * FROM ${tableName} ORDER BY id; """

        // Create UDAF function
        sql """ DROP FUNCTION IF EXISTS py_sum(int) """
        
        sql """ CREATE AGGREGATE FUNCTION py_sum(int) RETURNS bigint PROPERTIES (
            "file"="file://${pyPath}",
            "symbol"="sum_int.SumInt",
            "type"="PYTHON_UDF",
            "always_nullable" = "true",
            "runtime_version" = "${runtime_version}"
        ); """

        // Test 1: Basic sum of all values
        qt_test1 """ SELECT py_sum(value) as total FROM ${tableName}; """
        
        // Test 2: Sum with GROUP BY
        qt_test2 """ SELECT category, py_sum(value) as sum_val 
                     FROM ${tableName} 
                     GROUP BY category 
                     ORDER BY category; """
        
        // Test 3: Compare with native SUM
        qt_test3 """ SELECT category, 
                            py_sum(value) as py_sum,
                            sum(value) as native_sum
                     FROM ${tableName} 
                     GROUP BY category 
                     ORDER BY category; """

    } finally {
        try_sql("DROP FUNCTION IF EXISTS py_sum(int);")
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}
