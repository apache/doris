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


suite("test_struct_name_case_sensitivity") {
    def tableName = "tbl_test_struct_name_case"

    sql """DROP TABLE IF EXISTS ${tableName}"""
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
              `k1` INT(11) NULL,
              `k2` STRUCT<Name:VARCHAR(10), Age:INT, Address:VARCHAR(100)> NULL,
              `k3` STRUCT<name:VARCHAR(10), age:INT, address:VARCHAR(100)> NULL,
              `k4` STRUCT<NAME:VARCHAR(10), AGE:INT, ADDRESS:VARCHAR(100)> NULL
            )
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            )
        """

    // Test struct constructor with different case field names
    sql """ INSERT INTO ${tableName} VALUES(1, 
        named_struct('Name', 'John', 'Age', 30, 'Address', 'New York'),
        named_struct('name', 'Jane', 'age', 25, 'address', 'London'),
        named_struct('NAME', 'Bob', 'AGE', 35, 'ADDRESS', 'Paris')
    ) """

    // Test struct_element function with different case field names
    qt_select_struct_element_1 "SELECT struct_element(k2, 'name'), struct_element(k2, 'Name'), struct_element(k2, 'NAME') FROM ${tableName} WHERE k1 = 1"
    qt_select_struct_element_2 "SELECT struct_element(k3, 'name'), struct_element(k3, 'Name'), struct_element(k3, 'NAME') FROM ${tableName} WHERE k1 = 1"
    qt_select_struct_element_3 "SELECT struct_element(k4, 'name'), struct_element(k4, 'Name'), struct_element(k4, 'NAME') FROM ${tableName} WHERE k1 = 1"

    // Test struct constructor with mixed case field names
    sql """ INSERT INTO ${tableName} VALUES(2,
        struct('Alice', 28, 'Berlin'),
        struct('Charlie', 32, 'Tokyo'),
        struct('David', 40, 'Sydney')
    ) """

    // Test struct_element function with mixed case field names
    qt_select_struct_element_4 "SELECT struct_element(k2, 1), struct_element(k2, 2), struct_element(k2, 3) FROM ${tableName} WHERE k1 = 2"
    qt_select_struct_element_5 "SELECT struct_element(k3, 1), struct_element(k3, 2), struct_element(k3, 3) FROM ${tableName} WHERE k1 = 2"
    qt_select_struct_element_6 "SELECT struct_element(k4, 1), struct_element(k4, 2), struct_element(k4, 3) FROM ${tableName} WHERE k1 = 2"

    // Test struct constructor with NULL values
    sql """ INSERT INTO ${tableName} VALUES(3,
        named_struct('Name', NULL, 'Age', NULL, 'Address', NULL),
        named_struct('name', NULL, 'age', NULL, 'address', NULL),
        named_struct('NAME', NULL, 'AGE', NULL, 'ADDRESS', NULL)
    ) """

    // Test struct_element function with NULL values
    qt_select_struct_element_7 "SELECT struct_element(k2, 'name'), struct_element(k2, 'age'), struct_element(k2, 'address') FROM ${tableName} WHERE k1 = 3"
    qt_select_struct_element_8 "SELECT struct_element(k3, 'name'), struct_element(k3, 'age'), struct_element(k3, 'address') FROM ${tableName} WHERE k1 = 3"
    qt_select_struct_element_9 "SELECT struct_element(k4, 'name'), struct_element(k4, 'age'), struct_element(k4, 'address') FROM ${tableName} WHERE k1 = 3"

    // Test struct constructor with literal values
    qt_sql "SELECT struct('Test', 100, 'Value')"
    qt_sql "SELECT named_struct('Name', 'Test', 'Age', 100, 'Address', 'Value')"
    qt_sql "SELECT struct_element(named_struct('Name', 'Test', 'Age', 100, 'Address', 'Value'), 'name')"
    qt_sql "SELECT struct_element(named_struct('Name', 'Test', 'Age', 100, 'Address', 'Value'), 'NAME')"
} 
