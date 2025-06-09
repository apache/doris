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

suite("test_array_index2") {
    def tableName1 = "array_test_supported"
    def tableName2 = "array_test_unsupported"

    def timeout = 60000
    def delta_time = 1000
    def alter_res = "null"
    def useTime = 0

    def wait_for_latest_op_on_table_finish = { table_name, OpTimeout ->
        for(int t = delta_time; t <= OpTimeout; t += delta_time) {
            alter_res = sql """SHOW ALTER TABLE COLUMN WHERE TableName = "${table_name}" ORDER BY CreateTime DESC LIMIT 1;"""
            alter_res = alter_res.toString()
            if(alter_res.contains("FINISHED")) {
                sleep(10000) // wait change table state to normal
                logger.info(table_name + " latest alter job finished, detail: " + alter_res)
                break
            }
            useTime = t
            sleep(delta_time)
        }
        assertTrue(useTime <= OpTimeout, "wait_for_latest_op_on_table_finish timeout")
    }

    sql "DROP TABLE IF EXISTS ${tableName1}"
    sql "DROP TABLE IF EXISTS ${tableName2}"

    // Create table with supported array types
    sql """
        CREATE TABLE ${tableName1} (
            id int,
            str_arr ARRAY<STRING>,
            int_arr ARRAY<INT>,
            date_arr ARRAY<DATE>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    // Insert test data before creating indexes
    sql """ INSERT INTO ${tableName1} VALUES
        (1, ['hello', 'world'], [1, 2, 3], ['2023-01-01', '2023-01-02']),
        (2, ['doris', 'apache'], [4, 5, 6], ['2023-02-01', '2023-02-02']),
        (3, NULL, NULL, NULL),
        (4, [], [], []),
        (5, ['test', 'array'], [7, 8, 9], ['2023-03-01', '2023-03-02']),
        (6, ['index', 'support'], [10, 11, 12], ['2023-04-01', '2023-04-02']);
    """

    // Create indexes on supported array types - should succeed
    sql """ ALTER TABLE ${tableName1} ADD INDEX idx_str_arr (str_arr) USING INVERTED; """
    wait_for_latest_op_on_table_finish(tableName1, timeout)

    sql """ ALTER TABLE ${tableName1} ADD INDEX idx_int_arr (int_arr) USING INVERTED; """
    wait_for_latest_op_on_table_finish(tableName1, timeout)

    sql """ ALTER TABLE ${tableName1} ADD INDEX idx_date_arr (date_arr) USING INVERTED; """
    wait_for_latest_op_on_table_finish(tableName1, timeout)

    // Create table with unsupported array types
    sql """
        CREATE TABLE ${tableName2} (
            id int,
            nested_arr ARRAY<ARRAY<STRING>>,
            map_arr ARRAY<MAP<STRING,INT>>,
            float_arr ARRAY<FLOAT>,
            struct_arr ARRAY<STRUCT<
                name:STRING,
                age:INT,
                score:FLOAT
            >>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    // Insert some data into unsupported array type table
    sql """ INSERT INTO ${tableName2} VALUES
        (1, [['a', 'b'], ['c', 'd']], [{'key1': 1, 'key2': 2}], [1.1, 2.2], array(named_struct('name', 'Alice', 'age', 20, 'score', 85.5))),
        (2, [['e', 'f']], [{'key3': 3}], [3.3], array(named_struct('name', 'Bob', 'age', 25, 'score', 90.0)));
    """

    test {
         sql """ ALTER TABLE ${tableName2} ADD INDEX idx_nested_arr (nested_arr) USING INVERTED; """
         exception "is not supported in"
    } 

    // Test creating index on array of map - should fail
    test {
        sql """ ALTER TABLE ${tableName2} ADD INDEX idx_map_arr (map_arr) USING INVERTED; """
        exception "is not supported in"
    }

    // Test creating index on array of float - should fail
    test {
        sql """ ALTER TABLE ${tableName2} ADD INDEX idx_float_arr (float_arr) USING INVERTED; """
        exception "is not supported in"
    }

    // Test creating index on array of struct - should fail
    test {
        sql """ ALTER TABLE ${tableName2} ADD INDEX idx_struct_arr (struct_arr) USING INVERTED; """
        exception "is not supported in"
    }

    // Test array_contains function
    qt_sql """ 
        SELECT id, str_arr, int_arr, date_arr 
        FROM ${tableName1} 
        WHERE array_contains(str_arr, 'world') 
            OR array_contains(int_arr, 8) 
            OR array_contains(date_arr, '2023-03-01')
        ORDER BY id;
    """

    // Test array_contains with multiple conditions
    qt_sql """
        SELECT id 
        FROM ${tableName1}
        WHERE array_contains(str_arr, 'apache')
            AND array_contains(int_arr, 5)
            AND array_contains(date_arr, '2023-02-02')
        ORDER BY id;
    """

    // Test array_contains with NULL and empty arrays
    qt_sql """
        SELECT id, str_arr
        FROM ${tableName1}
        WHERE array_contains(str_arr, 'test')
            OR str_arr IS NULL
        ORDER BY id;
    """

    sql "DROP TABLE IF EXISTS ${tableName1}"
    sql "DROP TABLE IF EXISTS ${tableName2}"
} 
