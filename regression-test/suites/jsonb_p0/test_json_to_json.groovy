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


suite("test_jsonb_to_json") {

    sql """ 
        set DEBUG_SKIP_FOLD_CONSTANT = true;
    """
    // Because there are currently display issues with JSON parsing on the frontend. 
    // For example, the number 123456789.123456789 will be displayed as 1.2345678912345679E8. 
    // Therefore, frontend calculations are disabled here.

    qt_sql """
        select to_json(123) ,  to_json(123.4);
    """

    qt_sql """
        select to_json("123") ,  to_json("123.4");
    """

    qt_sql """
        select to_json(array(1,2,3,4));
    """

    qt_sql """
        select to_json(array("string","1","2","3"));
    """

    qt_sql """
        select to_json(struct("123",456,array(123,456,789)));
    """


    sql "DROP TABLE IF EXISTS test_json_to_json_table"

    sql """
    CREATE TABLE IF NOT EXISTS test_json_to_json_table (
        id INT,
        int_val INT,
        double_val DOUBLE,
        string_val STRING,
        array_val ARRAY<INT>,
        struct_val STRUCT<a:STRING, b:INT>,
        decimal_val DECIMAL(12,4)
    )
    DISTRIBUTED BY HASH(id) BUCKETS 3
    PROPERTIES (
        "replication_num" = "1"
    )
    """

    sql """
    INSERT INTO test_json_to_json_table VALUES
    (1, 100, 123.45, 'hello',  [1, 2, 3], NAMED_STRUCT("a", "test", "b", 10), 1234.5678),
    (2, 200, 456.78, 'world',  [4, 5, 6], NAMED_STRUCT("a", "demo", "b", 20), 9876.5432),
    (3, 300, 789.01, 'doris',  [7, 8, 9], NAMED_STRUCT("a", "example", "b", 30), 5555.6666),
    (4, null , null, null,  null, null, null),
    (5, null , null ,null , [10, null , 11] , NAMED_STRUCT("a", null, "b", 20), null),
    (6, 400, null ,null , [12, 13, 14], NAMED_STRUCT("a", "test2", "b", null), 1234.0000),
    (7, 500, 678.90, 'example', [15, 16, 17], NAMED_STRUCT("a", "sample", "b", 40), null),
    (8, null , null ,null , [18, 19, null], NAMED_STRUCT("a", null, "b", null), null)
    """

    qt_sql """
    SELECT id, to_json(int_val) as int_json, to_json(double_val) as double_json, to_json(string_val) as string_json
    FROM test_json_to_json_table
    ORDER BY id
    """

    qt_sql """
    SELECT id, to_json(array_val) as array_json
    FROM test_json_to_json_table
    ORDER BY id
    """

    qt_sql """
    SELECT id, to_json(struct_val) as struct_json
    FROM test_json_to_json_table
    ORDER BY id
    """

    qt_sql """
    SELECT id, 
           to_json(struct(int_val, double_val, string_val)) as combined_struct,
           to_json(array(int_val, double_val)) as combined_array
    FROM test_json_to_json_table
    ORDER BY id
    """

    qt_sql """
    SELECT id, to_json(decimal_val) as decimal_json
    FROM test_json_to_json_table
    ORDER BY id
    """

    qt_sql """
    SELECT 
        to_json(CAST(1234.56 AS DECIMAL(10,2))) as decimal_10_2,
        to_json(CAST(1234.56789 AS DECIMAL(12,5))) as decimal_12_5,
        to_json(CAST(123456789.123456789 AS DECIMAL(20,9))) as decimal_20_9
    """

    qt_sql """
    SELECT id, 
        to_json(struct(int_val, decimal_val)) as struct_with_decimal,
        to_json(array(decimal_val, double_val)) as array_with_decimal
    FROM test_json_to_json_table
    ORDER BY id
    """

    sql "DROP TABLE IF EXISTS test_json_to_json_table"

    sql """
    CREATE TABLE IF NOT EXISTS test_json_to_json_table (
        id INT NOT NULL,
        int_val INT NOT NULL,
        double_val DOUBLE NOT NULL,
        string_val STRING NOT NULL,
        array_val ARRAY<INT> NOT NULL,
        struct_val STRUCT<a:STRING, b:INT> NOT NULL ,
        decimal_val DECIMAL(12,4) NOT NULL
    )
    DISTRIBUTED BY HASH(id) BUCKETS 3
    PROPERTIES (
        "replication_num" = "1"
    )
    """

    sql """
    INSERT INTO test_json_to_json_table VALUES
    (1, 100, 123.45, 'hello',  [1, 2, 3], NAMED_STRUCT("a", "test", "b", 10), 1234.5678),
    (2, 200, 456.78, 'world',  [4, 5, 6], NAMED_STRUCT("a", "demo", "b", 20), 9876.5432),
    (3, 300, 789.01, 'doris',  [7, 8, 9], NAMED_STRUCT("a", "example", "b", 30), 5555.6666)
    """

    qt_sql """
    SELECT id, to_json(int_val) as int_json, to_json(double_val) as double_json, to_json(string_val) as string_json
    FROM test_json_to_json_table
    ORDER BY id
    """

    qt_sql """
    SELECT id, to_json(array_val) as array_json
    FROM test_json_to_json_table
    ORDER BY id
    """

    qt_sql """
    SELECT id, to_json(struct_val) as struct_json
    FROM test_json_to_json_table
    ORDER BY id
    """

    qt_sql """
    SELECT id, 
           to_json(struct(int_val, double_val, string_val)) as combined_struct,
           to_json(array(int_val, double_val)) as combined_array
    FROM test_json_to_json_table
    ORDER BY id
    """

    qt_sql """
    SELECT id, to_json(decimal_val) as decimal_json
    FROM test_json_to_json_table
    ORDER BY id
    """

    qt_sql """
    SELECT 
        to_json(CAST(1234.56 AS DECIMAL(10,2))) as decimal_10_2,
        to_json(CAST(1234.56789 AS DECIMAL(12,5))) as decimal_12_5,
        to_json(CAST(123456789.123456789 AS DECIMAL(20,9))) as decimal_20_9
    """

    qt_sql """
    SELECT id, 
        to_json(struct(int_val, decimal_val)) as struct_with_decimal,
        to_json(array(decimal_val, double_val)) as array_with_decimal
    FROM test_json_to_json_table
    ORDER BY id
    """

    qt_sql """
    SELECT to_json(true),to_json(false);
    """

    try {
        sql "select to_json(makedate(2020,5));"
    } catch (Exception ex) {
        assert("${ex}".contains("to_json(DATE)"))
    }



     try {
        sql "select to_json(NAMED_STRUCT('00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000',5));"
    } catch (Exception ex) {
        assert("${ex}".contains("key size exceeds max limit"))
    }


}
