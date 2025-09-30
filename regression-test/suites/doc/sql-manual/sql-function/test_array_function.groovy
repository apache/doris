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

suite("test_array_function_doc", "p0") {
    
    def tableName = "array_table"
    sql """
        drop table if exists ${tableName};
    """
    sql """ set enable_decimal256 = true; """

    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT,
            array_boolean ARRAY<BOOLEAN>,
            array_tinyint ARRAY<TINYINT>,
            array_smallint ARRAY<SMALLINT>,
            array_int ARRAY<INT>,
            array_bigint ARRAY<BIGINT>,
            array_largeint ARRAY<LARGEINT>,
            array_float ARRAY<FLOAT>,
            array_double ARRAY<DOUBLE>,
            array_decimal32 ARRAY<DECIMAL(7, 2)>,
            array_decimal64 ARRAY<DECIMAL(15, 3)>,
            array_decimal128 ARRAY<DECIMAL(25, 5)>,
            array_decimal256 ARRAY<DECIMAL(40, 7)>,
            array_string ARRAY<STRING>,
            array_varchar ARRAY<VARCHAR(20)>,
            array_char ARRAY<CHAR(20)>,
            array_date ARRAY<DATE>,
            array_datetime ARRAY<DATETIME>,
            array_ipv4 ARRAY<IPV4>,
            array_ipv6 ARRAY<IPV6>,
            array_struct ARRAY<STRUCT<id: INT, name: STRING>>,
            array_array ARRAY<ARRAY<INT>>,
            array_map ARRAY<MAP<STRING, INT>>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        INSERT INTO ${tableName} (id, array_boolean, array_tinyint, array_smallint, array_int, array_bigint, array_largeint, array_float, array_double, array_decimal32, array_decimal64, array_decimal128, array_decimal256, array_string, array_varchar, array_char, array_date, array_datetime, array_ipv4, array_ipv6, array_struct, array_array, array_map) VALUES
        (1, [true, false, true], [1, 2, 3], [1, 2, 3], [1, 2, 3], [1, 2, 3], [1, 2, 3], [1.1, 2.2, 3.3], [1.1, 2.2, 3.3], [1.1, 2.2, 3.3], [1.1, 2.2, 3.3], [1.1, 2.2, 3.3], [1.1, 2.2, 3.3],
        ['hello', 'world', 'hello world'], ['hello', 'world', 'hello world'], ['hello', 'world', 'hello world'], ['2021-01-01', '2021-01-02', '2021-01-03'], ['2021-01-01 00:00:00', '2021-01-02 00:00:00', '2021-01-03 00:00:00'], ['192.168.1.1', '192.168.1.2', '192.168.1.3'], ['::1', '::2', '::3'],
        ARRAY(STRUCT(1, 'John'), STRUCT(2, 'Jane'), STRUCT(3, 'Jim')), [[1, 2, 3], [4, 5, 6], [7, 8, 9]], ARRAY(MAP('key1', 1), MAP('key2', 2), MAP('key3', 3)));
    """

    qt_sql """
        SELECT * FROM ${tableName};
    """

    qt_sql """ select array_zip(array_boolean, array_tinyint, array_smallint, array_int, array_bigint, array_largeint, array_float, array_double, array_decimal32, array_decimal64, array_decimal128, array_decimal256, array_string, array_varchar, array_char, array_date, array_datetime, array_ipv4, array_ipv6, array_struct, array_array, array_map) from ${tableName}; """

    qt_sql """ select array_zip(null, array_boolean) from ${tableName}; """

    test {
         sql """ select array_zip(array_boolean, [1, 2]) from ${tableName}; """
         exception "function array_zip's 2-th argument should have same offsets with first argument"
    }

    qt_sql """ select array_zip([null, null, null], array_boolean) from ${tableName}; """

    qt_sql """ SELECT ARRAY_ZIP(ARRAY(23, 24, 25), ARRAY("John", "Jane", "Jim"), ARRAY(true, false, true)) as result; """

    qt_sql """ SELECT ARRAY_ZIP(ARRAY(23, 24, 25), ARRAY("John", "Jane", "Jim"), ARRAY(true, false, true))[1] as result; """

    qt_sql """ SELECT ARRAY_ZIP(ARRAY(23, 24, 25), ARRAY("John", "Jane", "Jim"), NULL) as result; """

    qt_sql """ SELECT ARRAY_ZIP(ARRAY(23, NULL, 25), ARRAY("John", "Jane", NULL), ARRAY(NULL, false, true)) as result; """

    qt_sql """ SELECT ARRAYS_OVERLAP(array_boolean, array_boolean) from ${tableName}; """

    qt_sql """ SELECT ARRAYS_OVERLAP(array_tinyint, array_tinyint) from ${tableName}; """

    qt_sql """ SELECT ARRAYS_OVERLAP(array_smallint, array_smallint) from ${tableName}; """

    qt_sql """ SELECT ARRAYS_OVERLAP(array_int, array_int) from ${tableName}; """

    qt_sql """ SELECT ARRAYS_OVERLAP(array_bigint, array_bigint) from ${tableName}; """

    qt_sql """ SELECT ARRAYS_OVERLAP(array_largeint, array_largeint) from ${tableName}; """

    qt_sql """ SELECT ARRAYS_OVERLAP(array_float, array_float) from ${tableName}; """

    qt_sql """ SELECT ARRAYS_OVERLAP(array_double, array_double) from ${tableName}; """

    qt_sql """ SELECT ARRAYS_OVERLAP(array_decimal32, array_decimal32) from ${tableName}; """

    qt_sql """ SELECT ARRAYS_OVERLAP(array_decimal64, array_decimal64) from ${tableName}; """

    qt_sql """ SELECT ARRAYS_OVERLAP(array_decimal128, array_decimal128) from ${tableName}; """

    qt_sql """ SELECT ARRAYS_OVERLAP(array_decimal256, array_decimal256) from ${tableName}; """

    qt_sql """ SELECT ARRAYS_OVERLAP(array_string, array_string) from ${tableName}; """

    qt_sql """ SELECT ARRAYS_OVERLAP(array_varchar, array_varchar) from ${tableName}; """

    qt_sql """ SELECT ARRAYS_OVERLAP(array_char, array_char) from ${tableName}; """

    qt_sql """ SELECT ARRAYS_OVERLAP(array_date, array_date) from ${tableName}; """

    qt_sql """ SELECT ARRAYS_OVERLAP(array_datetime, array_datetime) from ${tableName}; """

    qt_sql """ SELECT ARRAYS_OVERLAP(array_ipv4, array_ipv4) from ${tableName}; """

    qt_sql """ SELECT ARRAYS_OVERLAP(array_ipv6, array_ipv6) from ${tableName}; """

    test {
        sql """ SELECT ARRAYS_OVERLAP(array_struct, array_struct) from ${tableName}; """
        exception "arrays_overlap does not support types: ARRAY<STRUCT<id:INT,name:TEXT>>"
    }

    test {
        sql """ SELECT ARRAYS_OVERLAP(array_array, array_array) from ${tableName}; """
        exception "arrays_overlap does not support types: ARRAY<ARRAY<INT>>"
    }

    test {
        sql """ SELECT ARRAYS_OVERLAP(array_map, array_map) from ${tableName}; """
        exception "arrays_overlap does not support types: ARRAY<MAP<TEXT,INT>>"
    }


   qt_sql """  SELECT ARRAYS_OVERLAP(ARRAY('hello', 'aloha'), ARRAY('hello', 'hi', 'hey')); """

   qt_sql  """  SELECT ARRAYS_OVERLAP(ARRAY('Pinnacle', 'aloha'), ARRAY('hi', 'hey')); """

    test {
        sql """ SELECT ARRAYS_OVERLAP(ARRAY(ARRAY('hello', 'aloha'), ARRAY('hi', 'hey')), ARRAY(ARRAY('hello', 'hi', 'hey'), ARRAY('aloha', 'hi'))); """
        exception "arrays_overlap does not support types: ARRAY<ARRAY<TEXT>>"
    }

    qt_sql """ SELECT ARRAYS_OVERLAP(ARRAY('HELLO', 'ALOHA'), NULL); """

    qt_sql """ SELECT ARRAYS_OVERLAP(NULL, NULL); """

    qt_sql """ SELECT ARRAYS_OVERLAP(ARRAY('HELLO', 'ALOHA'), ARRAY('HELLO', NULL)); """

    qt_sql """ SELECT ARRAYS_OVERLAP(ARRAY('PICKLE', 'ALOHA'), ARRAY('HELLO', NULL)); """

    qt_sql """ SELECT ARRAYS_OVERLAP(ARRAY(NULL), ARRAY('HELLO', NULL)); """

    sql """ drop table if exists arrays_overlap_table; """
    sql """ CREATE TABLE IF NOT EXISTS arrays_overlap_table (
            id INT,
            array_column ARRAY<STRING>,
            INDEX idx_array_column (array_column) USING INVERTED
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    sql """ INSERT INTO arrays_overlap_table (id, array_column) VALUES (1, ARRAY('HELLO', 'ALOHA')), (2, ARRAY('NO', 'WORLD')); """

    qt_sql """ SELECT * from arrays_overlap_table where ARRAYS_OVERLAP(array_column, ARRAY('HELLO', 'PICKLE')); """

    qt_sql """ SELECT ARRAYS_OVERLAP(array_column, ARRAY('HELLO', 'PICKLE')) from arrays_overlap_table;"""

    def tableName2 = "plain_table"

    sql """ drop table if exists ${tableName2}; """
    sql """ CREATE TABLE IF NOT EXISTS ${tableName2} (
            id INT,
            boolean_column BOOLEAN,
            tinyint_column TINYINT,
            smallint_column SMALLINT,
            int_column INT,
            bigint_column BIGINT,
            largeint_column LARGEINT,
            float_column FLOAT,
            double_column DOUBLE,
            decimal32_column DECIMAL(7, 2),
            decimal64_column DECIMAL(15, 3),
            decimal128_column DECIMAL(25, 5),
            decimal256_column DECIMAL(40, 7),
            string_column STRING,
            varchar_column VARCHAR(20),
            char_column CHAR(20),
            date_column DATE,
            datetime_column DATETIME,
            ipv4_column IPV4,
            ipv6_column IPV6,
            struct_column STRUCT<id: INT, name: STRING>,
            array_column ARRAY<INT>,
            map_column MAP<STRING, INT>,
            json_column JSON,
            variant_column VARIANT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    sql """ INSERT INTO ${tableName2} (id, boolean_column, tinyint_column, smallint_column, int_column, bigint_column, largeint_column, float_column, double_column, decimal32_column, decimal64_column, decimal128_column, decimal256_column, string_column, varchar_column, char_column, date_column, datetime_column, ipv4_column, ipv6_column, struct_column, array_column, map_column, json_column, variant_column) VALUES
    (1, true, 1, 1, 1, 1, 1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 'hello', 'hello', 'hello', '2021-01-01', '2021-01-01 00:00:00', '192.168.1.1', '::1', STRUCT(1, 'John'), [1, 2, 3], MAP('key1', 1), '{"a": 1}', '{"b" : "c"}'); """

    qt_sql """ SELECT ARRAY_WITH_CONSTANT(2, boolean_column) from ${tableName2}; """

    qt_sql """ SELECT ARRAY_WITH_CONSTANT(2, tinyint_column) from ${tableName2}; """

    qt_sql """ SELECT ARRAY_WITH_CONSTANT(2, smallint_column) from ${tableName2}; """

    qt_sql """ SELECT ARRAY_WITH_CONSTANT(2, int_column) from ${tableName2}; """

    qt_sql """ SELECT ARRAY_WITH_CONSTANT(2, bigint_column) from ${tableName2}; """

    qt_sql """ SELECT ARRAY_WITH_CONSTANT(2, largeint_column) from ${tableName2}; """

    qt_sql """ SELECT ARRAY_WITH_CONSTANT(2, float_column) from ${tableName2}; """

    qt_sql """ SELECT ARRAY_WITH_CONSTANT(2, double_column) from ${tableName2}; """

    qt_sql """ SELECT ARRAY_WITH_CONSTANT(2, decimal32_column) from ${tableName2}; """

    qt_sql """ SELECT ARRAY_WITH_CONSTANT(2, decimal64_column) from ${tableName2}; """

    qt_sql """ SELECT ARRAY_WITH_CONSTANT(2, decimal128_column) from ${tableName2}; """

    qt_sql """ SELECT ARRAY_WITH_CONSTANT(2, decimal256_column) from ${tableName2}; """

    qt_sql """ SELECT ARRAY_WITH_CONSTANT(2, string_column) from ${tableName2}; """

    qt_sql """ SELECT ARRAY_WITH_CONSTANT(2, varchar_column) from ${tableName2}; """

    qt_sql """ SELECT ARRAY_WITH_CONSTANT(2, char_column) from ${tableName2}; """

    qt_sql """ SELECT ARRAY_WITH_CONSTANT(2, date_column) from ${tableName2}; """

    qt_sql """ SELECT ARRAY_WITH_CONSTANT(2, datetime_column) from ${tableName2}; """

    qt_sql """ SELECT ARRAY_WITH_CONSTANT(2, ipv4_column) from ${tableName2}; """

    qt_sql """ SELECT ARRAY_WITH_CONSTANT(2, ipv6_column) from ${tableName2}; """

    qt_sql """ SELECT ARRAY_WITH_CONSTANT(2, struct_column) from ${tableName2}; """

    qt_sql """ SELECT ARRAY_WITH_CONSTANT(5, array_column) from ${tableName2}; """

    qt_sql """ SELECT ARRAY_WITH_CONSTANT(5, map_column) from ${tableName2}; """

    qt_sql """ SELECT ARRAY_WITH_CONSTANT(id, map_column) from ${tableName2}; """

    qt_sql """ SELECT ARRAY_WITH_CONSTANT(id, json_column) from ${tableName2}; """

    qt_sql """ SELECT ARRAY_WITH_CONSTANT(id, variant_column) from ${tableName2}; """

    test {
        sql """ SELECT ARRAY_WITH_CONSTANT(-1, 'hello');"""
        exception "Array size should in range(0, 1000000) in function: array_with_constant"
    }

    test {
        sql """ SELECT ARRAY_WITH_CONSTANT(1000001, 'hello');"""
        exception "Array size should in range(0, 1000000) in function: array_with_constant"
    }
    
    qt_sql """ SELECT ARRAY_WITH_CONSTANT(0, 'hello'); """

    qt_sql """ SELECT ARRAY_WITH_CONSTANT(NULL, 'hello'); """

    qt_sql """ SELECT ARRAY_WITH_CONSTANT(2, NULL); """

    qt_sql """ SELECT ARRAY_WITH_CONSTANT(NULL, NULL); """

    qt_sql """ SELECT ARRAY_REPEAT(boolean_column, 3) from ${tableName2}; """

    qt_sql """ SELECT ARRAY_REPEAT(tinyint_column, 3) from ${tableName2}; """

    qt_sql """ SELECT ARRAY_REPEAT(smallint_column, 3) from ${tableName2}; """

    qt_sql """ SELECT ARRAY_REPEAT(int_column, 3) from ${tableName2}; """

    qt_sql """ SELECT ARRAY_REPEAT(bigint_column, 3) from ${tableName2}; """

    qt_sql """ SELECT ARRAY_REPEAT(largeint_column, 3) from ${tableName2}; """

    qt_sql """ SELECT ARRAY_REPEAT(float_column, 3) from ${tableName2}; """

    qt_sql """ SELECT ARRAY_REPEAT(double_column, 3) from ${tableName2}; """

    qt_sql """ SELECT ARRAY_REPEAT(decimal32_column, 3) from ${tableName2}; """

    qt_sql """ SELECT ARRAY_REPEAT(decimal64_column, 3) from ${tableName2}; """

    qt_sql """ SELECT ARRAY_REPEAT(decimal128_column, 3) from ${tableName2}; """

    qt_sql """ SELECT ARRAY_REPEAT(decimal256_column, 3) from ${tableName2}; """

    qt_sql """ SELECT ARRAY_REPEAT(string_column, 3) from ${tableName2}; """

    qt_sql """ SELECT ARRAY_REPEAT(varchar_column, 3) from ${tableName2}; """

    qt_sql """ SELECT ARRAY_REPEAT(char_column, 3) from ${tableName2}; """

    qt_sql """ SELECT ARRAY_REPEAT(date_column, 3) from ${tableName2}; """

    qt_sql """ SELECT ARRAY_REPEAT(datetime_column, 3) from ${tableName2}; """

    qt_sql """ SELECT ARRAY_REPEAT(ipv4_column, 3) from ${tableName2}; """

    qt_sql """ SELECT ARRAY_REPEAT(ipv6_column, 3) from ${tableName2}; """

    qt_sql """ SELECT ARRAY_REPEAT(struct_column, 3) from ${tableName2}; """

    qt_sql """ SELECT ARRAY_REPEAT(array_column, 3) from ${tableName2}; """

    qt_sql """ SELECT ARRAY_REPEAT(map_column, 3) from ${tableName2}; """

    qt_sql """ SELECT ARRAY_REPEAT(json_column, 3) from ${tableName2}; """

    qt_sql """ SELECT ARRAY_REPEAT(variant_column, 3) from ${tableName2}; """

     qt_sql """ SELECT ARRAY_REPEAT(map_column, id) from ${tableName2}; """

    qt_sql """ SELECT ARRAY_REPEAT(json_column, id) from ${tableName2}; """

    qt_sql """ SELECT ARRAY_REPEAT(variant_column, id) from ${tableName2}; """

    test {
        sql """ SELECT ARRAY_REPEAT('hello',-1);"""
        exception "Array size should in range(0, 1000000) in function: array_repeat"
    }

    test {
        sql """ SELECT ARRAY_REPEAT('hello', 1000001);"""
        exception "Array size should in range(0, 1000000) in function: array_repeat"
    }

    qt_sql """ SELECT ARRAY_REPEAT('hello', 0); """

    qt_sql """ SELECT ARRAY_REPEAT('hello', NULL); """

    qt_sql """ SELECT ARRAY_REPEAT(NULL, 3); """

    qt_sql """ SELECT ARRAY_REPEAT(NULL, NULL); """
    
    qt_sql """ SELECT ARRAY_REPEAT(NULL, 'hello'); """
    
    qt_sql """ SELECT ARRAY_UNION(array_boolean, array_boolean) from ${tableName}; """

    qt_sql """ SELECT ARRAY_UNION(array_tinyint, array_tinyint) from ${tableName}; """

    qt_sql """ SELECT ARRAY_UNION(array_smallint, array_smallint) from ${tableName}; """

    qt_sql """ SELECT ARRAY_UNION(array_int, array_int) from ${tableName}; """

    qt_sql """ SELECT ARRAY_UNION(array_bigint, array_bigint) from ${tableName}; """

    qt_sql """ SELECT ARRAY_UNION(array_largeint, array_largeint) from ${tableName}; """

    qt_sql """ SELECT ARRAY_UNION(array_float, array_float) from ${tableName}; """

    qt_sql """ SELECT ARRAY_UNION(array_double, array_double) from ${tableName}; """

    qt_sql """ SELECT ARRAY_UNION(array_decimal32, array_decimal32) from ${tableName}; """

    qt_sql """ SELECT ARRAY_UNION(array_decimal64, array_decimal64) from ${tableName}; """

    qt_sql """ SELECT ARRAY_UNION(array_decimal128, array_decimal128) from ${tableName}; """

    qt_sql """ SELECT ARRAY_UNION(array_decimal256, array_decimal256) from ${tableName}; """

    qt_sql """ SELECT ARRAY_UNION(array_string, array_string) from ${tableName}; """

    qt_sql """ SELECT ARRAY_UNION(array_varchar, array_varchar) from ${tableName}; """

    qt_sql """ SELECT ARRAY_UNION(array_char, array_char) from ${tableName}; """

    qt_sql """ SELECT ARRAY_UNION(array_date, array_date) from ${tableName}; """

    qt_sql """ SELECT ARRAY_UNION(array_datetime, array_datetime) from ${tableName}; """

    qt_sql """ SELECT ARRAY_UNION(array_ipv4, array_ipv4) from ${tableName}; """

    qt_sql """ SELECT ARRAY_UNION(array_ipv6, array_ipv6) from ${tableName}; """
    
    test {
        sql """ SELECT ARRAY_UNION(array_struct, array_struct) from ${tableName}; """
        exception "array_union does not support types: ARRAY<STRUCT<id:INT,name:TEXT>>"
    }

    test {
        sql """ SELECT ARRAY_UNION(array_array, array_array) from ${tableName}; """
        exception "array_union does not support types: ARRAY<ARRAY<INT>>"
    }

    test {
        sql """ SELECT ARRAY_UNION(array_map, array_map) from ${tableName}; """
        exception "array_union does not support types: ARRAY<MAP<TEXT,INT>>"
    }
    
    qt_sql """ SELECT ARRAY_UNION(NULL, array_boolean) from ${tableName}; """

    qt_sql """ SELECT ARRAY_UNION(array_boolean, NULL) from ${tableName}; """

    qt_sql """ SELECT ARRAY_UNION(NULL, NULL); """

    qt_sql """ SELECT ARRAY_UNION(ARRAY(1, 2, 3), ARRAY(3, 5, 6)); """

    qt_sql """ SELECT ARRAY_UNION(ARRAY('hello', 'world'), ARRAY('hello', 'world')); """

    qt_sql """ SELECT ARRAY_UNION(ARRAY('hello', 'world'), ARRAY('hello', 'world'), NULL); """
 
    qt_sql """ SELECT ARRAY_UNION(ARRAY('hello', 'world'), ARRAY('hello', NULL)); """

    qt_sql """SELECT ARRAY_UNION(ARRAY(NULL, 'world'), ARRAY('hello', NULL)); """

    qt_sql """ SELECT ARRAY_SUM(array_tinyint) from ${tableName}; """

    qt_sql """ SELECT ARRAY_SUM(ARRAY(1, 2, 3, null)); """

    qt_sql """ SELECT ARRAY_SUM(ARRAY(null)); """

    test {
        sql """ SELECT ARRAY_REMOVE(array(array(1, 2), array(3, 4)), array(1, 2)); """
        exception "array_remove does not support types: ARRAY<ARRAY<TINYINT>>"
    }

    test {
        sql """ SELECT CountEqual(array(array(1, 2), array(3, 4)), array(1, 2)); """
        exception "countequal does not support types: ARRAY<ARRAY<TINYINT>>"
    }

    test {
        sql """ SELECT ARRAY_SORTBY(array(array(1, 2), array(3, 4)), array(1, 2)); """
        exception "array_reverse_sort does not support types: ARRAY<ARRAY<TINYINT>>"
    }

    test {
        sql """ SELECT ARRAY_SORT(array(array(1, 2), array(3, 4))); """
        exception "array_sort does not support types: ARRAY<ARRAY<TINYINT>>"
    }

    test {
        sql """ SELECT ARRAY_REVERSE_SORT(array(array(1, 2), array(3, 4))); """
        exception "array_reverse_sort does not support types: ARRAY<ARRAY<TINYINT>>"
    }

    qt_sql """ SELECT ARRAY_REMOVE(ARRAY(1, 2, 3, 2, null), 2); """
    qt_sql """ SELECT ARRAY_REMOVE(array_int, 2) from ${tableName}; """

    qt_sql """ SELECT ARRAY_RANGE(5); """
    qt_sql """ SELECT ARRAY_RANGE(1, 5); """
    qt_sql """ SELECT ARRAY_RANGE(1, 10, 2); """
    qt_sql """ SELECT ARRAY_RANGE(id) from ${tableName}; """
    qt_sql """ SELECT ARRAY_RANGE(id, id + 3) from ${tableName}; """
    qt_sql """ SELECT ARRAY_RANGE(id, id + 6, 2) from ${tableName}; """

    qt_sql """ SELECT ARRAY_REPEAT('x', 3); """
    qt_sql """ SELECT ARRAY_REPEAT(id, 2) from ${tableName}; """

    qt_sql """ SELECT ARRAY_REVERSE_SORT(ARRAY(3, 1, 2, null)); """
    qt_sql """ SELECT ARRAY_REVERSE_SORT(array_string) from ${tableName}; """
    qt_sql """ SELECT ARRAY_REVERSE_SORT(array_int) from ${tableName}; """

    qt_sql """ SELECT ARRAY_REVERSE_SPLIT([1,2,3,4,5], [0,0,0,1,0]); """
    qt_sql """ SELECT ARRAY_REVERSE_SPLIT(x -> (x > 1), array_int) from ${tableName}; """

    qt_sql """ SELECT ARRAY_SHUFFLE([1,2,3,4,5], 0); """
    qt_sql """ SELECT ARRAY_SIZE(ARRAY_SHUFFLE(array_string, 0)) from ${tableName}; """

    qt_sql """ SELECT ARRAY_SIZE(ARRAY('a','b',null)); """
    qt_sql """ SELECT ARRAY_SIZE(array_string) from ${tableName}; """

    qt_sql """ SELECT ARRAY_SLICE([1,2,3,4,5,6], 2, 3); """
    qt_sql """ SELECT ARRAY_SLICE(array_int, 2, 2) from ${tableName}; """

    qt_sql """ SELECT ARRAY_SORT([3,1,2,null]); """
    qt_sql """ SELECT ARRAY_SORT(array_string) from ${tableName}; """
    qt_sql """ SELECT ARRAY_SORT(array_int) from ${tableName}; """

    qt_sql """ SELECT ARRAY_SORTBY(['a','b','c'], [3,2,1]); """
    qt_sql """ SELECT ARRAY_SORTBY(array_string, array_int) from ${tableName}; """

    qt_sql """ SELECT ARRAY_SPLIT([1,2,3,4,5], [1,0,0,0,0]); """
    qt_sql """ SELECT ARRAY_SPLIT(x -> (x > 1), array_int) from ${tableName}; """

    qt_sql """ SELECT CountEqual([1,2,2,3,null], 2); """
    qt_sql """ SELECT CountEqual(array_int, id) from ${tableName}; """


    sql """
        INSERT INTO ${tableName} (id, array_boolean, array_tinyint, array_smallint, array_int, array_bigint, array_largeint, array_float, array_double, array_decimal32, array_decimal64, array_decimal128, array_decimal256, array_string, array_varchar, array_char, array_date, array_datetime, array_ipv4, array_ipv6, array_struct, array_array, array_map) VALUES
        (1, [true, false, true, null], [1, 2, 3, null], [1, 2, 3, null], [1, 2, 3, null], [1, 2, 3, null], [1, 2, 3, null], [1.1, 2.2, 3.3, null], [1.1, 2.2, 3.3, null], [1.1, 2.2, 3.3, null], [1.1, 2.2, 3.3, null], [1.1, 2.2, 3.3, null], [1.1, 2.2, 3.3, null],
        ['hello', 'world', 'hello world', null], ['hello', 'world', 'hello world', null], ['hello', 'world', 'hello world', null], ['2021-01-01', '2021-01-02', '2021-01-03', null], ['2021-01-01 00:00:00', '2021-01-02 00:00:00', '2021-01-03 00:00:00', null], ['192.168.1.1', '192.168.1.2', '192.168.1.3', null], ['::1', '::2', '::3', null],
        ARRAY(STRUCT(1, 'John'), STRUCT(2, 'Jane'), STRUCT(3, 'Jim'), null), [[1, 2, 3], [4, 5, 6], [7, 8, 9], null], ARRAY(MAP('key1', 1), MAP('key2', 2), MAP('key3', 3), null));
    """

    qt_sql """ SELECT ARRAY_REMOVE(ARRAY(1, 2, 3, 2, null), null); """
    qt_sql """ SELECT ARRAY_REMOVE(array_int, null) from ${tableName}; """
    qt_sql """ SELECT ARRAY_REMOVE(array_boolean, null) from ${tableName}; """
    qt_sql """ SELECT ARRAY_REMOVE(array_tinyint, null) from ${tableName}; """
    qt_sql """ SELECT ARRAY_REMOVE(array_smallint, null) from ${tableName}; """
    qt_sql """ SELECT ARRAY_REMOVE(array_int, null) from ${tableName}; """
    qt_sql """ SELECT ARRAY_REMOVE(array_bigint, null) from ${tableName}; """
    qt_sql """ SELECT ARRAY_REMOVE(array_largeint, null) from ${tableName}; """
    qt_sql """ SELECT ARRAY_REMOVE(array_float, null) from ${tableName}; """
    qt_sql """ SELECT ARRAY_REMOVE(array_double, null) from ${tableName}; """
    qt_sql """ SELECT ARRAY_REMOVE(array_decimal32, null) from ${tableName}; """
    qt_sql """ SELECT ARRAY_REMOVE(array_decimal64, null) from ${tableName}; """
    qt_sql """ SELECT ARRAY_REMOVE(array_decimal128, null) from ${tableName}; """
    qt_sql """ SELECT ARRAY_REMOVE(array_decimal256, null) from ${tableName}; """
    qt_sql """ SELECT ARRAY_REMOVE(array_string, null) from ${tableName}; """
    qt_sql """ SELECT ARRAY_REMOVE(array_varchar, null) from ${tableName}; """
    qt_sql """ SELECT ARRAY_REMOVE(array_char, null) from ${tableName}; """
    qt_sql """ SELECT ARRAY_REMOVE(array_date, null) from ${tableName}; """
    qt_sql """ SELECT ARRAY_REMOVE(array_datetime, null) from ${tableName}; """
    qt_sql """ SELECT ARRAY_REMOVE(array_ipv4, null) from ${tableName}; """
    qt_sql """ SELECT ARRAY_REMOVE(array_ipv6, null) from ${tableName}; """
}