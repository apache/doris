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

suite("test_mc_write_types", "p2,external,maxcompute,external_remote,external_remote_maxcompute") {
    String enabled = context.config.otherConfigs.get("enableMaxComputeTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable MaxCompute test.")
        return
    }

    String ak = context.config.otherConfigs.get("ak")
    String sk = context.config.otherConfigs.get("sk")
    String mc_catalog_name = "test_mc_write_types"
    String defaultProject = "doris_test_schema"

    sql """drop catalog if exists ${mc_catalog_name}"""
    sql """
    CREATE CATALOG IF NOT EXISTS ${mc_catalog_name} PROPERTIES (
        "type" = "max_compute",
        "mc.default.project" = "${defaultProject}",
        "mc.access_key" = "${ak}",
        "mc.secret_key" = "${sk}",
        "mc.endpoint" = "http://service.cn-beijing-vpc.maxcompute.aliyun-inc.com/api",
        "mc.quota" = "pay-as-you-go",
        "mc.enable.namespace.schema" = "true"
    );
    """

    sql """switch ${mc_catalog_name}"""

    def uuid = UUID.randomUUID().toString().replace("-", "").substring(0, 8)
    String db = "mc_write_types_${uuid}"

    sql """drop database if exists ${db}"""
    sql """create database ${db}"""
    sql """use ${db}"""

    try {
        // Test 1: Basic numeric types
        String tb1 = "basic_types_${uuid}"
        sql """DROP TABLE IF EXISTS ${tb1}"""
        sql """
        CREATE TABLE ${tb1} (
            bool_col BOOLEAN,
            tinyint_col TINYINT,
            smallint_col SMALLINT,
            int_col INT,
            bigint_col BIGINT,
            float_col FLOAT,
            double_col DOUBLE
        )
        """
        sql """
        INSERT INTO ${tb1} VALUES
            (true, 127, 32767, 2147483647, 9223372036854775807, 3.14, 3.141592653589793),
            (false, -128, -32768, -2147483648, -9223372036854775808, -1.5, -2.718281828),
            (null, null, null, null, null, null, null)
        """
        order_qt_basic_types """ SELECT * FROM ${tb1} """

        // Test 2: Decimal types
        String tb2 = "decimal_types_${uuid}"
        sql """DROP TABLE IF EXISTS ${tb2}"""
        sql """
        CREATE TABLE ${tb2} (
            d1 DECIMAL(9,0),
            d2 DECIMAL(8,4),
            d3 DECIMAL(18,6),
            d4 DECIMAL(38,12)
        )
        """
        sql """
        INSERT INTO ${tb2} VALUES
            (CAST(123456789 AS DECIMAL(9,0)), CAST(1234.5678 AS DECIMAL(8,4)),
             CAST(123456.789012 AS DECIMAL(18,6)), CAST(123456789.012345678901 AS DECIMAL(38,12))),
            (null, null, null, null)
        """
        order_qt_decimal_types """ SELECT * FROM ${tb2} """

        // Test 3: String types (STRING, VARCHAR, CHAR)
        String tb3 = "string_types_${uuid}"
        sql """DROP TABLE IF EXISTS ${tb3}"""
        sql """
        CREATE TABLE ${tb3} (
            str_col STRING,
            varchar_col VARCHAR(100),
            char_col CHAR(50)
        )
        """
        sql """
        INSERT INTO ${tb3} VALUES
            ('hello world', 'varchar_val', 'char_val'),
            ('', '', ''),
            (null, null, null)
        """
        order_qt_string_types """ SELECT * FROM ${tb3} """

        // Test 4: Date and datetime types
        String tb4 = "datetime_types_${uuid}"
        sql """DROP TABLE IF EXISTS ${tb4}"""
        sql """
        CREATE TABLE ${tb4} (
            date_col DATE,
            datetime_col DATETIME
        )
        """
        sql """
        INSERT INTO ${tb4} VALUES
            ('2025-01-01', '2025-01-01 12:00:00'),
            ('2025-12-31', '2025-12-31 23:59:59'),
            (null, null)
        """
        order_qt_datetime_types """ SELECT * FROM ${tb4} """

        // Test 5: Array types
        String tb5 = "array_types_${uuid}"
        sql """DROP TABLE IF EXISTS ${tb5}"""
        sql """
        CREATE TABLE ${tb5} (
            id INT,
            arr_str ARRAY<STRING>,
            arr_int ARRAY<INT>,
            arr_double ARRAY<DOUBLE>
        )
        """
        sql """
        INSERT INTO ${tb5} VALUES
            (1, ['a', 'b', 'c'], [1, 2, 3], [1.1, 2.2, 3.3]),
            (2, [], [], []),
            (3, null, null, null)
        """
        order_qt_array_types """ SELECT * FROM ${tb5} """

        // Test 6: Map types
        String tb6 = "map_types_${uuid}"
        sql """DROP TABLE IF EXISTS ${tb6}"""
        sql """
        CREATE TABLE ${tb6} (
            id INT,
            map_str MAP<STRING, STRING>,
            map_int MAP<INT, INT>
        )
        """
        sql """
        INSERT INTO ${tb6} VALUES
            (1, {'k1':'v1', 'k2':'v2'}, {1:10, 2:20}),
            (2, {'k3':'v3'}, {3:30}),
            (3, null, null)
        """
        order_qt_map_types """ SELECT * FROM ${tb6} """

        // Test 7: Struct types
        String tb7 = "struct_types_${uuid}"
        sql """DROP TABLE IF EXISTS ${tb7}"""
        sql """
        CREATE TABLE ${tb7} (
            id INT,
            s1 STRUCT<name:STRING, age:INT>,
            s2 STRUCT<city:STRING, code:INT>
        )
        """
        sql """
        INSERT INTO ${tb7} VALUES
            (1, named_struct('name', 'Alice', 'age', 30), named_struct('city', 'Beijing', 'code', 100)),
            (2, null, null)
        """
        order_qt_struct_types """ SELECT * FROM ${tb7} """

        // Test 8: All types combined in one table
        String tb8 = "all_types_${uuid}"
        sql """DROP TABLE IF EXISTS ${tb8}"""
        sql """
        CREATE TABLE ${tb8} (
            bool_col BOOLEAN,
            int_col INT,
            bigint_col BIGINT,
            float_col FLOAT,
            double_col DOUBLE,
            decimal_col DECIMAL(18,6),
            string_col STRING,
            varchar_col VARCHAR(200),
            char_col CHAR(50),
            date_col DATE,
            datetime_col DATETIME,
            arr_col ARRAY<STRING>,
            map_col MAP<STRING, STRING>,
            struct_col STRUCT<f1:STRING, f2:INT>
        )
        """
        sql """
        INSERT INTO ${tb8} VALUES (
            true, 42, 9999999999, 1.23, 4.56,
            CAST(123.456 AS DECIMAL(18,6)),
            'test', 'varchar_test', 'char_test',
            '2025-06-15', '2025-06-15 10:30:00',
            ['x', 'y'], {'key':'val'},
            named_struct('f1', 'hello', 'f2', 100)
        )
        """
        order_qt_all_types """ SELECT * FROM ${tb8} """

        // Test 9: Array of structs
        String tb9 = "arr_struct_${uuid}"
        sql """DROP TABLE IF EXISTS ${tb9}"""
        sql """
        CREATE TABLE ${tb9} (
            id INT,
            arr_struct ARRAY<STRUCT<name:STRING, val:INT>>
        )
        """
        sql """
        INSERT INTO ${tb9} VALUES
            (1, array(named_struct('name','a','val',1), named_struct('name','b','val',2))),
            (2, null)
        """
        order_qt_arr_struct """ SELECT * FROM ${tb9} """

        // Test 10: Map with array values
        String tb10 = "map_arr_${uuid}"
        sql """DROP TABLE IF EXISTS ${tb10}"""
        sql """
        CREATE TABLE ${tb10} (
            id INT,
            map_arr MAP<STRING, ARRAY<INT>>
        )
        """
        sql """
        INSERT INTO ${tb10} VALUES
            (1, map('k1',array(1,2,3), 'k2',array(4,5))),
            (2, null)
        """
        order_qt_map_arr """ SELECT * FROM ${tb10} """

        // Test 11: Struct with nested struct
        String tb11 = "nested_struct_${uuid}"
        sql """DROP TABLE IF EXISTS ${tb11}"""
        sql """
        CREATE TABLE ${tb11} (
            id INT,
            s STRUCT<outer_f:STRING, inner_f:STRUCT<a:INT, b:STRING>>
        )
        """
        sql """
        INSERT INTO ${tb11} VALUES
            (1, named_struct('outer_f','hello','inner_f',named_struct('a',10,'b','world'))),
            (2, null)
        """
        order_qt_nested_struct """ SELECT * FROM ${tb11} """

        // Test 12: Struct with array and map fields
        String tb12 = "struct_complex_${uuid}"
        sql """DROP TABLE IF EXISTS ${tb12}"""
        sql """
        CREATE TABLE ${tb12} (
            id INT,
            s STRUCT<tags:ARRAY<STRING>, props:MAP<STRING,STRING>>
        )
        """
        sql """
        INSERT INTO ${tb12} VALUES
            (1, named_struct('tags',array('t1','t2'),'props',map('p1','v1'))),
            (2, null)
        """
        order_qt_struct_complex """ SELECT * FROM ${tb12} """

        // Test 13: Map with struct values
        String tb13 = "map_struct_${uuid}"
        sql """DROP TABLE IF EXISTS ${tb13}"""
        sql """
        CREATE TABLE ${tb13} (
            id INT,
            m MAP<STRING, STRUCT<x:INT, y:INT>>
        )
        """
        sql """
        INSERT INTO ${tb13} VALUES
            (1, map('point1',named_struct('x',1,'y',2), 'point2',named_struct('x',3,'y',4))),
            (2, null)
        """
        order_qt_map_struct """ SELECT * FROM ${tb13} """

        // Test 14: Array of maps
        String tb14 = "arr_map_${uuid}"
        sql """DROP TABLE IF EXISTS ${tb14}"""
        sql """
        CREATE TABLE ${tb14} (
            id INT,
            arr_map ARRAY<MAP<STRING, INT>>
        )
        """
        sql """
        INSERT INTO ${tb14} VALUES
            (1, array(map('a',1,'b',2), map('c',3))),
            (2, null)
        """
        order_qt_arr_map """ SELECT * FROM ${tb14} """
    } finally {
        sql """drop database if exists ${mc_catalog_name}.${db}"""
    }
}
