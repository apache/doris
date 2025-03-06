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

suite("alter_complextype_to_variant") {

    def timeout = 60000
    def delta_time = 1000
    def useTime = 0
    def wait_for_latest_op_on_table_finish = { tableName, OpTimeout, errorMsg ->
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            def alter_res = sql """SHOW ALTER TABLE COLUMN WHERE TableName = "${tableName}" ORDER BY CreateTime DESC LIMIT 1;"""
            alter_res = alter_res.toString()
            if(alter_res.contains("FINISHED")) {
                sleep(3000) // wait change table state to normal
                logger.info(tableName + " latest alter job finished, detail: " + alter_res)
                break
            } else if (alter_res.contains("FAILED")) {
                assertTrue(alter_res.contains(errorMsg))
            }
            useTime = t
            sleep(delta_time)
        }
        assertTrue(useTime <= OpTimeout, "wait_for_latest_op_on_table_finish timeout")
    }

    // 1. create table
    sql "DROP TABLE IF EXISTS test_valid_variant_table FORCE;"
    sql """
        CREATE TABLE test_valid_variant_table (
            id INT,
            -- 数组类型，支持嵌套各种标量类型
            arr_int ARRAY<INT> COMMENT 'array of int',
            arr_string ARRAY<STRING> COMMENT 'array of string',
            arr_decimal ARRAY<DECIMAL(10,2)> COMMENT 'array of decimal',
            arr_datetime ARRAY<DATETIME> COMMENT 'array of datetime',
            arr_ipv4 ARRAY<IPV4> COMMENT 'array of ip',
            arr_ipv6 ARRAY<IPV6> COMMENT 'array of ip',
            -- 嵌套数组
            arr_nested ARRAY<ARRAY<INT>> COMMENT 'nested array of int',
            -- 数组嵌套 Map 和 Struct
            arr_map ARRAY<MAP<STRING, INT>> COMMENT 'array of map with string key and int value',
            arr_struct ARRAY<STRUCT<name:STRING, value:INT>> COMMENT 'array of struct with name and value',
            -- 结构体类型，包含各类标量
            struct_all STRUCT<
                field_int:INT,
                field_string:STRING,
                field_decimal:DECIMAL(10,2),
                field_datetime:DATETIME,
                field_ip:IPV6> COMMENT 'struct with all scalar types',
            -- 结构体内包含嵌套的数组与 map
            struct_nested STRUCT<
                nested_arr:ARRAY<STRING>,
                nested_map:MAP<STRING,INT>
            > COMMENT 'struct with nested array and map',
            -- Map 类型，键为字符串，值为不同标量类型
            map_int MAP<STRING, INT> COMMENT 'map of int',
            map_string MAP<STRING, STRING> COMMENT 'map of string',
            map_decimal MAP<STRING, DECIMAL(10,2)> COMMENT 'map of decimal',
            map_datetime MAP<STRING, DATETIME> COMMENT 'map of datetime',
            map_ip MAP<STRING, IPV4> COMMENT 'map of ip',
            -- JSON 类型
            j JSON COMMENT 'json type'
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            'replication_num' = '1'
        );
    """

    // 2. insert data
    sql """
            INSERT INTO test_valid_variant_table VALUES
            -- 行 1：所有字段均提供正常数据
            (1,
             [1,2,3],
             ['a','b','c'],
             [10.50, 20.75],
             ['2023-01-01 10:00:00', '2023-01-02 11:00:00'],
             ['192.168.1.1', '10.0.0.1'],
             ['::', '2001:db8::'],
             [[1,2],[3,4]],
             [{'key1': 1}, {'key2': 2}],
             [{'item1', 100}, {'item2', 200}],
             {100, 'Alice', 99.99, '2023-02-01 08:00:00', '::'},
             {['x','y'], {'key1': 1, 'key2': 2}},
             {'a': 1, 'b': 2},
             {'k1': 'v1', 'k2': 'v2'},
             {'d1': 1.11, 'd2': 2.22},
             {'dt1': '2023-03-01 12:00:00', 'dt2': '2023-03-02 12:00:00'},
             {'ip1': '192.168.2.1', 'ip2': '10.10.10.10'},
             '{"key": "value"}'
            ),
            -- 行 2：所有复杂列均为 NULL
            (2,
             null, null, null, null, null, null, null, null, null,
             null, null, null, null, null, null, null,
             null
            ),
            -- 行 3：部分嵌套元素为 NULL
            (3,
             [null, 5],
             [null, 'b'],
             [null, 30.00],
             [null, '2023-01-01 00:00:00'],
             [null, '127.0.0.1'],
             [null, '2001:db8::'],
             [[null, 2],[3, null]],
             [null, {'amory': 7}],
             [null, {null, null}],
             {null, 'Bob', null, '2023-04-01 09:00:00', null},
             {[null, 'z'], {'key1': null, 'key2': 2}},
             {'a': null},
             {'k1': null},
             {'d1': null},
             {'dt1': null},
             {'ip1': null},
             '{"a": null}'
            ),
            -- 行 4：使用空数组、空 Map 以及默认值填充结构体
            (4,
             [], [], [], [], [], [],[], [], [],
             {0, '', 0, '1970-01-01 00:00:00', '0.0.0.0'},
             {[], {}},
             {}, {}, {}, {}, {},
             '{}'
            );
        """

    // 2.1 check insert data is done
    qt_sql_ori """ select * from test_valid_variant_table order by id; """

    // 3. 执行 schema change：将所有复杂类型列转换为 variant 类型
    def columns = [
        "arr_int", "arr_string", "arr_decimal", "arr_datetime", "arr_ipv4", "arr_ipv6",
        "arr_nested", "arr_map", "arr_struct", "struct_all", "struct_nested", "map_int", "map_string",
        "map_decimal", "map_datetime", "map_ip", "j"
    ]
    for (col in columns) {
      sql "ALTER TABLE test_valid_variant_table MODIFY COLUMN ${col} VARIANT"
      // 3.1  check schema change is done which means cast to variant is finished
        wait_for_latest_op_on_table_finish("test_valid_variant_table", timeout, null)
    }

    // 4. 校验元数据，确保各列类型中包含 "variant"，并且注释未丢失
    def schemaDesc = sql("DESC test_valid_variant_table")
    columns.each { col ->
       assert schemaDesc.find {
           println(it)
           // it[0] is column name, it[1] is column type
           it[0].toLowerCase().equals(col) && it[1].toLowerCase().equals("variant")
       } != null
    }

    // 5. 校验数据转换后的正确性
    qt_sql_1 """
        SELECT
            *
        FROM test_valid_variant_table
        ORDER BY id
    """

    // struct to variant use '[]'
    qt_sql_struct """
        SELECT
            id,
            struct_all['field_ip'], struct_nested['nested_map']
        FROM test_valid_variant_table
        ORDER BY id
    """

    // json to variant use '[]'
    qt_sql_json """
        SELECT
            id,
            j['key']
        FROM test_valid_variant_table
        ORDER BY id
    """
    
    // then we should insert some data into variant column
    sql """
            INSERT INTO test_valid_variant_table VALUES
            -- 行 1：所有字段均提供正常数据
            (5,
             [1,2,3],
             ['a','b','c'],
             [10.50, 20.75],
             ['2023-01-01 10:00:00', '2023-01-02 11:00:00'],
             ['192.168.1.1', '10.0.0.1'],
             ['::', '2001:db8::'],
             [[1,2],[3,4]],
             [{'key1': 1}, {'key2': 2}],
             [{'item1', 100}, {'item2', 200}],
             {100, 'Alice', 99.99, '2023-02-01 08:00:00', '::'},
             {['x','y'], {'key1': 1, 'key2': 2}},
             {'a': 1, 'b': 2},
             {'k1': 'v1', 'k2': 'v2'},
             {'d1': 1.11, 'd2': 2.22},
             {'dt1': '2023-03-01 12:00:00', 'dt2': '2023-03-02 12:00:00'},
             {'ip1': '192.168.2.1', 'ip2': '10.10.10.10'},
             '{"key": "value"}'
            ),
            -- 行 2：所有复杂列均为 NULL
            (6,
             null, null, null, null, null, null, null, null, null,
             null, null, null, null, null, null, null,
             null
            ),
            -- 行 3：部分嵌套元素为 NULL
            (7,
             [null, 5],
             [null, 'b'],
             [null, 30.00],
             [null, '2023-01-01 00:00:00'],
             [null, '127.0.0.1'],
             [null, '2001:db8::'],
             [[null, 2],[3, null]],
             [null, {'amory': 7}],
             [null, {null, null}],
             {null, 'Bob', null, '2023-04-01 09:00:00', null},
             {[null, 'z'], {'key1': null, 'key2': 2}},
             {'a': null},
             {'k1': null},
             {'d1': null},
             {'dt1': null},
             {'ip1': null},
             '{"a": null}'b
            ),
            -- 行 4：使用空数组、空 Map 以及默认值填充结构体
            (8,
             [], [], [], [], [], [],[], [], [],
             {0, '', 0, '1970-01-01 00:00:00', '0.0.0.0'},
             {[], {}},
             {}, {}, {}, {}, {},
             '{}'
            );
        """

    qt_sql_after """ select * from test_valid_variant_table order by id; """
    // then insert variant data using str
    sql """
            INSERT INTO test_valid_variant_table VALUES
            -- 行 1：所有字段均提供正常数据
            (9,
             '[1,2,3]',
             '["a","b","c"]',
             '[10.50, 20.75]',
             '["2023-01-01 10:00:00", "2023-01-02 11:00:00"]',
             '["192.168.1.1", "10.0.0.1"]',
             '["::", "2001:db8::"]',
                '[[1,2],[3,4]]',
                '[{"key1":1}, {"key2":2}]',
                '[{"item1", 100}, {"item2", 200}]',
                '{"field_int":100, "field_string":"Alice", "field_decimal":99.99, "field_datetime":"2023-02-01 08:00:00", "field_ip":"::"}',
                '{"nested_arr":["x","y"], "nested_map":{"key1":1, "key2":2}}',
                '{"a":1, "b":2}',
                '{"k1":"v1", "k2":"v2"}',
                '{"d1":1.11, "d2":2.22}',
                '{"dt1":"2023-03-01 12:00:00", "dt2":"2023-03-02 12:00:00"}',
                '{"ip1":"192.168.2.1", "ip2":"10.10.10.1"}',
                '{"key":"value"}'
            ),
            -- 行 2：所有复杂列均为 NULL
            (10,
             null, null, null, null, null, null, null, null, null,
             null, null, null, null, null, null, null,
             null
            ),
            -- 行 3：部分嵌套元素为 NULL
            (11,
             '[null, 5]',
             '[null, "b"]',
             '[null, 30.00]',
             '[null, "2023-01-01 00:00:00"]',
             '[null, "127.0.0.1"]',
                '[null, "2001:db8::"]',
                '[[null, 2],[3, null]]',
                '[null, {"amory": 7}]',
                '[null, {null, null}]',
                '{"field_int":null, "field_string":"Bob", "field_decimal":null, "field_datetime":"2023-04-01 09:00:00", "field_ip":null}',
                '{"nested_arr":[null, "z"], "nested_map":{"key1":null, "key2":2}}',
                '{"a":null}',
                '{"k1":null}',
                '{"d1":null}',
                '{"dt1":null}',
                '{"ip1":null}',
                '{"a":null}'
            ),
            -- 行 4：使用空数组、空 Map 以及默认值填充结构体
            (12,
             '[]', '[]', '[]', '[]', '[]', '[]', '[]', '[]', '[]',
             '{"field_int":0, "field_string":"", "field_decimal":0, "field_datetime":"1970-01-01 00:00:00", "field_ip":"0.0.0.0"}',
                '{"nested_arr":[], "nested_map":{}}',
                '{}', '{}', '{}', '{}', '{}',
                '{}'
            );
        """

    // 5.1 check insert data is done
    qt_sql_after2 """ select * from test_valid_variant_table order by id; """

    // test valid complex type to variant
    // 1. create table
    sql "DROP TABLE IF EXISTS test_illegal_variant_table FORCE;"
    sql """
        CREATE TABLE test_illegal_variant_table (
            id INT,
            -- 非法：map key 为 INT
            map_int MAP<INT, INT> COMMENT 'map of int, key is INT, illegal for variant conversion',
            -- 非法：map key为 DECIMAL(10,2)
            map_string MAP<DECIMAL(10,2), STRING> COMMENT 'map of string, key is DECIMAL, illegal for variant conversion',
            -- 非法：map key为 IPV4（不是字符串类型）
            map_decimal MAP<IPV4, DECIMAL(10,2)> COMMENT 'map of decimal, key is IPV4, illegal for variant conversion',
            -- 非法：map key为 DATE
            map_datetime MAP<DATE, DATETIME> COMMENT 'map of datetime, key is DATE, illegal for variant conversion',
            -- 非法：结构体中嵌套的 map 的 key 为 INT
            struct_nested STRUCT<nested_map:MAP<INT, INT>> COMMENT 'struct with nested illegal map',
            -- 非法：数组中元素为 map，map key 为 INT
            arr_map_int ARRAY<MAP<INT, INT>> COMMENT 'array of map with int key, illegal for variant conversion',
            -- 非法：数组中元素为 map，map key 为 IPV6
            arr_map_ipv6 ARRAY<MAP<IPV6, INT>> COMMENT 'array of map with IPV6 key, illegal for variant conversion'
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            'replication_num' = '1'
        );
    """

    // 2. insert some data
    sql """
        INSERT INTO test_illegal_variant_table VALUES
        (1,
         {1:10, 2:20},
         {1.23: 'a', 4.56: 'b'},
         {'192.168.1.1': 100.50},
         {'2023-01-01': '2023-01-01 12:00:00'},
         {{3:30}},
         [ {4:40}, {5:50} ],
         [ {'::1': 60} ]
        )
    """

    // 3. 定义需要转换为 VARIANT 的非法列列表
    def illegalColumns = [
        "map_int",
        "map_string",
        "map_decimal",
        "map_datetime",
        "struct_nested",
        "arr_map_int",
        "arr_map_ipv6"
    ]

    // 4. 针对每个非法列，尝试执行 ALTER TABLE 修改为 VARIANT，并校验操作失败
    illegalColumns.each { col ->
        try {
            sql "ALTER TABLE test_illegal_variant_table MODIFY COLUMN ${col} VARIANT"
        } catch(Exception e) {
            String lowerMsg = e.getMessage().toLowerCase()
            assert lowerMsg.contains("cannot change")
        }
    }
}
