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

suite("test_string_len_complex") {
    // test array one level
    def create_table_array_one_level = {
        sql """ DROP TABLE IF EXISTS test_string_len_array_one_level """
        sql """
            CREATE TABLE `test_string_len_array_one_level` (
              `id` int(11),
              `info` array<char(11)>
            ) PROPERTIES ("replication_num" = "1");
        """
    }
    create_table_array_one_level()
    sql """
    set enable_insert_strict = false;
    """
    sql """
    insert into test_string_len_array_one_level values (1, ['12345678901', '12345678902', '12345678903x']),
    (2, ['22345678901', '22345678902', '22345678903x']),
    (3, ['32345678901', '32345678902', '32345678903x']);
    """
    qt_sql_array_all1 """ select * from test_string_len_array_one_level order by id """

    create_table_array_one_level()
    sql """
    set enable_insert_strict = true;
    """
    test {
        sql """
        insert into test_string_len_array_one_level values (1, ['12345678901', '12345678902', '12345678903x']),
        (2, ['22345678901', '22345678902', '22345678903x']),
        (3, ['32345678901', '32345678902', '32345678903x']);
        """
        exception "Insert has filtered data in strict mode"
    }
    qt_sql_array_all2 """ select * from test_string_len_array_one_level order by id """

    // test array one level
    def create_table_array_two_level = {
        sql """ DROP TABLE IF EXISTS test_string_len_array_two_level """
        sql """
            CREATE TABLE `test_string_len_array_two_level` (
              `id` int(11),
              `info` array<array<char(11)>>
            ) PROPERTIES ("replication_num" = "1");
        """
    }
    create_table_array_two_level()
    sql """
    set enable_insert_strict = false;
    """
    sql """
    insert into test_string_len_array_two_level values (1, [['12345678901', '12345678902'], ['12345678903x']]),
    (2, [['22345678901', '22345678902'], ['22345678903x']]),
    (3, [['32345678901', '32345678902'], ['32345678903x']]);
    """
    qt_sql_array_all3 """ select * from test_string_len_array_two_level order by id """

    create_table_array_two_level()
    sql """
    set enable_insert_strict = true;
    """
    test {
        sql """
        insert into test_string_len_array_two_level values (1, [['12345678901', '12345678902'], ['12345678903x']]),
        (2, [['22345678901', '22345678902'], ['22345678903x']]),
        (3, [['32345678901', '32345678902'], ['32345678903x']]);
        """
        exception "Insert has filtered data in strict mode"
    }
    qt_sql_array_all4 """ select * from test_string_len_array_two_level order by id """

    // test struct one level
    def create_table_struct_one_level = {
        sql """ DROP TABLE IF EXISTS test_string_len_struct_one_level """
        sql """
            CREATE TABLE `test_string_len_struct_one_level` (
              `id` int(11),
              `info` struct<phone:char(11)>
            ) PROPERTIES ("replication_num" = "1");
        """
    }

    sql """
    set enable_insert_strict = false;
    """
    create_table_struct_one_level()
    sql """
    insert into test_string_len_struct_one_level values (1, {'12345678901'}), (2, {'12345678902'}), (3, {'12345678903x'});
    """
    qt_sql_struct_all1 """ select * from test_string_len_struct_one_level order by id; """

    sql """
    set enable_insert_strict = true;
    """
    create_table_struct_one_level()
    test {
        sql """
        insert into test_string_len_struct_one_level values (1, {'12345678901'}), (2, {'12345678902'}), (3, {'12345678903x'});
        """
        exception "Insert has filtered data in strict mode"
    }
    qt_sql_struct_all2 """ select * from test_string_len_struct_one_level order by id; """

    // test struct two level
    def create_table_struct_two_level = {
        sql """ DROP TABLE IF EXISTS test_string_len_struct_two_level """
        sql """
            CREATE TABLE `test_string_len_struct_two_level` (
              `id` int(11),
              `info` struct<phone:char(11), addr:struct<province:char(11), city:char(11)>>
            ) PROPERTIES ("replication_num" = "1");
        """
    }
    create_table_struct_two_level()
    sql """
    set enable_insert_strict = false;
    """
    sql """
    insert into test_string_len_struct_two_level values (1, {'12345678901', {'12345678902', '12345678903'}}),
    (2, {'22345678901', {'22345678902', '22345678903'}}),
    (3, {'32345678901', {'32345678902x', '32345678903x'}});
    """
    qt_sql_struct_all3 """ select * from test_string_len_struct_two_level order by id; """

    create_table_struct_two_level()
    sql """
    set enable_insert_strict = true;
    """
    test {
        sql """
        insert into test_string_len_struct_two_level values (1, {'12345678901', {'12345678902', '12345678903'}}),
        (2, {'22345678901', {'22345678902', '22345678903'}}),
        (3, {'32345678901', {'32345678902x', '32345678903x'}});
        """
        exception "Insert has filtered data in strict mode"
    }
    qt_sql_struct_all4 """  
        select * from test_string_len_struct_two_level order by id;
    """

    // test map one level
    def create_table_map_one_level = {
        sql """ DROP TABLE IF EXISTS test_string_len_map_one_level """
        sql """
            CREATE TABLE `test_string_len_map_one_level` (
              `id` int(11),
              `info` map<char(11), char(11)>
            ) PROPERTIES ("replication_num" = "1");
        """
    }
    create_table_map_one_level()
    sql """
    set enable_insert_strict = false;
    """
    sql """
    insert into test_string_len_map_one_level values (1, {'01234567891':'12345678901', '01234567892':'12345678902'}), 
    (2, {'01234567891':'22345678901', '01234567892':'22345678902'}), 
    (3, {'01234567891':'32345678901', '01234567892':'32345678902x'});
    """
    qt_sql_map_all1 """ select * from test_string_len_map_one_level order by id; """

    create_table_map_one_level()
    sql """
    set enable_insert_strict = true;
    """
    test {
        sql """
        insert into test_string_len_map_one_level values (1, {'01234567891':'12345678901', '01234567892':'12345678902'}), 
        (2, {'01234567891':'22345678901', '01234567892':'22345678902'}), 
        (3, {'01234567891':'32345678901', '01234567892':'32345678902x'});
        """
        exception "Insert has filtered data in strict mode"
    }
    qt_sql_map_all2 """ select * from test_string_len_map_one_level order by id; """

    // test map two level
    def create_table_map_two_level = {
        sql """ DROP TABLE IF EXISTS test_string_len_map_two_level """
        sql """
            CREATE TABLE `test_string_len_map_two_level` (
              `id` int(11),
              `info` map<map<char(11), char(11)>, map<char(11), char(11)>>
            ) PROPERTIES ("replication_num" = "1");
        """
    }

    create_table_map_two_level()
    sql """
    set enable_insert_strict = false;
    """
    sql """
    insert into test_string_len_map_two_level values (1, { {'01234567891':'12345678901'}:{'11234567891':'12345678901'}, {'01234567892':'12345678902'}:{'11234567892':'12345678902'} } ), 
    (2, { {'01234567891':'22345678901'}:{'11234567891':'22345678901'}, {'01234567892':'22345678902'}:{'11234567892':'22345678902'} } ), 
    (3, { {'01234567891':'32345678901'}:{'11234567891':'32345678901'}, {'01234567892':'32345678902x'}:{'11234567892':'32345678902x'} } );
    """
    qt_sql_map_all3 """ select * from test_string_len_map_two_level order by id; """

    create_table_map_two_level()
    sql """
    set enable_insert_strict = true;
    """
    test {
        sql """
        insert into test_string_len_map_two_level values (1, { {'01234567891':'12345678901'}:{'11234567891':'12345678901'}, {'01234567892':'12345678902'}:{'11234567892':'12345678902'} } ), 
        (2, { {'01234567891':'22345678901'}:{'11234567891':'22345678901'}, {'01234567892':'22345678902'}:{'11234567892':'22345678902'} } ), 
        (3, { {'01234567891':'32345678901'}:{'11234567891':'32345678901'}, {'01234567892':'32345678902x'}:{'11234567892':'32345678902x'} } );
        """
        exception "Insert has filtered data in strict mode"
    }
    qt_sql_map_all4 """ select * from test_string_len_map_two_level order by id; """

}