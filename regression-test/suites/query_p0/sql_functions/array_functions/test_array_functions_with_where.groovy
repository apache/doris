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

suite("test_array_functions_with_where") {
    def tableName = "tbl_test_array_functions_with_where"
    // array functions only supported in vectorized engine
    sql """DROP TABLE IF EXISTS ${tableName}"""
    sql """ 
            CREATE TABLE IF NOT EXISTS ${tableName} (
              `k1` int(11) NULL COMMENT "",
              `k2` ARRAY<int(11)> NOT NULL COMMENT "",
              `k3` ARRAY<VARCHAR(20)> NULL COMMENT "",
              `k4` ARRAY<int(11)> NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
            )
        """
    sql """ INSERT INTO ${tableName} VALUES(1, [1, 2, 3], ["a", "b", ""], [1, 2]) """
    sql """ INSERT INTO ${tableName} VALUES(2, [4], NULL, [5]) """
    sql """ INSERT INTO ${tableName} VALUES(3, [], [], NULL) """
    sql """ INSERT INTO ${tableName} VALUES(NULL, [], NULL, NULL) """

    qt_select "SELECT k1, size(k2) FROM ${tableName} WHERE size(k2)=3 ORDER BY k1"
    qt_select "SELECT k1, size(k2) FROM ${tableName} WHERE array_contains(k2, 4) ORDER BY k1"
    qt_select "SELECT k1, size(k2) FROM ${tableName} WHERE element_at(k2, 1)=1 ORDER BY k1"
    qt_select "SELECT k1, size(k2) FROM ${tableName} WHERE arrays_overlap(k2, k4) ORDER BY k1"
    qt_select "SELECT k1, size(k2) FROM ${tableName} WHERE cardinality(k2)>0 ORDER BY k1, size(k2)"
    qt_select_array_with_constant "SELECT k1, array_with_constant(5, k1), array_repeat(k1, 5) FROM ${tableName} WHERE k1 is null ORDER BY k1, size(k2)"
    qt_select "SELECT k1, array(5, k1) FROM ${tableName} WHERE k1 is not null ORDER BY k1, size(k2)"
    qt_select "SELECT k1, array(k1, 'abc') FROM ${tableName} WHERE k1 is not null ORDER BY k1, size(k2)"
    qt_select "SELECT k1, array(null, k1) FROM ${tableName} WHERE k1 is not null ORDER BY k1, size(k2)"

    tableName = "tbl_test_array_functions_with_where2"
    sql """DROP TABLE IF EXISTS ${tableName}"""
    sql """ 
            CREATE TABLE ${tableName} (
              `id` int(11) NULL,
              `c_bool` array<boolean> NULL COMMENT ' type is boolean',
              `c_byte` array<tinyint(4)> NULL COMMENT ' type is byte',
              `c_date` array<datev2> NULL COMMENT ' type is date, format is yyyy-MM-dd',
              `c_datetime` array<datetimev2(0)> NULL COMMENT ' type is date, format is yyyy-MM-dd HH:mm:ss',
              `c_double` array<double> NULL COMMENT ' type is double',
              `c_float` array<float> NULL COMMENT ' type is float',
              `c_half_float` array<float> NULL COMMENT ' type is half_float',
              `c_integer` array<int(11)> NULL COMMENT ' type is integer',
              `c_ip` array<text> NULL COMMENT ' type is ip',
              `c_keyword` array<text> NULL COMMENT ' type is keyword',
              `c_long` array<bigint(20)> NULL COMMENT ' type is long',
              `c_person` array<text> NULL COMMENT ' no type',
              `c_scaled_float` array<double> NULL COMMENT ' type is scaled_float',
              `c_short` array<smallint(6)> NULL COMMENT ' type is short',
              `c_text` array<text> NULL COMMENT ' type is text',
              `c_unsigned_long` array<largeint(40)> NULL COMMENT ' type is unsigned_long',
              `test1` text NULL COMMENT ' type is keyword',
              `test2` text NULL COMMENT ' type is text',
              `test3` double NULL COMMENT ' type is double',
              `test4` datetimev2(0) NULL COMMENT ' type is date, no format',
              `test5` datetimev2(0) NULL COMMENT ' type is date, format is yyyy-MM-dd HH:mm:ss',
              `test6` bigint(20) NULL COMMENT ' type is date, format is epoch_millis',
              `test7` text NULL COMMENT ' type is date, format is yyyy-MM-dd HH:mm:ss  not support, use String type'
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "false"
            ); 
        """
        sql """
        INSERT INTO ${tableName} (id, c_bool, c_byte, c_date, c_datetime, c_double, c_float, c_half_float, c_integer, c_ip, c_keyword, c_long, c_person, c_scaled_float, c_short, c_text, c_unsigned_long, test1, test2, test3, test4, test5, test6, test7)
        VALUES 
        (1, '[1, 0, 1, 1]', '[1, -2, -3, 4]', '[2020-01-01, 2020-01-02]', '[2020-01-01 12:00:00, 2020-01-02 13:01:01]', '[1, 2, 3, 4]', '[1, 1.1, 1.2, 1.3]', '[1, 2, 3, 4]', '[32768, 32769, -32769, -32770]', "['192.168.0.1', '127.0.0.1']", "['a', 'b', 'c']", '[-1, 0, 1, 2]', '["{\\"name\\":\\"Andy\\",\\"age\\":18}", "{\\"name\\":\\"Tim\\",\\"age\\":28}"]', '[1, 2, 3, 4]', '[128, 129, -129, -130]', "['d', 'e', 'f']", '[0, 1, 2, 3]', 'string3', 'text3_4*5', 5, '2022-08-08 00:00:00', '2022-08-10 12:10:10', 1660018210000, '2022-08-10 12:10:10'),
        (1, '[1, 0, 1, 1]', '[1, -2, -3, 4]', '[2020-01-01, 2020-01-02]', '[2020-01-01 12:00:00, 2020-01-02 13:01:01]', '[1, 2, 3, 4]', '[1, 1.1, 1.2, 1.3]', '[1, 2, 3, 4]', '[32768, 32769, -32769, -32770]', "['192.168.0.1', '127.0.0.1']", "['a', 'b', 'c']", '[-1, 0, 1, 2]', '["{\\"name\\":\\"Andy\\",\\"age\\":18}", "{\\"name\\":\\"Tim\\",\\"age\\":28}"]', '[1, 2, 3, 4]', '[128, 129, -129, -130]', "['d', 'e', 'f']", '[0, 1, 2, 3]', 'string2', 'text2', 4, '2022-08-08 00:00:00', '2022-08-09 12:10:10', 1660018210000, '2022-08-09 12:10:10')
        """
        sql """
        INSERT INTO ${tableName} (id, c_bool, c_byte, c_date, c_datetime, c_double, c_float, c_half_float, c_integer, c_ip, c_keyword, c_long, c_person, c_scaled_float, c_short, c_text, c_unsigned_long, test1, test2, test3, test4, test5, test6, test7)
        VALUES 
        (1, '[1, 0, 1, 1]', '[1, -2, -3, 4]', '[2020-01-01, 2020-01-02]', '[2020-01-01 12:00:00, 2020-01-02 13:01:01]', '[1, 2, 3, 4]', '[1, 1.1, 1.2, 1.3]', '[1, 2, 3, 4]', '[32768, 32769, -32769, -32770]', "['192.168.0.1', '127.0.0.1']", "['a', 'b', 'c']", '[-1, 0, 1, 2]', '["{\\"name\\":\\"Andy\\",\\"age\\":18}", "{\\"name\\":\\"Tim\\",\\"age\\":28}"]', '[1, 2, 3, 4]', '[128, 129, -129, -130]', "['d', 'e', 'f']", '[0, 1, 2, 3]', 'string3', 'text3_4*5', 5, '2022-08-08 00:00:00', '2022-08-10 12:10:10', 1660018210000, '2022-08-10 12:10:10'),
        (1, '[1, 0, 1, 1]', '[1, -2, -3, 4]', '[2020-01-01, 2020-01-02]', '[2020-01-01 12:00:00, 2020-01-02 13:01:01]', '[1, 2, 3, 4]', '[1, 1.1, 1.2, 1.3]', '[1, 2, 3, 4]', '[32768, 32769, -32769, -32770]', "['192.168.0.1', '127.0.0.1']", "['a', 'b', 'c']", '[-1, 0, 1, 2]', '["{\\"name\\":\\"Andy\\",\\"age\\":18}", "{\\"name\\":\\"Tim\\",\\"age\\":28}"]', '[1, 2, 3, 4]', '[128, 129, -129, -130]', "['d', 'e', 'f']", '[0, 1, 2, 3]', 'string2', 'text2', 4, '2022-08-08 00:00:00', '2022-08-09 12:10:10', 1660018210000, '2022-08-09 12:10:10')
        """
        sql """
        INSERT INTO ${tableName} (id, c_bool, c_byte, c_date, c_datetime, c_double, c_float, c_half_float, c_integer, c_ip, c_keyword, c_long, c_person, c_scaled_float, c_short, c_text, c_unsigned_long, test1, test2, test3, test4, test5, test6, test7)
        VALUES 
        (1, '[1, 0, 1, 1]', '[1, -2, -3, 4]', '[2020-01-01, 2020-01-02]', '[2020-01-01 12:00:00, 2020-01-02 13:01:01]', '[1, 2, 3, 4]', '[1, 1.1, 1.2, 1.3]', '[1, 2, 3, 4]', '[32768, 32769, -32769, -32770]', "['192.168.0.1', '127.0.0.1']", "['a', 'b', 'c']", '[-1, 0, 1, 2]', '["{\\"name\\":\\"Andy\\",\\"age\\":18}", "{\\"name\\":\\"Tim\\",\\"age\\":28}"]', '[1, 2, 3, 4]', '[128, 129, -129, -130]', "['d', 'e', 'f']", '[0, 1, 2, 3]', 'string3', 'text3_4*5', 5, '2022-08-08 00:00:00', '2022-08-10 12:10:10', 1660018210000, '2022-08-10 12:10:10'),
        (1, '[1, 0, 1, 1]', '[1, -2, -3, 4]', '[2020-01-01, 2020-01-02]', '[2020-01-01 12:00:00, 2020-01-02 13:01:01]', '[1, 2, 3, 4]', '[1, 1.1, 1.2, 1.3]', '[1, 2, 3, 4]', '[32768, 32769, -32769, -32770]', "['192.168.0.1', '127.0.0.1']", "['a', 'b', 'c']", '[-1, 0, 1, 2]', '["{\\"name\\":\\"Andy\\",\\"age\\":18}", "{\\"name\\":\\"Tim\\",\\"age\\":28}"]', '[1, 2, 3, 4]', '[128, 129, -129, -130]', "['d', 'e', 'f']", '[0, 1, 2, 3]', 'string2', 'text2', 4, '2022-08-08 00:00:00', '2022-08-09 12:10:10', 1660018210000, '2022-08-09 12:10:10')
        """
        qt_select "select * from ${tableName} where substring(test2, 2) = 'ext2';" 
        qt_select "select c_unsigned_long from ${tableName} where not substring(test2, 2) = 'ext2';" 
}
