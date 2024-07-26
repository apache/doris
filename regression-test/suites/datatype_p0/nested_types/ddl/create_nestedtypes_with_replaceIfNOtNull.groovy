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

suite("create_nestedtypes_with_repalceIfNotNull", "p0") {
    def colNameArr = ["c_bool", "c_tinyint", "c_smallint", "c_int", "c_bigint", "c_largeint", "c_float",
                      "c_double", "c_decimal", "c_decimalv3", "c_date", "c_datetime", "c_datev2", "c_datetimev2",
                      "c_char", "c_varchar", "c_string"]
    def colTypeArr = ["BOOLEAN", "TINYINT", "SMALLINT", "INT", "BIGINT", "LARGEINT", "FLOAT", "DOUBLE",
                      "DECIMAL(10, 2)", "DECIMAL(10, 2)", "DATE", "DATETIME", "DATE", "DATETIME",
                      "CHAR(10)", "VARCHAR(1)", "STRING"]

    def create_nested_table = {testTablex, nested_type ->
        def stmt = "CREATE TABLE IF NOT EXISTS " + testTablex + "(\n" +
                "`k1` bigint(11) NULL,\n"

         for (int i = 0; i < colTypeArr.size(); i++) {
             def nestedType = ""
             if (nested_type == 0) {
                 nestedType = "ARRAY<" + colTypeArr[i] + ">"
             } else if (nested_type == 1) {
                 nestedType = "MAP<" + colTypeArr[i] + ", " + colTypeArr[i] + ">"
             } else if (nested_type == 2) {
                 nestedType = "STRUCT<f1: " + colTypeArr[i] + ">"
             }

             String strTmp = "`" + colNameArr[i] + "` " + nestedType + " REPLACE_IF_NOT_NULL,\n";
             stmt += strTmp
         }
        stmt = stmt.substring(0, stmt.length()-2)
        stmt += ") ENGINE=OLAP\n" +
                "AGGREGATE KEY(`k1`)\n" +
                "COMMENT 'OLAP'\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 10\n" +
                "PROPERTIES(\"replication_num\" = \"1\");"
        sql "DROP TABLE IF EXISTS $testTablex"
        sql stmt
    }

    // array
    create_nested_table.call("test_array_agg_replace_if_not_null", 0)
    // insert data
    sql """insert into test_array_agg_replace_if_not_null values(
            1,
            [true, false],
            [1, 2],
            [100, 200],
            [1000, 2000],
            [10000, 20000],
            [100000, 200000],
            [1.1, 2.2],
            [1.11, 2.22],
            [10.01, 20.02],
            [30.03, 40.04],
            ['2024-01-01', '2024-01-02'],
            ['2024-01-01 12:00:00', '2024-01-02 12:00:00'],
            ['2024-01-03', '2024-01-04'],
            ['2024-01-03 12:00:00', '2024-01-04 12:00:00'],
            ['char1', 'char2'],
            ['a', 'b'],
            ['string1', 'string2']) """
    sql """ insert into test_array_agg_replace_if_not_null(k1) values(5)"""
    sql """ insert into test_array_agg_replace_if_not_null values(
                7, 
                [true, true, false], 
                [12, 13, 14], 
                [1200, 1300, 1400], 
                [12000, 13000, 14000], 
                [120000, 130000, 140000], 
                [1200000, 1300000, 1400000], 
                [12.12, 13.13, 14.14], 
                [12.12, 13.13, 14.14], 
                [120.12, 130.13, 140.14], 
                [140.14, 150.15, 160.16], 
                ['2024-07-01', '2024-07-02', '2024-07-03'], 
                ['2024-07-01 12:00:00', '2024-07-02 12:00:00', '2024-07-03 12:00:00'], 
                ['2024-07-03', '2024-07-04', '2024-07-05'], 
                ['2024-07-03 12:00:00', '2024-07-04 12:00:00', '2024-07-05 12:00:00'], 
                ['char12', 'char13', 'char14'], 
                ['l', 'm', 'n'], 
                ['string12', 'string13', 'string14']
            ); """
    qt_sql_all "select *  from test_array_agg_replace_if_not_null order by k1;"
    sql """ insert into test_array_agg_replace_if_not_null values(
                1, 
                [true, true, false], 
                [12, 13, 14], 
                [1200, 1300, 1400], 
                [12000, 13000, 14000], 
                [120000, 130000, 140000], 
                [1200000, 1300000, 1400000], 
                [12.12, 13.13, 14.14], 
                [12.12, 13.13, 14.14], 
                [120.12, 130.13, 140.14], 
                [140.14, 150.15, 160.16], 
                ['2024-07-01', '2024-07-02', '2024-07-03'], 
                ['2024-07-01 12:00:00', '2024-07-02 12:00:00', '2024-07-03 12:00:00'], 
                ['2024-07-03', '2024-07-04', '2024-07-05'], 
                ['2024-07-03 12:00:00', '2024-07-04 12:00:00', '2024-07-05 12:00:00'], 
                ['char12', 'char13', 'char14'], 
                ['l', 'm', 'n'], 
                ['string12', 'string13', 'string14'])"""
    sql """ insert into test_array_agg_replace_if_not_null(k1) values(7)"""
    qt_sql_replace_1 "select * from test_array_agg_replace_if_not_null order by k1;"
    sql """ insert into test_array_agg_replace_if_not_null values(
                    5, 
                    [true], 
                    [9], 
                    [900], 
                    [9000], 
                    [90000], 
                    [900000], 
                    [9.9], 
                    [9.99], 
                    [90.09], 
                    [110.11], 
                    ['2024-05-01'], 
                    ['2024-05-01 12:00:00'], 
                    ['2024-05-02'], 
                    ['2024-05-02 12:00:00'], 
                    ['char9'], 
                    ['i'], 
                    ['string9']
                );
                """
    qt_sql_replace_2 "select * from test_array_agg_replace_if_not_null order by k1;"
    // map
    create_nested_table.call("test_map_agg_replace_if_not_null", 1)
    // insert data
    sql """INSERT INTO test_map_agg_replace_if_not_null VALUES
                    (
                        1, 
                        {true: false, false: true}, 
                        {1: 2, 2: 3}, 
                        {100: 200, 200: 300}, 
                        {1000: 2000, 2000: 3000}, 
                        {10000: 20000, 20000: 30000}, 
                        {100000: 200000, 200000: 300000}, 
                        {1.1: 2.2, 2.2: 3.3}, 
                        {1.11: 2.22, 2.22: 3.33}, 
                        {10.01: 20.02, 20.02: 30.03}, 
                        {30.03: 40.04, 40.04: 50.05}, 
                        {'2024-01-01': '2024-01-02', '2024-01-02': '2024-01-03'}, 
                        {'2024-01-01 12:00:00': '2024-01-02 12:00:00', '2024-01-02 12:00:00': '2024-01-03 12:00:00'}, 
                        {'2024-01-03': '2024-01-04', '2024-01-04': '2024-01-05'}, 
                        {'2024-01-03 12:00:00': '2024-01-04 12:00:00', '2024-01-04 12:00:00': '2024-01-05 12:00:00'}, 
                        {'char1': 'char2', 'char2': 'char3'}, 
                        {'a': 'b', 'b': 'c'}, 
                        {'string1': 'string2', 'string2': 'string3'}
                    ); """
    sql """ insert into test_map_agg_replace_if_not_null(k1) values(5)"""
    sql """ insert into test_map_agg_replace_if_not_null values(
                        7, 
                        {false: true}, 
                        {7: 70}, 
                        {700: 7000}, 
                        {7000: 70000}, 
                        {70000: 700000}, 
                        {700000: 7000000}, 
                        {7.7: 7.77}, 
                        {7.77: 7.777}, 
                        {70.07: 700.70}, 
                        {90.09: 900.90}, 
                        {'2024-04-01': '2024-04-02'}, 
                        {'2024-04-01 12:00:00': '2024-04-02 12:00:00'}, 
                        {'2024-04-03': '2024-04-04'}, 
                        {'2024-04-03 12:00:00': '2024-04-04 12:00:00'}, 
                        {'char7': 'charG'}, 
                        {'g': 'G'}, 
                        {'string7': 'STRING7'}
                        ); """
    qt_sql_map_all "select *  from test_map_agg_replace_if_not_null order by k1;"
    sql """ insert into test_map_agg_replace_if_not_null values(
                        1, 
                        {false: true}, 
                        {7: 70}, 
                        {700: 7000}, 
                        {7000: 70000}, 
                        {70000: 700000}, 
                        {700000: 7000000}, 
                        {7.7: 7.77}, 
                        {7.77: 7.777}, 
                        {70.07: 700.70}, 
                        {90.09: 900.90}, 
                        {'2024-04-01': '2024-04-02'}, 
                        {'2024-04-01 12:00:00': '2024-04-02 12:00:00'}, 
                        {'2024-04-03': '2024-04-04'}, 
                        {'2024-04-03 12:00:00': '2024-04-04 12:00:00'}, 
                        {'char7': 'charG'}, 
                        {'g': 'G'}, 
                        {'string7': 'STRING7'}
                    ); """
    sql """ insert into test_map_agg_replace_if_not_null(k1) values(7)"""
    qt_sql_replace_map_1 "select * from test_map_agg_replace_if_not_null order by k1;"
    sql """ insert into test_map_agg_replace_if_not_null values(
                        5, 
                        {true: false}, 
                        {9: 90}, 
                        {900: 9000}, 
                        {9000: 90000}, 
                        {90000: 900000}, 
                        {900000: 9000000}, 
                        {9.9: 9.99}, 
                        {9.99: 9.999}, 
                        {90.09: 900.90}, 
                        {110.11: 1100.11}, 
                        {'2024-05-01': '2024-05-02'}, 
                        {'2024-05-01 12:00:00': '2024-05-02 12:00:00'}, 
                        {'2024-05-02': '2024-05-03'}, 
                        {'2024-05-02 12:00:00': '2024-05-03 12:00:00'}, 
                        {'char9': 'charG'}, 
                        {'g': 'G'}, 
                        {'string9': 'STRING9'}
                    );
                    """
    qt_sql_replace_map_2 "select * from test_map_agg_replace_if_not_null order by k1;"

    // struct
    create_nested_table.call("test_struct_agg_replace_if_not_null", 2)
    // insert data
    sql """INSERT INTO test_struct_agg_replace_if_not_null (
                        `k1`, `c_bool`, `c_tinyint`, `c_smallint`, `c_int`, `c_bigint`, `c_largeint`, 
                        `c_float`, `c_double`, `c_decimal`, `c_decimalv3`, `c_date`, `c_datetime`, 
                        `c_datev2`, `c_datetimev2`, `c_char`, `c_varchar`, `c_string`
                    ) VALUES
                    (
                        1, 
                       {true}, 
                        {1}, 
                        {100}, 
                        {1000}, 
                        {10000}, 
                        {100000}, 
                        {1.1}, 
                        {1.11}, 
                        {10.01}, 
                        {20.02}, 
                        {'2024-01-01'}, 
                        {'2024-01-01 12:00:00'}, 
                        {'2024-01-02'}, 
                        {'2024-01-02 12:00:00'}, 
                        {'char1'}, 
                        {'a'}, 
                        {'string1'}
                    ); """
    sql """ insert into test_struct_agg_replace_if_not_null(k1) values(5)"""
    sql """ insert into test_struct_agg_replace_if_not_null values(
                        7, 
                         {true}, 
                            {7}, 
                            {700}, 
                            {7000}, 
                            {70000}, 
                            {700000}, 
                            {7.7}, 
                            {7.77}, 
                            {130.13}, 
                            {140.14}, 
                            {'2024-07-01'}, 
                            {'2024-07-01 12:00:00'}, 
                            {'2024-07-02'}, 
                            {'2024-07-02 12:00:00'}, 
                            {'char7'}, 
                            {'g'}, 
                            {'string7'}
                        ); """
    qt_sql_struct_all "select *  from test_struct_agg_replace_if_not_null order by k1;"
    sql """ insert into test_struct_agg_replace_if_not_null values(
                        1, 
                              {true}, 
                        {7}, 
                        {700}, 
                        {7000}, 
                        {70000}, 
                        {700000}, 
                        {7.7}, 
                        {7.77}, 
                        {130.13}, 
                        {140.14}, 
                        {'2024-07-01'}, 
                        {'2024-07-01 12:00:00'}, 
                        {'2024-07-02'}, 
                        {'2024-07-02 12:00:00'}, 
                        {'char7'}, 
                        {'g'}, 
                        {'string7'}
                    ); """
    sql """ insert into test_struct_agg_replace_if_not_null(k1) values(7)"""
    qt_sql_replace_struct_1 "select * from test_struct_agg_replace_if_not_null order by k1;"
    sql """ insert into test_struct_agg_replace_if_not_null values(
                        5, 
                           {true}, 
                        {5}, 
                        {500}, 
                        {5000}, 
                        {50000}, 
                        {500000}, 
                        {5.5}, 
                        {5.55}, 
                        {90.09}, 
                        {100.10}, 
                        {'2024-05-01'}, 
                        {'2024-05-01 12:00:00'}, 
                        {'2024-05-02'}, 
                        {'2024-05-02 12:00:00'}, 
                        {'char5'}, 
                        {'e'}, 
                        {'string5'}
                    );
                    """
    qt_sql_replace_struct_2 "select * from test_struct_agg_replace_if_not_null order by k1;"
}
