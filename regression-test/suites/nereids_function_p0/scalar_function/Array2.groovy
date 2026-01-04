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

suite("nereids_scalar_fn_Array2") {
    sql 'use regression_test_nereids_function_p0'
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'
    sql 'set enable_decimal256=true;'

    order_qt_sql_array_sort_1 """SELECT array_sort((x, y) -> CASE WHEN x IS NULL THEN -1
                                 WHEN y IS NULL THEN 1
                                 WHEN x < y THEN 1
                                 WHEN x = y THEN 0
                                 ELSE -1 END,
                                 [3, 2, null, 5, null, 1, 2])"""
    order_qt_sql_array_sort_2 """SELECT array_sort((x, y) -> CASE WHEN x IS NULL THEN 1
                                 WHEN y IS NULL THEN -1
                                 WHEN x < y THEN 1
                                 WHEN x = y THEN 0
                                 ELSE -1 END,
                                 [3, 2, null, 5, null, 1, 2])"""
    order_qt_sql_array_sort_3 """SELECT array_sort((x, y) -> IF(x < y, 1, IF(x = y, 0, -1)), [3, 2, 5, 1, 2])"""
    order_qt_sql_array_sort_4 """SELECT array_sort((x, y) -> IF(x < y, 1, IF(x = y, 0, -1)), ['bc', 'ab', 'dc'])"""
    order_qt_sql_array_sort_5 """SELECT array_sort((x, y) -> IF(length(x) < length(y), -1,
                               IF(length(x) = length(y), 0, 1)),
                               ['a', 'abcd', 'abc'])"""
    order_qt_sql_array_sort_6 """SELECT array_sort((x, y) -> IF(cardinality(x) < cardinality(y), -1,
                               IF(cardinality(x) = cardinality(y), 0, 1)),
                               [[2, 3, 1], [4, 2, 1, 4], [1, 2]])"""
    order_qt_sql_array_sort_7 """SELECT array_sort((x, y) -> IF(IPV4_STRING_TO_NUM_OR_NULL(x) < IPV4_STRING_TO_NUM_OR_NULL(y), -1,
                               IF(IPV4_STRING_TO_NUM_OR_NULL(x) = IPV4_STRING_TO_NUM_OR_NULL(y), 0, 1)),
                               ['192.168.0.3', '192.168.0.1', '192.168.0.2'])"""
    order_qt_sql_array_sort_8 """SELECT array_sort((x, y) -> IF(x < y, 1, IF(x = y, 0, -1)), [3, -2.1, 5.34, 1.2, 2.2])"""

    order_qt_sql_array_sort_Tinyint """SELECT array_sort((x, y) -> CASE WHEN x IS NULL THEN -1
                                 WHEN y IS NULL THEN 1
                                 WHEN x < y THEN 1
                                 WHEN x = y THEN 0
                                 ELSE -1 END, arr) FROM (SELECT array_agg(ktint) AS arr FROM fn_test)t"""
    order_qt_sql_array_sort_Tinyint_notnull """SELECT array_sort((x, y) -> IF(x < y, 1, IF(x = y, 0, -1)), arr) FROM (SELECT array_agg(ktint) AS arr FROM fn_test_not_nullable)t"""
    order_qt_sql_array_sort_Smallint """SELECT array_sort((x, y) -> CASE WHEN x IS NULL THEN -1
                                 WHEN y IS NULL THEN 1
                                 WHEN x < y THEN 1
                                 WHEN x = y THEN 0
                                 ELSE -1 END, arr) FROM (SELECT array_agg(ksint) AS arr FROM fn_test)t"""
    order_qt_sql_array_sort_Smallint_notnull """SELECT array_sort((x, y) -> IF(x < y, 1, IF(x = y, 0, -1)), arr) FROM (SELECT array_agg(ksint) AS arr FROM fn_test_not_nullable)t"""
    order_qt_sql_array_sort_Int """SELECT array_sort((x, y) -> CASE WHEN x IS NULL THEN -1
                                 WHEN y IS NULL THEN 1
                                 WHEN x < y THEN 1
                                 WHEN x = y THEN 0
                                 ELSE -1 END, arr) FROM (SELECT array_agg(kint) AS arr FROM fn_test)t"""
    order_qt_sql_array_sort_Int_notnull """SELECT array_sort((x, y) -> IF(x < y, 1, IF(x = y, 0, -1)), arr) FROM (SELECT array_agg(kint) AS arr FROM fn_test_not_nullable)t"""
    order_qt_sql_array_sort_Bigint """SELECT array_sort((x, y) -> CASE WHEN x IS NULL THEN -1
                                 WHEN y IS NULL THEN 1
                                 WHEN x < y THEN 1
                                 WHEN x = y THEN 0
                                 ELSE -1 END, arr) FROM (SELECT array_agg(kbint) AS arr FROM fn_test)t"""
    order_qt_sql_array_sort_Bigint_notnull """SELECT array_sort((x, y) -> IF(x < y, 1, IF(x = y, 0, -1)), arr) FROM (SELECT array_agg(kbint) AS arr FROM fn_test_not_nullable)t"""
    order_qt_sql_array_sort_Largeint """SELECT array_sort((x, y) -> CASE WHEN x IS NULL THEN -1
                                 WHEN y IS NULL THEN 1
                                 WHEN x < y THEN 1
                                 WHEN x = y THEN 0
                                 ELSE -1 END, arr) FROM (SELECT array_agg(klint) AS arr FROM fn_test)t"""
    order_qt_sql_array_sort_LargeInt_notnull """SELECT array_sort((x, y) -> IF(x < y, 1, IF(x = y, 0, -1)), arr) FROM (SELECT array_agg(klint) AS arr FROM fn_test_not_nullable)t"""
    order_qt_sql_array_sort_Float """SELECT array_sort((x, y) -> CASE WHEN x IS NULL THEN -1
                                 WHEN y IS NULL THEN 1
                                 WHEN x < y THEN 1
                                 WHEN x = y THEN 0
                                 ELSE -1 END, arr) FROM (SELECT array_agg(kfloat) AS arr FROM fn_test)t"""
    order_qt_sql_array_sort_Float_notnull """SELECT array_sort((x, y) -> IF(x < y, 1, IF(x = y, 0, -1)), arr) FROM (SELECT array_agg(kfloat) AS arr FROM fn_test_not_nullable)t"""
    order_qt_sql_array_sort_Double """SELECT array_sort((x, y) -> CASE WHEN x IS NULL THEN -1
                                 WHEN y IS NULL THEN 1
                                 WHEN x < y THEN 1
                                 WHEN x = y THEN 0
                                 ELSE -1 END, arr) FROM (SELECT array_agg(kdbl) AS arr FROM fn_test)t"""
    order_qt_sql_array_sort_Double_notnull """SELECT array_sort((x, y) -> IF(x < y, 1, IF(x = y, 0, -1)), arr) FROM (SELECT array_agg(kdbl) AS arr FROM fn_test_not_nullable)t"""
    order_qt_sql_array_sort_Demical1 """SELECT array_sort((x, y) -> CASE WHEN x IS NULL THEN -1
                                 WHEN y IS NULL THEN 1
                                 WHEN x < y THEN 1
                                 WHEN x = y THEN 0
                                 ELSE -1 END, arr) FROM (SELECT array_agg(kdcmls1) AS arr FROM fn_test)t"""
    order_qt_sql_array_sort_Demical1_notnull """SELECT array_sort((x, y) -> IF(x < y, 1, IF(x = y, 0, -1)), arr) FROM (SELECT array_agg(kdcmls1) AS arr FROM fn_test_not_nullable)t"""
    order_qt_sql_array_sort_Demical2 """SELECT array_sort((x, y) -> CASE WHEN x IS NULL THEN -1
                                 WHEN y IS NULL THEN 1
                                 WHEN x < y THEN 1
                                 WHEN x = y THEN 0
                                 ELSE -1 END, arr) FROM (SELECT array_agg(kdcmls2) AS arr FROM fn_test)t"""
    order_qt_sql_array_sort_Demical2_notnull """SELECT array_sort((x, y) -> IF(x < y, 1, IF(x = y, 0, -1)), arr) FROM (SELECT array_agg(kdcmls2) AS arr FROM fn_test_not_nullable)t"""
    order_qt_sql_array_sort_Demical3 """SELECT array_sort((x, y) -> CASE WHEN x IS NULL THEN -1
                                 WHEN y IS NULL THEN 1
                                 WHEN x < y THEN 1
                                 WHEN x = y THEN 0
                                 ELSE -1 END, arr) FROM (SELECT array_agg(kdcmls3) AS arr FROM fn_test)t"""
    order_qt_sql_array_sort_Demical3_notnull """SELECT array_sort((x, y) -> IF(x < y, 1, IF(x = y, 0, -1)), arr) FROM (SELECT array_agg(kdcmls3) AS arr FROM fn_test_not_nullable)t"""
    order_qt_sql_array_sort_Demical4 """SELECT array_sort((x, y) -> CASE WHEN x IS NULL THEN -1
                                 WHEN y IS NULL THEN 1
                                 WHEN x < y THEN 1
                                 WHEN x = y THEN 0
                                 ELSE -1 END, arr) FROM (SELECT array_agg(kdcmlv3s1) AS arr FROM fn_test)t"""
    order_qt_sql_array_sort_Demical4_notnull """SELECT array_sort((x, y) -> IF(x < y, 1, IF(x = y, 0, -1)), arr) FROM (SELECT array_agg(kdcmlv3s1) AS arr FROM fn_test_not_nullable)t"""
    order_qt_sql_array_sort_Demical5 """SELECT array_sort((x, y) -> CASE WHEN x IS NULL THEN -1
                                 WHEN y IS NULL THEN 1
                                 WHEN x < y THEN 1
                                 WHEN x = y THEN 0
                                 ELSE -1 END, arr) FROM (SELECT array_agg(kdcmlv3s2) AS arr FROM fn_test)t"""
    order_qt_sql_array_sort_Demical5_notnull """SELECT array_sort((x, y) -> IF(x < y, 1, IF(x = y, 0, -1)), arr) FROM (SELECT array_agg(kdcmlv3s2) AS arr FROM fn_test_not_nullable)t"""
    order_qt_sql_array_sort_Demical6 """SELECT array_sort((x, y) -> CASE WHEN x IS NULL THEN -1
                                 WHEN y IS NULL THEN 1
                                 WHEN x < y THEN 1
                                 WHEN x = y THEN 0
                                 ELSE -1 END, arr) FROM (SELECT array_agg(kdcmlv3s3) AS arr FROM fn_test)t"""
    order_qt_sql_array_sort_Demical6_notnull """SELECT array_sort((x, y) -> IF(x < y, 1, IF(x = y, 0, -1)), arr) FROM (SELECT array_agg(kdcmlv3s3) AS arr FROM fn_test_not_nullable)t"""
    order_qt_sql_array_sort_Char """SELECT array_sort((x, y) -> CASE WHEN x IS NULL THEN -1
                                 WHEN y IS NULL THEN 1
                                 WHEN x < y THEN 1
                                 WHEN x = y THEN 0
                                 ELSE -1 END, arr) FROM (SELECT array_agg(kchrs3) AS arr FROM fn_test)t"""
    order_qt_sql_array_sort_Char_notnull """SELECT array_sort((x, y) -> IF(x < y, 1, IF(x = y, 0, -1)), arr) FROM (SELECT array_agg(kchrs3) AS arr FROM fn_test_not_nullable)t"""
    order_qt_sql_array_sort_Varchar """SELECT array_sort((x, y) -> CASE WHEN x IS NULL THEN -1
                                 WHEN y IS NULL THEN 1
                                 WHEN x < y THEN 1
                                 WHEN x = y THEN 0
                                 ELSE -1 END, arr) FROM (SELECT array_agg(kvchrs3) AS arr FROM fn_test)t"""
    order_qt_sql_array_sort_Varchar_notnull """SELECT array_sort((x, y) -> IF(x < y, 1, IF(x = y, 0, -1)), arr) FROM (SELECT array_agg(kvchrs3) AS arr FROM fn_test_not_nullable)t"""
    order_qt_sql_array_sort_String """SELECT array_sort((x, y) -> CASE WHEN x IS NULL THEN -1
                                 WHEN y IS NULL THEN 1
                                 WHEN x < y THEN 1
                                 WHEN x = y THEN 0
                                 ELSE -1 END, arr) FROM (SELECT array_agg(kstr) AS arr FROM fn_test)t"""
    order_qt_sql_array_sort_String_notnull """SELECT array_sort((x, y) -> IF(x < y, 1, IF(x = y, 0, -1)), arr) FROM (SELECT array_agg(kstr) AS arr FROM fn_test_not_nullable)t"""
    order_qt_sql_array_sort_Date """SELECT array_sort((x, y) -> CASE WHEN x IS NULL THEN -1
                                 WHEN y IS NULL THEN 1
                                 WHEN x < y THEN 1
                                 WHEN x = y THEN 0
                                 ELSE -1 END, arr) FROM (SELECT array_agg(kdt) AS arr FROM fn_test)t"""
    order_qt_sql_array_sort_Date_notnull """SELECT array_sort((x, y) -> IF(x < y, 1, IF(x = y, 0, -1)), arr) FROM (SELECT array_agg(kdt) AS arr FROM fn_test_not_nullable)t"""
    order_qt_sql_array_sort_DateV2 """SELECT array_sort((x, y) -> CASE WHEN x IS NULL THEN -1
                                 WHEN y IS NULL THEN 1
                                 WHEN x < y THEN 1
                                 WHEN x = y THEN 0
                                 ELSE -1 END, arr) FROM (SELECT array_agg(kdtv2) AS arr FROM fn_test)t"""
    order_qt_sql_array_sort_DateV2_notnull """SELECT array_sort((x, y) -> IF(x < y, 1, IF(x = y, 0, -1)), arr) FROM (SELECT array_agg(kdtv2) AS arr FROM fn_test_not_nullable)t"""
    order_qt_sql_array_sort_Datetime """SELECT array_sort((x, y) -> CASE WHEN x IS NULL THEN -1
                                 WHEN y IS NULL THEN 1
                                 WHEN x < y THEN 1
                                 WHEN x = y THEN 0
                                 ELSE -1 END, arr) FROM (SELECT array_agg(kdtm) AS arr FROM fn_test)t"""
    order_qt_sql_array_sort_Datetime_notnull """SELECT array_sort((x, y) -> IF(x < y, 1, IF(x = y, 0, -1)), arr) FROM (SELECT array_agg(kdtm) AS arr FROM fn_test_not_nullable)t"""
    order_qt_sql_array_sort_DatetimeV2 """SELECT array_sort((x, y) -> CASE WHEN x IS NULL THEN -1
                                 WHEN y IS NULL THEN 1
                                 WHEN x < y THEN 1
                                 WHEN x = y THEN 0
                                 ELSE -1 END, arr) FROM (SELECT array_agg(kdtmv2s3) AS arr FROM fn_test)t"""
    order_qt_sql_array_sort_DatetimeV2_notnull """SELECT array_sort((x, y) -> IF(x < y, 1, IF(x = y, 0, -1)), arr) FROM (SELECT array_agg(kdtmv2s3) AS arr FROM fn_test_not_nullable)t"""
    order_qt_sql_array_sort_Boolean """SELECT array_sort((x, y) -> CASE WHEN x IS NULL THEN -1
                                 WHEN y IS NULL THEN 1
                                 WHEN x < y THEN 1
                                 WHEN x = y THEN 0
                                 ELSE -1 END, arr) FROM (SELECT array_agg(kbool) AS arr FROM fn_test)t"""
    order_qt_sql_array_sort_Boolean_notnull """SELECT array_sort((x, y) -> IF(x < y, 1, IF(x = y, 0, -1)), arr) FROM (SELECT array_agg(kbool) AS arr FROM fn_test_not_nullable)t"""

    // Test multiple rows of data
    sql """ DROP TABLE IF EXISTS fn_test_array_sort"""
    sql """
        CREATE TABLE IF NOT EXISTS `fn_test_array_sort` (
            `id` int null,
            `katint` array<tinyint(4)> null,
            `kasint` array<smallint(6)> null,
            `kaint` array<int> null,
            `kabint` array<bigint(20)> null,
            `kalint` array<largeint(40)> null,
            `kafloat` array<float> null,
            `kadbl` array<double> null,
            `kadt` array<date> null,
            `kadtm` array<datetime> null,
            `kadtv2` array<datev2> null,
            `kadtmv2_` array<datetimev2(0)> null,
            `kadtmv2` array<datetimev2(6)> null,
            `kachr` array<char(255)> null,
            `kavchr` array<varchar(65533)> null,
            `kastr` array<string> null,
            `kadcml2` array<decimal(38, 38)> null,
            `kaipv4` array<ipv4> null,
            `kaipv6` array<ipv6> null
        ) engine=olap
        DISTRIBUTED BY HASH(`id`) BUCKETS 4
        properties("replication_num" = "1","store_row_column" = "true")
    """

    sql """INSERT INTO fn_test_array_sort (
            id, katint, kasint, kaint, kabint, kalint, kafloat, kadbl,
            kadt, kadtm, kadtv2, kadtmv2_, kadtmv2,
            kachr, kavchr, kastr,
            kadcml2, kaipv4, kaipv6
        ) VALUES
        (
            1,
            [3, 1, NULL, 5, 2],
            [3, 1, NULL, 5, 2],
            [3, 1, NULL, 5, 2],
            [3, 1, NULL, 5, 2],
            [3, 1, NULL, 5, 2],
            [3.1, 1.1, NULL, 5.1, 2.1],
            [3.1, 1.1, NULL, 5.1, 2.1],
            ['2024-01-03', '2024-01-01', NULL, '2024-01-05', '2024-01-02'],
            ['2024-01-03 10:00:00','2024-01-01 10:00:00',NULL,'2024-01-05 10:00:00','2024-01-02 10:00:00'],
            ['2024-01-03','2024-01-01',NULL,'2024-01-05','2024-01-02'],
            ['2024-01-03 10:00:00', '2024-01-01 10:00:00', NULL, '2024-01-05 10:00:00', '2024-01-02 10:00:00'],
            ['2024-01-03 10:00:00.000000','2024-01-01 10:00:00.000000',NULL,'2024-01-05 10:00:00.000000','2024-01-02 10:00:00.000000'],
            ['c','a',NULL,'e','b'],
            ['ccc','aaa',NULL,'eee','bbb'],
            ['ccc','aaa',NULL,'eee','bbb'],
            [0.00000000000000000000000000000000000003, NULL,
            0.00000000000000000000000000000000000001,
            0.00000000000000000000000000000000000005,
            0.00000000000000000000000000000000000002],
            ['127.0.0.3','127.0.0.1',NULL,'127.0.0.5','127.0.0.2'],
            ['::3','::1',NULL,'::5','::2']
        ),
        (
            2,
            [9, 7, NULL, 8, 6],
            [9, 7, NULL, 8, 6],
            [9, 7, NULL, 8, 6],
            [9, 7, NULL, 8, 6],
            [9, 7, NULL, 8, 6],
            [9.1, 7.1, NULL, 8.1, 6.1],
            [9.1, 7.1, NULL, 8.1, 6.1],
            ['2024-02-09','2024-02-07',NULL,'2024-02-08','2024-02-06'],
            ['2024-02-09 10:00:00','2024-02-07 10:00:00',NULL,'2024-02-08 10:00:00','2024-02-06 10:00:00'],
            ['2024-02-09','2024-02-07',NULL,'2024-02-08','2024-02-06'],
            ['2024-02-09 10:00:00','2024-02-07 10:00:00',NULL,'2024-02-08 10:00:00','2024-02-06 10:00:00'],
            ['2024-02-09 10:00:00.000000','2024-02-07 10:00:00.000000',NULL,'2024-02-08 10:00:00.000000','2024-02-06 10:00:00.000000'],
            ['i','g',NULL,'h','f'],
            ['iii','ggg',NULL,'hhh','fff'],
            ['iii','ggg',NULL,'hhh','fff'],
            [0.00000000000000000000000000000000000009, NULL,
            0.00000000000000000000000000000000000007,
            0.00000000000000000000000000000000000008,
            0.00000000000000000000000000000000000006],
            ['127.0.0.9','127.0.0.7',NULL,'127.0.0.8','127.0.0.6'],
            ['::9','::7',NULL,'::8','::6']
        ),
        (
            3,
            [4, 3, NULL, 2, 1],
            [4, 3, NULL, 2, 1],
            [4, 3, NULL, 2, 1],
            [4, 3, NULL, 2, 1],
            [4, 3, NULL, 2, 1],
            [4.1, 3.1, NULL, 2.1, 1.1],
            [4.1, 3.1, NULL, 2.1, 1.1],
            ['2024-03-04','2024-03-03',NULL,'2024-03-02','2024-03-01'],
            ['2024-03-04 10:00:00','2024-03-03 10:00:00',NULL,'2024-03-02 10:00:00','2024-03-01 10:00:00'],
            ['2024-03-04','2024-03-03',NULL,'2024-03-02','2024-03-01'],
            ['2024-03-04 10:00:00','2024-03-03 10:00:00',NULL,'2024-03-02 10:00:00','2024-03-01 10:00:00'],
            ['2024-03-04 10:00:00.000000','2024-03-03 10:00:00.000000',NULL,'2024-03-02 10:00:00.000000','2024-03-01 10:00:00.000000'],
            ['d','c',NULL,'b','a'],
            ['ddd','ccc',NULL,'bbb','aaa'],
            ['ddd','ccc',NULL,'bbb','aaa'],
            [0.00000000000000000000000000000000000004, NULL,
            0.00000000000000000000000000000000000003,
            0.00000000000000000000000000000000000002,
            0.00000000000000000000000000000000000001],
            ['127.0.0.4','127.0.0.3',NULL,'127.0.0.2','127.0.0.1'],
            ['::4','::3',NULL,'::2','::1']
        )
    """

    order_qt_sql_array_sort_lambda_multir_tinyint """
        SELECT id, array_sort(
                (x, y) -> CASE WHEN x IS NULL THEN -1 
                            WHEN y IS NULL THEN 1
                            WHEN x < y THEN 1
                            WHEN x = y THEN 0
                            ELSE -1 END,
                katint)
        FROM fn_test_array_sort
        ORDER BY id
    """

    order_qt_sql_array_sort_lambda_multir_smallint """
        SELECT id, array_sort(
                (x, y) -> CASE WHEN x IS NULL THEN -1 
                            WHEN y IS NULL THEN 1
                            WHEN x < y THEN 1
                            WHEN x = y THEN 0
                            ELSE -1 END,
                kasint)
        FROM fn_test_array_sort
        ORDER BY id
    """

    order_qt_sql_array_sort_lambda_multir_int """
        SELECT id, array_sort(
                (x, y) -> CASE WHEN x IS NULL THEN -1 
                            WHEN y IS NULL THEN 1
                            WHEN x < y THEN 1
                            WHEN x = y THEN 0
                            ELSE -1 END,
                kaint)
        FROM fn_test_array_sort
        ORDER BY id
    """

    order_qt_sql_array_sort_lambda_multir_bigint """
        SELECT id, array_sort(
                (x, y) -> CASE WHEN x IS NULL THEN -1 
                            WHEN y IS NULL THEN 1
                            WHEN x < y THEN 1
                            WHEN x = y THEN 0
                            ELSE -1 END,
                kabint)
        FROM fn_test_array_sort
        ORDER BY id
    """

    order_qt_sql_array_sort_lambda_multir_largeint """
        SELECT id, array_sort(
                (x, y) -> CASE WHEN x IS NULL THEN -1 
                            WHEN y IS NULL THEN 1
                            WHEN x < y THEN 1
                            WHEN x = y THEN 0
                            ELSE -1 END,
                kalint)
        FROM fn_test_array_sort
        ORDER BY id
    """

    order_qt_sql_array_sort_lambda_multir_float """
        SELECT id, array_sort(
                (x, y) -> CASE WHEN x IS NULL THEN -1 
                            WHEN y IS NULL THEN 1
                            WHEN x < y THEN 1
                            WHEN x = y THEN 0
                            ELSE -1 END,
                kafloat)
        FROM fn_test_array_sort
        ORDER BY id
    """

    order_qt_sql_array_sort_lambda_multir_double """
        SELECT id, array_sort(
                (x, y) -> CASE WHEN x IS NULL THEN -1 
                            WHEN y IS NULL THEN 1
                            WHEN x < y THEN 1
                            WHEN x = y THEN 0
                            ELSE -1 END,
                kadbl)
        FROM fn_test_array_sort
        ORDER BY id
    """

    order_qt_sql_array_sort_lambda_multir_date """
        SELECT id, array_sort(
                (x, y) -> CASE WHEN x IS NULL THEN -1 
                            WHEN y IS NULL THEN 1
                            WHEN x < y THEN 1
                            WHEN x = y THEN 0
                            ELSE -1 END,
                kadt)
        FROM fn_test_array_sort
        ORDER BY id
    """

    order_qt_sql_array_sort_lambda_multir_datetime """
        SELECT id, array_sort(
                (x, y) -> CASE WHEN x IS NULL THEN -1 
                            WHEN y IS NULL THEN 1
                            WHEN x < y THEN 1
                            WHEN x = y THEN 0
                            ELSE -1 END,
                kadtm)
        FROM fn_test_array_sort
        ORDER BY id
    """

    order_qt_sql_array_sort_lambda_multir_datev2 """
        SELECT id, array_sort(
                (x, y) -> CASE WHEN x IS NULL THEN -1 
                            WHEN y IS NULL THEN 1
                            WHEN x < y THEN 1
                            WHEN x = y THEN 0
                            ELSE -1 END,
                kadtv2)
        FROM fn_test_array_sort
        ORDER BY id
    """

    order_qt_sql_array_sort_lambda_multir_datetimev2_0 """
        SELECT id, array_sort(
                (x, y) -> CASE WHEN x IS NULL THEN -1 
                            WHEN y IS NULL THEN 1
                            WHEN x < y THEN 1
                            WHEN x = y THEN 0
                            ELSE -1 END,
                kadtmv2_)
        FROM fn_test_array_sort
        ORDER BY id
    """

    order_qt_sql_array_sort_lambda_multir_datetimev2_6 """
        SELECT id, array_sort(
                (x, y) -> CASE WHEN x IS NULL THEN -1 
                            WHEN y IS NULL THEN 1
                            WHEN x < y THEN 1
                            WHEN x = y THEN 0
                            ELSE -1 END,
                kadtmv2)
        FROM fn_test_array_sort
        ORDER BY id
    """

    order_qt_sql_array_sort_lambda_multir_char """
        SELECT id, array_sort(
                (x, y) -> CASE WHEN x IS NULL THEN -1 
                            WHEN y IS NULL THEN 1
                            WHEN x < y THEN 1
                            WHEN x = y THEN 0
                            ELSE -1 END,
                kachr)
        FROM fn_test_array_sort
        ORDER BY id
    """

    order_qt_sql_array_sort_lambda_multir_varchar """
        SELECT id, array_sort(
                (x, y) -> CASE WHEN x IS NULL THEN -1 
                            WHEN y IS NULL THEN 1
                            WHEN x < y THEN 1
                            WHEN x = y THEN 0
                            ELSE -1 END,
                kavchr)
        FROM fn_test_array_sort
        ORDER BY id
    """

    order_qt_sql_array_sort_lambda_multir_string """
        SELECT id, array_sort(
                (x, y) -> CASE WHEN x IS NULL THEN -1 
                            WHEN y IS NULL THEN 1
                            WHEN x < y THEN 1
                            WHEN x = y THEN 0
                            ELSE -1 END,
                kastr)
        FROM fn_test_array_sort
        ORDER BY id
    """

    order_qt_sql_array_sort_lambda_multir_decimal38_38 """
        SELECT id, array_sort(
                (x, y) -> CASE WHEN x IS NULL THEN -1 
                            WHEN y IS NULL THEN 1
                            WHEN x < y THEN 1
                            WHEN x = y THEN 0
                            ELSE -1 END,
                kadcml2)
        FROM fn_test_array_sort
        ORDER BY id
    """

    order_qt_sql_array_sort_lambda_multir_ipv4 """
        SELECT id, array_sort(
                (x, y) -> CASE WHEN x IS NULL THEN -1 
                            WHEN y IS NULL THEN 1
                            WHEN x < y THEN 1
                            WHEN x = y THEN 0
                            ELSE -1 END,
                kaipv4)
        FROM fn_test_array_sort
        ORDER BY id
    """

    order_qt_sql_array_sort_lambda_multir_ipv6 """
        SELECT id, array_sort(
                (x, y) -> CASE WHEN x IS NULL THEN -1 
                            WHEN y IS NULL THEN 1
                            WHEN x < y THEN 1
                            WHEN x = y THEN 0
                            ELSE -1 END,
                kaipv6)
        FROM fn_test_array_sort
        ORDER BY id
    """
}
