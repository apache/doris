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

suite("test_format_round", "p0") {

    order_qt_format_round_0 """
        select format_round(1234567.8910,5),
                format_round(-1111.235,2),
                format_round(123.49 ,0),
                format_round(cast(1.44999 as double),5),
                format_round(cast(1.44999 as decimal(20,2)),5);
    """
    order_qt_format_round_1 """ select format_round(1, 0) """
    order_qt_format_round_2 """ select format_round(123456, 0) """
    order_qt_format_round_3 """ select format_round(123456, 3) """
    order_qt_format_round_4 """ select format_round(123456, 10) """
    order_qt_format_round_5 """ select format_round(123456.123456, 0) """
    order_qt_format_round_6 """ select format_round(123456.123456, 3) """
    order_qt_format_round_7 """ select format_round(123456.123456, 8) """

     sql """ DROP TABLE IF EXISTS test_format_round """
        sql """
        CREATE TABLE IF NOT EXISTS test_format_round (
            `user_id` LARGEINT NOT NULL COMMENT "用户id",
            `int_col` int COMMENT "",
            `bigint_col` bigint COMMENT "",
            `largeint_col` largeint COMMENT "",
            `double_col` double COMMENT "",
            `decimal_col` decimal COMMENT ""
            )
            DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
        """

        sql """ INSERT INTO test_format_round VALUES
                    (1, 123, 123456, 123455677788, 123456.1234567, 123456.1234567),
                    (2, 123, 12313, 094720913, 434.1234567, 34.1234567);
            """
        qt_select_default """ SELECT * FROM test_format_round t ORDER BY user_id; """

    order_qt_format_round_8 """ select format_round(int_col, 6) from test_format_round order by user_id"""
    order_qt_format_round_9 """ select format_round(bigint_col, 6) from test_format_round order by user_id"""
    order_qt_format_round_10 """ select format_round(largeint_col, 6) from test_format_round order by user_id"""
    order_qt_format_round_12 """ select format_round(double_col, 6) from test_format_round order by user_id"""
    order_qt_format_round_13 """ select format_round(decimal_col, 6) from test_format_round order by user_id"""

    test {
        sql """select format_round(1234567.8910, -1) """
        exception "it should be in range [0, 1024]"
    }

    test {
        sql """select format_round(1234567.8910, 1025) """
        exception "it should be in range [0, 1024]"
    }

    order_qt_format_round_14 """ SELECT format_round(9876.54321, 0) AS result; """
    order_qt_format_round_15 """ SELECT format_round(0.0000001, 7) AS result; """
    order_qt_format_round_16 """ SELECT format_round(999999999.999999, 6) AS result; """
    order_qt_format_round_17 """ SELECT format_round(-123.456789, 3) AS result; """
    order_qt_format_round_18 """ SELECT format_round(1.23456789, 10) AS result; """
    order_qt_format_round_19 """ SELECT format_round(0.0, 2) AS result; """
    order_qt_format_round_20 """ SELECT format_round(1234567890.123456789, 9) AS result; """
    order_qt_format_round_21 """ SELECT format_round(0.0000000001, 10) AS result; """
    order_qt_format_round_22 """ SELECT format_round(-999999999.999999, 6) AS result; """
    order_qt_format_round_23 """ SELECT format_round(123.456789, 1) AS result; """
    order_qt_format_round_24 """ SELECT format_round(123.456789, 20) AS result; """
    order_qt_format_round_25 """ SELECT format_round(1.7976931348623157E+308, 2) AS result; """
    order_qt_format_round_26 """ SELECT format_round(2.2250738585072014E-308, 20) AS result; """
    order_qt_format_round_27 """ SELECT format_round(0.0, 0) AS result; """
    order_qt_format_round_28 """ SELECT format_round(0.0, 10) AS result; """
    order_qt_format_round_29 """ SELECT format_round(1.0, 0) AS result; """
    order_qt_format_round_30 """ SELECT format_round(1.0, 10) AS result; """
    order_qt_format_round_31 """ SELECT format_round(0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001, 324) AS result; """
    order_qt_format_round_32 """ SELECT format_round(1.7976931348623157E+308, 0) AS result; """
    order_qt_format_round_33 """ SELECT format_round(2.2250738585072014E-308, 0) AS result; """
    order_qt_format_round_34 """ SELECT format_round(1.7976931348623157E+308, 10) AS result; """
    order_qt_format_round_35 """ SELECT format_round(2.2250738585072014E-308, 10) AS result; """
    order_qt_format_round_36 """ SELECT format_round(1.7976931348623157E+308, 324) AS result; """
    order_qt_format_round_37 """ SELECT format_round(2.2250738585072014E-308, 324) AS result; """
    order_qt_format_round_38 """ SELECT format_round(1.0, 324) AS result; """
    order_qt_format_round_39 """ SELECT format_round(0.0, 324) AS result; """
    order_qt_format_round_40 """ SELECT format_round(0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001, 0) AS result; """
    order_qt_format_round_41 """ SELECT format_round(0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001, 10) AS result; """
    order_qt_format_round_42 """ SELECT format_round(0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001, 324) AS result; """
    order_qt_format_round_43 """ SELECT format_round(1.0, 0) AS result; """
    order_qt_format_round_44 """ SELECT format_round(1.0, 10) AS result; """
    order_qt_format_round_45 """ SELECT format_round(1.0, 324) AS result; """
    order_qt_format_round_46 """ SELECT format_round(0.0, 0) AS result; """
    order_qt_format_round_47 """ SELECT format_round(0.0, 10) AS result; """
    order_qt_format_round_48 """ SELECT format_round(0.0, 324) AS result; """
    order_qt_format_round_49 """ SELECT format_round(1.7976931348623157E+308, 0) AS result; """
    order_qt_format_round_50 """ SELECT format_round(1.7976931348623157E+308, 10) AS result; """
    order_qt_format_round_51 """ SELECT format_round(1.7976931348623157E+308, 324) AS result; """
    order_qt_format_round_52 """ SELECT format_round(2.2250738585072014E-308, 0) AS result; """
    order_qt_format_round_53 """ SELECT format_round(2.2250738585072014E-308, 10) AS result; """
    order_qt_format_round_54 """ SELECT format_round(2.2250738585072014E-308, 324) AS result; """
    order_qt_format_round_55 """ SELECT format_round(0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001, 0) AS result; """
    order_qt_format_round_56 """ SELECT format_round(0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001, 10) AS result; """
    order_qt_format_round_57 """ SELECT format_round(0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001, 324) AS result; """
    order_qt_format_round_58 """ SELECT format_round(1.0, 0) AS result; """
    order_qt_format_round_59 """ SELECT format_round(1.0, 10) AS result; """
    order_qt_format_round_60 """ SELECT format_round(1.0, 324) AS result; """
    order_qt_format_round_61 """ SELECT format_round(0.0, 0) AS result; """
    order_qt_format_round_62 """ SELECT format_round(0.0, 10) AS result; """
    order_qt_format_round_63 """ SELECT format_round(0.0, 324) AS result; """
    order_qt_format_round_64 """ SELECT format_round(1.7976931348623157E+308, 0) AS result; """
    order_qt_format_round_65 """ SELECT format_round(1.7976931348623157E+308, 10) AS result; """
    order_qt_format_round_66 """ SELECT format_round(1.7976931348623157E+308, 324) AS result; """
    order_qt_format_round_67 """ SELECT format_round(2.2250738585072014E-308, 0) AS result; """
    order_qt_format_round_68 """ SELECT format_round(2.2250738585072014E-308, 10) AS result; """
    order_qt_format_round_69 """ SELECT format_round(2.2250738585072014E-308, 324) AS result; """
    order_qt_format_round_70 """ SELECT format_round(0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001, 0) AS result; """
    order_qt_format_round_71 """ SELECT format_round(0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001, 10) AS result; """
}