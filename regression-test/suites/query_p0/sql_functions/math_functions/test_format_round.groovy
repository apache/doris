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
                    (1, 123, 123456, 123455677788, 123456.1234567, 123456.1234567);
            """
        qt_select_default """ SELECT * FROM test_format_round t ORDER BY user_id; """

    order_qt_format_round_8 """ select format_round(int_col, 6) from test_format_round"""
    order_qt_format_round_9 """ select format_round(bigint_col, 6) from test_format_round"""
    order_qt_format_round_10 """ select format_round(largeint_col, 6) from test_format_round"""
    order_qt_format_round_12 """ select format_round(double_col, 6) from test_format_round"""
    order_qt_format_round_13 """ select format_round(decimal_col, 6) from test_format_round"""

    test {
        sql """select format_round(1234567.8910, -1) """
        exception "it can not be less than 0"
    }

}