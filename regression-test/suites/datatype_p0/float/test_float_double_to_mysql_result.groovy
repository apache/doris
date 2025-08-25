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

suite("test_float_to_mysql_result") {
    sql "drop table if exists test_float_to_mysql_result";
    sql """
        create table test_float_to_mysql_result (
            f1 int,
            f2 float
        ) properties("replication_num" = "1");
    """
    sql """
        insert into test_float_to_mysql_result values
            (1, "123.456"),
            (2, "123.456789"),
            (3, "123.456789123"),
            (4, "123456.123456789"),
            (5, "123456789.12345"),
            (6, "1234567890.12345"),
            (7, "0.123456789"),
            (8, "0.000123456"),
            (9, "0.0000123456"),
            (10, "0.0000123456789"),
            (11, "1234567890123456.12345"),
            (12, "12345678901234567.12345"),
            (13, "0.0"),
            (14, "-0.0"),
            (15, "Infinity"),
            (16, "-Infinity"),
            (17, "NaN");
    """
    qt_select1 "select * from test_float_to_mysql_result order by 1"

    sql "drop table if exists test_double_to_mysql_result";
    sql """
        create table test_double_to_mysql_result (
            f1 int,
            f2 double
        ) properties("replication_num" = "1");
    """
    sql """
        insert into test_double_to_mysql_result values
            (1, "123.456"),
            (2, "123.456789"),
            (3, "123.456789123"),
            (4, "123456.123456789"),
            (5, "123456789.12345"),
            (6, "1234567890123456.12345"),
            (7, "12345678901234567.12345"),
            (8, "123456789012345678.12345"),
            (9, "1234567890123456789.12345"),
            (10, "0.123456789"),
            (11, "0.000123456"),
            (12, "0.0000123456"),
            (13, "0.0"),
            (14, "-0.0"),
            (15, "Infinity"),
            (16, "-Infinity"),
            (17, "NaN");
    """
    qt_select2 "select * from test_double_to_mysql_result order by 1"

}
