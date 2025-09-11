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


suite("test_cast_to_string_from_float") {
    sql """set debug_skip_fold_constant = "true";"""
    def float_values = [
        "0.0", "-0.0", "1.23", "123.456", "123", "1234567", "123456.12345", "1234567.12345", "12345678.12345", "123456789.12345",
        "1234567890000.12345", "0.33", "123.456789", "123.456789123",
        "987654336.0", "16777216.0",
        "0.000123456", "0.0001234567", "0.00012345678",
        "0.0000123456", "0.00001234567", "0.0000123456789",
        "0.000000000000001234567",
        "0.000000000000001234567890123456",
        "0.1234567", "0.123456789",
        "1234567890123456.12345", "12345678901234567.12345",
        "1.175494e-38", "-3.402823e+38", "1.401298e-45", "3.402823e+38",
        "-Infinity", "Infinity", "NaN"
    ]

    // for (b in ["false", "true"]) {
    // sql """set debug_skip_fold_constant = "${b}";"""
    for (test_str in float_values) {
        qt_sql_float_be """select "${test_str}", cast("${test_str}" as float), cast(cast("${test_str}" as float) as string);"""
    }

    sql """
        drop table if exists test_cast_to_string_from_float;
    """
    sql """
        create table test_cast_to_string_from_float (
            k1 int,
            v1 float
        ) properties("replication_num" = "1");
    """
    def insert_sql_str_float = "insert into test_cast_to_string_from_float values "
    def index = 0
    for (test_str in float_values) {
        insert_sql_str_float += """(${index}, "${test_str}"), """
        index++
    }
    insert_sql_str_float = insert_sql_str_float[0..-3]
    sql insert_sql_str_float
    qt_sql_float_to_string """
        select k1, v1, cast(v1 as string) from test_cast_to_string_from_float order by k1;
    """

    def double_values = [
        "0.0",
        "-0.0",
        "1.230",
        "123.456000",
        "123.000",
        "1234567.000",
        "123456.12345",
        "1234567.12345",
        "12345678.12345",
        "123456789.12345",
        "1234567890000.12345",
        "0.33",
        "123.456",
        "123.456789",
        "123.456789123",
        "123456.123456789",
        "1234567.123456789",
        "987654336.0",
        "16777216.0",
        "0.000123456",
        "0.0001234567",
        "0.00012345678",
        "0.0000123456",
        "0.00001234567",
        "0.0000123456789",
        "0.000000000000001234567",
        "0.000000000000001234567890123456",
        "0.1234567",
        "0.123456789",
        "1234567890123456.12345",
        "12345678901234567.12345",
        "123456789012345678.12345",
        "1.175494350822288e-38",
        "-3.402823466385289e+38",
        "1.401298464324817e-45",
        "3.402823466385289e+38",
        "2.225073858507201e-308",
        "-1.797693134862316e+308",
        "4.940656458412465e-324",
        "1.797693134862316e+308",
        "Infinity",
        "-Infinity",
        "NaN"
    ]
    for (test_str in double_values) {
        qt_sql_double_be """select "${test_str}", cast("${test_str}" as double), cast(cast("${test_str}" as double) as string);"""
    }

    sql """
        drop table if exists test_cast_to_string_from_double;
    """
    sql """
        create table test_cast_to_string_from_double (
            k1 int,
            v1 double
        ) properties("replication_num" = "1");
    """
    def insert_sql_str_double = "insert into test_cast_to_string_from_double values "
    index = 0
    for (test_str in double_values) {
        insert_sql_str_double += """(${index}, "${test_str}"), """
        index++
    }
    insert_sql_str_double = insert_sql_str_double[0..-3]
    sql insert_sql_str_double
    qt_sql_double_to_string """
        select k1, v1, cast(v1 as string) from test_cast_to_string_from_double order by k1;
    """
}
