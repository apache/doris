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


suite("test_cast_to_tinyint_from_double_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_tinyint_from_double_overflow_0;"
    sql "create table test_cast_to_tinyint_from_double_overflow_0(f1 int, f2 double) properties('replication_num'='1');"
    sql """insert into test_cast_to_tinyint_from_double_overflow_0 values (0, "1.7976931348623157e+308"),(1, "-1.7976931348623157e+308"),(2, "inf"),(3, "nan"),(4, "128"),(5, "-129"),(6, "32768"),(7, "-32769"),(8, "4294967296"),(9, "-4294967296"),(10, "1.8446744073709552e+19"),(11, "-1.8446744073709552e+19"),(12, "3.402823669209385e+38"),(13, "-3.402823669209385e+38"),(14, "6.80564733841877e+38"),(15, "-6.80564733841877e+38");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_tinyint_from_double_overflow_0_data_start_index = 0
    def test_cast_to_tinyint_from_double_overflow_0_data_end_index = 16
    for (int data_index = test_cast_to_tinyint_from_double_overflow_0_data_start_index; data_index < test_cast_to_tinyint_from_double_overflow_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as tinyint) from test_cast_to_tinyint_from_double_overflow_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as tinyint) from test_cast_to_tinyint_from_double_overflow_0 order by 1;'

}