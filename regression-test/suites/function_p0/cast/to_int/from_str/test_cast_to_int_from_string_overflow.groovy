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


suite("test_cast_to_int_from_string_overflow") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_int_from_string_overflow_0;"
    sql "create table test_cast_to_int_from_string_overflow_0(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_int_from_string_overflow_0 values (0, "2147483648"),(1, "-2147483649"),(2, "2147483649"),(3, "-2147483650"),(4, "2147483650"),(5, "-2147483651"),(6, "2147483651"),(7, "-2147483652"),(8, "2147483652"),(9, "-2147483653"),(10, "2147483653"),(11, "-2147483654"),(12, "2147483654"),(13, "-2147483655"),(14, "2147483655"),(15, "-2147483656"),(16, "2147483656"),(17, "-2147483657"),(18, "2147483657"),(19, "-2147483658"),
      (20, "4294967295"),(21, "-4294967295"),(22, "4294967294"),(23, "-4294967294"),(24, "4294967293"),(25, "-4294967293"),(26, "4294967292"),(27, "-4294967292"),(28, "4294967291"),(29, "-4294967291"),(30, "4294967290"),(31, "-4294967290"),(32, "4294967289"),(33, "-4294967289"),(34, "4294967288"),(35, "-4294967288"),(36, "4294967287"),(37, "-4294967287"),(38, "4294967286"),(39, "-4294967286"),
      (40, "9223372036854775807"),(41, "-9223372036854775808"),(42, "170141183460469231731687303715884105727"),(43, "-170141183460469231731687303715884105728"),(44, "57896044618658097711785492504343953926634992332820282019728792003956564819967"),(45, "-57896044618658097711785492504343953926634992332820282019728792003956564819968");
    """

    sql "set enable_strict_cast=true;"

    def test_cast_to_int_from_string_overflow_0_data_start_index = 0
    def test_cast_to_int_from_string_overflow_0_data_end_index = 46
    for (int data_index = test_cast_to_int_from_string_overflow_0_data_start_index; data_index < test_cast_to_int_from_string_overflow_0_data_end_index; data_index++) {
        test {
            sql "select f1, cast(f2 as int) from test_cast_to_int_from_string_overflow_0 where f1 = ${data_index}"
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as int) from test_cast_to_int_from_string_overflow_0 order by 1;'

}