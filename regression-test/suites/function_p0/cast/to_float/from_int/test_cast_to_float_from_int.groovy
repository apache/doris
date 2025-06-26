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


suite("test_cast_to_float_from_int") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_float_from_int_0_nullable;"
    sql "create table test_cast_to_float_from_int_0_nullable(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_float_from_int_0_nullable values (0, "0"),(1, "0"),(2, "1"),(3, "10"),(4, "100"),(5, "-1"),(6, "-10"),(7, "-100"),(8, "2"),(9, "4"),(10, "8"),(11, "16"),(12, "32"),(13, "64"),(14, "-2"),(15, "-4"),(16, "-8"),(17, "-16"),(18, "-32"),(19, "-64"),
      (20, "-128"),(21, "-2147483648"),(22, "2147483647"),(23, "-2147483647"),(24, "2147483646")
      ,(25, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as float) from test_cast_to_float_from_int_0_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as float) from test_cast_to_float_from_int_0_nullable order by 1;'

    sql "drop table if exists test_cast_to_float_from_int_0_not_nullable;"
    sql "create table test_cast_to_float_from_int_0_not_nullable(f1 int, f2 int) properties('replication_num'='1');"
    sql """insert into test_cast_to_float_from_int_0_not_nullable values (0, "0"),(1, "0"),(2, "1"),(3, "10"),(4, "100"),(5, "-1"),(6, "-10"),(7, "-100"),(8, "2"),(9, "4"),(10, "8"),(11, "16"),(12, "32"),(13, "64"),(14, "-2"),(15, "-4"),(16, "-8"),(17, "-16"),(18, "-32"),(19, "-64"),
      (20, "-128"),(21, "-2147483648"),(22, "2147483647"),(23, "-2147483647"),(24, "2147483646");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as float) from test_cast_to_float_from_int_0_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as float) from test_cast_to_float_from_int_0_not_nullable order by 1;'

}