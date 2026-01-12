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


suite("test_cast_to_bigint_from_float") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_bigint_from_float_0_nullable;"
    sql "create table test_cast_to_bigint_from_float_0_nullable(f1 int, f2 float) properties('replication_num'='1');"
    sql """insert into test_cast_to_bigint_from_float_0_nullable values (0, "0"),(1, "-0"),(2, "0"),(3, "1"),(4, "9"),(5, "123"),(6, "127.9"),(7, "-1"),(8, "-9"),(9, "-123"),(10, "-128.9"),(11, "-9.223372e+18"),(12, "1.9"),(13, "-1.9"),(14, "0.9999"),(15, "-0.9999"),(16, "1e-45"),(17, "-1e-45"),(18, "4294967300"),(19, "-4294967300"),
      (20, "4.611686e+18"),(21, "-4.611686e+18"),(22, "32768.9"),(23, "-32769.9"),(24, "999999.9"),(25, "-999999.9"),(26, "1073741800"),(27, "-1073741800"),(28, "32767.9"),(29, "-32768.9")
      ,(30, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as bigint) from test_cast_to_bigint_from_float_0_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as bigint) from test_cast_to_bigint_from_float_0_nullable order by 1;'

    sql "drop table if exists test_cast_to_bigint_from_float_0_not_nullable;"
    sql "create table test_cast_to_bigint_from_float_0_not_nullable(f1 int, f2 float) properties('replication_num'='1');"
    sql """insert into test_cast_to_bigint_from_float_0_not_nullable values (0, "0"),(1, "-0"),(2, "0"),(3, "1"),(4, "9"),(5, "123"),(6, "127.9"),(7, "-1"),(8, "-9"),(9, "-123"),(10, "-128.9"),(11, "-9.223372e+18"),(12, "1.9"),(13, "-1.9"),(14, "0.9999"),(15, "-0.9999"),(16, "1e-45"),(17, "-1e-45"),(18, "4294967300"),(19, "-4294967300"),
      (20, "4.611686e+18"),(21, "-4.611686e+18"),(22, "32768.9"),(23, "-32769.9"),(24, "999999.9"),(25, "-999999.9"),(26, "1073741800"),(27, "-1073741800"),(28, "32767.9"),(29, "-32768.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as bigint) from test_cast_to_bigint_from_float_0_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as bigint) from test_cast_to_bigint_from_float_0_not_nullable order by 1;'

}