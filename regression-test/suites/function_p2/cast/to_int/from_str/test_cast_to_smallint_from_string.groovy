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


suite("test_cast_to_smallint_from_string") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_smallint_from_string_0_nullable;"
    sql "create table test_cast_to_smallint_from_string_0_nullable(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_smallint_from_string_0_nullable values (0, "+0000"),(1, "+0001"),(2, "+0009"),(3, "+000123"),(4, "+00032767"),(5, "-0001"),(6, "-0009"),(7, "-000123"),(8, "-00032768"),(9, "+00032766"),(10, "-00032767"),(11, "+0"),(12, "+1"),(13, "+9"),(14, "+123"),(15, "+32767"),(16, "-1"),(17, "-9"),(18, "-123"),(19, "-32768"),
      (20, "+32766"),(21, "-32767"),(22, "0000"),(23, "0001"),(24, "0009"),(25, "000123"),(26, "00032767"),(27, "-0001"),(28, "-0009"),(29, "-000123"),(30, "-00032768"),(31, "00032766"),(32, "-00032767"),(33, "0"),(34, "1"),(35, "9"),(36, "123"),(37, "32767"),(38, "-1"),(39, "-9"),
      (40, "-123"),(41, "-32768"),(42, "32766"),(43, "-32767")
      ,(44, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as smallint) from test_cast_to_smallint_from_string_0_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as smallint) from test_cast_to_smallint_from_string_0_nullable order by 1;'

    sql "drop table if exists test_cast_to_smallint_from_string_0_not_nullable;"
    sql "create table test_cast_to_smallint_from_string_0_not_nullable(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_smallint_from_string_0_not_nullable values (0, "+0000"),(1, "+0001"),(2, "+0009"),(3, "+000123"),(4, "+00032767"),(5, "-0001"),(6, "-0009"),(7, "-000123"),(8, "-00032768"),(9, "+00032766"),(10, "-00032767"),(11, "+0"),(12, "+1"),(13, "+9"),(14, "+123"),(15, "+32767"),(16, "-1"),(17, "-9"),(18, "-123"),(19, "-32768"),
      (20, "+32766"),(21, "-32767"),(22, "0000"),(23, "0001"),(24, "0009"),(25, "000123"),(26, "00032767"),(27, "-0001"),(28, "-0009"),(29, "-000123"),(30, "-00032768"),(31, "00032766"),(32, "-00032767"),(33, "0"),(34, "1"),(35, "9"),(36, "123"),(37, "32767"),(38, "-1"),(39, "-9"),
      (40, "-123"),(41, "-32768"),(42, "32766"),(43, "-32767");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as smallint) from test_cast_to_smallint_from_string_0_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as smallint) from test_cast_to_smallint_from_string_0_not_nullable order by 1;'

}