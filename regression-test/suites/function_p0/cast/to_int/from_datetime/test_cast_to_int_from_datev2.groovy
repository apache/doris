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


suite("test_cast_to_int_from_datev2") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_int_from_datev2_0_nullable;"
    sql "create table test_cast_to_int_from_datev2_0_nullable(f1 int, f2 datev2) properties('replication_num'='1');"
    sql """insert into test_cast_to_int_from_datev2_0_nullable values (0, "0000-01-01"),(1, "0000-01-10"),(2, "0000-01-28"),(3, "0000-12-01"),(4, "0000-12-10"),(5, "0000-12-28"),(6, "0001-01-01"),(7, "0001-01-10"),(8, "0001-01-28"),(9, "0001-12-01"),(10, "0001-12-10"),(11, "0001-12-28"),(12, "0010-01-01"),(13, "0010-01-10"),(14, "0010-01-28"),(15, "0010-12-01"),(16, "0010-12-10"),(17, "0010-12-28"),(18, "0100-01-01"),(19, "0100-01-10"),
      (20, "0100-01-28"),(21, "0100-12-01"),(22, "0100-12-10"),(23, "0100-12-28"),(24, "2025-01-01"),(25, "2025-01-10"),(26, "2025-01-28"),(27, "2025-12-01"),(28, "2025-12-10"),(29, "2025-12-28"),(30, "9999-01-01"),(31, "9999-01-10"),(32, "9999-01-28"),(33, "9999-12-01"),(34, "9999-12-10"),(35, "9999-12-28")
      ,(36, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as int) from test_cast_to_int_from_datev2_0_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as int) from test_cast_to_int_from_datev2_0_nullable order by 1;'

    sql "drop table if exists test_cast_to_int_from_datev2_0_not_nullable;"
    sql "create table test_cast_to_int_from_datev2_0_not_nullable(f1 int, f2 datev2) properties('replication_num'='1');"
    sql """insert into test_cast_to_int_from_datev2_0_not_nullable values (0, "0000-01-01"),(1, "0000-01-10"),(2, "0000-01-28"),(3, "0000-12-01"),(4, "0000-12-10"),(5, "0000-12-28"),(6, "0001-01-01"),(7, "0001-01-10"),(8, "0001-01-28"),(9, "0001-12-01"),(10, "0001-12-10"),(11, "0001-12-28"),(12, "0010-01-01"),(13, "0010-01-10"),(14, "0010-01-28"),(15, "0010-12-01"),(16, "0010-12-10"),(17, "0010-12-28"),(18, "0100-01-01"),(19, "0100-01-10"),
      (20, "0100-01-28"),(21, "0100-12-01"),(22, "0100-12-10"),(23, "0100-12-28"),(24, "2025-01-01"),(25, "2025-01-10"),(26, "2025-01-28"),(27, "2025-12-01"),(28, "2025-12-10"),(29, "2025-12-28"),(30, "9999-01-01"),(31, "9999-01-10"),(32, "9999-01-28"),(33, "9999-12-01"),(34, "9999-12-10"),(35, "9999-12-28");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as int) from test_cast_to_int_from_datev2_0_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as int) from test_cast_to_int_from_datev2_0_not_nullable order by 1;'

}