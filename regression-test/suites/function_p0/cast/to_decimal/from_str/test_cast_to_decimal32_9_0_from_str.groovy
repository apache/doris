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


suite("test_cast_to_decimal32_9_0_from_str") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_decimal32_9_0_from_str_2_9_0;"
    sql "create table test_cast_to_decimal32_9_0_from_str_2_9_0(f1 int, f2 string) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal32_9_0_from_str_2_9_0 values (52, "0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647"),(53, "-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647"),(54, "0"),(55, "1"),(56, "9"),(57, "99999999"),(58, "900000000"),(59, "900000001"),(60, "999999998"),(61, "999999999"),
      (62, "0."),(63, "1."),(64, "9."),(65, "99999999."),(66, "900000000."),(67, "900000001."),(68, "999999998."),(69, "999999999."),(70, "-0"),(71, "-1"),
      (72, "-9"),(73, "-99999999"),(74, "-900000000"),(75, "-900000001"),(76, "-999999998"),(77, "-999999999"),(78, "-0."),(79, "-1."),(80, "-9."),(81, "-99999999."),
      (82, "-900000000."),(83, "-900000001."),(84, "-999999998."),(85, "-999999999."),(86, "0.49999"),(87, "1.49999"),(88, "9.49999"),(89, "99999999.49999"),(90, "900000000.49999"),(91, "900000001.49999"),
      (92, "999999998.49999"),(93, "999999999.49999"),(94, "0.5"),(95, "1.5"),(96, "9.5"),(97, "99999999.5"),(98, "900000000.5"),(99, "900000001.5"),(100, "999999998.5"),(101, "999999999.49999"),
      (102, "-0.49999"),(103, "-1.49999"),(104, "-9.49999"),(105, "-99999999.49999"),(106, "-900000000.49999"),(107, "-900000001.49999"),(108, "-999999998.49999"),(109, "-999999999.49999"),(110, "-0.5"),(111, "-1.5"),
      (112, "-9.5"),(113, "-99999999.5"),(114, "-900000000.5"),(115, "-900000001.5"),(116, "-999999998.5"),(117, "-999999999.49999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_str_2_9_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(9, 0)) from test_cast_to_decimal32_9_0_from_str_2_9_0 order by 1;'

}