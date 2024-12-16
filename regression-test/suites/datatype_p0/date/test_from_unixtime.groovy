
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

suite("test_from_unixtime") {
    sql "set enable_fold_constant_by_be=true"

    sql "set time_zone='+08:00'"

    qt_sql1 "select from_unixtime(1553152255)"

    sql "set time_zone='+00:00'"

    qt_sql2 "select from_unixtime(1553152255)"

    qt_sql3 "select from_unixtime(0, \"%Y-%m-%d\")"
    qt_sql4 "select from_unixtime(0);"

    qt_sql5 "select from_unixtime(-1, \"%Y-%m-%d\");"
    qt_sql6 "select from_unixtime(-1);"

    // https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_from-unixtime
    // 32536771199 is mysql max valid timestamp on 64bit system, return 3001-01-18 23:59:59
    // INT32_MAX(2147483647) < 32536771199 < INT64_MAX
    qt_sql7 "select from_unixtime(32536771199)"

    // Return NULL, same with msyql
    qt_sql8 "select from_unixtime(32536771199 + 1)"

    qt_sql9 "select from_unixtime(-7629445119491449, \"%Y-%m-%d\");"
    qt_sql10 "select from_unixtime(-7629445119491449);"

    qt_long "select from_unixtime(1196440219, '%f %V %f %l %V %I %S %p %w %r %j %f %l %I %D %w %j %D %e %s %V %f %D %M %s %X %U %v %c %u %x %r %j %a %h %s %m %a %v %u %b');"
}
