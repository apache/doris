
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

    sql """
        drop table if exists from_unixtime_table;
    """
    sql """
        create table if not exists from_unixtime_table (rowid integer, str TEXT NULL)
        distributed by hash(rowid)
        properties("replication_num"="1")
    """
    sql """
        insert into from_unixtime_table values (1, "123456.123"), (2, "-123456.123"), (3, "123456.1234567"), (4, "-123456.1234567");
    """

    sql """
        insert into from_unixtime_table values (5, "32536771199.999999"), (6, "32536771199.9999995"), (7, "32536771200.0");
    """

    sql """
        insert into from_unixtime_table values (8, NULL);
    """

    qt_sql11 """select from_unixtime(cast("123456.123" as Decimal(9,3)));"""
    qt_sql12 """select from_unixtime(cast("-123456.123" as Decimal(9,3)));"""
    qt_sql13 """select from_unixtime(cast("123456.1234567" as Decimal(13,7)));"""
    qt_sql14 """select from_unixtime(cast("123456.1234567" as Decimal(13,7)));"""
    qt_sql15 """select from_unixtime(32536771199.999999);"""
    qt_sql16 """select from_unixtime(cast("123456.1234567" as Decimal(13,7)), "%Y-%m-%d %H:%i:%s");"""
    qt_sql17 """
        select *, cast(str as Decimal(20, 7)), from_unixtime(cast(str as Decimal(20, 7))), from_unixtime(cast(str as Decimal(20, 7)), NULL)from from_unixtime_table order by rowid
    """
}
