
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
    sql "set enable_query_cache=false"

    sql "set time_zone='+08:00'"

    sql "drop table if exists test1"
    sql """
        create table test1(
            k0 bigint null,
            k1 decimal(30, 10) null
        )
        DISTRIBUTED BY HASH(`k0`) BUCKETS auto
        properties("replication_num" = "1");
    """
    sql """insert into test1 values(100, 100.123), (20000000000, 20000000000.0010),
    (90000000000, 90000000000.1234569);"""
    qt_sql0 """select from_unixtime(k0), from_unixtime(k1), from_unixtime(k0, 'yyyy-MM-dd HH:mm:ss'),
    from_unixtime(k0, 'yyyy-MM-dd HH:mm:ss'), from_unixtime(k1, '%W%w') from test1 order by k0, k1"""
    testFoldConst ("""select from_unixtime(100), from_unixtime(100.123), from_unixtime(20000000000), from_unixtime(20000000000.0010),
    from_unixtime(90000000000), from_unixtime(90000000000.1234569)""")

    qt_sql1 "select from_unixtime(1553152255)"

    sql "set time_zone='+00:00'"

    qt_sql2 "select from_unixtime(1553152255)"

    qt_sql3 "select from_unixtime(0, \"%Y-%m-%d\")"
    qt_sql4 "select from_unixtime(0);"

    // qt_sql5 "select from_unixtime(-1, \"%Y-%m-%d\");"
    // qt_sql6 "select from_unixtime(-1);"

    qt_sql7 "select from_unixtime(32536771199)"
    qt_sql8 "select from_unixtime(32536771199 + 1)"

    // qt_sql9 "select from_unixtime(-7629445119491449, \"%Y-%m-%d\");"
    // qt_sql10 "select from_unixtime(-7629445119491449);"

    // qt_long "select from_unixtime(1196440219, '%f %V %f %l %V %I %S %p %w %r %j %f %l %I %D %w %j %D %e %s %V %f %D %M %s %X %U %v %c %u %x %r %j %a %h %s %m %a %v %u %b');"

    // HOUR(FROM_UNIXTIME(ts)) -> hour_from_unixtime(ts)
    sql "set debug_skip_fold_constant = true;"
    qt_hour_from_unixtime1 "SELECT HOUR(FROM_UNIXTIME(k0)) FROM test1 ORDER BY k0;"
    qt_hour_from_unixtime2 "SELECT HOUR(FROM_UNIXTIME(114514, 'yyyy-MM-dd'));"
    qt_hour_from_unixtime3 "SELECT HOUR(FROM_UNIXTIME(114514, 'yyyy-MM-dd HH:mm:ss'));"
    qt_hour_from_unixtime4 "SELECT HOUR(FROM_UNIXTIME(1145.14));"
    qt_hour_from_unixtime5 "SELECT HOUR(FROM_UNIXTIME(32536771200));"
    sql "set time_zone = '-08:00'"
    qt_hour_from_unixtime6 "SELECT HOUR(FROM_UNIXTIME(3));"
    test {
        sql """ SELECT HOUR(FROM_UNIXTIME(-1)); """
        exception "The input value of TimeFiled(from_unixtime()) must between 0 and 253402243199"
    }
    explain {
        sql """ SELECT HOUR(FROM_UNIXTIME(k0)) FROM test1; """
        contains "hour_from_unixtime"
    }
    // should only applicable to the `from_unixtime` function with a single parameter.
    explain {
        sql """SELECT HOUR(FROM_UNIXTIME(k0, 'yyyy-MM-dd HH:mm:ss')) FROM test1;"""
        notContains "hour_from_unixtime"
    }
    testFoldConst("SELECT HOUR_FROM_UNIXTIME(1145.14);")
    testFoldConst("SELECT HOUR_FROM_UNIXTIME(NULL);")
    testFoldConst("SELECT HOUR_FROM_UNIXTIME(32536771200);")
    sql "set time_zone = '+00:00'"

    // MINUTE(FROM_UNIXTIME(ts)) -> minute_from_unixtime(ts)
    qt_minute_from_unixtime1 "SELECT MINUTE(FROM_UNIXTIME(k0)) FROM test1 ORDER BY k0;"
    qt_minute_from_unixtime2 "SELECT MINUTE(FROM_UNIXTIME(114514, 'yyyy-MM-dd'));"
    qt_minute_from_unixtime3 "SELECT MINUTE(FROM_UNIXTIME(114514, 'yyyy-MM-dd HH:mm:ss'));"
    qt_minute_from_unixtime4 "SELECT MINUTE(FROM_UNIXTIME(1145.14));"
    qt_minute_from_unixtime5 "SELECT MINUTE(FROM_UNIXTIME(32536771200));"
    test {
        sql """ SELECT MINUTE(FROM_UNIXTIME(-1)); """
        exception "The input value of TimeFiled(from_unixtime()) must between 0 and 253402243199"
    }
    explain {
        sql """ SELECT MINUTE(FROM_UNIXTIME(k0)) FROM test1; """
        contains "minute_from_unixtime"
    }
    explain {
        sql """SELECT MINUTE(FROM_UNIXTIME(k0, 'yyyy-MM-dd HH:mm:ss')) FROM test1;"""
        notContains "minute_from_unixtime"
    }
    testFoldConst("SELECT MINUTE_FROM_UNIXTIME(1145.14);")
    testFoldConst("SELECT MINUTE_FROM_UNIXTIME(NULL);")
    testFoldConst("SELECT MINUTE_FROM_UNIXTIME(32536771200);")

    // SECOND(FROM_UNIXTIME(ts)) -> second_from_unixtime(ts)
    qt_second_from_unixtime1 "SELECT SECOND(FROM_UNIXTIME(k0)) FROM test1 ORDER BY k0;"
    qt_second_from_unixtime2 "SELECT SECOND(FROM_UNIXTIME(114514, 'yyyy-MM-dd'));"
    qt_second_from_unixtime3 "SELECT SECOND(FROM_UNIXTIME(114514, 'yyyy-MM-dd HH:mm:ss'));"
    qt_second_from_unixtime4 "SELECT SECOND(FROM_UNIXTIME(1145.14));"
    qt_second_from_unixtime5 "SELECT SECOND(FROM_UNIXTIME(32536771200));"
    test {
        sql """ SELECT SECOND(FROM_UNIXTIME(-1)); """
        exception "The input value of TimeFiled(from_unixtime()) must between 0 and 253402243199"
    }
    explain {
        sql """ SELECT SECOND(FROM_UNIXTIME(k0)) FROM test1; """
        contains "second_from_unixtime"
    }
    explain {
        sql """SELECT SECOND(FROM_UNIXTIME(k0, 'yyyy-MM-dd HH:mm:ss')) FROM test1;"""
        notContains "second_from_unixtime"
    }
    testFoldConst("SELECT SECOND_FROM_UNIXTIME(1145.14);")
    testFoldConst("SELECT SECOND_FROM_UNIXTIME(NULL);")
    testFoldConst("SELECT SECOND_FROM_UNIXTIME(32536771200);")

    // MICROSECOND(FROM_UNIXTIME(ts)) -> microsecond_from_unixtime(ts)
    qt_microsecond_from_unixtime1 "SELECT MICROSECOND(FROM_UNIXTIME(k1)) FROM test1 ORDER BY k1;"
    qt_microsecond_from_unixtime2 "SELECT MICROSECOND(FROM_UNIXTIME(114514.123456, 'yyyy-MM-dd'));"
    qt_microsecond_from_unixtime3 "SELECT MICROSECOND(FROM_UNIXTIME(114514.123456, '%Y-%m-%d %H:%i:%s.%f'));"
    qt_microsecond_from_unixtime4 "SELECT MICROSECOND(FROM_UNIXTIME(1145.14));"
    qt_microsecond_from_unixtime5 "SELECT MICROSECOND(FROM_UNIXTIME(32536771200));"
    test {
        sql """ SELECT MICROSECOND(FROM_UNIXTIME(-1)); """
        exception "The input value of TimeFiled(from_unixtime()) must between 0 and 253402243199"
    }
    explain {
        sql """ SELECT MICROSECOND(FROM_UNIXTIME(k1)) FROM test1; """
        contains "microsecond_from_unixtime"
    }
    explain {
        sql """SELECT MICROSECOND(FROM_UNIXTIME(k1, 'yyyy-MM-dd HH:mm:ss')) FROM test1;"""
        notContains "microsecond_from_unixtime"
    }
    testFoldConst("SELECT MICROSECOND_FROM_UNIXTIME(1145.14);")
    testFoldConst("SELECT MICROSECOND_FROM_UNIXTIME(NULL);")
    testFoldConst("SELECT MICROSECOND_FROM_UNIXTIME(32536771200);")
    sql "set debug_skip_fold_constant = false;"
}