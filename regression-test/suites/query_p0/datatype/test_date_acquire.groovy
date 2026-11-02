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

suite("test_date_acquire") {
    sql 'set enable_fold_nondeterministic_fn=true'

    String res1 = sql 'explain select now(), now(3), curdate(), current_date(), current_timestamp(), current_timestamp(3)'
    res1 = res1.split('VUNION')[1]
    assertFalse(res1.contains("()") || res1.contains("(3)"))

    sql "set enable_fold_constant_by_be=true"

    sql "set time_zone='+08:00'"
    qt_sql "select from_unixtime(1553152255), unix_timestamp('2007-11-30 10:30%3A19', '%Y-%m-%d %H:%i%%3A%s')"

    sql "set time_zone='+00:00'"
    qt_sql "select from_unixtime(1553152255), unix_timestamp('2007-11-30 10:30%3A19', '%Y-%m-%d %H:%i%%3A%s')"

    sql "set time_zone='+04:00'"
    qt_sql "select from_unixtime(1553152255), unix_timestamp('2007-11-30 10:30%3A19', '%Y-%m-%d %H:%i%%3A%s')"

    sql "DROP TABLE IF EXISTS utc_function_folding"
    sql """
        CREATE TABLE utc_function_folding (
            ts DATETIME NOT NULL,
            v INT
        )
        DUPLICATE KEY(ts)
        PARTITION BY RANGE(ts) (
            PARTITION p_before_1970 VALUES LESS THAN ('1970-01-01'),
            PARTITION p_before_2000 VALUES LESS THAN ('2000-01-01'),
            PARTITION p_current VALUES LESS THAN MAXVALUE
        )
        DISTRIBUTED BY HASH(v) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO utc_function_folding VALUES
        ('1960-01-01', 1), ('1990-01-01', 2), ('9999-01-01', 3)
    """

    def utcValues = """
        SELECT UTC_DATE() = CAST(UTC_TIMESTAMP() AS DATE),
               UTC_TIME() = CAST(UTC_TIMESTAMP() AS TIME),
               UTC_TIME(0) = CAST(UTC_TIMESTAMP(0) AS TIME(0)),
               UTC_TIME(1) = CAST(UTC_TIMESTAMP(1) AS TIME(1)),
               UTC_TIME(2) = CAST(UTC_TIMESTAMP(2) AS TIME(2)),
               UTC_TIME(3) = CAST(UTC_TIMESTAMP(3) AS TIME(3)),
               UTC_TIME(4) = CAST(UTC_TIMESTAMP(4) AS TIME(4)),
               UTC_TIME(5) = CAST(UTC_TIMESTAMP(5) AS TIME(5)),
               UTC_TIME(6) = CAST(UTC_TIMESTAMP(6) AS TIME(6))
    """
    def today = "SELECT v FROM utc_function_folding WHERE ts >= UTC_DATE() ORDER BY v"
    def yesterday = """SELECT v FROM utc_function_folding
        WHERE ts >= UTC_DATE() - INTERVAL 1 DAY ORDER BY v"""
    def previous = sql "SELECT @@time_zone, @@enable_fold_constant_by_be, @@debug_skip_fold_constant"
    try {
        for (def foldByBe : [false, true]) {
            sql "SET enable_fold_constant_by_be=${foldByBe}"
            for (def timeZone : ["+00:00", "+08:00", "-08:00"]) {
                sql "SET time_zone='${timeZone}'"
                sql "SET debug_skip_fold_constant=false"
                explain {
                    sql today
                    contains "partitions=1/3 (p_current)"
                    notContains "utc_date()"
                }
                explain {
                    sql yesterday
                    contains "partitions=1/3 (p_current)"
                    notContains "utc_date()"
                }
                explain {
                    sql "SELECT UTC_DATE(), UTC_TIME(), UTC_TIME(0), UTC_TIME(1), UTC_TIME(2), " +
                            "UTC_TIME(3), UTC_TIME(4), UTC_TIME(5), UTC_TIME(6)"
                    notContains "utc_date("
                    notContains "utc_time("
                }
                qt_utc_today today
                qt_utc_yesterday yesterday
                qt_utc_values utcValues
                testFoldConst(today)
                testFoldConst(yesterday)
                testFoldConst(utcValues)
            }
        }
    } finally {
        sql "SET time_zone='${previous[0][0]}'"
        sql "SET enable_fold_constant_by_be=${previous[0][1]}"
        sql "SET debug_skip_fold_constant=${previous[0][2]}"
        sql "DROP TABLE IF EXISTS utc_function_folding"
    }
}
