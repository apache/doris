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

suite("test_timestamptz_fe_output_boundary") {
    def originalZone = sql("select @@time_zone")[0][0]
    def originalStrict = sql("select @@enable_strict_cast")[0][0]
    def originalFold = sql("select @@enable_fold_constant_by_be")[0][0]
    try {
        for (def strict : [false, true]) {
            sql "set enable_strict_cast = ${strict}"
            for (def fold : [false, true]) {
                sql "set enable_fold_constant_by_be = ${fold}"
                for (def scale : [0, 3, 6]) {
                    def fraction = scale == 0 ? "" : "." + "9" * scale
                    for (def entry : [["+08:00", "9999-12-31 23:59:59${fraction}+00:00"],
                                      ["-08:00", "0000-01-01 00:00:00+00:00"]]) {
                        sql "set time_zone = '${entry[0]}'"
                        def timestamp = "cast('${entry[1]}' as timestamptz(${scale}))"
                        for (def value : [timestamp, "array(${timestamp}, null)",
                                          "map(1, ${timestamp})", "named_struct('ts', ${timestamp})"]) {
                            // Constant and materialized values must obey the same local display range.
                            test {
                                sql "select ${value}"
                                exception "TIMESTAMPTZ local year is outside [0, 9999]"
                            }
                            test {
                                sql "select ${value} from numbers('number' = '1')"
                                exception "TIMESTAMPTZ local year is outside [0, 9999]"
                            }
                        }
                    }
                }
            }
        }

        sql "set time_zone = '-08:00'"
        sql "drop table if exists test_timestamptz_negative_default"
        test {
            sql """create table test_timestamptz_negative_default (
                id int, ts timestamptz(6) default '0000-01-01 00:00:00+08:00')
                distributed by hash(id) buckets 1 properties ("replication_num" = "1")"""
            exception "Datetime value is out of range"
        }
        sql "drop table if exists test_datetime_negative_default"
        test {
            sql """create table test_datetime_negative_default (
                id int, ts datetime(6) default '0000-01-01 00:00:00+00:00')
                distributed by hash(id) buckets 1 properties ("replication_num" = "1")"""
            exception "Datetime value is out of range"
        }

        sql "set time_zone = '+08:00'"
        sql "drop table if exists test_timestamptz_fe_boundary_ctas"
        sql """
            create table test_timestamptz_fe_boundary_ctas
            properties ("replication_num" = "1") as
            select 1 as id,
                   cast('0000-01-01 00:00:00+00:00' as timestamptz(6)) as min_ts,
                   cast('9999-12-31 23:59:59.999999+00:00' as timestamptz(6)) as max_ts
        """
        // A valid UTC instant remains writable even when its session-local display overflows.
        sql """insert into test_timestamptz_fe_boundary_ctas values
               (2, '0000-01-01 00:00:00+00:00', '9999-12-31 23:59:59.999999+00:00')"""
        qt_ctas_count "select count(*) from test_timestamptz_fe_boundary_ctas"
        test {
            sql "select max_ts from test_timestamptz_fe_boundary_ctas"
            exception "TIMESTAMPTZ local year is outside [0, 9999]"
        }
        sql "set time_zone = '-08:00'"
        test {
            sql "select min_ts from test_timestamptz_fe_boundary_ctas"
            exception "TIMESTAMPTZ local year is outside [0, 9999]"
        }
        sql "set time_zone = 'UTC'"
        qt_ctas_utc "select * from test_timestamptz_fe_boundary_ctas order by id"

        sql "set time_zone = '+08:00'"
        qt_masked_null """select if(false,
            cast('9999-12-31 23:59:59.999999+00:00' as timestamptz(6)), null)"""
        qt_local_upper_edge """select cast('9999-12-31 15:59:59.999999+00:00' as timestamptz(6))"""
        sql "set time_zone = '-08:00'"
        qt_local_lower_edge """select cast('0000-01-01 08:00:00+00:00' as timestamptz(6))"""

        sql "set time_zone = 'Asia/Shanghai'"
        qt_historical_constant """select cast('1900-01-01 00:00:00.123456+00:00' as timestamptz(6))"""
        qt_historical_materialized """select cast('1900-01-01 00:00:00.123456+00:00' as timestamptz(6))
            from numbers('number' = '1')"""
        qt_historical_array """select array(cast('1900-01-01 00:00:00.123456+00:00' as timestamptz(6)), null)"""
        qt_historical_struct """select named_struct('ts',
            cast('1900-01-01 00:00:00.123456+00:00' as timestamptz(6)))"""
        sql "set time_zone = 'America/New_York'"
        qt_historical_negative_constant """select cast('1880-01-01 00:00:00.123456+00:00' as timestamptz(6))"""
        qt_historical_negative_materialized """select cast('1880-01-01 00:00:00.123456+00:00' as timestamptz(6))
            from numbers('number' = '1')"""
    } finally {
        sql "set time_zone = '${originalZone}'"
        sql "set enable_strict_cast = ${originalStrict}"
        sql "set enable_fold_constant_by_be = ${originalFold}"
    }
}
