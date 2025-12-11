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


suite("test_cast_timestamptz") {
    // Test casting from integer types to boolean
    sql "set debug_skip_fold_constant=true;"

    sql "set enable_strict_cast=false;"

    sql " set time_zone = '+07:00'; "

    qt_cast_str_to_timetz_standard """
    SELECT 
        cast('2020-01-01 00:00:00 +03:00' as TIMESTAMPTZ) as ts_utc,
        cast('2020-06-01 12:34:56 +03:00' as TIMESTAMPTZ) as ts_summer,
        cast('2020-12-31 23:59:59 +03:00' as TIMESTAMPTZ) as ts_winter,
        cast('2020-01-01 00:00:00 -03:00' as TIMESTAMPTZ) as ts_neg_utc,
        cast('2020-06-01 12:34:56 -03:00' as TIMESTAMPTZ) as ts_neg_summer,
        cast('2020-12-31 23:59:59 -03:00' as TIMESTAMPTZ) as ts_neg_winter,
        cast('2020-01-01 00:00:00 +00:00' as TIMESTAMPTZ) as ts_zero,
        cast('2020-06-01 12:34:56 +00:00' as TIMESTAMPTZ) as ts_zero_summer,
        cast('2020-12-31 23:59:59 +00:00' as TIMESTAMPTZ) as ts_zero_winter;
    """

    qt_cast_str_to_timetz_no_tz_out_of_range0 """
    SELECT 
        cast('0000-01-01' as TIMESTAMPTZ),
        cast('0000-01-01 00:00:00' as TIMESTAMPTZ),
        cast('0000-01-01 00:00:00.123456' as TIMESTAMPTZ),
        cast('0000-01-01 00:00:00.123456' as TIMESTAMPTZ(6));
    """
    qt_cast_str_to_timetz_no_tz_out_of_range0 """
    SELECT 
        cast('0000-01-01 06:59:59' as TIMESTAMPTZ),
        cast('0000-01-01 06:59:59.123456' as TIMESTAMPTZ),
        cast('0000-01-01 06:59:59.123456' as TIMESTAMPTZ(6));
    """

    qt_cast_str_to_timetz_no_tz """
    SELECT 
        cast('2020-01-01 00:00:00' as TIMESTAMPTZ) as ts_no_tz,
        cast('2020-06-01 12:34:56' as TIMESTAMPTZ) as ts_no_tz_summer,
        cast('2020-12-31 23:59:59' as TIMESTAMPTZ) as ts_no_tz_winter;
    """

    qt_cast_str_to_timetz_invalid """
    SELECT 
        cast('2020-13-01 00:00:00 +03:00' as TIMESTAMPTZ) as ts_invalid_month,
        cast('2020-001-01 00:00:00 +03:00' as TIMESTAMPTZ) as ts_invalid_day,
        cast('2020-01-01 24:00:00 +03:00' as TIMESTAMPTZ) as ts_invalid_hour,
        cast('2020-01-01 00:60:00 +03:00' as TIMESTAMPTZ) as ts_invalid_minute,
        cast('2020-01-01 00:00:60 +03:00' as TIMESTAMPTZ) as ts_invalid_second,
        cast('2020-01-01 00:00:00 +15:00' as TIMESTAMPTZ) as ts_invalid_tz_hour,
        cast('2020-01-01 00:00:00 +03:60' as TIMESTAMPTZ) as ts_invalid_tz_minute,
        cast('invalid-string' as TIMESTAMPTZ) as ts_invalid_string;
    """


    qt_sql """
        select cast(cast("2020-01-01 00:00:00.1236" as datetime(4)) as timestamptz(3));
    """

    qt_sql """
       select cast(cast("2020-01-01 00:00:00.1236 +03:00" as timestamptz(4)) as datetime(3));
    """

    sql "set enable_strict_cast=true;"
    
 

    
    qt_cast_str_to_timetz_standard """
    SELECT 
        cast('2020-01-01 00:00:00 +03:00' as TIMESTAMPTZ) as ts_utc,
        cast('2020-06-01 12:34:56 +03:00' as TIMESTAMPTZ) as ts_summer,
        cast('2020-12-31 23:59:59 +03:00' as TIMESTAMPTZ) as ts_winter,
        cast('2020-01-01 00:00:00 -03:00' as TIMESTAMPTZ) as ts_neg_utc,
        cast('2020-06-01 12:34:56 -03:00' as TIMESTAMPTZ) as ts_neg_summer,
        cast('2020-12-31 23:59:59 -03:00' as TIMESTAMPTZ) as ts_neg_winter,
        cast('2020-01-01 00:00:00 +00:00' as TIMESTAMPTZ) as ts_zero,
        cast('2020-06-01 12:34:56 +00:00' as TIMESTAMPTZ) as ts_zero_summer,
        cast('2020-12-31 23:59:59 +00:00' as TIMESTAMPTZ) as ts_zero_winter;
    """

    qt_cast_str_to_timetz_no_tz """
    SELECT 
        cast('2020-01-01 00:00:00' as TIMESTAMPTZ) as ts_no_tz,
        cast('2020-06-01 12:34:56' as TIMESTAMPTZ) as ts_no_tz_summer,
        cast('2020-12-31 23:59:59' as TIMESTAMPTZ) as ts_no_tz_winter;
    """

    test {
        sql """  SELECT cast('2020-13-01 00:00:00 +03:00' as TIMESTAMPTZ) as ts_invalid_month; """
        exception "parse 2020-13-01 00:00:00 +03:00 to timestamptz "
    }


    // cast to datetime

    sql " set enable_strict_cast=false; "

    qt_cast_timestamptz_to_datetime """
    SELECT 
        cast(cast('2020-01-01 00:00:00 +03:00' as TIMESTAMPTZ) as datetime ) as dt_utc,
        cast(cast('2020-06-01 12:34:56 +03:00' as TIMESTAMPTZ) as datetime ) as dt_summer,
        cast(cast('2020-12-31 23:59:59 +03:00' as TIMESTAMPTZ) as datetime ) as dt_winter,
        cast(cast('2020-01-01 00:00:00 -03:00' as TIMESTAMPTZ) as datetime ) as dt_neg_utc,
        cast(cast('2020-06-01 12:34:56 -03:00' as TIMESTAMPTZ) as datetime ) as dt_neg_summer,
        cast(cast('2020-12-31 23:59:59 -03:00' as TIMESTAMPTZ) as datetime ) as dt_neg_winter,
        cast(cast('2020-01-01 00:00:00 +00:00' as TIMESTAMPTZ) as datetime ) as dt_zero,
        cast(cast('2020-06-01 12:34:56 +00:00' as TIMESTAMPTZ) as datetime ) as dt_zero_summer,
        cast(cast('2020-12-31 23:59:59 +00:00' as TIMESTAMPTZ) as datetime ) as dt_zero_winter;
    """

    sql " set enable_strict_cast=true; "

    qt_cast_timestamptz_to_datetime """
    SELECT 
        cast(cast('2020-01-01 00:00:00 +03:00' as TIMESTAMPTZ) as datetime ) as dt_utc,
        cast(cast('2020-06-01 12:34:56 +03:00' as TIMESTAMPTZ) as datetime ) as dt_summer,
        cast(cast('2020-12-31 23:59:59 +03:00' as TIMESTAMPTZ) as datetime ) as dt_winter,
        cast(cast('2020-01-01 00:00:00 -03:00' as TIMESTAMPTZ) as datetime ) as dt_neg_utc,
        cast(cast('2020-06-01 12:34:56 -03:00' as TIMESTAMPTZ) as datetime ) as dt_neg_summer,
        cast(cast('2020-12-31 23:59:59 -03:00' as TIMESTAMPTZ) as datetime ) as dt_neg_winter,
        cast(cast('2020-01-01 00:00:00 +00:00' as TIMESTAMPTZ) as datetime ) as dt_zero,
        cast(cast('2020-06-01 12:34:56 +00:00' as TIMESTAMPTZ) as datetime ) as dt_zero_summer,
        cast(cast('2020-12-31 23:59:59 +00:00' as TIMESTAMPTZ) as datetime ) as dt_zero_winter;
    """


    qt_sql """
        select cast(cast("2020-01-01 00:00:00.1236" as datetime(4)) as timestamptz(3));
    """

    qt_sql """
       select cast(cast("2020-01-01 00:00:00.1236 +03:00" as timestamptz(4)) as datetime(3));
    """


    // cast datetime to timestamptz


    sql " set enable_strict_cast=false; "

    qt_cast_datetime_to_timestamptz """
    SELECT 
        cast( cast('2020-01-01 00:00:00' as datetime) as timestamptz ) as ts_no_tz,
        cast( cast('2020-06-01 12:34:56' as datetime) as timestamptz ) as ts_no_tz_summer,
        cast( cast('2020-12-31 23:59:59' as datetime) as timestamptz ) as ts_no_tz_winter;
    """ 

    sql " set enable_strict_cast=true; "

    qt_cast_datetime_to_timestamptz """
    SELECT 
        cast( cast('2020-01-01 00:00:00' as datetime) as timestamptz ) as ts_no_tz,
        cast( cast('2020-06-01 12:34:56' as datetime) as timestamptz ) as ts_no_tz_summer,
        cast( cast('2020-12-31 23:59:59' as datetime) as timestamptz ) as ts_no_tz_winter;
    """ 



    // table test

    sql """ drop table if exists test_cast_timestamptzdb; """

    sql """
         CREATE TABLE IF NOT EXISTS test_cast_timestamptzdb (
              `id` INT,
              `str` string
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
        );
    """

    sql """ 
    insert into test_cast_timestamptzdb values 
        (1, '2020-01-01 00:00:00 +03:00') ,
        (2, '2020-06-01 12:34:56 +03:00') ,
        (3, '2020-12-31 23:59:59 +03:00') ,
        (4, '2020-01-01 00:00:00 -03:00') ,
        (5, '2020-06-01 12:34:56 -03:00') ,
        (6, '2020-12-31 23:59:59 -03:00') ,
        (7, '2020-01-01 00:00:00 +00:00') ,
        (8, '2020-06-01 12:34:56 +00:00') ;
    """


    qt_table_cast_str_to_timestamptz """
    select id, str, cast(str as timestamptz) as ts from test_cast_timestamptzdb order by id;
    """


    qt_sql """
        select cast("2020-01-01 00:00:00.123456 +03:00" as timestamptz(4));
    """

    sql """
        set time_zone = default;
    """
}