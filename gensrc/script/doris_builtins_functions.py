#!/usr/bin/env python
# encoding: utf-8

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# This is a list of all the functions that are not auto-generated.
# It contains all the meta data that describes the function.

# The format is:
#   [sql aliases], <return_type>, [<args>], <backend symbol>,
# With an optional
#   <prepare symbol>, <close symbol>, <vec>, <nullable mode>
#
# 'sql aliases' are the function names that can be used from sql. There must be at least
# one per function.
#
# 'vec' means that whether the vec engine support this function. There are two values of
# this params: 'vec', ''. If the value is 'vec', it means both normal and vec engine support
# this function. If the value is '', it means only normal engine support this function.
#
# 'nullable mode' reflects whether the return value of the function is null. See @Function.NullableMode
# for the specific mode and meaning.
#
# The symbol can be empty for functions that are not yet implemented or are special-cased
# in Expr::CreateExpr() (i.e., functions that are implemented via a custom Expr class
# rather than a single function).
visible_functions = [
    # Bit and Byte functions
    # For functions corresponding to builtin operators, we can reuse the implementations
    [['bitand'], 'TINYINT', ['TINYINT', 'TINYINT'],
        '_ZN5doris9Operators32bitand_tiny_int_val_tiny_int_valEPN9doris_udf'
        '15FunctionContextERKNS1_10TinyIntValES6_', '', '', '', ''],
    [['bitand'], 'SMALLINT', ['SMALLINT', 'SMALLINT'],
        '_ZN5doris9Operators34bitand_small_int_val_small_int_valEPN9doris_udf'
        '15FunctionContextERKNS1_11SmallIntValES6_', '', '', '', ''],
    [['bitand'], 'INT', ['INT', 'INT'],
        '_ZN5doris9Operators22bitand_int_val_int_valEPN9doris_udf'
        '15FunctionContextERKNS1_6IntValES6_', '', '', '', ''],
    [['bitand'], 'BIGINT', ['BIGINT', 'BIGINT'],
        '_ZN5doris9Operators30bitand_big_int_val_big_int_valEPN9doris_udf'
        '15FunctionContextERKNS1_9BigIntValES6_', '', '', '', ''],
    [['bitand'], 'LARGEINT', ['LARGEINT', 'LARGEINT'],
        '_ZN5doris9Operators34bitand_large_int_val_large_int_valEPN9doris_udf'
        '15FunctionContextERKNS1_11LargeIntValES6_', '', '', '', ''],

    [['bitor'], 'TINYINT', ['TINYINT', 'TINYINT'],
        '_ZN5doris9Operators31bitor_tiny_int_val_tiny_int_valEPN9doris_udf'
        '15FunctionContextERKNS1_10TinyIntValES6_', '', '', '', ''],
    [['bitor'], 'SMALLINT', ['SMALLINT', 'SMALLINT'],
        '_ZN5doris9Operators33bitor_small_int_val_small_int_valEPN9doris_udf'
        '15FunctionContextERKNS1_11SmallIntValES6_', '', '', '', ''],
    [['bitor'], 'INT', ['INT', 'INT'],
        '_ZN5doris9Operators21bitor_int_val_int_valEPN9doris_udf'
        '15FunctionContextERKNS1_6IntValES6_', '', '', '', ''],
    [['bitor'], 'BIGINT', ['BIGINT', 'BIGINT'],
        '_ZN5doris9Operators29bitor_big_int_val_big_int_valEPN9doris_udf'
        '15FunctionContextERKNS1_9BigIntValES6_', '', '', '', ''],
    [['bitor'], 'LARGEINT', ['LARGEINT', 'LARGEINT'],
        '_ZN5doris9Operators33bitor_large_int_val_large_int_valEPN9doris_udf'
        '15FunctionContextERKNS1_11LargeIntValES6_', '', '', '', ''],

    [['bitxor'], 'TINYINT', ['TINYINT', 'TINYINT'],
        '_ZN5doris9Operators32bitxor_tiny_int_val_tiny_int_valEPN9doris_udf'
        '15FunctionContextERKNS1_10TinyIntValES6_', '', '', '', ''],
    [['bitxor'], 'SMALLINT', ['SMALLINT', 'SMALLINT'],
        '_ZN5doris9Operators34bitxor_small_int_val_small_int_valEPN9doris_udf'
        '15FunctionContextERKNS1_11SmallIntValES6_', '', '', '', ''],
    [['bitxor'], 'INT', ['INT', 'INT'],
        '_ZN5doris9Operators22bitxor_int_val_int_valEPN9doris_udf'
        '15FunctionContextERKNS1_6IntValES6_', '', '', '', ''],
    [['bitxor'], 'BIGINT', ['BIGINT', 'BIGINT'],
        '_ZN5doris9Operators30bitxor_big_int_val_big_int_valEPN9doris_udf'
        '15FunctionContextERKNS1_9BigIntValES6_', '', '', '', ''],
    [['bitxor'], 'LARGEINT', ['LARGEINT', 'LARGEINT'],
        '_ZN5doris9Operators34bitxor_large_int_val_large_int_valEPN9doris_udf'
        '15FunctionContextERKNS1_11LargeIntValES6_', '', '', '', ''],

    [['bitnot'], 'TINYINT', ['TINYINT'],
        '_ZN5doris9Operators19bitnot_tiny_int_valEPN9doris_udf'
        '15FunctionContextERKNS1_10TinyIntValE', '', '', '', ''],
    [['bitnot'], 'SMALLINT', ['SMALLINT'],
        '_ZN5doris9Operators20bitnot_small_int_valEPN9doris_udf'
        '15FunctionContextERKNS1_11SmallIntValE', '', '', '', ''],
    [['bitnot'], 'INT', ['INT'],
        '_ZN5doris9Operators14bitnot_int_valEPN9doris_udf'
        '15FunctionContextERKNS1_6IntValE', '', '', '', ''],
    [['bitnot'], 'BIGINT', ['BIGINT'],
        '_ZN5doris9Operators18bitnot_big_int_valEPN9doris_udf'
        '15FunctionContextERKNS1_9BigIntValE', '', '', '', ''],
    [['bitnot'], 'LARGEINT', ['LARGEINT'],
        '_ZN5doris9Operators20bitnot_large_int_valEPN9doris_udf'
        '15FunctionContextERKNS1_11LargeIntValE', '', '', '', ''],

    # array functions
    [['array'], 'ARRAY', ['INT', '...'],
        '_ZN5doris14ArrayFunctions5arrayEPN9doris_udf15FunctionContextEiPKNS1_6IntValE', '', '', '', ''],
    [['array'], 'ARRAY', ['VARCHAR', '...'],
        '_ZN5doris14ArrayFunctions5arrayEPN9doris_udf15FunctionContextEiPKNS1_9StringValE', '', '', '', ''],
    [['array'], 'ARRAY', ['ARRAY', '...'], '', '', '', '', ''],
    [['array'], 'ARRAY', ['MAP', '...'], '', '', '', '', ''],
    [['array'], 'ARRAY', ['STRUCT', '...'], '', '', '', '', ''],
    [['%element_extract%'], 'VARCHAR', ['ARRAY', 'INT'],  '', '', '', '', ''],
    [['%element_extract%'], 'VARCHAR', ['ARRAY', 'VARCHAR'],  '', '', '', '', ''],
    [['%element_extract%'], 'VARCHAR', ['MAP', 'VARCHAR'],  '', '', '', '', ''],
    [['%element_extract%'], 'VARCHAR', ['MAP', 'INT'],  '', '', '', '', ''],
    [['%element_extract%'], 'VARCHAR', ['STRUCT', 'INT'],  '', '', '', '', ''],
    [['%element_extract%'], 'VARCHAR', ['STRUCT', 'VARCHAR'],  '', '', '', '', ''],

    # Timestamp functions
    [['unix_timestamp'], 'INT', [],
        '_ZN5doris18TimestampFunctions7to_unixEPN9doris_udf15FunctionContextE',
        '', '', '', 'ALWAYS_NOT_NULLABLE'],
    [['unix_timestamp'], 'INT', ['DATETIME'],
        '_ZN5doris18TimestampFunctions7to_unixEPN9doris_udf15FunctionContextERKNS1_11DateTimeValE',
        '', '', 'vec', ''],
    [['unix_timestamp'], 'INT', ['DATE'],
        '_ZN5doris18TimestampFunctions7to_unixEPN9doris_udf15FunctionContextERKNS1_11DateTimeValE',
        '', '', 'vec', ''],
    [['unix_timestamp'], 'INT', ['VARCHAR', 'VARCHAR'],
        '_ZN5doris18TimestampFunctions7to_unixEPN9doris_udf15FunctionContextERKNS1_9StringValES6_',
        '', '', '', 'ALWAYS_NULLABLE'],
    [['from_unixtime'], 'VARCHAR', ['INT'],
        '_ZN5doris18TimestampFunctions9from_unixEPN9doris_udf15FunctionContextERKNS1_6IntValE',
        '', '', 'vec', ''],
    [['from_unixtime'], 'VARCHAR', ['INT', 'VARCHAR'],
        '_ZN5doris18TimestampFunctions9from_unixEPN9doris_udf'
        '15FunctionContextERKNS1_6IntValERKNS1_9StringValE',
        '_ZN5doris18TimestampFunctions14format_prepareEPN9doris_udf'
        '15FunctionContextENS2_18FunctionStateScopeE',
        '_ZN5doris18TimestampFunctions12format_closeEPN9doris_udf'
        '15FunctionContextENS2_18FunctionStateScopeE', 'vec', ''],
    [['now', 'current_timestamp', 'localtime', 'localtimestamp'], 'DATETIME', [],
        '_ZN5doris18TimestampFunctions3nowEPN9doris_udf15FunctionContextE',
        '', '', '', 'ALWAYS_NOT_NULLABLE'],
    [['curtime', 'current_time'], 'TIME', [],
        '_ZN5doris18TimestampFunctions7curtimeEPN9doris_udf15FunctionContextE',
        '', '', '', 'ALWAYS_NOT_NULLABLE'],
    [['curdate', 'current_date'], 'DATE', [],
        '_ZN5doris18TimestampFunctions7curdateEPN9doris_udf15FunctionContextE',
        '', '', '', 'ALWAYS_NOT_NULLABLE'],
    [['utc_timestamp'], 'DATETIME', [],
        '_ZN5doris18TimestampFunctions13utc_timestampEPN9doris_udf15FunctionContextE',
        '', '', '', 'ALWAYS_NOT_NULLABLE'],
    [['timestamp'], 'DATETIME', ['DATETIME'],
        '_ZN5doris18TimestampFunctions9timestampEPN9doris_udf15FunctionContextERKNS1_11DateTimeValE',
        '', '', 'vec', ''],

    [['from_days'], 'DATE', ['INT'],
        '_ZN5doris18TimestampFunctions9from_daysEPN9doris_udf15FunctionContextERKNS1_6IntValE',
        '', '', '', ''],
    [['to_days'], 'INT', ['DATE'],
        '_ZN5doris18TimestampFunctions7to_daysEPN9doris_udf15FunctionContextERKNS1_11DateTimeValE',
        '', '', 'vec', ''],

    [['year'], 'INT', ['DATETIME'],
        '_ZN5doris18TimestampFunctions4yearEPN9doris_udf15FunctionContextERKNS1_11DateTimeValE',
        '', '', 'vec', ''],
    [['month'], 'INT', ['DATETIME'],
        '_ZN5doris18TimestampFunctions5monthEPN9doris_udf15FunctionContextERKNS1_11DateTimeValE',
        '', '', 'vec', ''],
    [['quarter'], 'INT', ['DATETIME'],
        '_ZN5doris18TimestampFunctions7quarterEPN9doris_udf15FunctionContextERKNS1_11DateTimeValE',
        '', '', 'vec', ''],
    [['dayofweek'], 'INT', ['DATETIME'],
        '_ZN5doris18TimestampFunctions11day_of_weekEPN9doris_udf15FunctionContextERKNS1_11DateTimeValE',
        '', '', 'vec', ''],
    [['day', 'dayofmonth'], 'INT', ['DATETIME'],
        '_ZN5doris18TimestampFunctions12day_of_monthEPN9doris_udf'
        '15FunctionContextERKNS1_11DateTimeValE', '', '', 'vec', ''],
    [['dayofyear'], 'INT', ['DATETIME'],
        '_ZN5doris18TimestampFunctions11day_of_yearEPN9doris_udf'
        '15FunctionContextERKNS1_11DateTimeValE', '', '', 'vec', ''],
    [['weekofyear'], 'INT', ['DATETIME'],
        '_ZN5doris18TimestampFunctions12week_of_yearEPN9doris_udf'
        '15FunctionContextERKNS1_11DateTimeValE', '', '', 'vec', ''],
    [['yearweek'], 'INT', ['DATETIME'],
        '_ZN5doris18TimestampFunctions9year_weekEPN9doris_udf15FunctionContextERKNS1_11DateTimeValE',
        '', '', '', ''],
    [['yearweek'], 'INT', ['DATETIME', 'INT'],
        '_ZN5doris18TimestampFunctions9year_weekEPN9doris_udf15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE',
        '', '', '', ''],
    [['week'], 'INT', ['DATETIME'],
        '_ZN5doris18TimestampFunctions4weekEPN9doris_udf15FunctionContextERKNS1_11DateTimeValE',
        '', '', '', ''],
    [['week'], 'INT', ['DATETIME', 'INT'],
        '_ZN5doris18TimestampFunctions4weekEPN9doris_udf15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE',
        '', '', '', ''],
    [['hour'], 'INT', ['DATETIME'],
        '_ZN5doris18TimestampFunctions4hourEPN9doris_udf15FunctionContextERKNS1_11DateTimeValE',
        '', '', 'vec', ''],
    [['minute'], 'INT', ['DATETIME'],
        '_ZN5doris18TimestampFunctions6minuteEPN9doris_udf15FunctionContextERKNS1_11DateTimeValE',
        '', '', 'vec', ''],
    [['second'], 'INT', ['DATETIME'],
        '_ZN5doris18TimestampFunctions6secondEPN9doris_udf15FunctionContextERKNS1_11DateTimeValE',
        '', '', 'vec', ''],

    [['makedate'], 'DATETIME', ['INT', 'INT'],
        '_ZN5doris18TimestampFunctions9make_dateEPN9doris_udf15FunctionContextERKNS1_6IntValES6_',
        '', '', '', ''],
    [['years_add'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN5doris18TimestampFunctions9years_addEPN9doris_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE',
        '', '', 'vec', ''],
    [['years_sub'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN5doris18TimestampFunctions9years_subEPN9doris_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE',
        '', '', 'vec', ''],
    [['months_add', 'add_months'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN5doris18TimestampFunctions10months_addEPN9doris_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE',
        '', '', 'vec', ''],
    [['months_sub'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN5doris18TimestampFunctions10months_subEPN9doris_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE',
        '', '', 'vec', ''],
    [['weeks_add'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN5doris18TimestampFunctions9weeks_addEPN9doris_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE',
        '', '', 'vec', ''],
    [['weeks_sub'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN5doris18TimestampFunctions9weeks_subEPN9doris_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE',
        '', '', 'vec', ''],
    [['days_add', 'date_add', 'adddate'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN5doris18TimestampFunctions8days_addEPN9doris_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE',
        '', '', 'vec', ''],
    [['days_sub', 'date_sub', 'subdate'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN5doris18TimestampFunctions8days_subEPN9doris_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE',
        '', '', 'vec', ''],
    [['hours_add'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN5doris18TimestampFunctions9hours_addEPN9doris_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE',
        '', '', 'vec', ''],
    [['hours_sub'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN5doris18TimestampFunctions9hours_subEPN9doris_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE',
        '', '', 'vec', ''],
    [['minutes_add'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN5doris18TimestampFunctions11minutes_addEPN9doris_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE',
        '', '', 'vec', ''],
    [['minutes_sub'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN5doris18TimestampFunctions11minutes_subEPN9doris_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE',
        '', '', 'vec', ''],
    [['seconds_add'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN5doris18TimestampFunctions11seconds_addEPN9doris_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE',
        '', '', 'vec', ''],
    [['seconds_sub'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN5doris18TimestampFunctions11seconds_subEPN9doris_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE',
        '', '', 'vec', ''],
    [['microseconds_add'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN5doris18TimestampFunctions10micros_addEPN9doris_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE',
        '', '', '', ''],
    [['microseconds_sub'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN5doris18TimestampFunctions10micros_subEPN9doris_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE',
        '', '', '', ''],

    [['datediff'], 'INT', ['DATETIME', 'DATETIME'],
        '_ZN5doris18TimestampFunctions9date_diffEPN9doris_udf'
        '15FunctionContextERKNS1_11DateTimeValES6_', '', '', 'vec', ''],
    [['timediff'], 'TIME', ['DATETIME', 'DATETIME'],
            '_ZN5doris18TimestampFunctions9time_diffEPN9doris_udf'
            '15FunctionContextERKNS1_11DateTimeValES6_', '', '', 'vec', ''],

    [['str_to_date'], 'DATETIME', ['VARCHAR', 'VARCHAR'],
        '_ZN5doris18TimestampFunctions11str_to_dateEPN9doris_udf'
        '15FunctionContextERKNS1_9StringValES6_', '', '', 'vec', 'ALWAYS_NULLABLE'],
    [['date_format'], 'VARCHAR', ['DATETIME', 'VARCHAR'],
        '_ZN5doris18TimestampFunctions11date_formatEPN9doris_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_9StringValE',
        '_ZN5doris18TimestampFunctions14format_prepareEPN9doris_udf'
        '15FunctionContextENS2_18FunctionStateScopeE',
        '_ZN5doris18TimestampFunctions12format_closeEPN9doris_udf'
        '15FunctionContextENS2_18FunctionStateScopeE', 'vec', ''],
    [['date_format'], 'VARCHAR', ['DATE', 'VARCHAR'],
        '_ZN5doris18TimestampFunctions11date_formatEPN9doris_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_9StringValE',
        '_ZN5doris18TimestampFunctions14format_prepareEPN9doris_udf'
        '15FunctionContextENS2_18FunctionStateScopeE',
        '_ZN5doris18TimestampFunctions12format_closeEPN9doris_udf'
        '15FunctionContextENS2_18FunctionStateScopeE', 'vec', ''],
    [['date', 'to_date'], 'DATE', ['DATETIME'],
        '_ZN5doris18TimestampFunctions7to_dateEPN9doris_udf15FunctionContextERKNS1_11DateTimeValE',
        '', '', 'vec', ''],

    [['dayname'], 'VARCHAR', ['DATETIME'],
        '_ZN5doris18TimestampFunctions8day_nameEPN9doris_udf'
        '15FunctionContextERKNS1_11DateTimeValE', '', '', 'vec', ''],
    [['monthname'], 'VARCHAR', ['DATETIME'],
        '_ZN5doris18TimestampFunctions10month_nameEPN9doris_udf'
        '15FunctionContextERKNS1_11DateTimeValE', '', '', 'vec', ''],

    [['convert_tz'], 'DATETIME', ['DATETIME', 'VARCHAR', 'VARCHAR'],
            '_ZN5doris18TimestampFunctions10convert_tzEPN9doris_udf15FunctionContextERKNS1_11DateTimeValERKNS1_9StringValES9_',
            '_ZN5doris18TimestampFunctions18convert_tz_prepareEPN9doris_udf15FunctionContextENS2_18FunctionStateScopeE',
            '_ZN5doris18TimestampFunctions16convert_tz_closeEPN9doris_udf15FunctionContextENS2_18FunctionStateScopeE',
            'vec', 'ALWAYS_NULLABLE'],

    [['years_diff'], 'BIGINT', ['DATETIME', 'DATETIME'],
            '_ZN5doris18TimestampFunctions10years_diffEPN9doris_udf15FunctionContextERKNS1_11DateTimeValES6_',
            '', '', 'vec', ''],
    [['months_diff'], 'BIGINT', ['DATETIME', 'DATETIME'],
            '_ZN5doris18TimestampFunctions11months_diffEPN9doris_udf15FunctionContextERKNS1_11DateTimeValES6_',
            '', '', 'vec', ''],
    [['weeks_diff'], 'BIGINT', ['DATETIME', 'DATETIME'],
            '_ZN5doris18TimestampFunctions10weeks_diffEPN9doris_udf15FunctionContextERKNS1_11DateTimeValES6_',
            '', '', 'vec', ''],
    [['days_diff'], 'BIGINT', ['DATETIME', 'DATETIME'],
            '_ZN5doris18TimestampFunctions9days_diffEPN9doris_udf15FunctionContextERKNS1_11DateTimeValES6_',
            '', '', 'vec', ''],
    [['hours_diff'], 'BIGINT', ['DATETIME', 'DATETIME'],
            '_ZN5doris18TimestampFunctions10hours_diffEPN9doris_udf15FunctionContextERKNS1_11DateTimeValES6_',
            '', '', 'vec', ''],
    [['minutes_diff'], 'BIGINT', ['DATETIME', 'DATETIME'],
            '_ZN5doris18TimestampFunctions12minutes_diffEPN9doris_udf15FunctionContextERKNS1_11DateTimeValES6_',
            '', '', 'vec', ''],
    [['seconds_diff'], 'BIGINT', ['DATETIME', 'DATETIME'],
            '_ZN5doris18TimestampFunctions12seconds_diffEPN9doris_udf15FunctionContextERKNS1_11DateTimeValES6_',
            '', '', 'vec', ''],

    [['year_floor'], 'DATETIME', ['DATETIME'],
            '_ZN5doris18TimestampFunctions10year_floorEPN9doris_udf15FunctionContextERKNS1_11DateTimeValE',
            '', '', '', ''],
    [['year_floor'], 'DATETIME', ['DATETIME', 'DATETIME'],
            '_ZN5doris18TimestampFunctions10year_floorEPN9doris_udf15FunctionContextERKNS1_11DateTimeValES6_',
            '', '', '', ''],
    [['year_floor'], 'DATETIME', ['DATETIME', 'INT'],
            '_ZN5doris18TimestampFunctions10year_floorEPN9doris_udf15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE',
            '', '', '', ''],
    [['year_floor'], 'DATETIME', ['DATETIME', 'INT', 'DATETIME'],
            '_ZN5doris18TimestampFunctions10year_floorEPN9doris_udf15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValES6_',
            '', '', '', ''],
    [['year_ceil'], 'DATETIME', ['DATETIME'],
            '_ZN5doris18TimestampFunctions9year_ceilEPN9doris_udf15FunctionContextERKNS1_11DateTimeValE',
            '', '', '', ''],
    [['year_ceil'], 'DATETIME', ['DATETIME', 'DATETIME'],
            '_ZN5doris18TimestampFunctions9year_ceilEPN9doris_udf15FunctionContextERKNS1_11DateTimeValES6_',
            '', '', '', ''],
    [['year_ceil'], 'DATETIME', ['DATETIME', 'INT'],
            '_ZN5doris18TimestampFunctions9year_ceilEPN9doris_udf15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE',
            '', '', '', ''],
    [['year_ceil'], 'DATETIME', ['DATETIME', 'INT', 'DATETIME'],
            '_ZN5doris18TimestampFunctions9year_ceilEPN9doris_udf15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValES6_',
            '', '', '', ''],
    [['month_floor'], 'DATETIME', ['DATETIME'],
            '_ZN5doris18TimestampFunctions11month_floorEPN9doris_udf15FunctionContextERKNS1_11DateTimeValE',
            '', '', '', ''],
    [['month_floor'], 'DATETIME', ['DATETIME', 'DATETIME'],
            '_ZN5doris18TimestampFunctions11month_floorEPN9doris_udf15FunctionContextERKNS1_11DateTimeValES6_',
            '', '', '', ''],
    [['month_floor'], 'DATETIME', ['DATETIME', 'INT'],
            '_ZN5doris18TimestampFunctions11month_floorEPN9doris_udf15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE',
            '', '', '', ''],
    [['month_floor'], 'DATETIME', ['DATETIME', 'INT', 'DATETIME'],
            '_ZN5doris18TimestampFunctions11month_floorEPN9doris_udf15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValES6_',
            '', '', '', ''],
    [['month_ceil'], 'DATETIME', ['DATETIME'],
            '_ZN5doris18TimestampFunctions10month_ceilEPN9doris_udf15FunctionContextERKNS1_11DateTimeValE',
            '', '', '', ''],
    [['month_ceil'], 'DATETIME', ['DATETIME', 'DATETIME'],
            '_ZN5doris18TimestampFunctions10month_ceilEPN9doris_udf15FunctionContextERKNS1_11DateTimeValES6_',
            '', '', '', ''],
    [['month_ceil'], 'DATETIME', ['DATETIME', 'INT'],
            '_ZN5doris18TimestampFunctions10month_ceilEPN9doris_udf15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE',
            '', '', '', ''],
    [['month_ceil'], 'DATETIME', ['DATETIME', 'INT', 'DATETIME'],
            '_ZN5doris18TimestampFunctions10month_ceilEPN9doris_udf15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValES6_',
            '', '', '', ''],
    [['week_floor'], 'DATETIME', ['DATETIME'],
            '_ZN5doris18TimestampFunctions10week_floorEPN9doris_udf15FunctionContextERKNS1_11DateTimeValE',
            '', '', '', ''],
    [['week_floor'], 'DATETIME', ['DATETIME', 'DATETIME'],
            '_ZN5doris18TimestampFunctions10week_floorEPN9doris_udf15FunctionContextERKNS1_11DateTimeValES6_',
            '', '', '', ''],
    [['week_floor'], 'DATETIME', ['DATETIME', 'INT'],
            '_ZN5doris18TimestampFunctions10week_floorEPN9doris_udf15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE',
            '', '', '', ''],
    [['week_floor'], 'DATETIME', ['DATETIME', 'INT', 'DATETIME'],
            '_ZN5doris18TimestampFunctions10week_floorEPN9doris_udf15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValES6_',
            '', '', '', ''],
    [['week_ceil'], 'DATETIME', ['DATETIME'],
            '_ZN5doris18TimestampFunctions9week_ceilEPN9doris_udf15FunctionContextERKNS1_11DateTimeValE',
            '', '', '', ''],
    [['week_ceil'], 'DATETIME', ['DATETIME', 'DATETIME'],
            '_ZN5doris18TimestampFunctions9week_ceilEPN9doris_udf15FunctionContextERKNS1_11DateTimeValES6_',
            '', '', '', ''],
    [['week_ceil'], 'DATETIME', ['DATETIME', 'INT'],
            '_ZN5doris18TimestampFunctions9week_ceilEPN9doris_udf15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE',
            '', '', '', ''],
    [['week_ceil'], 'DATETIME', ['DATETIME', 'INT', 'DATETIME'],
            '_ZN5doris18TimestampFunctions9week_ceilEPN9doris_udf15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValES6_',
            '', '', '', ''],
    [['day_floor'], 'DATETIME', ['DATETIME'],
            '_ZN5doris18TimestampFunctions9day_floorEPN9doris_udf15FunctionContextERKNS1_11DateTimeValE',
            '', '', '', ''],
    [['day_floor'], 'DATETIME', ['DATETIME', 'DATETIME'],
            '_ZN5doris18TimestampFunctions9day_floorEPN9doris_udf15FunctionContextERKNS1_11DateTimeValES6_',
            '', '', '', ''],
    [['day_floor'], 'DATETIME', ['DATETIME', 'INT'],
            '_ZN5doris18TimestampFunctions9day_floorEPN9doris_udf15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE',
            '', '', '', ''],
    [['day_floor'], 'DATETIME', ['DATETIME', 'INT', 'DATETIME'],
            '_ZN5doris18TimestampFunctions9day_floorEPN9doris_udf15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValES6_',
            '', '', '', ''],
    [['day_ceil'], 'DATETIME', ['DATETIME'],
            '_ZN5doris18TimestampFunctions8day_ceilEPN9doris_udf15FunctionContextERKNS1_11DateTimeValE',
            '', '', '', ''],
    [['day_ceil'], 'DATETIME', ['DATETIME', 'DATETIME'],
            '_ZN5doris18TimestampFunctions8day_ceilEPN9doris_udf15FunctionContextERKNS1_11DateTimeValES6_',
            '', '', '', ''],
    [['day_ceil'], 'DATETIME', ['DATETIME', 'INT'],
            '_ZN5doris18TimestampFunctions8day_ceilEPN9doris_udf15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE',
            '', '', '', ''],
    [['day_ceil'], 'DATETIME', ['DATETIME', 'INT', 'DATETIME'],
            '_ZN5doris18TimestampFunctions8day_ceilEPN9doris_udf15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValES6_',
            '', '', '', ''],
    [['hour_floor'], 'DATETIME', ['DATETIME'],
            '_ZN5doris18TimestampFunctions10hour_floorEPN9doris_udf15FunctionContextERKNS1_11DateTimeValE',
            '', '', '', ''],
    [['hour_floor'], 'DATETIME', ['DATETIME', 'DATETIME'],
            '_ZN5doris18TimestampFunctions10hour_floorEPN9doris_udf15FunctionContextERKNS1_11DateTimeValES6_',
            '', '', '', ''],
    [['hour_floor'], 'DATETIME', ['DATETIME', 'INT'],
            '_ZN5doris18TimestampFunctions10hour_floorEPN9doris_udf15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE',
            '', '', '', ''],
    [['hour_floor'], 'DATETIME', ['DATETIME', 'INT', 'DATETIME'],
            '_ZN5doris18TimestampFunctions10hour_floorEPN9doris_udf15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValES6_',
            '', '', '', ''],
    [['hour_ceil'], 'DATETIME', ['DATETIME'],
            '_ZN5doris18TimestampFunctions9hour_ceilEPN9doris_udf15FunctionContextERKNS1_11DateTimeValE',
            '', '', '', ''],
    [['hour_ceil'], 'DATETIME', ['DATETIME', 'DATETIME'],
            '_ZN5doris18TimestampFunctions9hour_ceilEPN9doris_udf15FunctionContextERKNS1_11DateTimeValES6_',
            '', '', '', ''],
    [['hour_ceil'], 'DATETIME', ['DATETIME', 'INT'],
            '_ZN5doris18TimestampFunctions9hour_ceilEPN9doris_udf15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE',
            '', '', '', ''],
    [['hour_ceil'], 'DATETIME', ['DATETIME', 'INT', 'DATETIME'],
            '_ZN5doris18TimestampFunctions9hour_ceilEPN9doris_udf15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValES6_',
            '', '', '', ''],
    [['minute_floor'], 'DATETIME', ['DATETIME'],
            '_ZN5doris18TimestampFunctions12minute_floorEPN9doris_udf15FunctionContextERKNS1_11DateTimeValE',
            '', '', '', ''],
    [['minute_floor'], 'DATETIME', ['DATETIME', 'DATETIME'],
            '_ZN5doris18TimestampFunctions12minute_floorEPN9doris_udf15FunctionContextERKNS1_11DateTimeValES6_',
            '', '', '', ''],
    [['minute_floor'], 'DATETIME', ['DATETIME', 'INT'],
            '_ZN5doris18TimestampFunctions12minute_floorEPN9doris_udf15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE',
            '', '', '', ''],
    [['minute_floor'], 'DATETIME', ['DATETIME', 'INT', 'DATETIME'],
            '_ZN5doris18TimestampFunctions12minute_floorEPN9doris_udf15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValES6_',
            '', '', '', ''],
    [['minute_ceil'], 'DATETIME', ['DATETIME'],
            '_ZN5doris18TimestampFunctions11minute_ceilEPN9doris_udf15FunctionContextERKNS1_11DateTimeValE',
            '', '', '', ''],
    [['minute_ceil'], 'DATETIME', ['DATETIME', 'DATETIME'],
            '_ZN5doris18TimestampFunctions11minute_ceilEPN9doris_udf15FunctionContextERKNS1_11DateTimeValES6_',
            '', '', '', ''],
    [['minute_ceil'], 'DATETIME', ['DATETIME', 'INT'],
            '_ZN5doris18TimestampFunctions11minute_ceilEPN9doris_udf15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE',
            '', '', '', ''],
    [['minute_ceil'], 'DATETIME', ['DATETIME', 'INT', 'DATETIME'],
            '_ZN5doris18TimestampFunctions11minute_ceilEPN9doris_udf15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValES6_',
            '', '', '', ''],
    [['second_floor'], 'DATETIME', ['DATETIME'],
            '_ZN5doris18TimestampFunctions12second_floorEPN9doris_udf15FunctionContextERKNS1_11DateTimeValE',
            '', '', '', ''],
    [['second_floor'], 'DATETIME', ['DATETIME', 'DATETIME'],
            '_ZN5doris18TimestampFunctions12second_floorEPN9doris_udf15FunctionContextERKNS1_11DateTimeValES6_',
            '', '', '', ''],
    [['second_floor'], 'DATETIME', ['DATETIME', 'INT'],
            '_ZN5doris18TimestampFunctions12second_floorEPN9doris_udf15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE',
            '', '', '', ''],
    [['second_floor'], 'DATETIME', ['DATETIME', 'INT', 'DATETIME'],
            '_ZN5doris18TimestampFunctions12second_floorEPN9doris_udf15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValES6_',
            '', '', '', ''],
    [['second_ceil'], 'DATETIME', ['DATETIME'],
            '_ZN5doris18TimestampFunctions11second_ceilEPN9doris_udf15FunctionContextERKNS1_11DateTimeValE',
            '', '', '', ''],
    [['second_ceil'], 'DATETIME', ['DATETIME', 'DATETIME'],
            '_ZN5doris18TimestampFunctions11second_ceilEPN9doris_udf15FunctionContextERKNS1_11DateTimeValES6_',
            '', '', '', ''],
    [['second_ceil'], 'DATETIME', ['DATETIME', 'INT'],
            '_ZN5doris18TimestampFunctions11second_ceilEPN9doris_udf15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE',
            '', '', '', ''],
    [['second_ceil'], 'DATETIME', ['DATETIME', 'INT', 'DATETIME'],
            '_ZN5doris18TimestampFunctions11second_ceilEPN9doris_udf15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValES6_',
            '', '', '', ''],

    # Math builtin functions
    [['pi'], 'DOUBLE', [],
        '_ZN5doris13MathFunctions2piEPN9doris_udf15FunctionContextE', '', '', 'vec', 'ALWAYS_NULLABLE'],
    [['e'], 'DOUBLE', [],
        '_ZN5doris13MathFunctions1eEPN9doris_udf15FunctionContextE', '', '', 'vec', 'ALWAYS_NULLABLE'],

    [['abs'], 'DOUBLE', ['DOUBLE'],
        '_ZN5doris13MathFunctions3absEPN9doris_udf15FunctionContextERKNS1_9DoubleValE', '', '', 'vec', ''],
    [['abs'], 'FLOAT', ['FLOAT'],
        '_ZN5doris13MathFunctions3absEPN9doris_udf15FunctionContextERKNS1_8FloatValE', '', '', 'vec', ''],
    [['abs'], 'LARGEINT', ['LARGEINT'],
        '_ZN5doris13MathFunctions3absEPN9doris_udf15FunctionContextERKNS1_11LargeIntValE', '', '', 'vec', ''],
    [['abs'], 'LARGEINT', ['BIGINT'],
        '_ZN5doris13MathFunctions3absEPN9doris_udf15FunctionContextERKNS1_9BigIntValE', '', '', 'vec', ''],
    [['abs'], 'INT', ['SMALLINT'],
        '_ZN5doris13MathFunctions3absEPN9doris_udf15FunctionContextERKNS1_11SmallIntValE', '', '', 'vec', ''],
    [['abs'], 'BIGINT', ['INT'],
        '_ZN5doris13MathFunctions3absEPN9doris_udf15FunctionContextERKNS1_6IntValE', '', '', 'vec', ''],
    [['abs'], 'SMALLINT', ['TINYINT'],
        '_ZN5doris13MathFunctions3absEPN9doris_udf15FunctionContextERKNS1_10TinyIntValE', '', '', 'vec', ''],
    [['abs'], 'DECIMALV2', ['DECIMALV2'],
        '_ZN5doris13MathFunctions3absEPN9doris_udf15FunctionContextERKNS1_12DecimalV2ValE', '', '', 'vec', ''],

    [['sign'], 'FLOAT', ['DOUBLE'],
        '_ZN5doris13MathFunctions4signEPN9doris_udf15FunctionContextERKNS1_9DoubleValE', '', '', 'vec', ''],

    [['sin'], 'DOUBLE', ['DOUBLE'],
        '_ZN5doris13MathFunctions3sinEPN9doris_udf15FunctionContextERKNS1_9DoubleValE', '', '', 'vec', ''],
    [['asin'], 'DOUBLE', ['DOUBLE'],
        '_ZN5doris13MathFunctions4asinEPN9doris_udf15FunctionContextERKNS1_9DoubleValE', '', '', 'vec', ''],
    [['cos'], 'DOUBLE', ['DOUBLE'],
        '_ZN5doris13MathFunctions3cosEPN9doris_udf15FunctionContextERKNS1_9DoubleValE', '', '', 'vec', ''],
    [['acos'], 'DOUBLE', ['DOUBLE'],
        '_ZN5doris13MathFunctions4acosEPN9doris_udf15FunctionContextERKNS1_9DoubleValE', '', '', 'vec', ''],
    [['tan'], 'DOUBLE', ['DOUBLE'],
            '_ZN5doris13MathFunctions3tanEPN9doris_udf15FunctionContextERKNS1_9DoubleValE', '', '', 'vec', ''],
    [['atan'], 'DOUBLE', ['DOUBLE'],
            '_ZN5doris13MathFunctions4atanEPN9doris_udf15FunctionContextERKNS1_9DoubleValE', '', '', 'vec', ''],

    [['ceil', 'ceiling', 'dceil'], 'BIGINT', ['DOUBLE'],
            '_ZN5doris13MathFunctions4ceilEPN9doris_udf15FunctionContextERKNS1_9DoubleValE', '', '', 'vec', ''],
    [['floor', 'dfloor'], 'BIGINT', ['DOUBLE'],
            '_ZN5doris13MathFunctions5floorEPN9doris_udf15FunctionContextERKNS1_9DoubleValE', '', '', 'vec', ''],
    [['round', 'dround'], 'BIGINT', ['DOUBLE'],
            '_ZN5doris13MathFunctions5roundEPN9doris_udf15FunctionContextERKNS1_9DoubleValE', '', '', '', ''],
    [['round', 'dround'], 'DOUBLE', ['DOUBLE', 'INT'],
            '_ZN5doris13MathFunctions11round_up_toEPN9doris_udf'
            '15FunctionContextERKNS1_9DoubleValERKNS1_6IntValE', '', '', '', ''],
    [['truncate'], 'DOUBLE', ['DOUBLE', 'INT'],
            '_ZN5doris13MathFunctions8truncateEPN9doris_udf'
            '15FunctionContextERKNS1_9DoubleValERKNS1_6IntValE', '', '', 'vec', ''],

    [['ln', 'dlog1'], 'DOUBLE', ['DOUBLE'],
            '_ZN5doris13MathFunctions2lnEPN9doris_udf15FunctionContextERKNS1_9DoubleValE', '', '', 'vec', ''],
    [['log'], 'DOUBLE', ['DOUBLE', 'DOUBLE'],
            '_ZN5doris13MathFunctions3logEPN9doris_udf15FunctionContextERKNS1_9DoubleValES6_', '', '', 'vec', ''],
    [['log2'], 'DOUBLE', ['DOUBLE'],
            '_ZN5doris13MathFunctions4log2EPN9doris_udf15FunctionContextERKNS1_9DoubleValE', '', '', 'vec', ''],
    [['log10', 'dlog10'], 'DOUBLE', ['DOUBLE'],
            '_ZN5doris13MathFunctions5log10EPN9doris_udf15FunctionContextERKNS1_9DoubleValE', '', '', 'vec', ''],
    [['exp', 'dexp'], 'DOUBLE', ['DOUBLE'],
            '_ZN5doris13MathFunctions3expEPN9doris_udf15FunctionContextERKNS1_9DoubleValE', '', '', 'vec', ''],

    [['radians'], 'DOUBLE', ['DOUBLE'],
            '_ZN5doris13MathFunctions7radiansEPN9doris_udf15FunctionContextERKNS1_9DoubleValE', '', '', 'vec', ''],
    [['degrees'], 'DOUBLE', ['DOUBLE'],
            '_ZN5doris13MathFunctions7degreesEPN9doris_udf15FunctionContextERKNS1_9DoubleValE', '', '', 'vec', ''],

    [['sqrt', 'dsqrt'], 'DOUBLE', ['DOUBLE'],
            '_ZN5doris13MathFunctions4sqrtEPN9doris_udf15FunctionContextERKNS1_9DoubleValE', '', '', 'vec', ''],
    [['pow', 'power', 'dpow', 'fpow'], 'DOUBLE', ['DOUBLE', 'DOUBLE'],
            '_ZN5doris13MathFunctions3powEPN9doris_udf15FunctionContextERKNS1_9DoubleValES6_', '', '', 'vec', ''],

    [['rand', 'random'], 'DOUBLE', [],
            '_ZN5doris13MathFunctions4randEPN9doris_udf15FunctionContextE',
            '_ZN5doris13MathFunctions12rand_prepareEPN9doris_udf'
            '15FunctionContextENS2_18FunctionStateScopeE',
            '_ZN5doris13MathFunctions10rand_closeEPN9doris_udf'
            '15FunctionContextENS2_18FunctionStateScopeE', '', 'ALWAYS_NOT_NULLABLE'],
    [['rand', 'random'], 'DOUBLE', ['BIGINT'],
            '_ZN5doris13MathFunctions9rand_seedEPN9doris_udf15FunctionContextERKNS1_9BigIntValE',
            '_ZN5doris13MathFunctions12rand_prepareEPN9doris_udf'
            '15FunctionContextENS2_18FunctionStateScopeE',
            '_ZN5doris13MathFunctions10rand_closeEPN9doris_udf'
            '15FunctionContextENS2_18FunctionStateScopeE', '', ''],

    [['bin'], 'VARCHAR', ['BIGINT'],
            '_ZN5doris13MathFunctions3binEPN9doris_udf15FunctionContextERKNS1_9BigIntValE', '', '', '', ''],
    [['hex'], 'VARCHAR', ['BIGINT'],
            '_ZN5doris13MathFunctions7hex_intEPN9doris_udf15FunctionContextERKNS1_9BigIntValE', '', '', '', ''],
    [['hex'], 'VARCHAR', ['VARCHAR'],
            '_ZN5doris13MathFunctions10hex_stringEPN9doris_udf15FunctionContextERKNS1_9StringValE', '', '', '', ''],
    [['unhex'], 'VARCHAR', ['VARCHAR'],
            '_ZN5doris13MathFunctions5unhexEPN9doris_udf15FunctionContextERKNS1_9StringValE', '', '', '', ''],

    [['conv'], 'VARCHAR', ['BIGINT', 'TINYINT', 'TINYINT'],
            '_ZN5doris13MathFunctions8conv_intEPN9doris_udf'
            '15FunctionContextERKNS1_9BigIntValERKNS1_10TinyIntValES9_', '', '', '', ''],
    [['conv'], 'VARCHAR', ['VARCHAR', 'TINYINT', 'TINYINT'],
            '_ZN5doris13MathFunctions11conv_stringEPN9doris_udf'
            '15FunctionContextERKNS1_9StringValERKNS1_10TinyIntValES9_', '', '', '', ''],

    [['pmod'], 'BIGINT', ['BIGINT', 'BIGINT'],
            '_ZN5doris13MathFunctions11pmod_bigintEPN9doris_udf'
            '15FunctionContextERKNS1_9BigIntValES6_', '', '', '', 'ALWAYS_NULLABLE'],
    [['pmod'], 'DOUBLE', ['DOUBLE', 'DOUBLE'],
            '_ZN5doris13MathFunctions11pmod_doubleEPN9doris_udf'
            '15FunctionContextERKNS1_9DoubleValES6_', '', '', '', 'ALWAYS_NULLABLE'],
    [['mod'], 'TINYINT', ['TINYINT', 'TINYINT'],
            '_ZN5doris9Operators29mod_tiny_int_val_tiny_int_valEPN9doris_udf'
            '15FunctionContextERKNS1_10TinyIntValES6_', '', '', '', 'ALWAYS_NULLABLE'],
    [['mod'], 'SMALLINT', ['SMALLINT', 'SMALLINT'],
            '_ZN5doris9Operators31mod_small_int_val_small_int_valEPN9doris_udf'
            '15FunctionContextERKNS1_11SmallIntValES6_', '', '', '', 'ALWAYS_NULLABLE'],
    [['mod'], 'INT', ['INT', 'INT'],
            '_ZN5doris9Operators19mod_int_val_int_valEPN9doris_udf'
            '15FunctionContextERKNS1_6IntValES6_', '', '', '', 'ALWAYS_NULLABLE'],
    [['mod'], 'BIGINT', ['BIGINT', 'BIGINT'],
            '_ZN5doris9Operators27mod_big_int_val_big_int_valEPN9doris_udf'
            '15FunctionContextERKNS1_9BigIntValES6_', '', '', '', 'ALWAYS_NULLABLE'],
    [['mod'], 'LARGEINT', ['LARGEINT', 'LARGEINT'],
            '_ZN5doris9Operators31mod_large_int_val_large_int_valEPN9doris_udf'
            '15FunctionContextERKNS1_11LargeIntValES6_', '', '', '', 'ALWAYS_NULLABLE'],
    [['mod'], 'DECIMALV2', ['DECIMALV2', 'DECIMALV2'],
            '_ZN5doris18DecimalV2Operators31mod_decimalv2_val_decimalv2_valEPN9doris_udf'
            '15FunctionContextERKNS1_12DecimalV2ValES6_', '', '', '', 'ALWAYS_NULLABLE'],
    [['mod', 'fmod'], 'FLOAT', ['FLOAT', 'FLOAT'],
        '_ZN5doris13MathFunctions10fmod_floatEPN9doris_udf15FunctionContextERKNS1_8FloatValES6_',
        '', '', '', 'ALWAYS_NULLABLE'],
    [['mod', 'fmod'], 'DOUBLE', ['DOUBLE', 'DOUBLE'],
        '_ZN5doris13MathFunctions11fmod_doubleEPN9doris_udf15FunctionContextERKNS1_9DoubleValES6_',
        '', '', '', 'ALWAYS_NULLABLE'],

    [['positive'], 'BIGINT', ['BIGINT'],
            '_ZN5doris13MathFunctions15positive_bigintEPN9doris_udf'
            '15FunctionContextERKNS1_9BigIntValE', '', '', 'vec', ''],
    [['positive'], 'DOUBLE', ['DOUBLE'],
            '_ZN5doris13MathFunctions15positive_doubleEPN9doris_udf'
            '15FunctionContextERKNS1_9DoubleValE', '', '', 'vec', ''],
    [['positive'], 'DECIMALV2', ['DECIMALV2'],
            '_ZN5doris13MathFunctions16positive_decimalEPN9doris_udf'
            '15FunctionContextERKNS1_12DecimalV2ValE', '', '', 'vec', ''],
    [['negative'], 'BIGINT', ['BIGINT'],
            '_ZN5doris13MathFunctions15negative_bigintEPN9doris_udf'
            '15FunctionContextERKNS1_9BigIntValE', '', '', 'vec', ''],
    [['negative'], 'DOUBLE', ['DOUBLE'],
            '_ZN5doris13MathFunctions15negative_doubleEPN9doris_udf'
            '15FunctionContextERKNS1_9DoubleValE', '', '', 'vec', ''],
    [['negative'], 'DECIMALV2', ['DECIMALV2'],
            '_ZN5doris13MathFunctions16negative_decimalEPN9doris_udf'
            '15FunctionContextERKNS1_12DecimalV2ValE', '', '', 'vec', ''],

    [['least'], 'TINYINT', ['TINYINT', '...'],
            '_ZN5doris13MathFunctions5leastEPN9doris_udf15FunctionContextEiPKNS1_10TinyIntValE',
            '', '', '', ''],
    [['least'], 'SMALLINT', ['SMALLINT', '...'],
            '_ZN5doris13MathFunctions5leastEPN9doris_udf15FunctionContextEiPKNS1_11SmallIntValE',
            '', '', '', ''],
    [['least'], 'INT', ['INT', '...'],
            '_ZN5doris13MathFunctions5leastEPN9doris_udf15FunctionContextEiPKNS1_6IntValE',
            '', '', '', ''],
    [['least'], 'BIGINT', ['BIGINT', '...'],
            '_ZN5doris13MathFunctions5leastEPN9doris_udf15FunctionContextEiPKNS1_9BigIntValE',
            '', '', '', ''],
    [['least'], 'LARGEINT', ['LARGEINT', '...'],
            '_ZN5doris13MathFunctions5leastEPN9doris_udf15FunctionContextEiPKNS1_11LargeIntValE',
            '', '', '', ''],
    [['least'], 'FLOAT', ['FLOAT', '...'],
            '_ZN5doris13MathFunctions5leastEPN9doris_udf15FunctionContextEiPKNS1_8FloatValE',
            '', '', '', ''],
    [['least'], 'DOUBLE', ['DOUBLE', '...'],
            '_ZN5doris13MathFunctions5leastEPN9doris_udf15FunctionContextEiPKNS1_9DoubleValE',
            '', '', '', ''],
    [['least'], 'DATETIME', ['DATETIME', '...'],
            '_ZN5doris13MathFunctions5leastEPN9doris_udf15FunctionContextEiPKNS1_11DateTimeValE',
            '', '', '', ''],
    [['least'], 'DECIMALV2', ['DECIMALV2', '...'],
            '_ZN5doris13MathFunctions5leastEPN9doris_udf15FunctionContextEiPKNS1_12DecimalV2ValE',
            '', '', '', ''],
    [['least'], 'VARCHAR', ['VARCHAR', '...'],
            '_ZN5doris13MathFunctions5leastEPN9doris_udf15FunctionContextEiPKNS1_9StringValE',
            '', '', '', ''],

    [['greatest'], 'TINYINT', ['TINYINT', '...'],
            '_ZN5doris13MathFunctions8greatestEPN9doris_udf15FunctionContextEiPKNS1_10TinyIntValE',
            '', '', '', ''],
    [['greatest'], 'SMALLINT', ['SMALLINT', '...'],
            '_ZN5doris13MathFunctions8greatestEPN9doris_udf15FunctionContextEiPKNS1_11SmallIntValE',
            '', '', '', ''],
    [['greatest'], 'INT', ['INT', '...'],
            '_ZN5doris13MathFunctions8greatestEPN9doris_udf15FunctionContextEiPKNS1_6IntValE',
            '', '', '', ''],
    [['greatest'], 'BIGINT', ['BIGINT', '...'],
            '_ZN5doris13MathFunctions8greatestEPN9doris_udf15FunctionContextEiPKNS1_9BigIntValE',
            '', '', '', ''],
    [['greatest'], 'LARGEINT', ['LARGEINT', '...'],
            '_ZN5doris13MathFunctions8greatestEPN9doris_udf15FunctionContextEiPKNS1_11LargeIntValE',
            '', '', '', ''],
    [['greatest'], 'FLOAT', ['FLOAT', '...'],
            '_ZN5doris13MathFunctions8greatestEPN9doris_udf15FunctionContextEiPKNS1_8FloatValE',
            '', '', '', ''],
    [['greatest'], 'DOUBLE', ['DOUBLE', '...'],
            '_ZN5doris13MathFunctions8greatestEPN9doris_udf15FunctionContextEiPKNS1_9DoubleValE',
            '', '', '', ''],
    [['greatest'], 'DECIMALV2', ['DECIMALV2', '...'],
            '_ZN5doris13MathFunctions8greatestEPN9doris_udf15FunctionContextEiPKNS1_12DecimalV2ValE',
            '', '', '', ''],
    [['greatest'], 'DATETIME', ['DATETIME', '...'],
            '_ZN5doris13MathFunctions8greatestEPN9doris_udf15FunctionContextEiPKNS1_11DateTimeValE',
            '', '', '', ''],
    [['greatest'], 'VARCHAR', ['VARCHAR', '...'],
            '_ZN5doris13MathFunctions8greatestEPN9doris_udf15FunctionContextEiPKNS1_9StringValE',
            '', '', '', ''],

    # Conditional Functions
    # Some of these have empty symbols because the BE special-cases them based on the
    # function name
    [['if'], 'BOOLEAN', ['BOOLEAN', 'BOOLEAN', 'BOOLEAN'], '', '', '', 'vec', 'CUSTOM'],
    [['if'], 'TINYINT', ['BOOLEAN', 'TINYINT', 'TINYINT'], '', '', '', 'vec', 'CUSTOM'],
    [['if'], 'SMALLINT', ['BOOLEAN', 'SMALLINT', 'SMALLINT'], '', '', '', 'vec', 'CUSTOM'],
    [['if'], 'INT', ['BOOLEAN', 'INT', 'INT'], '', '', '', 'vec', 'CUSTOM'],
    [['if'], 'BIGINT', ['BOOLEAN', 'BIGINT', 'BIGINT'], '', '', '', 'vec', 'CUSTOM'],
    [['if'], 'LARGEINT', ['BOOLEAN', 'LARGEINT', 'LARGEINT'], '', '', '', 'vec', 'CUSTOM'],
    [['if'], 'FLOAT', ['BOOLEAN', 'FLOAT', 'FLOAT'], '', '', '', 'vec', 'CUSTOM'],
    [['if'], 'DOUBLE', ['BOOLEAN', 'DOUBLE', 'DOUBLE'], '', '', '', 'vec', 'CUSTOM'],
    [['if'], 'DATETIME', ['BOOLEAN', 'DATETIME', 'DATETIME'], '', '', '', 'vec', 'CUSTOM'],
    [['if'], 'DATE', ['BOOLEAN', 'DATE', 'DATE'], '', '', '', 'vec', 'CUSTOM'],
    [['if'], 'DECIMALV2', ['BOOLEAN', 'DECIMALV2', 'DECIMALV2'], '', '', '', 'vec', 'CUSTOM'],
    [['if'], 'BITMAP', ['BOOLEAN', 'BITMAP', 'BITMAP'], '', '', '', 'vec', 'CUSTOM'],
    # The priority of varchar should be lower than decimal in IS_SUPERTYPE_OF mode.
    [['if'], 'VARCHAR', ['BOOLEAN', 'VARCHAR', 'VARCHAR'], '', '', '', 'vec', 'CUSTOM'],

    [['nullif'], 'BOOLEAN', ['BOOLEAN', 'BOOLEAN'], '', '', '', 'vec', 'ALWAYS_NULLABLE'],
    [['nullif'], 'TINYINT', ['TINYINT', 'TINYINT'], '', '', '', 'vec', 'ALWAYS_NULLABLE'],
    [['nullif'], 'SMALLINT', ['SMALLINT', 'SMALLINT'], '', '', '', 'vec', 'ALWAYS_NULLABLE'],
    [['nullif'], 'INT', ['INT', 'INT'], '', '', '', 'vec', 'ALWAYS_NULLABLE'],
    [['nullif'], 'BIGINT', ['BIGINT', 'BIGINT'], '', '', '', 'vec', 'ALWAYS_NULLABLE'],
    [['nullif'], 'LARGEINT', ['LARGEINT', 'LARGEINT'], '', '', '', 'vec', 'ALWAYS_NULLABLE'],
    [['nullif'], 'FLOAT', ['FLOAT', 'FLOAT'], '', '', '', 'vec', 'ALWAYS_NULLABLE'],
    [['nullif'], 'DOUBLE', ['DOUBLE', 'DOUBLE'], '', '', '', 'vec', 'ALWAYS_NULLABLE'],
    [['nullif'], 'DATETIME', ['DATETIME', 'DATETIME'], '', '', '', 'vec', 'ALWAYS_NULLABLE'],
    [['nullif'], 'DATE', ['DATE', 'DATE'], '', '', '', 'vec', 'ALWAYS_NULLABLE'],
    [['nullif'], 'DECIMALV2', ['DECIMALV2', 'DECIMALV2'], '', '', '', 'vec', 'ALWAYS_NULLABLE'],
    # The priority of varchar should be lower than decimal in IS_SUPERTYPE_OF mode.
    [['nullif'], 'VARCHAR', ['VARCHAR', 'VARCHAR'], '', '', '', 'vec', 'ALWAYS_NULLABLE'],

    [['ifnull'], 'BOOLEAN', ['BOOLEAN', 'BOOLEAN'], '', '', '', 'vec', 'CUSTOM'],
    [['ifnull'], 'TINYINT', ['TINYINT', 'TINYINT'], '', '', '', 'vec', 'CUSTOM'],
    [['ifnull'], 'SMALLINT', ['SMALLINT', 'SMALLINT'], '', '', '', 'vec', 'CUSTOM'],
    [['ifnull'], 'INT', ['INT', 'INT'], '', '', '', 'vec', 'CUSTOM'],
    [['ifnull'], 'BIGINT', ['BIGINT', 'BIGINT'], '', '', '', 'vec', 'CUSTOM'],
    [['ifnull'], 'LARGEINT', ['LARGEINT', 'LARGEINT'], '', '', '', 'vec', 'CUSTOM'],
    [['ifnull'], 'FLOAT', ['FLOAT', 'FLOAT'], '', '', '', 'vec', 'CUSTOM'],
    [['ifnull'], 'DOUBLE', ['DOUBLE', 'DOUBLE'], '', '', '', 'vec', 'CUSTOM'],
    [['ifnull'], 'DATE', ['DATE', 'DATE'], '', '', '', 'vec', 'CUSTOM'],
    [['ifnull'], 'DATETIME', ['DATETIME', 'DATETIME'], '', '', '', 'vec', 'CUSTOM'],
    [['ifnull'], 'DATETIME', ['DATE', 'DATETIME'], '', '', '', 'vec', 'CUSTOM'],
    [['ifnull'], 'DATETIME', ['DATETIME', 'DATE'], '', '', '', 'vec', 'CUSTOM'],
    [['ifnull'], 'DECIMALV2', ['DECIMALV2', 'DECIMALV2'], '', '', '', 'vec', 'CUSTOM'],
    [['ifnull'], 'BITMAP', ['BITMAP', 'BITMAP'], '', '', '', 'vec', 'CUSTOM'],
    # The priority of varchar should be lower than decimal in IS_SUPERTYPE_OF mode.
    [['ifnull'], 'VARCHAR', ['VARCHAR', 'VARCHAR'], '', '', '', 'vec', 'CUSTOM'],

    [['coalesce'], 'BOOLEAN', ['BOOLEAN', '...'], '', '', '', '', 'CUSTOM'],
    [['coalesce'], 'TINYINT', ['TINYINT', '...'], '', '', '', '', 'CUSTOM'],
    [['coalesce'], 'SMALLINT', ['SMALLINT', '...'], '', '', '', '', 'CUSTOM'],
    [['coalesce'], 'INT', ['INT', '...'], '', '', '', '', 'CUSTOM'],
    [['coalesce'], 'BIGINT', ['BIGINT', '...'], '', '', '', '', 'CUSTOM'],
    [['coalesce'], 'LARGEINT', ['LARGEINT', '...'], '', '', '', '', 'CUSTOM'],
    [['coalesce'], 'FLOAT', ['FLOAT', '...'], '', '', '', '', 'CUSTOM'],
    [['coalesce'], 'DOUBLE', ['DOUBLE', '...'], '', '', '', '', 'CUSTOM'],
    [['coalesce'], 'DATETIME', ['DATETIME', '...'], '', '', '', '', 'CUSTOM'],
    [['coalesce'], 'DATE', ['DATE', '...'], '', '', '', '', 'CUSTOM'],
    [['coalesce'], 'DECIMALV2', ['DECIMALV2', '...'], '', '', '', '', 'CUSTOM'],
    [['coalesce'], 'BITMAP', ['BITMAP', '...'], '', '', '', '', 'CUSTOM'],
    # The priority of varchar should be lower than decimal in IS_SUPERTYPE_OF mode.
    [['coalesce'], 'VARCHAR', ['VARCHAR', '...'], '', '', '', '', 'CUSTOM'],

    [['esquery'], 'BOOLEAN', ['VARCHAR', 'VARCHAR'],
        '_ZN5doris11ESFunctions5matchEPN'
        '9doris_udf15FunctionContextERKNS1_9StringValES6_', '', '', '', ''],

    # String builtin functions
    [['substr', 'substring'], 'VARCHAR', ['VARCHAR', 'INT'],
        '_ZN5doris15StringFunctions9substringEPN'
        '9doris_udf15FunctionContextERKNS1_9StringValERKNS1_6IntValE', '', '', '', 'ALWAYS_NULLABLE'],
    [['substr', 'substring'], 'VARCHAR', ['VARCHAR', 'INT', 'INT'],
        '_ZN5doris15StringFunctions9substringEPN'
        '9doris_udf15FunctionContextERKNS1_9StringValERKNS1_6IntValES9_', '', '', 'vec', 'ALWAYS_NULLABLE'],
    [['strleft', 'left'], 'VARCHAR', ['VARCHAR', 'INT'],
        '_ZN5doris15StringFunctions4leftEPN9doris_udf'
        '15FunctionContextERKNS1_9StringValERKNS1_6IntValE', '', '', 'vec', ''],
    [['strright', 'right'], 'VARCHAR', ['VARCHAR', 'INT'],
        '_ZN5doris15StringFunctions5rightEPN9doris_udf'
        '15FunctionContextERKNS1_9StringValERKNS1_6IntValE', '', '', 'vec', ''],
    [['ends_with'], 'BOOLEAN', ['VARCHAR', 'VARCHAR'],
        '_ZN5doris15StringFunctions9ends_withEPN9doris_udf15FunctionContextERKNS1_9StringValES6_',
        '', '', 'vec', ''],
    [['starts_with'], 'BOOLEAN', ['VARCHAR', 'VARCHAR'],
        '_ZN5doris15StringFunctions11starts_withEPN9doris_udf15FunctionContextERKNS1_9StringValES6_',
        '', '', 'vec', ''],
    [['null_or_empty'], 'BOOLEAN', ['VARCHAR'],
        '_ZN5doris15StringFunctions13null_or_emptyEPN9doris_udf15FunctionContextERKNS1_9StringValE',
        '', '', 'vec', 'ALWAYS_NOT_NULLABLE'],
    [['space'], 'VARCHAR', ['INT'],
        '_ZN5doris15StringFunctions5spaceEPN9doris_udf15FunctionContextERKNS1_6IntValE', '', '', 'vec', ''],
    [['repeat'], 'VARCHAR', ['VARCHAR', 'INT'],
        '_ZN5doris15StringFunctions6repeatEPN9doris_udf'
        '15FunctionContextERKNS1_9StringValERKNS1_6IntValE', '', '', 'vec', ''],
    [['lpad'], 'VARCHAR', ['VARCHAR', 'INT', 'VARCHAR'],
            '_ZN5doris15StringFunctions4lpadEPN9doris_udf'
            '15FunctionContextERKNS1_9StringValERKNS1_6IntValES6_', '', '', 'vec', ''],
    [['rpad'], 'VARCHAR', ['VARCHAR', 'INT', 'VARCHAR'],
            '_ZN5doris15StringFunctions4rpadEPN9doris_udf'
            '15FunctionContextERKNS1_9StringValERKNS1_6IntValES6_', '', '', 'vec', ''],
    [['append_trailing_char_if_absent'], 'VARCHAR', ['VARCHAR', 'VARCHAR'],
	        '_ZN5doris15StringFunctions30append_trailing_char_if_absentEPN9doris_udf15FunctionContextERKNS1_9StringValES6_',
	        '', '', 'vec', 'ALWAYS_NULLABLE'],
    [['length'], 'INT', ['VARCHAR'],
            '_ZN5doris15StringFunctions6lengthEPN9doris_udf15FunctionContextERKNS1_9StringValE',
            '', '', 'vec', ''],
    [['bit_length'], 'INT', ['VARCHAR'],
            '_ZN5doris15StringFunctions10bit_lengthEPN9doris_udf15FunctionContextERKNS1_9StringValE','', '', 'vec', ''],

    [['char_length', 'character_length'], 'INT', ['VARCHAR'],
            '_ZN5doris15StringFunctions16char_utf8_lengthEPN9doris_udf15FunctionContextERKNS1_9StringValE',
            '', '', 'vec', ''],
    [['lower', 'lcase'], 'VARCHAR', ['VARCHAR'],
            '_ZN5doris15StringFunctions5lowerEPN9doris_udf15FunctionContextERKNS1_9StringValE', '', '', 'vec', ''],
    [['upper', 'ucase'], 'VARCHAR', ['VARCHAR'],
            '_ZN5doris15StringFunctions5upperEPN9doris_udf15FunctionContextERKNS1_9StringValE', '', '', 'vec', ''],
    [['reverse'], 'VARCHAR', ['VARCHAR'],
            '_ZN5doris15StringFunctions7reverseEPN9doris_udf15FunctionContextERKNS1_9StringValE', '', '', 'vec', ''],
    [['trim'], 'VARCHAR', ['VARCHAR'],
            '_ZN5doris15StringFunctions4trimEPN9doris_udf15FunctionContextERKNS1_9StringValE', '', '', 'vec', ''],
    [['ltrim'], 'VARCHAR', ['VARCHAR'],
            '_ZN5doris15StringFunctions5ltrimEPN9doris_udf15FunctionContextERKNS1_9StringValE', '', '', 'vec', ''],
    [['rtrim'], 'VARCHAR', ['VARCHAR'],
            '_ZN5doris15StringFunctions5rtrimEPN9doris_udf15FunctionContextERKNS1_9StringValE', '', '', 'vec', ''],
    [['ascii'], 'INT', ['VARCHAR'],
            '_ZN5doris15StringFunctions5asciiEPN9doris_udf15FunctionContextERKNS1_9StringValE', '', '', 'vec', ''],
    [['instr'], 'INT', ['VARCHAR', 'VARCHAR'],
            '_ZN5doris15StringFunctions5instrEPN9doris_udf15FunctionContextERKNS1_9StringValES6_', '', '', 'vec', ''],
    [['locate'], 'INT', ['VARCHAR', 'VARCHAR'],
            '_ZN5doris15StringFunctions6locateEPN9doris_udf15FunctionContextERKNS1_9StringValES6_', '', '', 'vec', ''],
    [['locate'], 'INT', ['VARCHAR', 'VARCHAR', 'INT'],
            '_ZN5doris15StringFunctions10locate_posEPN9doris_udf'
            '15FunctionContextERKNS1_9StringValES6_RKNS1_6IntValE', '', '', 'vec', ''],
    [['regexp_extract'], 'VARCHAR', ['VARCHAR', 'VARCHAR', 'BIGINT'],
            '_ZN5doris15StringFunctions14regexp_extractEPN9doris_udf'
            '15FunctionContextERKNS1_9StringValES6_RKNS1_9BigIntValE',
            '_ZN5doris15StringFunctions14regexp_prepareEPN9doris_udf'
            '15FunctionContextENS2_18FunctionStateScopeE',
            '_ZN5doris15StringFunctions12regexp_closeEPN9doris_udf'
            '15FunctionContextENS2_18FunctionStateScopeE', '', ''],
    [['regexp_replace'], 'VARCHAR', ['VARCHAR', 'VARCHAR', 'VARCHAR'],
            '_ZN5doris15StringFunctions14regexp_replaceEPN9doris_udf'
            '15FunctionContextERKNS1_9StringValES6_S6_',
            '_ZN5doris15StringFunctions14regexp_prepareEPN9doris_udf'
            '15FunctionContextENS2_18FunctionStateScopeE',
            '_ZN5doris15StringFunctions12regexp_closeEPN9doris_udf'
            '15FunctionContextENS2_18FunctionStateScopeE', '', ''],
    [['concat'], 'VARCHAR', ['VARCHAR', '...'],
            '_ZN5doris15StringFunctions6concatEPN9doris_udf15FunctionContextEiPKNS1_9StringValE',
            '', '', 'vec', ''],
    [['replace'], 'VARCHAR', ['VARCHAR', 'VARCHAR', 'VARCHAR'],
            '_ZN5doris15StringFunctions7replaceEPN9doris_udf15FunctionContextERKNS1_9StringValES6_S6_',
            '', '', 'vec', ''],
    [['concat_ws'], 'VARCHAR', ['VARCHAR', 'VARCHAR', '...'],
            '_ZN5doris15StringFunctions9concat_wsEPN9doris_udf'
            '15FunctionContextERKNS1_9StringValEiPS5_', '', '', 'vec', 'CUSTOM'],
    [['find_in_set'], 'INT', ['VARCHAR', 'VARCHAR'],
            '_ZN5doris15StringFunctions11find_in_setEPN9doris_udf'
            '15FunctionContextERKNS1_9StringValES6_', '', '', 'vec', ''],
    [['parse_url'], 'VARCHAR', ['VARCHAR', 'VARCHAR'],
            '_ZN5doris15StringFunctions9parse_urlEPN9doris_udf'
            '15FunctionContextERKNS1_9StringValES6_',
            '_ZN5doris15StringFunctions17parse_url_prepareEPN9doris_udf'
            '15FunctionContextENS2_18FunctionStateScopeE',
            '_ZN5doris15StringFunctions15parse_url_closeEPN9doris_udf'
            '15FunctionContextENS2_18FunctionStateScopeE', '', ''],
    [['parse_url'], 'VARCHAR', ['VARCHAR', 'VARCHAR', 'VARCHAR'],
            '_ZN5doris15StringFunctions13parse_url_keyEPN9doris_udf'
            '15FunctionContextERKNS1_9StringValES6_S6_',
            '_ZN5doris15StringFunctions17parse_url_prepareEPN9doris_udf'
            '15FunctionContextENS2_18FunctionStateScopeE',
            '_ZN5doris15StringFunctions15parse_url_closeEPN9doris_udf'
            '15FunctionContextENS2_18FunctionStateScopeE', '', ''],
    [['money_format'], 'VARCHAR', ['BIGINT'],
        '_ZN5doris15StringFunctions12money_formatEPN9doris_udf15FunctionContextERKNS1_9BigIntValE',
        '', '', '', ''],
    [['money_format'], 'VARCHAR', ['LARGEINT'],
        '_ZN5doris15StringFunctions12money_formatEPN9doris_udf15FunctionContextERKNS1_11LargeIntValE',
        '', '', '', ''],
    [['money_format'], 'VARCHAR', ['DOUBLE'],
        '_ZN5doris15StringFunctions12money_formatEPN9doris_udf15FunctionContextERKNS1_9DoubleValE',
        '', '', '', ''],
    [['money_format'], 'VARCHAR', ['DECIMALV2'],
        '_ZN5doris15StringFunctions12money_formatEPN9doris_udf15FunctionContextERKNS1_12DecimalV2ValE',
        '', '', '', ''],
    [['split_part'], 'VARCHAR', ['VARCHAR', 'VARCHAR', 'INT'],
        '_ZN5doris15StringFunctions10split_partEPN9doris_udf15FunctionContextERKNS1_9StringValES6_RKNS1_6IntValE',
        '', '', '', ''],

    # Utility functions
    [['sleep'], 'BOOLEAN', ['INT'],
        '_ZN5doris16UtilityFunctions5sleepEPN9doris_udf15FunctionContextERKNS1_6IntValE',
        '', '', '', ''],
    [['version'], 'VARCHAR', [],
        '_ZN5doris16UtilityFunctions7versionEPN9doris_udf15FunctionContextE',
        '', '', '', 'ALWAYS_NOT_NULLABLE'],

    # Json functions
    [['get_json_int'], 'INT', ['VARCHAR', 'VARCHAR'],
        '_ZN5doris13JsonFunctions12get_json_intEPN9doris_udf15FunctionContextERKNS1_9StringValES6_',
        '_ZN5doris13JsonFunctions17json_path_prepareEPN9doris_udf15FunctionContextENS2_18FunctionStateScopeE',
        '_ZN5doris13JsonFunctions15json_path_closeEPN9doris_udf15FunctionContextENS2_18FunctionStateScopeE',
        'vec', ''],
    [['get_json_double'], 'DOUBLE', ['VARCHAR', 'VARCHAR'],
        '_ZN5doris13JsonFunctions15get_json_doubleEPN9doris_udf15FunctionContextERKNS1_9StringValES6_',
        '_ZN5doris13JsonFunctions17json_path_prepareEPN9doris_udf15FunctionContextENS2_18FunctionStateScopeE',
        '_ZN5doris13JsonFunctions15json_path_closeEPN9doris_udf15FunctionContextENS2_18FunctionStateScopeE',
        'vec', ''],
    [['get_json_string'], 'VARCHAR', ['VARCHAR', 'VARCHAR'],
        '_ZN5doris13JsonFunctions15get_json_stringEPN9doris_udf15FunctionContextERKNS1_9StringValES6_',
        '_ZN5doris13JsonFunctions17json_path_prepareEPN9doris_udf15FunctionContextENS2_18FunctionStateScopeE',
        '_ZN5doris13JsonFunctions15json_path_closeEPN9doris_udf15FunctionContextENS2_18FunctionStateScopeE',
        'vec', ''],

    #hll function
    [['hll_cardinality'], 'BIGINT', ['VARCHAR'],
        '_ZN5doris12HllFunctions15hll_cardinalityEPN9doris_udf15FunctionContextERKNS1_9StringValE',
        '', '', 'vec', 'ALWAYS_NOT_NULLABLE'],
    [['hll_hash'], 'HLL', ['VARCHAR'],
        '_ZN5doris12HllFunctions8hll_hashEPN9doris_udf15FunctionContextERKNS1_9StringValE',
        '', '', 'vec', 'ALWAYS_NOT_NULLABLE'],
    [['hll_empty'], 'HLL', [],
        '_ZN5doris12HllFunctions9hll_emptyEPN9doris_udf15FunctionContextE',
        '', '', 'vec', 'ALWAYS_NOT_NULLABLE'],

    #bitmap function

    [['to_bitmap'], 'BITMAP', ['VARCHAR'],
        '_ZN5doris15BitmapFunctions9to_bitmapEPN9doris_udf15FunctionContextERKNS1_9StringValE',
        '', '', 'vec', ''],
    [['bitmap_hash'], 'BITMAP', ['VARCHAR'],
        '_ZN5doris15BitmapFunctions11bitmap_hashEPN9doris_udf15FunctionContextERKNS1_9StringValE',
        '', '', 'vec', ''],
    [['bitmap_count'], 'BIGINT', ['BITMAP'],
        '_ZN5doris15BitmapFunctions12bitmap_countEPN9doris_udf15FunctionContextERKNS1_9StringValE',
        '', '', 'vec', ''],
    [['bitmap_empty'], 'BITMAP', [],
        '_ZN5doris15BitmapFunctions12bitmap_emptyEPN9doris_udf15FunctionContextE',
        '', '', 'vec', 'ALWAYS_NOT_NULLABLE'],
    [['bitmap_or'], 'BITMAP', ['BITMAP','BITMAP'],
        '_ZN5doris15BitmapFunctions9bitmap_orEPN9doris_udf15FunctionContextERKNS1_9StringValES6_',
        '', '', 'vec', ''],
    [['bitmap_xor'], 'BITMAP', ['BITMAP','BITMAP'],
        '_ZN5doris15BitmapFunctions10bitmap_xorEPN9doris_udf15FunctionContextERKNS1_9StringValES6_',
        '', '', 'vec', ''],
    [['bitmap_not'], 'BITMAP', ['BITMAP','BITMAP'],
        '_ZN5doris15BitmapFunctions10bitmap_notEPN9doris_udf15FunctionContextERKNS1_9StringValES6_',
        '', '', 'vec', ''],
    [['bitmap_and'], 'BITMAP', ['BITMAP','BITMAP'],
        '_ZN5doris15BitmapFunctions10bitmap_andEPN9doris_udf15FunctionContextERKNS1_9StringValES6_',
        '', '', 'vec', ''],
    [['bitmap_to_string'], 'VARCHAR', ['BITMAP'],
        '_ZN5doris15BitmapFunctions16bitmap_to_stringEPN9doris_udf15FunctionContextERKNS1_9StringValE',
        '', '', 'vec', ''],
    [['bitmap_from_string'], 'BITMAP', ['VARCHAR'],
        '_ZN5doris15BitmapFunctions18bitmap_from_stringEPN9doris_udf15FunctionContextERKNS1_9StringValE',
        '', '', 'vec', ''],
    [['bitmap_contains'], 'BOOLEAN', ['BITMAP','BIGINT'],
        '_ZN5doris15BitmapFunctions15bitmap_containsEPN9doris_udf15FunctionContextERKNS1_9StringValERKNS1_9BigIntValE',
        '', '', 'vec', ''],
    [['bitmap_has_any'], 'BOOLEAN', ['BITMAP','BITMAP'],
        '_ZN5doris15BitmapFunctions14bitmap_has_anyEPN9doris_udf15FunctionContextERKNS1_9StringValES6_',
        '', '', 'vec', ''],
    [['bitmap_min'], 'BIGINT', ['BITMAP'],
        '_ZN5doris15BitmapFunctions10bitmap_minEPN9doris_udf15FunctionContextERKNS1_9StringValE',
        '', '', 'vec', ''],

    # hash functions
    [['murmur_hash3_32'], 'INT', ['VARCHAR', '...'],
        '_ZN5doris13HashFunctions15murmur_hash3_32EPN9doris_udf15FunctionContextEiPKNS1_9StringValE',
        '', '', '', ''],

    # aes and base64 function
    [['aes_encrypt'], 'VARCHAR', ['VARCHAR', 'VARCHAR'],
        '_ZN5doris19EncryptionFunctions11aes_encryptEPN9doris_udf'
        '15FunctionContextERKNS1_9StringValES6_', '', '', '', ''],
    [['aes_decrypt'], 'VARCHAR', ['VARCHAR', 'VARCHAR'],
        '_ZN5doris19EncryptionFunctions11aes_decryptEPN9doris_udf'
        '15FunctionContextERKNS1_9StringValES6_', '', '', '', ''],
    [['from_base64'], 'VARCHAR', ['VARCHAR'],
        '_ZN5doris19EncryptionFunctions11from_base64EPN9doris_udf'
        '15FunctionContextERKNS1_9StringValE', '', '', '', ''],
    [['to_base64'], 'VARCHAR', ['VARCHAR'],
        '_ZN5doris19EncryptionFunctions9to_base64EPN9doris_udf'
        '15FunctionContextERKNS1_9StringValE', '', '', '', ''],
    # for compatable with MySQL
    [['md5'], 'VARCHAR', ['VARCHAR'],
        '_ZN5doris19EncryptionFunctions3md5EPN9doris_udf15FunctionContextERKNS1_9StringValE', '', '', '', ''],
    [['md5sum'], 'VARCHAR', ['VARCHAR', '...'],
        '_ZN5doris19EncryptionFunctions6md5sumEPN9doris_udf15FunctionContextEiPKNS1_9StringValE', '', '', '', ''],

    # geo functions
    [['ST_Point'], 'VARCHAR', ['DOUBLE', 'DOUBLE'],
        '_ZN5doris12GeoFunctions8st_pointEPN9doris_udf15FunctionContextERKNS1_9DoubleValES6_', '', '', '', ''],
    [['ST_X'], 'DOUBLE', ['VARCHAR'],
        '_ZN5doris12GeoFunctions4st_xEPN9doris_udf15FunctionContextERKNS1_9StringValE',
        '', '', '', 'ALWAYS_NULLABLE'],
    [['ST_Y'], 'DOUBLE', ['VARCHAR'],
        '_ZN5doris12GeoFunctions4st_yEPN9doris_udf15FunctionContextERKNS1_9StringValE',
        '', '', '', 'ALWAYS_NULLABLE'],

    [['ST_Distance_Sphere'], 'DOUBLE', ['DOUBLE', 'DOUBLE', 'DOUBLE', 'DOUBLE'],
        '_ZN5doris12GeoFunctions18st_distance_sphereEPN9doris_udf15FunctionContextERKNS1_9DoubleValES6_S6_S6_',
        '', '', '', ''],

    [['ST_AsText', 'ST_AsWKT'], 'VARCHAR', ['VARCHAR'],
        '_ZN5doris12GeoFunctions9st_as_wktEPN9doris_udf15FunctionContextERKNS1_9StringValE',
        '', '', '', 'ALWAYS_NULLABLE'],
    [['ST_GeometryFromText', 'ST_GeomFromText'], 'VARCHAR', ['VARCHAR'],
        '_ZN5doris12GeoFunctions11st_from_wktEPN9doris_udf15FunctionContextERKNS1_9StringValE',
        '_ZN5doris12GeoFunctions19st_from_wkt_prepareEPN9doris_udf15FunctionContextENS2_18FunctionStateScopeE',
        '_ZN5doris12GeoFunctions17st_from_wkt_closeEPN9doris_udf15FunctionContextENS2_18FunctionStateScopeE',
        '', 'ALWAYS_NULLABLE'],

    [['ST_LineFromText', 'ST_LineStringFromText'], 'VARCHAR', ['VARCHAR'],
        '_ZN5doris12GeoFunctions7st_lineEPN9doris_udf15FunctionContextERKNS1_9StringValE',
        '_ZN5doris12GeoFunctions15st_line_prepareEPN9doris_udf15FunctionContextENS2_18FunctionStateScopeE',
        '_ZN5doris12GeoFunctions17st_from_wkt_closeEPN9doris_udf15FunctionContextENS2_18FunctionStateScopeE',
        '', 'ALWAYS_NULLABLE'],

    [['ST_Polygon', 'ST_PolyFromText', 'ST_PolygonFromText'], 'VARCHAR', ['VARCHAR'],
        '_ZN5doris12GeoFunctions10st_polygonEPN9doris_udf15FunctionContextERKNS1_9StringValE',
        '_ZN5doris12GeoFunctions18st_polygon_prepareEPN9doris_udf15FunctionContextENS2_18FunctionStateScopeE',
        '_ZN5doris12GeoFunctions17st_from_wkt_closeEPN9doris_udf15FunctionContextENS2_18FunctionStateScopeE',
        '', 'ALWAYS_NULLABLE'],

    [['ST_Circle'], 'VARCHAR', ['DOUBLE', 'DOUBLE', 'DOUBLE'],
        '_ZN5doris12GeoFunctions9st_circleEPN9doris_udf15FunctionContextERKNS1_9DoubleValES6_S6_',
        '_ZN5doris12GeoFunctions17st_circle_prepareEPN9doris_udf15FunctionContextENS2_18FunctionStateScopeE',
        '_ZN5doris12GeoFunctions17st_from_wkt_closeEPN9doris_udf15FunctionContextENS2_18FunctionStateScopeE',
        '', ''],

    [['ST_Contains'], 'BOOLEAN', ['VARCHAR', 'VARCHAR'],
        '_ZN5doris12GeoFunctions11st_containsEPN9doris_udf15FunctionContextERKNS1_9StringValES6_',
        '_ZN5doris12GeoFunctions19st_contains_prepareEPN9doris_udf15FunctionContextENS2_18FunctionStateScopeE',
        '_ZN5doris12GeoFunctions17st_contains_closeEPN9doris_udf15FunctionContextENS2_18FunctionStateScopeE',
        '', 'ALWAYS_NULLABLE'],
    # grouping sets functions
    [['grouping_id'], 'BIGINT', ['BIGINT'],
        '_ZN5doris21GroupingSetsFunctions11grouping_idEPN9doris_udf15FunctionContextERKNS1_9BigIntValE',
        '', '', '', 'ALWAYS_NOT_NULLABLE'],
    [['grouping'], 'BIGINT', ['BIGINT'],
        '_ZN5doris21GroupingSetsFunctions8groupingEPN9doris_udf15FunctionContextERKNS1_9BigIntValE',
        '' ,'', '', 'ALWAYS_NOT_NULLABLE'],
]

# Except the following functions, other function will directly return
# null if there is null parameters.
# Functions in this set will handle null values, not just return null.
#
# This set is only used to replace 'functions with null parameters' with NullLiteral
# when applying FoldConstantsRule rules on the FE side.
# TODO(cmy): Are these functions only required to handle null values?
non_null_result_with_null_param_functions = [
    'if',
    'hll_hash',
    'concat_ws',
    'ifnull',
    'nullif',
    'null_or_empty',
    'coalesce',
    'array'
]

# Nondeterministic functions may return different results each time they are called
nondeterministic_functions = [
    'rand',
    'now',
    'current_timestamp',
    'curdate',
    'curtime',
    'current_time',
    'utc_timestamp'
]
# This is the subset of ALWAYS_NULLABLE
# The function belongs to @null_result_with_one_null_param_functions,
# as long as one parameter is null, the function must return null.
null_result_with_one_null_param_functions = [
    'unix_timestamp',
    'str_to_date',
    'convert_tz',
    'pi',
    'e',
    'divide',
    'int_divide',
    'pmod',
    'mod',
    'fmod',
    'substr',
    'substring',
    'append_trailing_char_if_absent',
    'ST_X',
    'ST_Y',
    'ST_AsText',
    'ST_GeometryFromText',
    'ST_LineFromText',
    'ST_Polygon',
    'ST_Contains'
]

invisible_functions = [
]
