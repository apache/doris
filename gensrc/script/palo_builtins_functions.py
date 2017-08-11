# Modifications copyright (C) 2017, Baidu.com, Inc.
# Copyright 2017 The Apache Software Foundation

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
#   <prepare symbol>, <close symbol>
#
# 'sql aliases' are the function names that can be used from sql. There must be at least
# one per function.
#
# The symbol can be empty for functions that are not yet implemented or are special-cased
# in Expr::CreateExpr() (i.e., functions that are implemented via a custom Expr class
# rather than a single function).
visible_functions = [
    # Bit and Byte functions
    # For functions corresponding to builtin operators, we can reuse the implementations
    [['bitand'], 'TINYINT', ['TINYINT', 'TINYINT'], 
        '_ZN4palo9Operators32bitand_tiny_int_val_tiny_int_valEPN8palo_udf'
        '15FunctionContextERKNS1_10TinyIntValES6_'],
    [['bitand'], 'SMALLINT', ['SMALLINT', 'SMALLINT'], 
        '_ZN4palo9Operators34bitand_small_int_val_small_int_valEPN8palo_udf'
        '15FunctionContextERKNS1_11SmallIntValES6_'],
    [['bitand'], 'INT', ['INT', 'INT'], 
        '_ZN4palo9Operators22bitand_int_val_int_valEPN8palo_udf'
        '15FunctionContextERKNS1_6IntValES6_'],
    [['bitand'], 'BIGINT', ['BIGINT', 'BIGINT'],
        '_ZN4palo9Operators30bitand_big_int_val_big_int_valEPN8palo_udf'
        '15FunctionContextERKNS1_9BigIntValES6_'],
    [['bitand'], 'LARGEINT', ['LARGEINT', 'LARGEINT'],
        '_ZN4palo9Operators34bitand_large_int_val_large_int_valEPN8palo_udf'
        '15FunctionContextERKNS1_11LargeIntValES6_'],

    [['bitor'], 'TINYINT', ['TINYINT', 'TINYINT'], 
        '_ZN4palo9Operators31bitor_tiny_int_val_tiny_int_valEPN8palo_udf'
        '15FunctionContextERKNS1_10TinyIntValES6_'],
    [['bitor'], 'SMALLINT', ['SMALLINT', 'SMALLINT'], 
        '_ZN4palo9Operators33bitor_small_int_val_small_int_valEPN8palo_udf'
        '15FunctionContextERKNS1_11SmallIntValES6_'],
    [['bitor'], 'INT', ['INT', 'INT'], 
        '_ZN4palo9Operators21bitor_int_val_int_valEPN8palo_udf'
        '15FunctionContextERKNS1_6IntValES6_'],
    [['bitor'], 'BIGINT', ['BIGINT', 'BIGINT'], 
        '_ZN4palo9Operators29bitor_big_int_val_big_int_valEPN8palo_udf'
        '15FunctionContextERKNS1_9BigIntValES6_'],
    [['bitor'], 'LARGEINT', ['LARGEINT', 'LARGEINT'], 
        '_ZN4palo9Operators33bitor_large_int_val_large_int_valEPN8palo_udf'
        '15FunctionContextERKNS1_11LargeIntValES6_'],

    [['bitxor'], 'TINYINT', ['TINYINT', 'TINYINT'], 
        '_ZN4palo9Operators32bitxor_tiny_int_val_tiny_int_valEPN8palo_udf'
        '15FunctionContextERKNS1_10TinyIntValES6_'],
    [['bitxor'], 'SMALLINT', ['SMALLINT', 'SMALLINT'], 
        '_ZN4palo9Operators34bitxor_small_int_val_small_int_valEPN8palo_udf'
        '15FunctionContextERKNS1_11SmallIntValES6_'],
    [['bitxor'], 'INT', ['INT', 'INT'], 
        '_ZN4palo9Operators22bitxor_int_val_int_valEPN8palo_udf'
        '15FunctionContextERKNS1_6IntValES6_'],
    [['bitxor'], 'BIGINT', ['BIGINT', 'BIGINT'], 
        '_ZN4palo9Operators30bitxor_big_int_val_big_int_valEPN8palo_udf'
        '15FunctionContextERKNS1_9BigIntValES6_'],
    [['bitxor'], 'LARGEINT', ['LARGEINT', 'LARGEINT'], 
        '_ZN4palo9Operators34bitxor_large_int_val_large_int_valEPN8palo_udf'
        '15FunctionContextERKNS1_11LargeIntValES6_'],

    [['bitnot'], 'TINYINT', ['TINYINT'],
        '_ZN4palo9Operators19bitnot_tiny_int_valEPN8palo_udf'
        '15FunctionContextERKNS1_10TinyIntValE'],
    [['bitnot'], 'SMALLINT', ['SMALLINT'],
        '_ZN4palo9Operators20bitnot_small_int_valEPN8palo_udf'
        '15FunctionContextERKNS1_11SmallIntValE'],
    [['bitnot'], 'INT', ['INT'], 
        '_ZN4palo9Operators14bitnot_int_valEPN8palo_udf'
        '15FunctionContextERKNS1_6IntValE'],
    [['bitnot'], 'BIGINT', ['BIGINT'], 
        '_ZN4palo9Operators18bitnot_big_int_valEPN8palo_udf'
        '15FunctionContextERKNS1_9BigIntValE'],
    [['bitnot'], 'LARGEINT', ['LARGEINT'], 
        '_ZN4palo9Operators20bitnot_large_int_valEPN8palo_udf'
        '15FunctionContextERKNS1_11LargeIntValE'],

    # Timestamp functions
    [['unix_timestamp'], 'INT', [], 
        '_ZN4palo18TimestampFunctions7to_unixEPN8palo_udf15FunctionContextE'],
    [['unix_timestamp'], 'INT', ['DATETIME'], 
        '_ZN4palo18TimestampFunctions7to_unixEPN8palo_udf15FunctionContextERKNS1_11DateTimeValE'],
    [['unix_timestamp'], 'INT', ['VARCHAR', 'VARCHAR'], 
        '_ZN4palo18TimestampFunctions7to_unixEPN8palo_udf15FunctionContextERKNS1_9StringValES6_'],
    [['from_unixtime'], 'VARCHAR', ['INT'],
        '_ZN4palo18TimestampFunctions9from_unixEPN8palo_udf15FunctionContextERKNS1_6IntValE'],
    [['from_unixtime'], 'VARCHAR', ['INT', 'VARCHAR'],
        '_ZN4palo18TimestampFunctions9from_unixEPN8palo_udf'
        '15FunctionContextERKNS1_6IntValERKNS1_9StringValE'],
    [['now', 'current_timestamp'], 'DATETIME', [], 
        '_ZN4palo18TimestampFunctions3nowEPN8palo_udf15FunctionContextE'],
    [['curtime', 'current_time'], 'DATETIME', [], 
        '_ZN4palo18TimestampFunctions7curtimeEPN8palo_udf15FunctionContextE'],
    [['timestamp'], 'DATETIME', ['DATETIME'], 
        '_ZN4palo18TimestampFunctions9timestampEPN8palo_udf15FunctionContextERKNS1_11DateTimeValE'],

    [['from_days'], 'DATE', ['INT'], 
        '_ZN4palo18TimestampFunctions9from_daysEPN8palo_udf15FunctionContextERKNS1_6IntValE'],
    [['to_days'], 'INT', ['DATE'], 
        '_ZN4palo18TimestampFunctions7to_daysEPN8palo_udf15FunctionContextERKNS1_11DateTimeValE'],

    [['year'], 'INT', ['DATETIME'], 
        '_ZN4palo18TimestampFunctions4yearEPN8palo_udf15FunctionContextERKNS1_11DateTimeValE'],
    [['month'], 'INT', ['DATETIME'], 
        '_ZN4palo18TimestampFunctions5monthEPN8palo_udf15FunctionContextERKNS1_11DateTimeValE'],
    [['quarter'], 'INT', ['DATETIME'], 
        '_ZN4palo18TimestampFunctions7quarterEPN8palo_udf15FunctionContextERKNS1_11DateTimeValE'],
    [['day', 'dayofmonth'], 'INT', ['DATETIME'], 
        '_ZN4palo18TimestampFunctions12day_of_monthEPN8palo_udf'
        '15FunctionContextERKNS1_11DateTimeValE'],
    [['dayofyear'], 'INT', ['DATETIME'], 
        '_ZN4palo18TimestampFunctions11day_of_yearEPN8palo_udf'
        '15FunctionContextERKNS1_11DateTimeValE'],
    [['weekofyear'], 'INT', ['DATETIME'], 
        '_ZN4palo18TimestampFunctions12week_of_yearEPN8palo_udf'
        '15FunctionContextERKNS1_11DateTimeValE'],
    [['hour'], 'INT', ['DATETIME'], 
        '_ZN4palo18TimestampFunctions4hourEPN8palo_udf15FunctionContextERKNS1_11DateTimeValE'],
    [['minute'], 'INT', ['DATETIME'], 
        '_ZN4palo18TimestampFunctions6minuteEPN8palo_udf15FunctionContextERKNS1_11DateTimeValE'],
    [['second'], 'INT', ['DATETIME'], 
        '_ZN4palo18TimestampFunctions6secondEPN8palo_udf15FunctionContextERKNS1_11DateTimeValE'],

    [['years_add'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN4palo18TimestampFunctions9years_addEPN8palo_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE'],
    [['years_sub'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN4palo18TimestampFunctions9years_subEPN8palo_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE'],
    [['months_add', 'add_months'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN4palo18TimestampFunctions10months_addEPN8palo_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE'],
    [['months_sub'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN4palo18TimestampFunctions10months_subEPN8palo_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE'],
    [['weeks_add'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN4palo18TimestampFunctions9weeks_addEPN8palo_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE'],
    [['weeks_sub'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN4palo18TimestampFunctions9weeks_subEPN8palo_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE'],
    [['days_add', 'date_add', 'adddate'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN4palo18TimestampFunctions8days_addEPN8palo_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE'],
    [['days_sub', 'date_sub', 'subdate'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN4palo18TimestampFunctions8days_subEPN8palo_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE'],
    [['hours_add'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN4palo18TimestampFunctions9hours_addEPN8palo_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE'],
    [['hours_sub'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN4palo18TimestampFunctions9hours_subEPN8palo_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE'],
    [['minutes_add'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN4palo18TimestampFunctions11minutes_addEPN8palo_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE'],
    [['minutes_sub'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN4palo18TimestampFunctions11minutes_subEPN8palo_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE'],
    [['seconds_add'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN4palo18TimestampFunctions11seconds_addEPN8palo_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE'],
    [['seconds_sub'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN4palo18TimestampFunctions11seconds_subEPN8palo_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE'],
    [['microseconds_sub'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN4palo18TimestampFunctions10micros_addEPN8palo_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE'],
    [['microseconds_sub'], 'DATETIME', ['DATETIME', 'BIGINT'],
        '_ZN4palo18TimestampFunctions10micros_subEPN8palo_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE'],

    [['datediff'], 'INT', ['DATETIME', 'DATETIME'], 
        '_ZN4palo18TimestampFunctions9date_diffEPN8palo_udf'
        '15FunctionContextERKNS1_11DateTimeValES6_'],
    [['timediff'], 'DATETIME', ['DATETIME', 'DATETIME'], 
            '_ZN4palo18TimestampFunctions9time_diffEPN8palo_udf'
            '15FunctionContextERKNS1_11DateTimeValES6_'],

    [['str_to_date'], 'DATETIME', ['VARCHAR', 'VARCHAR'],
        '_ZN4palo18TimestampFunctions11str_to_dateEPN8palo_udf'
        '15FunctionContextERKNS1_9StringValES6_'],
    [['date_format'], 'VARCHAR', ['DATETIME', 'VARCHAR'],
        '_ZN4palo18TimestampFunctions11date_formatEPN8palo_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_9StringValE'],
    [['date', 'to_date'], 'DATE', ['DATETIME'], 
        '_ZN4palo18TimestampFunctions7to_dateEPN8palo_udf15FunctionContextERKNS1_11DateTimeValE'],

    [['dayname'], 'VARCHAR', ['DATETIME'], 
        '_ZN4palo18TimestampFunctions8day_nameEPN8palo_udf'
        '15FunctionContextERKNS1_11DateTimeValE'],
    [['monthname'], 'VARCHAR', ['DATETIME'], 
        '_ZN4palo18TimestampFunctions10month_nameEPN8palo_udf'
        '15FunctionContextERKNS1_11DateTimeValE'],

    # Math builtin functions
    [['pi'], 'DOUBLE', [], 
        '_ZN4palo13MathFunctions2piEPN8palo_udf15FunctionContextE'],
    [['e'], 'DOUBLE', [], 
        '_ZN4palo13MathFunctions1eEPN8palo_udf15FunctionContextE'],

    [['abs'], 'DOUBLE', ['DOUBLE'], 
        '_ZN4palo13MathFunctions3absEPN8palo_udf15FunctionContextERKNS1_9DoubleValE'],
    [['sign'], 'FLOAT', ['DOUBLE'], 
        '_ZN4palo13MathFunctions4signEPN8palo_udf15FunctionContextERKNS1_9DoubleValE'],

    [['sin'], 'DOUBLE', ['DOUBLE'], 
        '_ZN4palo13MathFunctions3sinEPN8palo_udf15FunctionContextERKNS1_9DoubleValE'],
    [['asin'], 'DOUBLE', ['DOUBLE'], 
        '_ZN4palo13MathFunctions4asinEPN8palo_udf15FunctionContextERKNS1_9DoubleValE'],
    [['cos'], 'DOUBLE', ['DOUBLE'], 
        '_ZN4palo13MathFunctions3cosEPN8palo_udf15FunctionContextERKNS1_9DoubleValE'],
    [['acos'], 'DOUBLE', ['DOUBLE'], 
        '_ZN4palo13MathFunctions4acosEPN8palo_udf15FunctionContextERKNS1_9DoubleValE'],
    [['tan'], 'DOUBLE', ['DOUBLE'], 
            '_ZN4palo13MathFunctions3tanEPN8palo_udf15FunctionContextERKNS1_9DoubleValE'],
    [['atan'], 'DOUBLE', ['DOUBLE'], 
            '_ZN4palo13MathFunctions4atanEPN8palo_udf15FunctionContextERKNS1_9DoubleValE'],

    [['ceil', 'ceiling', 'dceil'], 'BIGINT', ['DOUBLE'], 
            '_ZN4palo13MathFunctions4ceilEPN8palo_udf15FunctionContextERKNS1_9DoubleValE'],
    [['floor', 'dfloor'], 'BIGINT', ['DOUBLE'],
            '_ZN4palo13MathFunctions5floorEPN8palo_udf15FunctionContextERKNS1_9DoubleValE'],
    [['round', 'dround'], 'BIGINT', ['DOUBLE'],
            '_ZN4palo13MathFunctions5roundEPN8palo_udf15FunctionContextERKNS1_9DoubleValE'],
    [['round', 'dround'], 'DOUBLE', ['DOUBLE', 'INT'], 
            '_ZN4palo13MathFunctions11round_up_toEPN8palo_udf'
            '15FunctionContextERKNS1_9DoubleValERKNS1_6IntValE'],
    [['truncate'], 'DOUBLE', ['DOUBLE', 'INT'], 
            '_ZN4palo13MathFunctions8truncateEPN8palo_udf'
            '15FunctionContextERKNS1_9DoubleValERKNS1_6IntValE'],

    [['ln', 'dlog1'], 'DOUBLE', ['DOUBLE'], 
            '_ZN4palo13MathFunctions2lnEPN8palo_udf15FunctionContextERKNS1_9DoubleValE'],
    [['log'], 'DOUBLE', ['DOUBLE', 'DOUBLE'], 
            '_ZN4palo13MathFunctions3logEPN8palo_udf15FunctionContextERKNS1_9DoubleValES6_'],
    [['log2'], 'DOUBLE', ['DOUBLE'], 
            '_ZN4palo13MathFunctions4log2EPN8palo_udf15FunctionContextERKNS1_9DoubleValE'],
    [['log10', 'dlog10'], 'DOUBLE', ['DOUBLE'],
            '_ZN4palo13MathFunctions5log10EPN8palo_udf15FunctionContextERKNS1_9DoubleValE'],
    [['exp', 'dexp'], 'DOUBLE', ['DOUBLE'], 
            '_ZN4palo13MathFunctions3expEPN8palo_udf15FunctionContextERKNS1_9DoubleValE'],

    [['radians'], 'DOUBLE', ['DOUBLE'], 
            '_ZN4palo13MathFunctions7radiansEPN8palo_udf15FunctionContextERKNS1_9DoubleValE'],
    [['degrees'], 'DOUBLE', ['DOUBLE'], 
            '_ZN4palo13MathFunctions7degreesEPN8palo_udf15FunctionContextERKNS1_9DoubleValE'],

    [['sqrt', 'dsqrt'], 'DOUBLE', ['DOUBLE'],
            '_ZN4palo13MathFunctions4sqrtEPN8palo_udf15FunctionContextERKNS1_9DoubleValE'],
    [['pow', 'power', 'dpow', 'fpow'], 'DOUBLE', ['DOUBLE', 'DOUBLE'], 
            '_ZN4palo13MathFunctions3powEPN8palo_udf15FunctionContextERKNS1_9DoubleValES6_'],

    [['rand', 'random'], 'DOUBLE', [], 
            '_ZN4palo13MathFunctions4randEPN8palo_udf15FunctionContextE',
            '_ZN4palo13MathFunctions12rand_prepareEPN8palo_udf'
            '15FunctionContextENS2_18FunctionStateScopeE'],
    [['rand', 'random'], 'DOUBLE', ['BIGINT'], 
            '_ZN4palo13MathFunctions9rand_seedEPN8palo_udf15FunctionContextERKNS1_9BigIntValE',
            '_ZN4palo13MathFunctions12rand_prepareEPN8palo_udf'
            '15FunctionContextENS2_18FunctionStateScopeE'],

    [['bin'], 'VARCHAR', ['BIGINT'], 
            '_ZN4palo13MathFunctions3binEPN8palo_udf15FunctionContextERKNS1_9BigIntValE'],
    [['hex'], 'VARCHAR', ['BIGINT'], 
            '_ZN4palo13MathFunctions7hex_intEPN8palo_udf15FunctionContextERKNS1_9BigIntValE'],
    [['hex'], 'VARCHAR', ['VARCHAR'], 
            '_ZN4palo13MathFunctions10hex_stringEPN8palo_udf15FunctionContextERKNS1_9StringValE'],
    [['unhex'], 'VARCHAR', ['VARCHAR'], 
            '_ZN4palo13MathFunctions5unhexEPN8palo_udf15FunctionContextERKNS1_9StringValE'],

    [['conv'], 'VARCHAR', ['BIGINT', 'TINYINT', 'TINYINT'],
            '_ZN4palo13MathFunctions8conv_intEPN8palo_udf'
            '15FunctionContextERKNS1_9BigIntValERKNS1_10TinyIntValES9_'],
    [['conv'], 'VARCHAR', ['VARCHAR', 'TINYINT', 'TINYINT'],
            '_ZN4palo13MathFunctions11conv_stringEPN8palo_udf'
            '15FunctionContextERKNS1_9StringValERKNS1_10TinyIntValES9_'],

    [['pmod'], 'BIGINT', ['BIGINT', 'BIGINT'], 
            '_ZN4palo13MathFunctions11pmod_bigintEPN8palo_udf'
            '15FunctionContextERKNS1_9BigIntValES6_'],
    [['pmod'], 'DOUBLE', ['DOUBLE', 'DOUBLE'], 
            '_ZN4palo13MathFunctions11pmod_doubleEPN8palo_udf'
            '15FunctionContextERKNS1_9DoubleValES6_'],
    [['mod'], 'TINYINT', ['TINYINT', 'TINYINT'], 
            '_ZN4palo9Operators29mod_tiny_int_val_tiny_int_valEPN8palo_udf'
            '15FunctionContextERKNS1_10TinyIntValES6_'],
    [['mod'], 'SMALLINT', ['SMALLINT', 'SMALLINT'], 
            '_ZN4palo9Operators31mod_small_int_val_small_int_valEPN8palo_udf'
            '15FunctionContextERKNS1_11SmallIntValES6_'],
    [['mod'], 'INT', ['INT', 'INT'], 
            '_ZN4palo9Operators19mod_int_val_int_valEPN8palo_udf'
            '15FunctionContextERKNS1_6IntValES6_'],
    [['mod'], 'BIGINT', ['BIGINT', 'BIGINT'], 
            '_ZN4palo9Operators27mod_big_int_val_big_int_valEPN8palo_udf'
            '15FunctionContextERKNS1_9BigIntValES6_'],
    [['mod'], 'LARGEINT', ['LARGEINT', 'LARGEINT'], 
            '_ZN4palo9Operators31mod_large_int_val_large_int_valEPN8palo_udf'
            '15FunctionContextERKNS1_11LargeIntValES6_'],
    [['mod'], 'DECIMAL', ['DECIMAL', 'DECIMAL'], 
            '_ZN4palo16DecimalOperators27mod_decimal_val_decimal_valEPN8palo_udf'
            '15FunctionContextERKNS1_10DecimalValES6_'],
    [['mod', 'fmod'], 'FLOAT', ['FLOAT', 'FLOAT'], 
        '_ZN4palo13MathFunctions10fmod_floatEPN8palo_udf15FunctionContextERKNS1_8FloatValES6_'],
    [['mod', 'fmod'], 'DOUBLE', ['DOUBLE', 'DOUBLE'], 
        '_ZN4palo13MathFunctions11fmod_doubleEPN8palo_udf15FunctionContextERKNS1_9DoubleValES6_'],

    [['positive'], 'BIGINT', ['BIGINT'],
            '_ZN4palo13MathFunctions15positive_bigintEPN8palo_udf'
            '15FunctionContextERKNS1_9BigIntValE'],
    [['positive'], 'DOUBLE', ['DOUBLE'],
            '_ZN4palo13MathFunctions15positive_doubleEPN8palo_udf'
            '15FunctionContextERKNS1_9DoubleValE'],
    [['positive'], 'DECIMAL', ['DECIMAL'],
            '_ZN4palo13MathFunctions16positive_decimalEPN8palo_udf'
            '15FunctionContextERKNS1_10DecimalValE'],
    [['negative'], 'BIGINT', ['BIGINT'],
            '_ZN4palo13MathFunctions15negative_bigintEPN8palo_udf'
            '15FunctionContextERKNS1_9BigIntValE'],
    [['negative'], 'DOUBLE', ['DOUBLE'],
            '_ZN4palo13MathFunctions15negative_doubleEPN8palo_udf'
            '15FunctionContextERKNS1_9DoubleValE'],
    [['negative'], 'DECIMAL', ['DECIMAL'],
            '_ZN4palo13MathFunctions16negative_decimalEPN8palo_udf'
            '15FunctionContextERKNS1_10DecimalValE'],

    [['least'], 'TINYINT', ['TINYINT', '...'],
            '_ZN4palo13MathFunctions5leastEPN8palo_udf15FunctionContextEiPKNS1_10TinyIntValE'],
    [['least'], 'SMALLINT', ['SMALLINT', '...'],
            '_ZN4palo13MathFunctions5leastEPN8palo_udf15FunctionContextEiPKNS1_11SmallIntValE'],
    [['least'], 'INT', ['INT', '...'],
            '_ZN4palo13MathFunctions5leastEPN8palo_udf15FunctionContextEiPKNS1_6IntValE'],
    [['least'], 'BIGINT', ['BIGINT', '...'],
            '_ZN4palo13MathFunctions5leastEPN8palo_udf15FunctionContextEiPKNS1_9BigIntValE'],
    [['least'], 'LARGEINT', ['LARGEINT', '...'],
            '_ZN4palo13MathFunctions5leastEPN8palo_udf15FunctionContextEiPKNS1_11LargeIntValE'],
    [['least'], 'FLOAT', ['FLOAT', '...'],
            '_ZN4palo13MathFunctions5leastEPN8palo_udf15FunctionContextEiPKNS1_8FloatValE'],
    [['least'], 'DOUBLE', ['DOUBLE', '...'],
            '_ZN4palo13MathFunctions5leastEPN8palo_udf15FunctionContextEiPKNS1_9DoubleValE'],
    [['least'], 'VARCHAR', ['VARCHAR', '...'],
            '_ZN4palo13MathFunctions5leastEPN8palo_udf15FunctionContextEiPKNS1_9StringValE'],
    [['least'], 'DATETIME', ['DATETIME', '...'],
            '_ZN4palo13MathFunctions5leastEPN8palo_udf15FunctionContextEiPKNS1_11DateTimeValE'],
    [['least'], 'DECIMAL', ['DECIMAL', '...'],
            '_ZN4palo13MathFunctions5leastEPN8palo_udf15FunctionContextEiPKNS1_10DecimalValE'],

    [['greatest'], 'TINYINT', ['TINYINT', '...'],
            '_ZN4palo13MathFunctions8greatestEPN8palo_udf15FunctionContextEiPKNS1_10TinyIntValE'],
    [['greatest'], 'SMALLINT', ['SMALLINT', '...'],
            '_ZN4palo13MathFunctions8greatestEPN8palo_udf15FunctionContextEiPKNS1_11SmallIntValE'],
    [['greatest'], 'INT', ['INT', '...'],
            '_ZN4palo13MathFunctions8greatestEPN8palo_udf15FunctionContextEiPKNS1_6IntValE'],
    [['greatest'], 'BIGINT', ['BIGINT', '...'],
            '_ZN4palo13MathFunctions8greatestEPN8palo_udf15FunctionContextEiPKNS1_9BigIntValE'],
    [['greatest'], 'LARGEINT', ['LARGEINT', '...'],
            '_ZN4palo13MathFunctions8greatestEPN8palo_udf15FunctionContextEiPKNS1_11LargeIntValE'],
    [['greatest'], 'FLOAT', ['FLOAT', '...'],
            '_ZN4palo13MathFunctions8greatestEPN8palo_udf15FunctionContextEiPKNS1_8FloatValE'],
    [['greatest'], 'DOUBLE', ['DOUBLE', '...'],
            '_ZN4palo13MathFunctions8greatestEPN8palo_udf15FunctionContextEiPKNS1_9DoubleValE'],
    [['greatest'], 'VARCHAR', ['VARCHAR', '...'],
            '_ZN4palo13MathFunctions8greatestEPN8palo_udf15FunctionContextEiPKNS1_9StringValE'],
    [['greatest'], 'DATETIME', ['DATETIME', '...'],
            '_ZN4palo13MathFunctions8greatestEPN8palo_udf15FunctionContextEiPKNS1_11DateTimeValE'],
    [['greatest'], 'DECIMAL', ['DECIMAL', '...'],
            '_ZN4palo13MathFunctions8greatestEPN8palo_udf15FunctionContextEiPKNS1_10DecimalValE'],

    # Conditional Functions
    # Some of these have empty symbols because the BE special-cases them based on the
    # function name
    [['if'], 'BOOLEAN', ['BOOLEAN', 'BOOLEAN', 'BOOLEAN'], ''],
    [['if'], 'TINYINT', ['BOOLEAN', 'TINYINT', 'TINYINT'], ''],
    [['if'], 'SMALLINT', ['BOOLEAN', 'SMALLINT', 'SMALLINT'], ''],
    [['if'], 'INT', ['BOOLEAN', 'INT', 'INT'], ''],
    [['if'], 'BIGINT', ['BOOLEAN', 'BIGINT', 'BIGINT'], ''],
    [['if'], 'FLOAT', ['BOOLEAN', 'FLOAT', 'FLOAT'], ''],
    [['if'], 'DOUBLE', ['BOOLEAN', 'DOUBLE', 'DOUBLE'], ''],
    [['if'], 'VARCHAR', ['BOOLEAN', 'VARCHAR', 'VARCHAR'], ''],
    [['if'], 'DATETIME', ['BOOLEAN', 'DATETIME', 'DATETIME'], ''],
    [['if'], 'DECIMAL', ['BOOLEAN', 'DECIMAL', 'DECIMAL'], ''],

    [['nullif'], 'BOOLEAN', ['BOOLEAN', 'BOOLEAN'], ''],
    [['nullif'], 'TINYINT', ['TINYINT', 'TINYINT'], ''],
    [['nullif'], 'SMALLINT', ['SMALLINT', 'SMALLINT'], ''],
    [['nullif'], 'INT', ['INT', 'INT'], ''],
    [['nullif'], 'BIGINT', ['BIGINT', 'BIGINT'], ''],
    [['nullif'], 'FLOAT', ['FLOAT', 'FLOAT'], ''],
    [['nullif'], 'DOUBLE', ['DOUBLE', 'DOUBLE'], ''],
    [['nullif'], 'VARCHAR', ['VARCHAR', 'VARCHAR'], ''],
    [['nullif'], 'DATETIME', ['DATETIME', 'DATETIME'], ''],
    [['nullif'], 'DECIMAL', ['DECIMAL', 'DECIMAL'], ''],

    [['ifnull'], 'BOOLEAN', ['BOOLEAN', 'BOOLEAN'], ''],
    [['ifnull'], 'TINYINT', ['TINYINT', 'TINYINT'], ''],
    [['ifnull'], 'SMALLINT', ['SMALLINT', 'SMALLINT'], ''],
    [['ifnull'], 'INT', ['INT', 'INT'], ''],
    [['ifnull'], 'BIGINT', ['BIGINT', 'BIGINT'], ''],
    [['ifnull'], 'FLOAT', ['FLOAT', 'FLOAT'], ''],
    [['ifnull'], 'DOUBLE', ['DOUBLE', 'DOUBLE'], ''],
    [['ifnull'], 'VARCHAR', ['VARCHAR', 'VARCHAR'], ''],
    [['ifnull'], 'DATETIME', ['DATETIME', 'DATETIME'], ''],
    [['ifnull'], 'DECIMAL', ['DECIMAL', 'DECIMAL'], ''],

    [['coalesce'], 'BOOLEAN', ['BOOLEAN', '...'], ''],
    [['coalesce'], 'TINYINT', ['TINYINT', '...'], ''],
    [['coalesce'], 'SMALLINT', ['SMALLINT', '...'], ''],
    [['coalesce'], 'INT', ['INT', '...'], ''],
    [['coalesce'], 'BIGINT', ['BIGINT', '...'], ''],
    [['coalesce'], 'FLOAT', ['FLOAT', '...'], ''],
    [['coalesce'], 'DOUBLE', ['DOUBLE', '...'], ''],
    [['coalesce'], 'VARCHAR', ['VARCHAR', '...'], ''],
    [['coalesce'], 'DATETIME', ['DATETIME', '...'], ''],
    [['coalesce'], 'DECIMAL', ['DECIMAL', '...'], ''],

    # String builtin functions
    [['substr', 'substring'], 'VARCHAR', ['VARCHAR', 'INT'],
        '_ZN4palo15StringFunctions9substringEPN'
        '8palo_udf15FunctionContextERKNS1_9StringValERKNS1_6IntValE'],
    [['substr', 'substring'], 'VARCHAR', ['VARCHAR', 'INT', 'INT'],
        '_ZN4palo15StringFunctions9substringEPN'
        '8palo_udf15FunctionContextERKNS1_9StringValERKNS1_6IntValES9_'],
    [['strleft'], 'VARCHAR', ['VARCHAR', 'INT'], 
        '_ZN4palo15StringFunctions4leftEPN8palo_udf'
        '15FunctionContextERKNS1_9StringValERKNS1_6IntValE'],
    [['strright'], 'VARCHAR', ['VARCHAR', 'INT'], 
        '_ZN4palo15StringFunctions5rightEPN8palo_udf'
        '15FunctionContextERKNS1_9StringValERKNS1_6IntValE'],
    [['space'], 'VARCHAR', ['INT'], 
        '_ZN4palo15StringFunctions5spaceEPN8palo_udf15FunctionContextERKNS1_6IntValE'],
    [['repeat'], 'VARCHAR', ['VARCHAR', 'INT'], 
        '_ZN4palo15StringFunctions6repeatEPN8palo_udf'
        '15FunctionContextERKNS1_9StringValERKNS1_6IntValE'],
    [['lpad'], 'VARCHAR', ['VARCHAR', 'INT', 'VARCHAR'], 
            '_ZN4palo15StringFunctions4lpadEPN8palo_udf'
            '15FunctionContextERKNS1_9StringValERKNS1_6IntValES6_'],
    [['rpad'], 'VARCHAR', ['VARCHAR', 'INT', 'VARCHAR'], 
            '_ZN4palo15StringFunctions4rpadEPN8palo_udf'
            '15FunctionContextERKNS1_9StringValERKNS1_6IntValES6_'],
    [['length'], 'INT', ['VARCHAR'], 
            '_ZN4palo15StringFunctions6lengthEPN8palo_udf15FunctionContextERKNS1_9StringValE'],
    [['lower', 'lcase'], 'VARCHAR', ['VARCHAR'], 
            '_ZN4palo15StringFunctions5lowerEPN8palo_udf15FunctionContextERKNS1_9StringValE'],
    [['upper', 'ucase'], 'VARCHAR', ['VARCHAR'],
            '_ZN4palo15StringFunctions5upperEPN8palo_udf15FunctionContextERKNS1_9StringValE'],
    [['reverse'], 'VARCHAR', ['VARCHAR'], 
            '_ZN4palo15StringFunctions7reverseEPN8palo_udf15FunctionContextERKNS1_9StringValE'],
    [['trim'], 'VARCHAR', ['VARCHAR'], 
            '_ZN4palo15StringFunctions4trimEPN8palo_udf15FunctionContextERKNS1_9StringValE'],
    [['ltrim'], 'VARCHAR', ['VARCHAR'], 
            '_ZN4palo15StringFunctions5ltrimEPN8palo_udf15FunctionContextERKNS1_9StringValE'],
    [['rtrim'], 'VARCHAR', ['VARCHAR'],
            '_ZN4palo15StringFunctions5rtrimEPN8palo_udf15FunctionContextERKNS1_9StringValE'],
    [['ascii'], 'INT', ['VARCHAR'], 
            '_ZN4palo15StringFunctions5asciiEPN8palo_udf15FunctionContextERKNS1_9StringValE'],
    [['instr'], 'INT', ['VARCHAR', 'VARCHAR'], 
            '_ZN4palo15StringFunctions5instrEPN8palo_udf15FunctionContextERKNS1_9StringValES6_'],
    [['locate'], 'INT', ['VARCHAR', 'VARCHAR'],
            '_ZN4palo15StringFunctions6locateEPN8palo_udf15FunctionContextERKNS1_9StringValES6_'],
    [['locate'], 'INT', ['VARCHAR', 'VARCHAR', 'INT'],
            '_ZN4palo15StringFunctions10locate_posEPN8palo_udf'
            '15FunctionContextERKNS1_9StringValES6_RKNS1_6IntValE'],
    [['regexp_extract'], 'VARCHAR', ['VARCHAR', 'VARCHAR', 'BIGINT'],
            '_ZN4palo15StringFunctions14regexp_extractEPN8palo_udf'
            '15FunctionContextERKNS1_9StringValES6_RKNS1_9BigIntValE',
            '_ZN4palo15StringFunctions14regexp_prepareEPN8palo_udf'
            '15FunctionContextENS2_18FunctionStateScopeE',
            '_ZN4palo15StringFunctions12regexp_closeEPN8palo_udf'
            '15FunctionContextENS2_18FunctionStateScopeE'],
    [['regexp_replace'], 'VARCHAR', ['VARCHAR', 'VARCHAR', 'VARCHAR'],
            '_ZN4palo15StringFunctions14regexp_replaceEPN8palo_udf'
            '15FunctionContextERKNS1_9StringValES6_S6_',
            '_ZN4palo15StringFunctions14regexp_prepareEPN8palo_udf'
            '15FunctionContextENS2_18FunctionStateScopeE',
            '_ZN4palo15StringFunctions12regexp_closeEPN8palo_udf'
            '15FunctionContextENS2_18FunctionStateScopeE'],
    [['concat'], 'VARCHAR', ['VARCHAR', '...'], 
            '_ZN4palo15StringFunctions6concatEPN8palo_udf15FunctionContextEiPKNS1_9StringValE'],
    [['concat_ws'], 'VARCHAR', ['VARCHAR', 'VARCHAR', '...'],
            '_ZN4palo15StringFunctions9concat_wsEPN8palo_udf'
            '15FunctionContextERKNS1_9StringValEiPS5_'],
    [['find_in_set'], 'INT', ['VARCHAR', 'VARCHAR'],
            '_ZN4palo15StringFunctions11find_in_setEPN8palo_udf'
            '15FunctionContextERKNS1_9StringValES6_'],
    [['parse_url'], 'VARCHAR', ['VARCHAR', 'VARCHAR'], 
            '_ZN4palo15StringFunctions9parse_urlEPN8palo_udf'
            '15FunctionContextERKNS1_9StringValES6_',
            '_ZN4palo15StringFunctions17parse_url_prepareEPN8palo_udf'
            '15FunctionContextENS2_18FunctionStateScopeE',
            '_ZN4palo15StringFunctions15parse_url_closeEPN8palo_udf'
            '15FunctionContextENS2_18FunctionStateScopeE'],
    [['parse_url'], 'VARCHAR', ['VARCHAR', 'VARCHAR', 'VARCHAR'],
            '_ZN4palo15StringFunctions13parse_url_keyEPN8palo_udf'
            '15FunctionContextERKNS1_9StringValES6_S6_',
            '_ZN4palo15StringFunctions17parse_url_prepareEPN8palo_udf'
            '15FunctionContextENS2_18FunctionStateScopeE',
            '_ZN4palo15StringFunctions15parse_url_closeEPN8palo_udf'
            '15FunctionContextENS2_18FunctionStateScopeE'],

    # Utility functions
    [['sleep'], 'BOOLEAN', ['INT'], 
        '_ZN4palo16UtilityFunctions5sleepEPN8palo_udf15FunctionContextERKNS1_6IntValE'],
    [['version'], 'VARCHAR', [], 
        '_ZN4palo16UtilityFunctions7versionEPN8palo_udf15FunctionContextE'],

    # Json functions
    [['get_json_int'], 'INT', ['VARCHAR', 'VARCHAR'], 
        '_ZN4palo13JsonFunctions12get_json_intEPN8palo_udf15FunctionContextERKNS1_9StringValES6_'],
    [['get_json_double'], 'DOUBLE', ['VARCHAR', 'VARCHAR'], 
        '_ZN4palo13JsonFunctions15get_json_doubleEPN8palo_udf'
        '15FunctionContextERKNS1_9StringValES6_'],
    [['get_json_string'], 'VARCHAR', ['VARCHAR', 'VARCHAR'], 
        '_ZN4palo13JsonFunctions15get_json_stringEPN8palo_udf'
        '15FunctionContextERKNS1_9StringValES6_'],

    #hll function
    [['hll_cardinality'], 'VARCHAR', ['VARCHAR'],
        '_ZN4palo16HllHashFunctions15hll_cardinalityEPN8palo_udf'
        '15FunctionContextERKNS1_9StringValE'],
    [['hll_hash'], 'VARCHAR', ['VARCHAR'],
        '_ZN4palo16HllHashFunctions8hll_hashEPN8palo_udf15FunctionContextERKNS1_9StringValE'],
    
    # aes and base64 function
    [['aes_encrypt'], 'VARCHAR', ['VARCHAR', 'VARCHAR'],
        '_ZN4palo19EncryptionFunctions11aes_encryptEPN8palo_udf'
        '15FunctionContextERKNS1_9StringValES6_'],
    [['aes_decrypt'], 'VARCHAR', ['VARCHAR', 'VARCHAR'],
        '_ZN4palo19EncryptionFunctions11aes_decryptEPN8palo_udf'
        '15FunctionContextERKNS1_9StringValES6_'],
    [['from_base64'], 'VARCHAR', ['VARCHAR'],
        '_ZN4palo19EncryptionFunctions11from_base64EPN8palo_udf'
        '15FunctionContextERKNS1_9StringValE'],
    [['to_base64'], 'VARCHAR', ['VARCHAR'],
        '_ZN4palo19EncryptionFunctions9to_base64EPN8palo_udf'
        '15FunctionContextERKNS1_9StringValE'],
    # for compatable with MySQL
    [['md5'], 'VARCHAR', ['VARCHAR'],
        '_ZN4palo19EncryptionFunctions3md5EPN8palo_udf15FunctionContextERKNS1_9StringValE'],
    [['md5sum'], 'VARCHAR', ['VARCHAR', '...'],
        '_ZN4palo19EncryptionFunctions6md5sumEPN8palo_udf15FunctionContextEiPKNS1_9StringValE']
]

invisible_functions = [
]
