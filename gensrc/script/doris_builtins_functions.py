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
        '_ZN5doris9Operators32bitand_tiny_int_val_tiny_int_valEPN9doris_udf'
        '15FunctionContextERKNS1_10TinyIntValES6_'],
    [['bitand'], 'SMALLINT', ['SMALLINT', 'SMALLINT'], 
        '_ZN5doris9Operators34bitand_small_int_val_small_int_valEPN9doris_udf'
        '15FunctionContextERKNS1_11SmallIntValES6_'],
    [['bitand'], 'INT', ['INT', 'INT'], 
        '_ZN5doris9Operators22bitand_int_val_int_valEPN9doris_udf'
        '15FunctionContextERKNS1_6IntValES6_'],
    [['bitand'], 'BIGINT', ['BIGINT', 'BIGINT'],
        '_ZN5doris9Operators30bitand_big_int_val_big_int_valEPN9doris_udf'
        '15FunctionContextERKNS1_9BigIntValES6_'],
    [['bitand'], 'LARGEINT', ['LARGEINT', 'LARGEINT'],
        '_ZN5doris9Operators34bitand_large_int_val_large_int_valEPN9doris_udf'
        '15FunctionContextERKNS1_11LargeIntValES6_'],

    [['bitor'], 'TINYINT', ['TINYINT', 'TINYINT'], 
        '_ZN5doris9Operators31bitor_tiny_int_val_tiny_int_valEPN9doris_udf'
        '15FunctionContextERKNS1_10TinyIntValES6_'],
    [['bitor'], 'SMALLINT', ['SMALLINT', 'SMALLINT'], 
        '_ZN5doris9Operators33bitor_small_int_val_small_int_valEPN9doris_udf'
        '15FunctionContextERKNS1_11SmallIntValES6_'],
    [['bitor'], 'INT', ['INT', 'INT'], 
        '_ZN5doris9Operators21bitor_int_val_int_valEPN9doris_udf'
        '15FunctionContextERKNS1_6IntValES6_'],
    [['bitor'], 'BIGINT', ['BIGINT', 'BIGINT'], 
        '_ZN5doris9Operators29bitor_big_int_val_big_int_valEPN9doris_udf'
        '15FunctionContextERKNS1_9BigIntValES6_'],
    [['bitor'], 'LARGEINT', ['LARGEINT', 'LARGEINT'], 
        '_ZN5doris9Operators33bitor_large_int_val_large_int_valEPN9doris_udf'
        '15FunctionContextERKNS1_11LargeIntValES6_'],

    [['bitxor'], 'TINYINT', ['TINYINT', 'TINYINT'], 
        '_ZN5doris9Operators32bitxor_tiny_int_val_tiny_int_valEPN9doris_udf'
        '15FunctionContextERKNS1_10TinyIntValES6_'],
    [['bitxor'], 'SMALLINT', ['SMALLINT', 'SMALLINT'], 
        '_ZN5doris9Operators34bitxor_small_int_val_small_int_valEPN9doris_udf'
        '15FunctionContextERKNS1_11SmallIntValES6_'],
    [['bitxor'], 'INT', ['INT', 'INT'], 
        '_ZN5doris9Operators22bitxor_int_val_int_valEPN9doris_udf'
        '15FunctionContextERKNS1_6IntValES6_'],
    [['bitxor'], 'BIGINT', ['BIGINT', 'BIGINT'], 
        '_ZN5doris9Operators30bitxor_big_int_val_big_int_valEPN9doris_udf'
        '15FunctionContextERKNS1_9BigIntValES6_'],
    [['bitxor'], 'LARGEINT', ['LARGEINT', 'LARGEINT'], 
        '_ZN5doris9Operators34bitxor_large_int_val_large_int_valEPN9doris_udf'
        '15FunctionContextERKNS1_11LargeIntValES6_'],

    [['bitnot'], 'TINYINT', ['TINYINT'],
        '_ZN5doris9Operators19bitnot_tiny_int_valEPN9doris_udf'
        '15FunctionContextERKNS1_10TinyIntValE'],
    [['bitnot'], 'SMALLINT', ['SMALLINT'],
        '_ZN5doris9Operators20bitnot_small_int_valEPN9doris_udf'
        '15FunctionContextERKNS1_11SmallIntValE'],
    [['bitnot'], 'INT', ['INT'], 
        '_ZN5doris9Operators14bitnot_int_valEPN9doris_udf'
        '15FunctionContextERKNS1_6IntValE'],
    [['bitnot'], 'BIGINT', ['BIGINT'], 
        '_ZN5doris9Operators18bitnot_big_int_valEPN9doris_udf'
        '15FunctionContextERKNS1_9BigIntValE'],
    [['bitnot'], 'LARGEINT', ['LARGEINT'], 
        '_ZN5doris9Operators20bitnot_large_int_valEPN9doris_udf'
        '15FunctionContextERKNS1_11LargeIntValE'],

    # Timestamp functions
    [['unix_timestamp'], 'INT', [], 
        '_ZN5doris18TimestampFunctions7to_unixEPN9doris_udf15FunctionContextE'],
    [['unix_timestamp'], 'INT', ['DATETIME'], 
        '_ZN5doris18TimestampFunctions7to_unixEPN9doris_udf15FunctionContextERKNS1_11DateTimeValE'],
    [['unix_timestamp'], 'INT', ['VARCHAR', 'VARCHAR'], 
        '_ZN5doris18TimestampFunctions7to_unixEPN9doris_udf15FunctionContextERKNS1_9StringValES6_'],
    [['from_unixtime'], 'VARCHAR', ['INT'],
        '_ZN5doris18TimestampFunctions9from_unixEPN9doris_udf15FunctionContextERKNS1_6IntValE'],
    [['from_unixtime'], 'VARCHAR', ['INT', 'VARCHAR'],
        '_ZN5doris18TimestampFunctions9from_unixEPN9doris_udf'
        '15FunctionContextERKNS1_6IntValERKNS1_9StringValE'],
    [['now', 'current_timestamp'], 'DATETIME', [], 
        '_ZN5doris18TimestampFunctions3nowEPN9doris_udf15FunctionContextE'],
    [['curtime', 'current_time'], 'DATETIME', [], 
        '_ZN5doris18TimestampFunctions7curtimeEPN9doris_udf15FunctionContextE'],
    [['timestamp'], 'DATETIME', ['DATETIME'], 
        '_ZN5doris18TimestampFunctions9timestampEPN9doris_udf15FunctionContextERKNS1_11DateTimeValE'],

    [['from_days'], 'DATE', ['INT'], 
        '_ZN5doris18TimestampFunctions9from_daysEPN9doris_udf15FunctionContextERKNS1_6IntValE'],
    [['to_days'], 'INT', ['DATE'], 
        '_ZN5doris18TimestampFunctions7to_daysEPN9doris_udf15FunctionContextERKNS1_11DateTimeValE'],

    [['year'], 'INT', ['DATETIME'], 
        '_ZN5doris18TimestampFunctions4yearEPN9doris_udf15FunctionContextERKNS1_11DateTimeValE'],
    [['month'], 'INT', ['DATETIME'], 
        '_ZN5doris18TimestampFunctions5monthEPN9doris_udf15FunctionContextERKNS1_11DateTimeValE'],
    [['quarter'], 'INT', ['DATETIME'], 
        '_ZN5doris18TimestampFunctions7quarterEPN9doris_udf15FunctionContextERKNS1_11DateTimeValE'],
    [['day', 'dayofmonth'], 'INT', ['DATETIME'], 
        '_ZN5doris18TimestampFunctions12day_of_monthEPN9doris_udf'
        '15FunctionContextERKNS1_11DateTimeValE'],
    [['dayofyear'], 'INT', ['DATETIME'], 
        '_ZN5doris18TimestampFunctions11day_of_yearEPN9doris_udf'
        '15FunctionContextERKNS1_11DateTimeValE'],
    [['weekofyear'], 'INT', ['DATETIME'], 
        '_ZN5doris18TimestampFunctions12week_of_yearEPN9doris_udf'
        '15FunctionContextERKNS1_11DateTimeValE'],
    [['hour'], 'INT', ['DATETIME'], 
        '_ZN5doris18TimestampFunctions4hourEPN9doris_udf15FunctionContextERKNS1_11DateTimeValE'],
    [['minute'], 'INT', ['DATETIME'], 
        '_ZN5doris18TimestampFunctions6minuteEPN9doris_udf15FunctionContextERKNS1_11DateTimeValE'],
    [['second'], 'INT', ['DATETIME'], 
        '_ZN5doris18TimestampFunctions6secondEPN9doris_udf15FunctionContextERKNS1_11DateTimeValE'],

    [['years_add'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN5doris18TimestampFunctions9years_addEPN9doris_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE'],
    [['years_sub'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN5doris18TimestampFunctions9years_subEPN9doris_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE'],
    [['months_add', 'add_months'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN5doris18TimestampFunctions10months_addEPN9doris_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE'],
    [['months_sub'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN5doris18TimestampFunctions10months_subEPN9doris_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE'],
    [['weeks_add'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN5doris18TimestampFunctions9weeks_addEPN9doris_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE'],
    [['weeks_sub'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN5doris18TimestampFunctions9weeks_subEPN9doris_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE'],
    [['days_add', 'date_add', 'adddate'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN5doris18TimestampFunctions8days_addEPN9doris_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE'],
    [['days_sub', 'date_sub', 'subdate'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN5doris18TimestampFunctions8days_subEPN9doris_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE'],
    [['hours_add'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN5doris18TimestampFunctions9hours_addEPN9doris_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE'],
    [['hours_sub'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN5doris18TimestampFunctions9hours_subEPN9doris_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE'],
    [['minutes_add'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN5doris18TimestampFunctions11minutes_addEPN9doris_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE'],
    [['minutes_sub'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN5doris18TimestampFunctions11minutes_subEPN9doris_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE'],
    [['seconds_add'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN5doris18TimestampFunctions11seconds_addEPN9doris_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE'],
    [['seconds_sub'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN5doris18TimestampFunctions11seconds_subEPN9doris_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE'],
    [['microseconds_add'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN5doris18TimestampFunctions10micros_addEPN9doris_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE'],
    [['microseconds_sub'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN5doris18TimestampFunctions10micros_subEPN9doris_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE'],

    [['datediff'], 'INT', ['DATETIME', 'DATETIME'], 
        '_ZN5doris18TimestampFunctions9date_diffEPN9doris_udf'
        '15FunctionContextERKNS1_11DateTimeValES6_'],
    [['timediff'], 'DATETIME', ['DATETIME', 'DATETIME'], 
            '_ZN5doris18TimestampFunctions9time_diffEPN9doris_udf'
            '15FunctionContextERKNS1_11DateTimeValES6_'],

    [['str_to_date'], 'DATETIME', ['VARCHAR', 'VARCHAR'],
        '_ZN5doris18TimestampFunctions11str_to_dateEPN9doris_udf'
        '15FunctionContextERKNS1_9StringValES6_'],
    [['date_format'], 'VARCHAR', ['DATETIME', 'VARCHAR'],
        '_ZN5doris18TimestampFunctions11date_formatEPN9doris_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_9StringValE'],
    [['date', 'to_date'], 'DATE', ['DATETIME'], 
        '_ZN5doris18TimestampFunctions7to_dateEPN9doris_udf15FunctionContextERKNS1_11DateTimeValE'],

    [['dayname'], 'VARCHAR', ['DATETIME'], 
        '_ZN5doris18TimestampFunctions8day_nameEPN9doris_udf'
        '15FunctionContextERKNS1_11DateTimeValE'],
    [['monthname'], 'VARCHAR', ['DATETIME'], 
        '_ZN5doris18TimestampFunctions10month_nameEPN9doris_udf'
        '15FunctionContextERKNS1_11DateTimeValE'],

    # Math builtin functions
    [['pi'], 'DOUBLE', [], 
        '_ZN5doris13MathFunctions2piEPN9doris_udf15FunctionContextE'],
    [['e'], 'DOUBLE', [], 
        '_ZN5doris13MathFunctions1eEPN9doris_udf15FunctionContextE'],

    [['abs'], 'DOUBLE', ['DOUBLE'], 
        '_ZN5doris13MathFunctions3absEPN9doris_udf15FunctionContextERKNS1_9DoubleValE'],
    [['sign'], 'FLOAT', ['DOUBLE'], 
        '_ZN5doris13MathFunctions4signEPN9doris_udf15FunctionContextERKNS1_9DoubleValE'],

    [['sin'], 'DOUBLE', ['DOUBLE'], 
        '_ZN5doris13MathFunctions3sinEPN9doris_udf15FunctionContextERKNS1_9DoubleValE'],
    [['asin'], 'DOUBLE', ['DOUBLE'], 
        '_ZN5doris13MathFunctions4asinEPN9doris_udf15FunctionContextERKNS1_9DoubleValE'],
    [['cos'], 'DOUBLE', ['DOUBLE'], 
        '_ZN5doris13MathFunctions3cosEPN9doris_udf15FunctionContextERKNS1_9DoubleValE'],
    [['acos'], 'DOUBLE', ['DOUBLE'], 
        '_ZN5doris13MathFunctions4acosEPN9doris_udf15FunctionContextERKNS1_9DoubleValE'],
    [['tan'], 'DOUBLE', ['DOUBLE'], 
            '_ZN5doris13MathFunctions3tanEPN9doris_udf15FunctionContextERKNS1_9DoubleValE'],
    [['atan'], 'DOUBLE', ['DOUBLE'], 
            '_ZN5doris13MathFunctions4atanEPN9doris_udf15FunctionContextERKNS1_9DoubleValE'],

    [['ceil', 'ceiling', 'dceil'], 'BIGINT', ['DOUBLE'], 
            '_ZN5doris13MathFunctions4ceilEPN9doris_udf15FunctionContextERKNS1_9DoubleValE'],
    [['floor', 'dfloor'], 'BIGINT', ['DOUBLE'],
            '_ZN5doris13MathFunctions5floorEPN9doris_udf15FunctionContextERKNS1_9DoubleValE'],
    [['round', 'dround'], 'BIGINT', ['DOUBLE'],
            '_ZN5doris13MathFunctions5roundEPN9doris_udf15FunctionContextERKNS1_9DoubleValE'],
    [['round', 'dround'], 'DOUBLE', ['DOUBLE', 'INT'], 
            '_ZN5doris13MathFunctions11round_up_toEPN9doris_udf'
            '15FunctionContextERKNS1_9DoubleValERKNS1_6IntValE'],
    [['truncate'], 'DOUBLE', ['DOUBLE', 'INT'], 
            '_ZN5doris13MathFunctions8truncateEPN9doris_udf'
            '15FunctionContextERKNS1_9DoubleValERKNS1_6IntValE'],

    [['ln', 'dlog1'], 'DOUBLE', ['DOUBLE'], 
            '_ZN5doris13MathFunctions2lnEPN9doris_udf15FunctionContextERKNS1_9DoubleValE'],
    [['log'], 'DOUBLE', ['DOUBLE', 'DOUBLE'], 
            '_ZN5doris13MathFunctions3logEPN9doris_udf15FunctionContextERKNS1_9DoubleValES6_'],
    [['log2'], 'DOUBLE', ['DOUBLE'], 
            '_ZN5doris13MathFunctions4log2EPN9doris_udf15FunctionContextERKNS1_9DoubleValE'],
    [['log10', 'dlog10'], 'DOUBLE', ['DOUBLE'],
            '_ZN5doris13MathFunctions5log10EPN9doris_udf15FunctionContextERKNS1_9DoubleValE'],
    [['exp', 'dexp'], 'DOUBLE', ['DOUBLE'], 
            '_ZN5doris13MathFunctions3expEPN9doris_udf15FunctionContextERKNS1_9DoubleValE'],

    [['radians'], 'DOUBLE', ['DOUBLE'], 
            '_ZN5doris13MathFunctions7radiansEPN9doris_udf15FunctionContextERKNS1_9DoubleValE'],
    [['degrees'], 'DOUBLE', ['DOUBLE'], 
            '_ZN5doris13MathFunctions7degreesEPN9doris_udf15FunctionContextERKNS1_9DoubleValE'],

    [['sqrt', 'dsqrt'], 'DOUBLE', ['DOUBLE'],
            '_ZN5doris13MathFunctions4sqrtEPN9doris_udf15FunctionContextERKNS1_9DoubleValE'],
    [['pow', 'power', 'dpow', 'fpow'], 'DOUBLE', ['DOUBLE', 'DOUBLE'], 
            '_ZN5doris13MathFunctions3powEPN9doris_udf15FunctionContextERKNS1_9DoubleValES6_'],

    [['rand', 'random'], 'DOUBLE', [], 
            '_ZN5doris13MathFunctions4randEPN9doris_udf15FunctionContextE',
            '_ZN5doris13MathFunctions12rand_prepareEPN9doris_udf'
            '15FunctionContextENS2_18FunctionStateScopeE'],
    [['rand', 'random'], 'DOUBLE', ['BIGINT'], 
            '_ZN5doris13MathFunctions9rand_seedEPN9doris_udf15FunctionContextERKNS1_9BigIntValE',
            '_ZN5doris13MathFunctions12rand_prepareEPN9doris_udf'
            '15FunctionContextENS2_18FunctionStateScopeE'],

    [['bin'], 'VARCHAR', ['BIGINT'], 
            '_ZN5doris13MathFunctions3binEPN9doris_udf15FunctionContextERKNS1_9BigIntValE'],
    [['hex'], 'VARCHAR', ['BIGINT'], 
            '_ZN5doris13MathFunctions7hex_intEPN9doris_udf15FunctionContextERKNS1_9BigIntValE'],
    [['hex'], 'VARCHAR', ['VARCHAR'], 
            '_ZN5doris13MathFunctions10hex_stringEPN9doris_udf15FunctionContextERKNS1_9StringValE'],
    [['unhex'], 'VARCHAR', ['VARCHAR'], 
            '_ZN5doris13MathFunctions5unhexEPN9doris_udf15FunctionContextERKNS1_9StringValE'],

    [['conv'], 'VARCHAR', ['BIGINT', 'TINYINT', 'TINYINT'],
            '_ZN5doris13MathFunctions8conv_intEPN9doris_udf'
            '15FunctionContextERKNS1_9BigIntValERKNS1_10TinyIntValES9_'],
    [['conv'], 'VARCHAR', ['VARCHAR', 'TINYINT', 'TINYINT'],
            '_ZN5doris13MathFunctions11conv_stringEPN9doris_udf'
            '15FunctionContextERKNS1_9StringValERKNS1_10TinyIntValES9_'],

    [['pmod'], 'BIGINT', ['BIGINT', 'BIGINT'], 
            '_ZN5doris13MathFunctions11pmod_bigintEPN9doris_udf'
            '15FunctionContextERKNS1_9BigIntValES6_'],
    [['pmod'], 'DOUBLE', ['DOUBLE', 'DOUBLE'], 
            '_ZN5doris13MathFunctions11pmod_doubleEPN9doris_udf'
            '15FunctionContextERKNS1_9DoubleValES6_'],
    [['mod'], 'TINYINT', ['TINYINT', 'TINYINT'], 
            '_ZN5doris9Operators29mod_tiny_int_val_tiny_int_valEPN9doris_udf'
            '15FunctionContextERKNS1_10TinyIntValES6_'],
    [['mod'], 'SMALLINT', ['SMALLINT', 'SMALLINT'], 
            '_ZN5doris9Operators31mod_small_int_val_small_int_valEPN9doris_udf'
            '15FunctionContextERKNS1_11SmallIntValES6_'],
    [['mod'], 'INT', ['INT', 'INT'], 
            '_ZN5doris9Operators19mod_int_val_int_valEPN9doris_udf'
            '15FunctionContextERKNS1_6IntValES6_'],
    [['mod'], 'BIGINT', ['BIGINT', 'BIGINT'], 
            '_ZN5doris9Operators27mod_big_int_val_big_int_valEPN9doris_udf'
            '15FunctionContextERKNS1_9BigIntValES6_'],
    [['mod'], 'LARGEINT', ['LARGEINT', 'LARGEINT'], 
            '_ZN5doris9Operators31mod_large_int_val_large_int_valEPN9doris_udf'
            '15FunctionContextERKNS1_11LargeIntValES6_'],
    [['mod'], 'DECIMAL', ['DECIMAL', 'DECIMAL'], 
            '_ZN5doris16DecimalOperators27mod_decimal_val_decimal_valEPN9doris_udf'
            '15FunctionContextERKNS1_10DecimalValES6_'],
    [['mod'], 'DECIMALV2', ['DECIMALV2', 'DECIMALV2'], 
            '_ZN5doris18DecimalV2Operators31mod_decimalv2_val_decimalv2_valEPN9doris_udf'
            '15FunctionContextERKNS1_12DecimalV2ValES6_'],
    [['mod', 'fmod'], 'FLOAT', ['FLOAT', 'FLOAT'], 
        '_ZN5doris13MathFunctions10fmod_floatEPN9doris_udf15FunctionContextERKNS1_8FloatValES6_'],
    [['mod', 'fmod'], 'DOUBLE', ['DOUBLE', 'DOUBLE'], 
        '_ZN5doris13MathFunctions11fmod_doubleEPN9doris_udf15FunctionContextERKNS1_9DoubleValES6_'],

    [['positive'], 'BIGINT', ['BIGINT'],
            '_ZN5doris13MathFunctions15positive_bigintEPN9doris_udf'
            '15FunctionContextERKNS1_9BigIntValE'],
    [['positive'], 'DOUBLE', ['DOUBLE'],
            '_ZN5doris13MathFunctions15positive_doubleEPN9doris_udf'
            '15FunctionContextERKNS1_9DoubleValE'],
    [['positive'], 'DECIMAL', ['DECIMAL'],
            '_ZN5doris13MathFunctions16positive_decimalEPN9doris_udf'
            '15FunctionContextERKNS1_10DecimalValE'],
    [['positive'], 'DECIMALV2', ['DECIMALV2'],
            '_ZN5doris13MathFunctions16positive_decimalEPN9doris_udf'
            '15FunctionContextERKNS1_12DecimalV2ValE'],
    [['negative'], 'BIGINT', ['BIGINT'],
            '_ZN5doris13MathFunctions15negative_bigintEPN9doris_udf'
            '15FunctionContextERKNS1_9BigIntValE'],
    [['negative'], 'DOUBLE', ['DOUBLE'],
            '_ZN5doris13MathFunctions15negative_doubleEPN9doris_udf'
            '15FunctionContextERKNS1_9DoubleValE'],
    [['negative'], 'DECIMAL', ['DECIMAL'],
            '_ZN5doris13MathFunctions16negative_decimalEPN9doris_udf'
            '15FunctionContextERKNS1_10DecimalValE'],
    [['negative'], 'DECIMALV2', ['DECIMALV2'],
            '_ZN5doris13MathFunctions16negative_decimalEPN9doris_udf'
            '15FunctionContextERKNS1_12DecimalV2ValE'],

    [['least'], 'TINYINT', ['TINYINT', '...'],
            '_ZN5doris13MathFunctions5leastEPN9doris_udf15FunctionContextEiPKNS1_10TinyIntValE'],
    [['least'], 'SMALLINT', ['SMALLINT', '...'],
            '_ZN5doris13MathFunctions5leastEPN9doris_udf15FunctionContextEiPKNS1_11SmallIntValE'],
    [['least'], 'INT', ['INT', '...'],
            '_ZN5doris13MathFunctions5leastEPN9doris_udf15FunctionContextEiPKNS1_6IntValE'],
    [['least'], 'BIGINT', ['BIGINT', '...'],
            '_ZN5doris13MathFunctions5leastEPN9doris_udf15FunctionContextEiPKNS1_9BigIntValE'],
    [['least'], 'LARGEINT', ['LARGEINT', '...'],
            '_ZN5doris13MathFunctions5leastEPN9doris_udf15FunctionContextEiPKNS1_11LargeIntValE'],
    [['least'], 'FLOAT', ['FLOAT', '...'],
            '_ZN5doris13MathFunctions5leastEPN9doris_udf15FunctionContextEiPKNS1_8FloatValE'],
    [['least'], 'DOUBLE', ['DOUBLE', '...'],
            '_ZN5doris13MathFunctions5leastEPN9doris_udf15FunctionContextEiPKNS1_9DoubleValE'],
    [['least'], 'VARCHAR', ['VARCHAR', '...'],
            '_ZN5doris13MathFunctions5leastEPN9doris_udf15FunctionContextEiPKNS1_9StringValE'],
    [['least'], 'DATETIME', ['DATETIME', '...'],
            '_ZN5doris13MathFunctions5leastEPN9doris_udf15FunctionContextEiPKNS1_11DateTimeValE'],
    [['least'], 'DECIMAL', ['DECIMAL', '...'],
            '_ZN5doris13MathFunctions5leastEPN9doris_udf15FunctionContextEiPKNS1_10DecimalValE'],
    [['least'], 'DECIMALV2', ['DECIMALV2', '...'],
            '_ZN5doris13MathFunctions5leastEPN9doris_udf15FunctionContextEiPKNS1_12DecimalV2ValE'],

    [['greatest'], 'TINYINT', ['TINYINT', '...'],
            '_ZN5doris13MathFunctions8greatestEPN9doris_udf15FunctionContextEiPKNS1_10TinyIntValE'],
    [['greatest'], 'SMALLINT', ['SMALLINT', '...'],
            '_ZN5doris13MathFunctions8greatestEPN9doris_udf15FunctionContextEiPKNS1_11SmallIntValE'],
    [['greatest'], 'INT', ['INT', '...'],
            '_ZN5doris13MathFunctions8greatestEPN9doris_udf15FunctionContextEiPKNS1_6IntValE'],
    [['greatest'], 'BIGINT', ['BIGINT', '...'],
            '_ZN5doris13MathFunctions8greatestEPN9doris_udf15FunctionContextEiPKNS1_9BigIntValE'],
    [['greatest'], 'LARGEINT', ['LARGEINT', '...'],
            '_ZN5doris13MathFunctions8greatestEPN9doris_udf15FunctionContextEiPKNS1_11LargeIntValE'],
    [['greatest'], 'FLOAT', ['FLOAT', '...'],
            '_ZN5doris13MathFunctions8greatestEPN9doris_udf15FunctionContextEiPKNS1_8FloatValE'],
    [['greatest'], 'DOUBLE', ['DOUBLE', '...'],
            '_ZN5doris13MathFunctions8greatestEPN9doris_udf15FunctionContextEiPKNS1_9DoubleValE'],
    [['greatest'], 'DECIMAL', ['DECIMAL', '...'],
            '_ZN5doris13MathFunctions8greatestEPN9doris_udf15FunctionContextEiPKNS1_10DecimalValE'],
    [['greatest'], 'DECIMALV2', ['DECIMALV2', '...'],
            '_ZN5doris13MathFunctions8greatestEPN9doris_udf15FunctionContextEiPKNS1_12DecimalV2ValE'],
    [['greatest'], 'VARCHAR', ['VARCHAR', '...'],
            '_ZN5doris13MathFunctions8greatestEPN9doris_udf15FunctionContextEiPKNS1_9StringValE'],
    [['greatest'], 'DATETIME', ['DATETIME', '...'],
            '_ZN5doris13MathFunctions8greatestEPN9doris_udf15FunctionContextEiPKNS1_11DateTimeValE'],

    # Conditional Functions
    # Some of these have empty symbols because the BE special-cases them based on the
    # function name
    [['if'], 'BOOLEAN', ['BOOLEAN', 'BOOLEAN', 'BOOLEAN'], ''],
    [['if'], 'TINYINT', ['BOOLEAN', 'TINYINT', 'TINYINT'], ''],
    [['if'], 'SMALLINT', ['BOOLEAN', 'SMALLINT', 'SMALLINT'], ''],
    [['if'], 'INT', ['BOOLEAN', 'INT', 'INT'], ''],
    [['if'], 'BIGINT', ['BOOLEAN', 'BIGINT', 'BIGINT'], ''],
    [['if'], 'LARGEINT', ['BOOLEAN', 'LARGEINT', 'LARGEINT'], ''],
    [['if'], 'FLOAT', ['BOOLEAN', 'FLOAT', 'FLOAT'], ''],
    [['if'], 'DOUBLE', ['BOOLEAN', 'DOUBLE', 'DOUBLE'], ''],
    [['if'], 'VARCHAR', ['BOOLEAN', 'VARCHAR', 'VARCHAR'], ''],
    [['if'], 'DATETIME', ['BOOLEAN', 'DATETIME', 'DATETIME'], ''],
    [['if'], 'DECIMAL', ['BOOLEAN', 'DECIMAL', 'DECIMAL'], ''],
    [['if'], 'DECIMALV2', ['BOOLEAN', 'DECIMALV2', 'DECIMALV2'], ''],

    [['nullif'], 'BOOLEAN', ['BOOLEAN', 'BOOLEAN'], ''],
    [['nullif'], 'TINYINT', ['TINYINT', 'TINYINT'], ''],
    [['nullif'], 'SMALLINT', ['SMALLINT', 'SMALLINT'], ''],
    [['nullif'], 'INT', ['INT', 'INT'], ''],
    [['nullif'], 'BIGINT', ['BIGINT', 'BIGINT'], ''],
    [['nullif'], 'LARGEINT', ['LARGEINT', 'LARGEINT'], ''],
    [['nullif'], 'FLOAT', ['FLOAT', 'FLOAT'], ''],
    [['nullif'], 'DOUBLE', ['DOUBLE', 'DOUBLE'], ''],
    [['nullif'], 'VARCHAR', ['VARCHAR', 'VARCHAR'], ''],
    [['nullif'], 'DATETIME', ['DATETIME', 'DATETIME'], ''],
    [['nullif'], 'DECIMAL', ['DECIMAL', 'DECIMAL'], ''],
    [['nullif'], 'DECIMALV2', ['DECIMALV2', 'DECIMALV2'], ''],

    [['ifnull'], 'BOOLEAN', ['BOOLEAN', 'BOOLEAN'], ''],
    [['ifnull'], 'TINYINT', ['TINYINT', 'TINYINT'], ''],
    [['ifnull'], 'SMALLINT', ['SMALLINT', 'SMALLINT'], ''],
    [['ifnull'], 'INT', ['INT', 'INT'], ''],
    [['ifnull'], 'BIGINT', ['BIGINT', 'BIGINT'], ''],
    [['ifnull'], 'LARGEINT', ['LARGEINT', 'LARGEINT'], ''],
    [['ifnull'], 'FLOAT', ['FLOAT', 'FLOAT'], ''],
    [['ifnull'], 'DOUBLE', ['DOUBLE', 'DOUBLE'], ''],
    [['ifnull'], 'VARCHAR', ['VARCHAR', 'VARCHAR'], ''],
    [['ifnull'], 'DATETIME', ['DATETIME', 'DATETIME'], ''],
    [['ifnull'], 'DECIMAL', ['DECIMAL', 'DECIMAL'], ''],
    [['ifnull'], 'DECIMALV2', ['DECIMALV2', 'DECIMALV2'], ''],

    [['coalesce'], 'BOOLEAN', ['BOOLEAN', '...'], ''],
    [['coalesce'], 'TINYINT', ['TINYINT', '...'], ''],
    [['coalesce'], 'SMALLINT', ['SMALLINT', '...'], ''],
    [['coalesce'], 'INT', ['INT', '...'], ''],
    [['coalesce'], 'BIGINT', ['BIGINT', '...'], ''],
    [['coalesce'], 'LARGEINT', ['LARGEINT', '...'], ''],
    [['coalesce'], 'FLOAT', ['FLOAT', '...'], ''],
    [['coalesce'], 'DOUBLE', ['DOUBLE', '...'], ''],
    [['coalesce'], 'VARCHAR', ['VARCHAR', '...'], ''],
    [['coalesce'], 'DATETIME', ['DATETIME', '...'], ''],
    [['coalesce'], 'DECIMAL', ['DECIMAL', '...'], ''],
    [['coalesce'], 'DECIMALV2', ['DECIMALV2', '...'], ''],

    [['esquery'], 'BOOLEAN', ['VARCHAR', 'VARCHAR'], 
        '_ZN5doris11ESFunctions5matchEPN'
        '9doris_udf15FunctionContextERKNS1_9StringValES6_'],

    # String builtin functions
    [['substr', 'substring'], 'VARCHAR', ['VARCHAR', 'INT'],
        '_ZN5doris15StringFunctions9substringEPN'
        '9doris_udf15FunctionContextERKNS1_9StringValERKNS1_6IntValE'],
    [['substr', 'substring'], 'VARCHAR', ['VARCHAR', 'INT', 'INT'],
        '_ZN5doris15StringFunctions9substringEPN'
        '9doris_udf15FunctionContextERKNS1_9StringValERKNS1_6IntValES9_'],
    [['strleft'], 'VARCHAR', ['VARCHAR', 'INT'], 
        '_ZN5doris15StringFunctions4leftEPN9doris_udf'
        '15FunctionContextERKNS1_9StringValERKNS1_6IntValE'],
    [['strright'], 'VARCHAR', ['VARCHAR', 'INT'], 
        '_ZN5doris15StringFunctions5rightEPN9doris_udf'
        '15FunctionContextERKNS1_9StringValERKNS1_6IntValE'],
    [['space'], 'VARCHAR', ['INT'], 
        '_ZN5doris15StringFunctions5spaceEPN9doris_udf15FunctionContextERKNS1_6IntValE'],
    [['repeat'], 'VARCHAR', ['VARCHAR', 'INT'], 
        '_ZN5doris15StringFunctions6repeatEPN9doris_udf'
        '15FunctionContextERKNS1_9StringValERKNS1_6IntValE'],
    [['lpad'], 'VARCHAR', ['VARCHAR', 'INT', 'VARCHAR'], 
            '_ZN5doris15StringFunctions4lpadEPN9doris_udf'
            '15FunctionContextERKNS1_9StringValERKNS1_6IntValES6_'],
    [['rpad'], 'VARCHAR', ['VARCHAR', 'INT', 'VARCHAR'], 
            '_ZN5doris15StringFunctions4rpadEPN9doris_udf'
            '15FunctionContextERKNS1_9StringValERKNS1_6IntValES6_'],
    [['length'], 'INT', ['VARCHAR'], 
            '_ZN5doris15StringFunctions6lengthEPN9doris_udf15FunctionContextERKNS1_9StringValE'],
    [['lower', 'lcase'], 'VARCHAR', ['VARCHAR'], 
            '_ZN5doris15StringFunctions5lowerEPN9doris_udf15FunctionContextERKNS1_9StringValE'],
    [['upper', 'ucase'], 'VARCHAR', ['VARCHAR'],
            '_ZN5doris15StringFunctions5upperEPN9doris_udf15FunctionContextERKNS1_9StringValE'],
    [['reverse'], 'VARCHAR', ['VARCHAR'], 
            '_ZN5doris15StringFunctions7reverseEPN9doris_udf15FunctionContextERKNS1_9StringValE'],
    [['trim'], 'VARCHAR', ['VARCHAR'], 
            '_ZN5doris15StringFunctions4trimEPN9doris_udf15FunctionContextERKNS1_9StringValE'],
    [['ltrim'], 'VARCHAR', ['VARCHAR'], 
            '_ZN5doris15StringFunctions5ltrimEPN9doris_udf15FunctionContextERKNS1_9StringValE'],
    [['rtrim'], 'VARCHAR', ['VARCHAR'],
            '_ZN5doris15StringFunctions5rtrimEPN9doris_udf15FunctionContextERKNS1_9StringValE'],
    [['ascii'], 'INT', ['VARCHAR'], 
            '_ZN5doris15StringFunctions5asciiEPN9doris_udf15FunctionContextERKNS1_9StringValE'],
    [['instr'], 'INT', ['VARCHAR', 'VARCHAR'], 
            '_ZN5doris15StringFunctions5instrEPN9doris_udf15FunctionContextERKNS1_9StringValES6_'],
    [['locate'], 'INT', ['VARCHAR', 'VARCHAR'],
            '_ZN5doris15StringFunctions6locateEPN9doris_udf15FunctionContextERKNS1_9StringValES6_'],
    [['locate'], 'INT', ['VARCHAR', 'VARCHAR', 'INT'],
            '_ZN5doris15StringFunctions10locate_posEPN9doris_udf'
            '15FunctionContextERKNS1_9StringValES6_RKNS1_6IntValE'],
    [['regexp_extract'], 'VARCHAR', ['VARCHAR', 'VARCHAR', 'BIGINT'],
            '_ZN5doris15StringFunctions14regexp_extractEPN9doris_udf'
            '15FunctionContextERKNS1_9StringValES6_RKNS1_9BigIntValE',
            '_ZN5doris15StringFunctions14regexp_prepareEPN9doris_udf'
            '15FunctionContextENS2_18FunctionStateScopeE',
            '_ZN5doris15StringFunctions12regexp_closeEPN9doris_udf'
            '15FunctionContextENS2_18FunctionStateScopeE'],
    [['regexp_replace'], 'VARCHAR', ['VARCHAR', 'VARCHAR', 'VARCHAR'],
            '_ZN5doris15StringFunctions14regexp_replaceEPN9doris_udf'
            '15FunctionContextERKNS1_9StringValES6_S6_',
            '_ZN5doris15StringFunctions14regexp_prepareEPN9doris_udf'
            '15FunctionContextENS2_18FunctionStateScopeE',
            '_ZN5doris15StringFunctions12regexp_closeEPN9doris_udf'
            '15FunctionContextENS2_18FunctionStateScopeE'],
    [['concat'], 'VARCHAR', ['VARCHAR', '...'], 
            '_ZN5doris15StringFunctions6concatEPN9doris_udf15FunctionContextEiPKNS1_9StringValE'],
    [['concat_ws'], 'VARCHAR', ['VARCHAR', 'VARCHAR', '...'],
            '_ZN5doris15StringFunctions9concat_wsEPN9doris_udf'
            '15FunctionContextERKNS1_9StringValEiPS5_'],
    [['find_in_set'], 'INT', ['VARCHAR', 'VARCHAR'],
            '_ZN5doris15StringFunctions11find_in_setEPN9doris_udf'
            '15FunctionContextERKNS1_9StringValES6_'],
    [['parse_url'], 'VARCHAR', ['VARCHAR', 'VARCHAR'], 
            '_ZN5doris15StringFunctions9parse_urlEPN9doris_udf'
            '15FunctionContextERKNS1_9StringValES6_',
            '_ZN5doris15StringFunctions17parse_url_prepareEPN9doris_udf'
            '15FunctionContextENS2_18FunctionStateScopeE',
            '_ZN5doris15StringFunctions15parse_url_closeEPN9doris_udf'
            '15FunctionContextENS2_18FunctionStateScopeE'],
    [['parse_url'], 'VARCHAR', ['VARCHAR', 'VARCHAR', 'VARCHAR'],
            '_ZN5doris15StringFunctions13parse_url_keyEPN9doris_udf'
            '15FunctionContextERKNS1_9StringValES6_S6_',
            '_ZN5doris15StringFunctions17parse_url_prepareEPN9doris_udf'
            '15FunctionContextENS2_18FunctionStateScopeE',
            '_ZN5doris15StringFunctions15parse_url_closeEPN9doris_udf'
            '15FunctionContextENS2_18FunctionStateScopeE'],

    # Utility functions
    [['sleep'], 'BOOLEAN', ['INT'], 
        '_ZN5doris16UtilityFunctions5sleepEPN9doris_udf15FunctionContextERKNS1_6IntValE'],
    [['version'], 'VARCHAR', [], 
        '_ZN5doris16UtilityFunctions7versionEPN9doris_udf15FunctionContextE'],

    # Json functions
    [['get_json_int'], 'INT', ['VARCHAR', 'VARCHAR'], 
        '_ZN5doris13JsonFunctions12get_json_intEPN9doris_udf15FunctionContextERKNS1_9StringValES6_'],
    [['get_json_double'], 'DOUBLE', ['VARCHAR', 'VARCHAR'], 
        '_ZN5doris13JsonFunctions15get_json_doubleEPN9doris_udf'
        '15FunctionContextERKNS1_9StringValES6_'],
    [['get_json_string'], 'VARCHAR', ['VARCHAR', 'VARCHAR'], 
        '_ZN5doris13JsonFunctions15get_json_stringEPN9doris_udf'
        '15FunctionContextERKNS1_9StringValES6_'],

    #hll function
    [['hll_cardinality'], 'VARCHAR', ['VARCHAR'],
        '_ZN5doris16HllHashFunctions15hll_cardinalityEPN9doris_udf'
        '15FunctionContextERKNS1_9StringValE'],
    [['hll_hash'], 'VARCHAR', ['VARCHAR'],
        '_ZN5doris16HllHashFunctions8hll_hashEPN9doris_udf15FunctionContextERKNS1_9StringValE'],
    
    # aes and base64 function
    [['aes_encrypt'], 'VARCHAR', ['VARCHAR', 'VARCHAR'],
        '_ZN5doris19EncryptionFunctions11aes_encryptEPN9doris_udf'
        '15FunctionContextERKNS1_9StringValES6_'],
    [['aes_decrypt'], 'VARCHAR', ['VARCHAR', 'VARCHAR'],
        '_ZN5doris19EncryptionFunctions11aes_decryptEPN9doris_udf'
        '15FunctionContextERKNS1_9StringValES6_'],
    [['from_base64'], 'VARCHAR', ['VARCHAR'],
        '_ZN5doris19EncryptionFunctions11from_base64EPN9doris_udf'
        '15FunctionContextERKNS1_9StringValE'],
    [['to_base64'], 'VARCHAR', ['VARCHAR'],
        '_ZN5doris19EncryptionFunctions9to_base64EPN9doris_udf'
        '15FunctionContextERKNS1_9StringValE'],
    # for compatable with MySQL
    [['md5'], 'VARCHAR', ['VARCHAR'],
        '_ZN5doris19EncryptionFunctions3md5EPN9doris_udf15FunctionContextERKNS1_9StringValE'],
    [['md5sum'], 'VARCHAR', ['VARCHAR', '...'],
        '_ZN5doris19EncryptionFunctions6md5sumEPN9doris_udf15FunctionContextEiPKNS1_9StringValE']
]

invisible_functions = [
]
