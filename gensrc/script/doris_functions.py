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

"""
# This is a list of all the functions that are not auto-generated.
# It contains all the meta data that describes the function.  The format is:
# <function name>, <return_type>, [<args>], <backend function name>, [<sql aliases>]
#
# 'function name' is the base of what the opcode enum will be generated from.  It does not
# have to be unique, the script will mangle the name with the signature if necessary.
#
# 'sql aliases' are the function names that can be used from sql.  They are
# optional and there can be multiple aliases for a function.
#
# This is combined with the list in generated_functions to code-gen the opcode
# registry in the FE and BE.
"""

functions = [
  ['Compound_And', 'BOOLEAN', ['BOOLEAN', 'BOOLEAN'], \
        'CompoundPredicate::not_compute_fn', []],
  ['Compound_Or', 'BOOLEAN', ['BOOLEAN', 'BOOLEAN'], \
        'CompoundPredicate::not_compute_fn', []],
  ['Compound_Not', 'BOOLEAN', ['BOOLEAN', 'BOOLEAN'], \
        'CompoundPredicate::not_compute_fn', []],

  ['Constant_Regex', 'BOOLEAN', ['BOOLEAN', 'BOOLEAN'], \
        'LikePredicate::constant_regex_fn', []],
  ['Constant_Substring', 'BOOLEAN', ['BOOLEAN', 'BOOLEAN'], \
        'LikePredicate::constant_substring_fn', []],
  ['Like', 'BOOLEAN', ['VARCHAR', 'VARCHAR'], 'LikePredicate::like_fn', []],
  ['Regex', 'BOOLEAN', ['VARCHAR', 'VARCHAR'], 'LikePredicate::regex_fn', []],

  ['Math_Pi', 'DOUBLE', [], 'MathFunctions::pi', ['pi']],
  ['Math_E', 'DOUBLE', [], 'MathFunctions::e', ['e']],
  ['Math_Abs', 'DOUBLE', ['DOUBLE'], 'MathFunctions::abs', ['abs']],
  ['Math_Sign', 'FLOAT', ['DOUBLE'], 'MathFunctions::sign', ['sign']],
  ['Math_Sin', 'DOUBLE', ['DOUBLE'], 'MathFunctions::sin', ['sin']],
  ['Math_Asin', 'DOUBLE', ['DOUBLE'], 'MathFunctions::asin', ['asin']],
  ['Math_Cos', 'DOUBLE', ['DOUBLE'], 'MathFunctions::cos', ['cos']],
  ['Math_Acos', 'DOUBLE', ['DOUBLE'], 'MathFunctions::acos', ['acos']],
  ['Math_Tan', 'DOUBLE', ['DOUBLE'], 'MathFunctions::tan', ['tan']],
  ['Math_Atan', 'DOUBLE', ['DOUBLE'], 'MathFunctions::atan', ['atan']],
  ['Math_Radians', 'DOUBLE', ['DOUBLE'], 'MathFunctions::radians', ['radians']],
  ['Math_Degrees', 'DOUBLE', ['DOUBLE'], 'MathFunctions::degrees', ['degrees']],
  ['Math_Ceil', 'BIGINT', ['DOUBLE'], 'MathFunctions::ceil', ['ceil', 'ceiling']],
  ['Math_Floor', 'BIGINT', ['DOUBLE'], 'MathFunctions::floor', ['floor']],
  ['Math_Round', 'BIGINT', ['DOUBLE'], 'MathFunctions::round', ['round']],
  ['Math_Round', 'DOUBLE', ['DOUBLE', 'INT'], 'MathFunctions::round_up_to', ['round']],
  ['Math_Truncate', 'BIGINT', ['DOUBLE'], 'MathFunctions::truncate', ['truncate']],
  ['Math_Truncate', 'DOUBLE', ['DOUBLE', 'INT'], 'MathFunctions::truncate_to', ['truncate']],
  ['Math_Exp', 'DOUBLE', ['DOUBLE'], 'MathFunctions::exp', ['exp']],
  ['Math_Ln', 'DOUBLE', ['DOUBLE'], 'MathFunctions::ln', ['ln']],
  ['Math_Log10', 'DOUBLE', ['DOUBLE'], 'MathFunctions::log10', ['log10']],
  ['Math_Log2', 'DOUBLE', ['DOUBLE'], 'MathFunctions::log2', ['log2']],
  ['Math_Log', 'DOUBLE', ['DOUBLE', 'DOUBLE'], 'MathFunctions::log', ['log']],
  ['Math_Pow', 'DOUBLE', ['DOUBLE', 'DOUBLE'], 'MathFunctions::pow', ['pow', 'power']],
  ['Math_Sqrt', 'DOUBLE', ['DOUBLE'], 'MathFunctions::sqrt', ['sqrt']],
  ['Math_Rand', 'DOUBLE', [], 'MathFunctions::rand', ['rand']],
  ['Math_Rand', 'DOUBLE', ['INT'], 'MathFunctions::rand_seed', ['rand']],
  ['Math_Bin', 'VARCHAR', ['BIGINT'], 'MathFunctions::bin', ['bin']],
  ['Math_Hex', 'VARCHAR', ['BIGINT'], 'MathFunctions::hex_int', ['hex']],
  ['Math_Hex', 'VARCHAR', ['VARCHAR'], 'MathFunctions::hex_string', ['hex']],
  ['Math_Unhex', 'VARCHAR', ['VARCHAR'], 'MathFunctions::unhex', ['unhex']],
  ['Math_Conv', 'VARCHAR', ['BIGINT', 'TINYINT', 'TINYINT'], \
        'MathFunctions::conv_int', ['conv']],
  ['Math_Conv', 'VARCHAR', ['VARCHAR', 'TINYINT', 'TINYINT'], \
        'MathFunctions::conv_string', ['conv']],
  ['Math_Pmod', 'BIGINT', ['BIGINT', 'BIGINT'], 'MathFunctions::pmod_bigint', ['pmod']],
  ['Math_Pmod', 'DOUBLE', ['DOUBLE', 'DOUBLE'], 'MathFunctions::pmod_double', ['pmod']],
  ['Math_Positive', 'BIGINT', ['BIGINT'], 'MathFunctions::positive_bigint', ['positive']],
  ['Math_Positive', 'DOUBLE', ['DOUBLE'], 'MathFunctions::positive_double', ['positive']],
  ['Math_Negative', 'DOUBLE', ['DOUBLE'], 'MathFunctions::negative_double', ['negative']],
  ['Math_Negative', 'BIGINT', ['BIGINT'], 'MathFunctions::negative_bigint', ['negative']],
  ['Math_Greatest', 'BIGINT', ['BIGINT', '...'], 'MathFunctions::greatest_bigint', ['greatest']],
  ['Math_Greatest', 'DOUBLE', ['DOUBLE', '...'], 'MathFunctions::greatest_double', ['greatest']],
  ['Math_Greatest', 'DECIMAL', ['DECIMAL', '...'], 'MathFunctions::greatest_decimal', ['greatest']],
  ['Math_Greatest', 'DECIMALV2', ['DECIMALV2', '...'], 'MathFunctions::greatest_decimal', ['greatest']],
  ['Math_Greatest', 'VARCHAR', ['VARCHAR', '...'], 'MathFunctions::greatest_string', ['greatest']],
  ['Math_Greatest', 'DATETIME', ['DATETIME', '...'], \
        'MathFunctions::greatest_timestamp', ['greatest']],
  ['Math_Least', 'BIGINT', ['BIGINT', '...'], 'MathFunctions::least_bigint', ['least']],
  ['Math_Least', 'DOUBLE', ['DOUBLE', '...'], 'MathFunctions::least_double', ['least']],
  ['Math_Least', 'DECIMAL', ['DECIMAL', '...'], 'MathFunctions::least_decimal', ['least']],
  ['Math_Least', 'DECIMALV2', ['DECIMALV2', '...'], 'MathFunctions::least_decimalv2', ['least']],
  ['Math_Least', 'VARCHAR', ['VARCHAR', '...'], 'MathFunctions::least_string', ['least']],
  ['Math_Least', 'DATETIME', ['DATETIME', '...'], 'MathFunctions::least_timestamp', ['least']],

# left and right are key words, leave them out for now.
  ['String_Left', 'VARCHAR', ['VARCHAR', 'INT'], 'StringFunctions::left', ['strleft', 'left']],
  ['String_Right', 'VARCHAR', ['VARCHAR', 'INT'], 'StringFunctions::right', ['strright', 'right']],
  ['String_Length', 'INT', ['VARCHAR'], 'StringFunctions::length', ['length']],
  ['String_Lower', 'VARCHAR', ['VARCHAR'], 'StringFunctions::lower', ['lower', 'lcase']],
  ['String_Upper', 'VARCHAR', ['VARCHAR'], 'StringFunctions::upper', ['upper', 'ucase']],
  ['String_Reverse', 'VARCHAR', ['VARCHAR'], 'StringFunctions::reverse', ['reverse']],
  ['String_Trim', 'VARCHAR', ['VARCHAR'], 'StringFunctions::trim', ['trim']],
  ['String_Ltrim', 'VARCHAR', ['VARCHAR'], 'StringFunctions::ltrim', ['ltrim']],
  ['String_Rtrim', 'VARCHAR', ['VARCHAR'], 'StringFunctions::rtrim', ['rtrim']],
  ['String_Space', 'VARCHAR', ['INT'], 'StringFunctions::space', ['space']],
  ['String_Repeat', 'VARCHAR', ['VARCHAR', 'INT'], 'StringFunctions::repeat', ['repeat']],
  ['String_Ascii', 'INT', ['VARCHAR'], 'StringFunctions::ascii', ['ascii']],
  ['String_Lpad', 'VARCHAR', ['VARCHAR', 'INT', 'VARCHAR'], \
        'StringFunctions::lpad', ['lpad']],
  ['String_Rpad', 'VARCHAR', ['VARCHAR', 'INT', 'VARCHAR'], \
        'StringFunctions::rpad', ['rpad']],
  ['String_Instr', 'INT', ['VARCHAR', 'VARCHAR'], 'StringFunctions::instr', ['instr']],
  ['String_Locate', 'INT', ['VARCHAR', 'VARCHAR'], 'StringFunctions::locate', ['locate']],
  ['String_Locate', 'INT', ['VARCHAR', 'VARCHAR', 'INT'], \
        'StringFunctions::locate_pos', ['locate']],
  ['String_Regexp_Extract', 'VARCHAR', ['VARCHAR', 'VARCHAR', 'INT'], \
        'StringFunctions::regexp_extract', ['regexp_extract']],
  ['String_Regexp_Replace', 'VARCHAR', ['VARCHAR', 'VARCHAR', 'VARCHAR'], \
        'StringFunctions::regexp_replace', ['regexp_replace']],
  ['String_Concat', 'VARCHAR', ['VARCHAR', '...'], 'StringFunctions::concat', ['concat']],
  ['String_Concat_Ws', 'VARCHAR', ['VARCHAR', 'VARCHAR', '...'], \
        'StringFunctions::concat_ws', ['concat_ws']],
  ['String_Find_In_Set', 'INT', ['VARCHAR', 'VARCHAR'], \
        'StringFunctions::find_in_set', ['find_in_set']],
  ['String_Parse_Url', 'VARCHAR', ['VARCHAR', 'VARCHAR'], \
        'StringFunctions::parse_url', ['parse_url']],
  ['String_Parse_Url', 'VARCHAR', ['VARCHAR', 'VARCHAR', 'VARCHAR'], \
        'StringFunctions::parse_url_key', ['parse_url']],
  ['Utility_Sleep', 'BOOLEAN', ['INT'], 'UtilityFunctions::sleep', ['sleep']],
  ['Utility_Version', 'VARCHAR', [], 'UtilityFunctions::version', ['version']],

#JsonFunction
  ['Get_json_int', 'INT', ['VARCHAR', 'VARCHAR'], \
      'JsonFunctions::get_json_int_value', ['get_json_int']],
  ['Get_json_string', 'VARCHAR', ['VARCHAR', 'VARCHAR'], \
      'JsonFunctions::get_json_string_value', ['get_json_string']],
  ['Get_json_double', 'DOUBLE', ['VARCHAR', 'VARCHAR'], \
      'JsonFunctions::get_json_double_value', ['get_json_double']],

#hll function
  ['Hll_cardinality', 'VARCHAR', ['VARCHAR'], \
      'HllHashFunction::hll_cardinality', ['hll_cardinality']],
  ['Hll_hash_string', 'VARCHAR', ['VARCHAR'], \
     'HllHashFunction::hll_hash_string', ['hll_hash_string']],

# Timestamp Functions
#  ['Unix_Timestamp', 'INT', [], \
#        'TimestampFunctions::to_unix', ['unix_timestamp']],
#  ['Unix_Timestamp', 'INT', ['DATETIME'], \
#        'TimestampFunctions::to_unix', ['unix_timestamp']],
#  ['Unix_Timestamp', 'INT', ['DATETIME', 'VARCHAR'], \
#        'TimestampFunctions::to_unix', ['unix_timestamp']],
#  ['From_UnixTime', 'VARCHAR', ['INT'], \
#        'TimestampFunctions::from_unix', ['from_unixtime']],
#  ['From_UnixTime', 'VARCHAR', ['INT', 'VARCHAR'], \
#        'TimestampFunctions::from_unix', ['from_unixtime']],
  ['Timestamp_year', 'INT', ['DATETIME'], 'TimestampFunctions::year', ['year']],
  ['Timestamp_quarter', 'INT', ['DATETIME'], 'TimestampFunctions::quarter', ['quarter']],
  ['Timestamp_month', 'INT', ['DATETIME'], 'TimestampFunctions::month', ['month']],
  ['Timestamp_dayofmonth', 'INT', ['DATETIME'], \
        'TimestampFunctions::day_of_month', ['day', 'dayofmonth']],
  ['Timestamp_dayofyear', 'INT', ['DATETIME'],
        'TimestampFunctions::day_of_year', ['dayofyear']],
  ['Timestamp_weekofyear', 'INT', ['DATETIME'], \
        'TimestampFunctions::week_of_year', ['weekofyear']],
  ['Timestamp_hour', 'INT', ['DATETIME'], 'TimestampFunctions::hour', ['hour']],
  ['Timestamp_minute', 'INT', ['DATETIME'], 'TimestampFunctions::minute', ['minute']],
  ['Timestamp_second', 'INT', ['DATETIME'], 'TimestampFunctions::second', ['second']],
  ['Timestamp_now', 'DATETIME', [], \
        'TimestampFunctions::now', ['now', 'current_timestamp']],
  ['Time_now', 'DATETIME', [], \
        'TimestampFunctions::curtime', ['curtime', 'utc_time']],
  ['Timestamp_to_date', 'DATE', ['DATETIME'], \
        'TimestampFunctions::to_date', ['date', 'to_date']],
  ['Timestamp_years_add', 'DATETIME', ['DATETIME', 'INT'], \
        'TimestampFunctions::years_add', ['years_add']],
  ['Timestamp_years_sub', 'DATETIME', ['DATETIME', 'INT'], \
        'TimestampFunctions::years_sub', ['years_sub']],
  ['Timestamp_months_add', 'DATETIME', ['DATETIME', 'INT'], \
        'TimestampFunctions::months_add', ['months_add']],
  ['Timestamp_months_sub', 'DATETIME', ['DATETIME', 'INT'], \
        'TimestampFunctions::months_sub', ['months_sub']],
  ['Timestamp_weeks_add', 'DATETIME', ['DATETIME', 'INT'], \
        'TimestampFunctions::weeks_add', ['weeks_add']], 
  ['Timestamp_weeks_sub', 'DATETIME', ['DATETIME', 'INT'], \
        'TimestampFunctions::weeks_sub', ['weeks_sub']], 
  ['Timestamp_days_add', 'DATETIME', ['DATETIME', 'INT'], \
        'TimestampFunctions::days_add', ['days_add', 'date_add', 'adddate']],
  ['Timestamp_days_sub', 'DATETIME', ['DATETIME', 'INT'], \
        'TimestampFunctions::days_sub', ['days_sub', 'date_sub', 'subdate']],
  ['Timestamp_hours_add', 'DATETIME', ['DATETIME', 'INT'], \
        'TimestampFunctions::hours_add', ['hours_add']],
  ['Timestamp_hours_sub', 'DATETIME', ['DATETIME', 'INT'], \
        'TimestampFunctions::hours_sub', ['hours_sub']],
  ['Timestamp_minutes_add', 'DATETIME', ['DATETIME', 'INT'], \
        'TimestampFunctions::minutes_add', ['minutes_add']],
  ['Timestamp_minutes_sub', 'DATETIME', ['DATETIME', 'INT'], \
        'TimestampFunctions::minutes_sub', ['minutes_sub']],
  ['Timestamp_seconds_add', 'DATETIME', ['DATETIME', 'INT'], \
        'TimestampFunctions::seconds_add', ['seconds_add']],
  ['Timestamp_seconds_sub', 'DATETIME', ['DATETIME', 'INT'], \
        'TimestampFunctions::seconds_sub', ['seconds_sub']],
  ['Timestamp_microseconds_add', 'DATETIME', ['DATETIME', 'INT'], \
        'TimestampFunctions::micros_add', ['microseconds_add']],
  ['Timestamp_microseconds_sub', 'DATETIME', ['DATETIME', 'INT'], \
        'TimestampFunctions::micros_sub', ['microseconds_sub']],
  ['Timestamp_diff', 'INT', ['DATETIME', 'DATETIME'], \
        'TimestampFunctions::date_diff', ['datediff']],
  ['Time_diff', 'DATETIME', ['DATETIME', 'DATETIME'], \
        'TimestampFunctions::time_diff', ['timediff']],
  ['From_utc_timestamp', 'DATETIME', ['DATETIME', 'VARCHAR'], \
        'TimestampFunctions::from_utc', ['from_utc_timestamp']],
  ['To_utc_timestamp', 'DATETIME', ['DATETIME', 'VARCHAR'], \
        'TimestampFunctions::to_utc', ['to_utc_timestamp']],
  ['Timestamp_date_format', 'VARCHAR', ['DATETIME', 'VARCHAR'], \
        'TimestampFunctions::date_format', ['date_format']],
  ['Timestamp_from_days', 'DATE', ['INT'], \
        'TimestampFunctions::from_days', ['from_days']],
  ['Timestamp_to_days', 'INT', ['DATE'], \
        'TimestampFunctions::to_days', ['to_days']],
  ['Timestamp_str_to_date', 'DATETIME', ['VARCHAR', 'VARCHAR'], \
        'TimestampFunctions::str_to_date', ['str_to_date']],
  ['Timestamp_day_name', 'VARCHAR', ['DATE'], \
        'TimestampFunctions::day_name', ['dayname']],
  ['Timestamp_mont_name', 'VARCHAR', ['DATE'], \
        'TimestampFunctions::month_name', ['monthname']],
  ['Timestamp_timestamp', 'DATETIME', ['DATETIME'], \
        'TimestampFunctions::timestamp', ['timestamp']],

# Conditional Functions
#  ['Conditional_NullIf', 'BOOLEAN', ['BOOLEAN', 'BOOLEAN'], \
#        'ConditionalFunctions::null_if_bool', ['nullif']],
#  ['Conditional_NullIf', 'TINYINT', ['TINYINT', 'TINYINT'], \
#        'ConditionalFunctions::null_if_tinyint', ['nullif']],
#  ['Conditional_NullIf', 'SMALLINT', ['SMALLINT', 'SMALLINT'], \
#        'ConditionalFunctions::null_if_smallint', ['nullif']],
#  ['Conditional_NullIf', 'INT', ['INT', 'INT'], \
#        'ConditionalFunctions::null_if_int', ['nullif']],
#  ['Conditional_NullIf', 'BIGINT', ['BIGINT', 'BIGINT'], \
#        'ConditionalFunctions::null_if_bigint', ['nullif']],
#  ['Conditional_NullIf', 'FLOAT', ['FLOAT', 'FLOAT'], \
#        'ConditionalFunctions::null_if_float', ['nullif']],
#  ['Conditional_NullIf', 'DOUBLE', ['DOUBLE', 'DOUBLE'], \
#        'ConditionalFunctions::null_if_double', ['nullif']],
#  ['Conditional_NullIf', 'VARCHAR', ['VARCHAR', 'VARCHAR'], \
#        'ConditionalFunctions::null_if_string', ['nullif']],
#  ['Conditional_NullIf', 'DATETIME', ['DATETIME', 'DATETIME'], \
#        'ConditionalFunctions::null_if_timestamp', ['nullif']],
#  ['Conditional_Ifnull', 'BOOLEAN', ['BOOLEAN', 'BOOLEAN'], \
#        'ConditionalFunctions::if_null_bool', ['ifnull']],
#  ['Conditional_Ifnull', 'TINYINT', ['TINYINT', 'TINYINT'], \
#        'ConditionalFunctions::if_null_tinyint', ['ifnull']],
#  ['Conditional_Ifnull', 'SMALLINT', ['SMALLINT', 'SMALLINT'], \
#        'ConditionalFunctions::if_null_smallint', ['ifnull']],
#  ['Conditional_Ifnull', 'INT', ['INT', 'INT'], \
#        'ConditionalFunctions::if_null_int', ['ifnull']],
#  ['Conditional_Ifnull', 'BIGINT', ['BIGINT', 'BIGINT'], \
#        'ConditionalFunctions::if_null_bigint', ['ifnull']],
#  ['Conditional_Ifnull', 'FLOAT', ['FLOAT', 'FLOAT'], \
#        'ConditionalFunctions::if_null_float', ['ifnull']],
#  ['Conditional_Ifnull', 'DOUBLE', ['DOUBLE', 'DOUBLE'], \
#        'ConditionalFunctions::if_null_double', ['ifnull']],
#  ['Conditional_Ifnull', 'VARCHAR', ['VARCHAR', 'VARCHAR'], \
#        'ConditionalFunctions::if_null_string', ['ifnull']],
#  ['Conditional_Ifnull', 'DATETIME', ['DATETIME', 'DATETIME'], \
#        'ConditionalFunctions::if_null_timestamp', ['ifnull']],
#  ['Conditional_If', 'BOOLEAN', ['BOOLEAN', 'BOOLEAN', 'BOOLEAN'], \
#        'ConditionalFunctions::if_bool', ['if']],
#  ['Conditional_If', 'TINYINT', ['BOOLEAN', 'TINYINT', 'TINYINT'], \
#        'ConditionalFunctions::if_tinyint', ['if']],
#  ['Conditional_If', 'SMALLINT', ['BOOLEAN', 'SMALLINT', 'SMALLINT'], \
#        'ConditionalFunctions::if_smallint', ['if']],
#  ['Conditional_If', 'INT', ['BOOLEAN', 'INT', 'INT'], \
#        'ConditionalFunctions::if_int', ['if']],
#  ['Conditional_If', 'BIGINT', ['BOOLEAN', 'BIGINT', 'BIGINT'], \
#        'ConditionalFunctions::if_bigint', ['if']],
#  ['Conditional_If', 'FLOAT', ['BOOLEAN', 'FLOAT', 'FLOAT'], \
#        'ConditionalFunctions::if_float', ['if']],
#  ['Conditional_If', 'DOUBLE', ['BOOLEAN', 'DOUBLE', 'DOUBLE'], \
#        'ConditionalFunctions::if_double', ['if']],
#  ['Conditional_If', 'VARCHAR', ['BOOLEAN', 'VARCHAR', 'VARCHAR'], \
#        'ConditionalFunctions::if_string', ['if']],
#  ['Conditional_If', 'DATETIME', ['BOOLEAN', 'DATETIME', 'DATETIME'], \
#        'ConditionalFunctions::if_timestamp', ['if']],
#  ['Conditional_Coalesce', 'BOOLEAN', ['BOOLEAN', '...'], \
#        'ConditionalFunctions::coalesce_bool', ['coalesce']],
#  ['Conditional_Coalesce', 'BIGINT', ['BIGINT', '...'], \
#        'ConditionalFunctions::coalesce_int', ['coalesce']],
#  ['Conditional_Coalesce', 'DOUBLE', ['DOUBLE', '...'], \
#        'ConditionalFunctions::coalesce_float', ['coalesce']],
#  ['Conditional_Coalesce', 'VARCHAR', ['VARCHAR', '...'], \
#        'ConditionalFunctions::coalesce_string', ['coalesce']],
#  ['Conditional_Coalesce', 'DATETIME', ['DATETIME', '...'], \
#        'ConditionalFunctions::coalesce_timestamp', ['coalesce']],
]

# These functions are implemented against the UDF interface.
# TODO: this list should subsume the one above when all builtins are migrated.
udf_functions = [
  ['Udf_Math_Abs', 'DECIMAL', ['DECIMAL'], 'UdfBuiltins::decimal_abs', ['udf_abs'],
   ''],
  ['Udf_Math_Abs', 'DECIMALV2', ['DECIMALV2'], 'UdfBuiltins::decimal_abs', ['udf_abs'],
   ''],
  ['Udf_Sub_String', 'VARCHAR', ['VARCHAR', 'INT', 'INT'], 
  ['Udf_Sub_String', 'VARCHAR', ['VARCHAR', 'INT', 'INT'], 
      'UdfBuiltins::sub_string', ['udf_substring'], ''],
  ['Udf_Add_Two_Number', 'BIGINT', ['BIGINT', 'BIGINT'], 
      'UdfBuiltins::add_two_number', ['udf_add_two_number'], ''],
  ['Udf_Math_Pi', 'DOUBLE', [], 'UdfBuiltins::pi', ['udf_pi'],
   '_ZN6impala11UdfBuiltins2PiEPN10impala_udf15FunctionContextE'],
  ['Udf_Math_Abs', 'DOUBLE', ['DOUBLE'], 'UdfBuiltins::abs', ['udf_abs'],
   '_ZN6impala11UdfBuiltins3AbsEPN10impala_udf15FunctionContextERKNS1_9DoubleValE'],
  ['Udf_String_Lower', 'VARCHAR', ['VARCHAR'], 'UdfBuiltins::lower', ['udf_lower'],
   '_ZN6impala11UdfBuiltins5LowerEPN10impala_udf15FunctionContextERKNS1_9StringValE'],
]
