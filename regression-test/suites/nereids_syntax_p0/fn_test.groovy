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

suite("nereids_function") {
    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"

    // ddl begin
    sql "drop table if exists t1"
    sql "drop table if exists t2"
    sql "drop table if exists t3"

    sql """
        CREATE TABLE IF NOT EXISTS `t1` (
            `k0` boolean null comment "",
            `k1` tinyint(4) null comment "",
            `k2` smallint(6) null comment "",
            `k3` int(11) null comment "",
            `k4` bigint(20) null comment "",
            `k5` decimal(9, 3) null comment "",
            `k6` char(5) null comment "",
            `k7` varchar(20) null comment "",
            `k8` double max null comment "",
            `k9` float sum null comment "",
            `k10` date null comment "",
            `k11` datetime null comment "",
            `k12` string replace null comment "",
            `k13` largeint(40) replace null comment ""
        ) engine=olap
        DISTRIBUTED BY HASH(`k1`) BUCKETS 5
        properties("replication_num" = "1")
    """
    sql """
        CREATE TABLE IF NOT EXISTS `t2` (
            `k0` boolean null comment "",
            `k1` tinyint(4) null comment "",
            `k2` smallint(6) null comment "",
            `k3` int(11) null comment "",
            `k4` bigint(20) null comment "",
            `k5` decimal(9, 3) null comment "",
            `k6` char(5) null comment "",
            `k7` varchar(20) null comment "",
            `k8` double max null comment "",
            `k9` float sum null comment "",
            `k10` date null comment "",
            `k11` datetime null comment "",
            `k12` string replace_if_not_null null comment "",
            `k13` largeint(40) replace null comment ""
        ) engine=olap
        DISTRIBUTED BY HASH(`k1`) BUCKETS 5
        properties("replication_num" = "1")
    """
    sql """
        CREATE TABLE IF NOT EXISTS `t3` (
            `k0` boolean null comment "",
            `k1` tinyint(4) null comment "",
            `k2` smallint(6) null comment "",
            `k3` int(11) null comment "",
            `k4` bigint(20) null comment "",
            `k5` decimal(9, 3) null comment "",
            `k6` char(5) null comment "",
            `k7` varchar(20) null comment "",
            `k8` double max null comment "",
            `k9` float sum null comment "",
            `k10` date null comment "",
            `k11` datetime null comment "",
            `k12` string replace null comment "",
            `k13` largeint(40) replace null comment ""
        ) engine=olap
        DISTRIBUTED BY HASH(`k1`) BUCKETS 5
        properties("replication_num" = "1")
    """

    sql """
    insert into t1 values
    (0, 1, 1989, 1001, 11011902, 123.123, true, 1989-03-21, 1989-03-21 13:00:00, wangjuoo4, 0.1, 6.333, string12345, 170141183460469231731687303715884105727),
    (0, 2, 1986, 1001, 11011903, 1243.5, false, 1901-12-31, 1989-03-21 13:00:00, wangynnsf, 20.268, 789.25, string12345, -170141183460469231731687303715884105727),
    (0, 3, 1989, 1002, 11011905, 24453.325, false, 2012-03-14, 2000-01-01 00:00:00, yunlj8@nk, 78945, 3654.0, string12345, 0),
    (0, 4, 1991, 3021, -11011907, 243243.325, false, 3124-10-10, 2015-03-13 10:30:00, yanvjldjlll, 2.06, -0.001, string12345, 20220101),
    (0, 5, 1985, 5014, -11011903, 243.325, true, 2015-01-01, 2015-03-13 12:36:38, du3lnvl, -0.000, -365, string12345, 20220102),
    (0, 6, 32767, 3021, 123456, 604587.000, true, 2014-11-11, 2015-03-13 12:36:38, yanavnd, 0.1, 80699, string12345, 20220104),
    (0, 7, -32767, 1002, 7210457, 3.141, false, 1988-03-21, 1901-01-01 00:00:00, jiw3n4, 0.0, 6058, string12345, -20220101),
    (1, 8, 255, 2147483647, 11011920, -0.123, true, 1989-03-21, 9999-11-11 12:12:00, wangjuoo5, 987456.123, 12.14, string12345, -2022),
    (1, 9, 1991, -2147483647, 11011902, -654.654, true, 1991-08-11, 1989-03-21 13:11:00, wangjuoo4, 0.000, 69.123, string12345, 11011903),
    (1, 10, 1991, 5014, 9223372036854775807, -258.369, false, 2015-04-02, 2013-04-02 15:16:52, wangynnsf, -123456.54, 0.235, string12345, -11011903),
    (1, 11, 1989, 25699, -9223372036854775807, 0.666, true, 2015-04-02, 1989-03-21 13:11:00, yunlj8@nk, -987.001, 4.336, string12345, 1701411834604692317316873037158),
    (1, 12, 32767, -2147483647, 9223372036854775807, 243.325, false, 1991-08-11, 2013-04-02 15:16:52, lifsno, -564.898, 3.141592654, string12345, 1701604692317316873037158),
    (1, 13, -32767, 2147483647, -9223372036854775807, 100.001, false, 2015-04-02, 2015-04-02 00:00:00, wenlsfnl, 123.456, 3.141592653, string12345, 701411834604692317316873037158),
    (1, 14, 255, 103, 11011902, -0.000, false, 2015-04-02, 2015-04-02 00:00:00,  , 3.141592654, 2.036, string12345, 701411834604692317316873),
    (1, 15, 1992, 3021, 11011920, 0.00, true, 9999-12-12, 2015-04-02 00:00:00, , 3.141592653, 20.456, string12345, 701411834604692317),
    (null, null, null, null, null, null, null, null, null, null, null, null, null, null)
    """
    sql "insert into t2 select * from t1 where k1 <= 3"
    sql "insert into t3 select * from t1"
    // ddl end
    
    // function table begin
    // usage: write the new function in the list and construct answer.
    def scalar_function = [
        'abs' : [['double', 'double'], ['float', 'float'], ['largeint', 'largeint'], ['largeint', 'bigint'], ['integer', 'smallint'], ['bigint', 'integer'], ['smallint', 'tinyint'], ['decimal', 'decimal']]
        'acos' : [['double', 'double']]
        'aes_decrypt' : [['varchar'], ['string', 'string', 'string'], ['varchar'], ['string'], ['varchar'], ['string']]
        'aes_encrypt' : [['varchar'], ['string', 'string', 'string'], ['varchar'], ['string'], ['varchar'], ['string']]
        'any_value' : [['functionsignature.retarg(0', 'anydata']]
        'append_trailing_char_if_absent' : [['varchar'], ['string', 'string', 'string']]
        'array_contains' : [['boolean'], ['boolean'], ['boolean'], ['boolean'], ['boolean'], ['boolean']]
        'array_join' : [['string'], ['string'], ['string'], ['string'], ['string'], ['string'], ['string'], ['string'], ['string'], ['string'], ['string'], ['string'], ['string'], ['string'], ['string'], ['string'], ['string'], ['string'], ['string'], ['string'], ['string'], ['string'], ['string'], ['varchar'], ['string'], ['string'], ['string'], ['varchar'], ['varchar'], ['string']]
        'array_position' : [['bigint'], ['bigint'], ['bigint']]
        'arrays_overlap' : [['boolean'], ['boolean'], ['boolean'], ['boolean'], ['boolean'], ['boolean'], ['boolean'], ['boolean'], ['boolean'], ['boolean'], ['boolean'], ['boolean'], ['boolean'], ['boolean'], ['boolean']]
        'ascii' : [['integer', 'varchar'], ['integer', 'string']]
        'asin' : [['double', 'double']]
        'atan' : [['double', 'double']]
        'avg' : [['double', 'tinyint'], ['double', 'smallint'], ['double', 'integer'], ['double', 'bigint'], ['double', 'double'], ['decimal', 'decimal'], ['decimal.defaultdecimal128', 'decimal.defaultdecimal32'], ['decimal.defaultdecimal128', 'decimal.defaultdecimal64'], ['decimal.defaultdecimal128', 'decimal.defaultdecimal128']]
        'avg_weighted' : [['double', 'tinyint', 'double'], ['double', 'smallint', 'double'], ['double', 'integer', 'double'], ['double', 'bigint', 'double'], ['double', 'float', 'double'], ['double', 'double', 'double'], ['double', 'decimal', 'double']]
        'bin' : [['varchar', 'bigint']]
        'bit_length' : [['integer', 'varchar'], ['integer', 'string']]
        'bitmap_and' : [['bitmap.varargs(bitmap', 'bitmap']]
        'bitmap_and_count' : [['bigint.varargs(bitmap', 'bitmap']]
        'bitmap_and_not' : [['bitmap', 'bitmap', 'bitmap']]
        'bitmap_and_not_count' : [['bigint', 'bitmap', 'bitmap']]
        'bitmap_contains' : [['boolean', 'bitmap', 'bigint']]
        'bitmap_count' : [['bigint', 'bitmap']]
        'bitmap_empty' : [['bitmap', '']]
        'bitmap_from_string' : [['bitmap', 'varchar'], ['bitmap', 'string']]
        'bitmap_has_all' : [['boolean', 'bitmap', 'bitmap']]
        'bitmap_has_any' : [['boolean', 'bitmap', 'bitmap']]
        'bitmap_hash' : [['bitmap', 'varchar'], ['bitmap', 'string']]
        'bitmap_hash64' : [['bitmap', 'varchar'], ['bitmap', 'string']]
        'bitmap_intersect' : [['bitmap', 'bitmap']]
        'bitmap_max' : [['bigint', 'bitmap']]
        'bitmap_min' : [['bigint', 'bitmap']]
        'bitmap_not' : [['bitmap', 'bitmap', 'bitmap']]
        'bitmap_or' : [['bitmap.varargs(bitmap', 'bitmap']]
        'bitmap_or_count' : [['bigint.varargs(bitmap', 'bitmap']]
        'bitmap_subset_in_range' : [['bitmap']]
        'bitmap_subset_limit' : [['bitmap']]
        'bitmap_to_string' : [['string', 'bitmap']]
        'bitmap_union' : [['bitmap', 'bitmap']]
        'bitmap_union_count' : [['bigint', 'bitmap']]
        'bitmap_union_int' : [['bigint', 'smallint'], ['bigint', 'tinyint'], ['bigint', 'integer'], ['bigint', 'bigint']]
        'bitmap_xor' : [['bitmap.varargs(bitmap', 'bitmap']]
        'bitmap_xor_count' : [['bigint.varargs(bitmap', 'bitmap']]
        'cbrt' : [['double', 'double']]
        'ceil' : [['double', 'double']]
        'ceiling' : [['bigint', 'double']]
        'character_length' : [['integer', 'varchar'], ['integer', 'string']]
        'coalesce' : [['boolean.varargs(boolean'], ['tinyint.varargs(tinyint'], ['smallint.varargs(smallint'], ['integer.varargs(integer'], ['bigint.varargs(bigint'], ['largeint.varargs(largeint'], ['float.varargs(float'], ['double.varargs(double'], ['datetime.varargs(datetime'], ['date.varargs(date'], ['datetime.varargs(datetime'], ['date.varargs(date'], ['decimal.varargs(decimal'], ['bitmap.varargs(bitmap'], ['varchar.varargs(varchar'], ['string.varargs(string']]
        'concat' : [['varchar.varargs(varchar'], ['string.varargs(string']]
        'concat_ws' : [['varchar'], ['varchar'], ['string.varargs(string', 'string']]
        'connection_id' : [['varchar', '']]
        'conv' : [['varchar'], ['varchar'], ['varchar']]
        'convert_to' : [['varchar']]
        'convert_tz' : [['datetime'], ['datetime'], ['date']]
        'cos' : [['double', 'double']]
        'count' : [['bigint', ''], ['bigint.varargs(anydata']]
        'count_equal' : [['bigint'], ['bigint'], ['bigint']]
        'current_date' : [['date', '']]
        'current_time' : [['time', '']]
        'current_timestamp' : [['datetime', ''], ['datetime', 'integer']]
        'current_user' : [['bigint', '']]
        'curtime' : [['time', '']]
        'database' : [['varchar', '']]
        'date' : [['date', 'datetime'], ['date', 'datetime']]
        'date_diff' : [['integer', 'datetime', 'datetime'], ['integer'], ['integer', 'datetime', 'date'], ['integer', 'date', 'datetime'], ['integer', 'date', 'date'], ['integer', 'datetime', 'datetime'], ['integer', 'date', 'datetime']]
        'date_format' : [['varchar', 'datetime', 'varchar'], ['varchar', 'date', 'varchar'], ['varchar'], ['varchar', 'date', 'varchar']]
        'date_trunc' : [['datetime', 'datetime', 'varchar'], ['datetime']]
        'date_v2' : [['date', 'datetime']]
        'day' : [['integer', 'datetime'], ['integer', 'datetime'], ['integer', 'date']]
        'day_ceil' : [['datetime', 'datetime'], ['datetime', 'datetime'], ['date', 'date'], ['datetime', 'datetime', 'datetime'], ['datetime', 'datetime', 'integer'], ['datetime'], ['datetime'], ['date', 'date', 'date'], ['date', 'date', 'integer'], ['datetime'], ['datetime'], ['date']]
        'day_floor' : [['datetime', 'datetime'], ['datetime', 'datetime'], ['date', 'date'], ['datetime', 'datetime', 'datetime'], ['datetime', 'datetime', 'integer'], ['datetime'], ['datetime'], ['date', 'date', 'date'], ['date', 'date', 'integer'], ['datetime'], ['datetime'], ['date']]
        'day_name' : [['varchar', 'datetime'], ['varchar', 'datetime'], ['varchar', 'date']]
        'day_of_month' : [['integer', 'datetime'], ['integer', 'datetime'], ['integer', 'date']]
        'day_of_week' : [['integer', 'datetime'], ['integer', 'datetime'], ['integer', 'date']]
        'day_of_year' : [['integer', 'datetime'], ['integer', 'datetime'], ['integer', 'date']]
        'days_add' : [['datetime', 'datetime', 'integer'], ['datetime'], ['date', 'date', 'integer'], ['date', 'date', 'integer']]
        'days_diff' : [['bigint', 'datetime', 'datetime'], ['bigint'], ['bigint', 'date', 'datetime'], ['bigint', 'datetime', 'date'], ['bigint', 'date', 'date'], ['bigint', 'date', 'datetime'], ['bigint', 'datetime', 'date'], ['bigint', 'datetime', 'datetime'], ['bigint', 'datetime', 'datetime']]
        'days_sub' : [['datetime', 'datetime', 'integer'], ['datetime'], ['date', 'date', 'integer'], ['date', 'date', 'integer']]
        'dceil' : [['bigint', 'double']]
        'degrees' : [['double', 'double']]
        'dexp' : [['double', 'double']]
        'dfloor' : [['bigint', 'double']]
        'digital_masking' : [['varchar', 'bigint']]
        'dlog1' : [['double', 'double']]
        'dlog10' : [['double', 'double']]
        'domain' : [['string', 'string']]
        'domain_without_www' : [['string', 'string']]
        'dpow' : [['double', 'double', 'double']]
        'dround' : [['bigint', 'double'], ['double', 'double', 'integer']]
        'dsqrt' : [['double', 'double']]
        'e' : [['double', '']]
        'element_at' : [['datetime'], ['decimal'], ['varchar']]
        'element_extract' : [['datetime'], ['decimal'], ['varchar']]
        'elt' : [['varchar.varargs(integer', 'varchar'], ['string.varargs(integer', 'string']]
        'ends_with' : [['boolean', 'varchar', 'varchar'], ['boolean', 'string', 'string']]
        'es_query' : [['boolean', 'varchar', 'varchar']]
        'exp' : [['double', 'double']]
        'explode_bitmap' : [['bigint', 'bitmap']]
        'explode_bitmap_outer' : [['bigint', 'bitmap']]
        'explode_json_array_double' : [['double', 'varchar']]
        'explode_json_array_double_outer' : [['double', 'varchar']]
        'explode_json_array_int' : [['bigint', 'varchar']]
        'explode_json_array_int_outer' : [['bigint', 'varchar']]
        'explode_json_array_string' : [['varchar', 'varchar']]
        'explode_json_array_string_outer' : [['varchar', 'varchar']]
        'explode_numbers' : [['integer', 'integer']]
        'explode_numbers_outer' : [['integer', 'integer']]
        'explode_split' : [['varchar']]
        'explode_split_outer' : [['varchar']]
        'extract_url_parameter' : [['varchar']]
        'field' : [['integer.varargs(tinyint'], ['integer.varargs(smallint'], ['integer.varargs(integer'], ['integer.varargs(bigint'], ['integer.varargs(largeint'], ['integer.varargs(float'], ['integer.varargs(double'], ['integer.varargs(decimal'], ['integer.varargs(date'], ['integer.varargs(datetime'], ['integer.varargs(varchar'], ['integer.varargs(string']]
        'find_in_set' : [['integer', 'varchar', 'varchar'], ['integer', 'string', 'string']]
        'floor' : [['double', 'double']]
        'fmod' : [['float', 'float', 'float'], ['double', 'double', 'double']]
        'fpow' : [['double', 'double', 'double']]
        'from_base64' : [['varchar', 'varchar'], ['string', 'string']]
        'from_days' : [['date', 'integer']]
        'from_unixtime' : [['varchar', 'integer'], ['varchar', 'integer', 'varchar'], ['varchar', 'integer', 'string']]
        'get_json_double' : [['double', 'varchar', 'varchar'], ['double', 'string', 'string']]
        'get_json_int' : [['integer', 'varchar', 'varchar'], ['integer', 'string', 'string']]
        'get_json_string' : [['varchar'], ['string', 'string', 'string']]
        'greatest' : [['tinyint.varargs(tinyint'], ['smallint.varargs(smallint'], ['integer.varargs(integer'], ['bigint.varargs(bigint'], ['largeint.varargs(largeint'], ['float.varargs(float'], ['double.varargs(double'], ['decimal.varargs(decimal'], ['datetime.varargs(datetime'], ['datetime.varargs(datetime'], ['varchar.varargs(varchar'], ['string.varargs(string']]
        'group_bit_and' : [['tinyint', 'tinyint'], ['smallint', 'smallint'], ['integer', 'integer'], ['bigint', 'bigint'], ['largeint', 'largeint']]
        'group_bit_or' : [['tinyint', 'tinyint'], ['smallint', 'smallint'], ['integer', 'integer'], ['bigint', 'bigint'], ['largeint', 'largeint']]
        'group_bit_xor' : [['tinyint', 'tinyint'], ['smallint', 'smallint'], ['integer', 'integer'], ['bigint', 'bigint'], ['largeint', 'largeint']]
        'group_bitmap_xor' : [['bitmap', 'bitmap']]
        'group_concat' : [['varchar'], ['varchar']]
        'grouping' : [['return bigint', 'getargument(0.getdata(;']]
        'hex' : [['varchar', 'bigint'], ['varchar', 'varchar'], ['string', 'string']]
        'histogram' : [['varchar', 'boolean'], ['varchar', 'tinyint'], ['varchar', 'smallint'], ['varchar', 'integer'], ['varchar', 'bigint'], ['varchar', 'largeint'], ['varchar', 'float'], ['varchar', 'double'], ['varchar', 'decimal.catalogdefault'], ['varchar', 'date'], ['varchar', 'datetime'], ['varchar', 'date'], ['varchar', 'datetime'], ['varchar', 'char'], ['varchar', 'string']]
        'hll_cardinality' : [['bigint', 'hll']]
        'hll_empty' : [['hll', '']]
        'hll_hash' : [['hll', 'varchar'], ['hll', 'string']]
        'hll_union' : [['hll', 'hll']]
        'hll_union_agg' : [['bigint', 'hll']]
        'hour' : [['integer', 'datetime'], ['integer', 'datetime'], ['integer', 'date']]
        'hour_ceil' : [['datetime', 'datetime'], ['datetime', 'datetime'], ['datetime', 'date'], ['datetime', 'datetime', 'datetime'], ['datetime', 'datetime', 'integer'], ['datetime'], ['datetime'], ['datetime', 'date', 'date'], ['datetime', 'date', 'integer'], ['datetime'], ['datetime'], ['datetime']]
        'hour_floor' : [['datetime', 'datetime'], ['datetime', 'datetime'], ['datetime', 'date'], ['datetime', 'datetime', 'datetime'], ['datetime', 'datetime', 'integer'], ['datetime'], ['datetime'], ['datetime', 'date', 'date'], ['datetime', 'date', 'integer'], ['datetime'], ['datetime'], ['datetime']]
        'hours_add' : [['datetime', 'datetime', 'integer'], ['datetime'], ['datetime', 'date', 'integer'], ['datetime', 'date', 'integer']]
        'hours_diff' : [['bigint', 'datetime', 'datetime'], ['bigint'], ['bigint', 'date', 'datetime'], ['bigint', 'datetime', 'date'], ['bigint', 'date', 'date'], ['bigint', 'date', 'datetime'], ['bigint', 'datetime', 'date'], ['bigint', 'datetime', 'datetime'], ['bigint', 'datetime', 'datetime']]
        'hours_sub' : [['datetime', 'datetime', 'integer'], ['datetime'], ['datetime', 'date', 'integer'], ['datetime', 'date', 'integer']]
        'if' : [['boolean'], ['tinyint'], ['smallint'], ['integer'], ['bigint'], ['largeint'], ['float'], ['double'], ['datetime'], ['date', 'boolean', 'date', 'date'], ['datetime'], ['date'], ['decimal'], ['decimal.defaultdecimal32'], ['decimal.defaultdecimal64'], ['decimal.defaultdecimal128'], ['bitmap'], ['hll', 'boolean', 'hll', 'hll'], ['varchar'], ['string'], ['varchar'], ['varchar'], ['varchar'], ['varchar'], ['varchar'], ['varchar']]
        'initcap' : [['varchar', 'varchar']]
        'instr' : [['integer', 'varchar', 'varchar'], ['integer', 'string', 'string']]
        'json_array' : [['varchar.varargs(varchar']]
        'json_object' : [['varchar.varargs(varchar']]
        'json_quote' : [['varchar', 'varchar']]
        'jsonb_exists_path' : [['boolean', 'json', 'varchar'], ['boolean', 'json', 'string']]
        'jsonb_extract' : [['json', 'json', 'varchar'], ['json', 'json', 'string']]
        'jsonb_extract_bigint' : [['bigint', 'json', 'varchar'], ['bigint', 'json', 'string']]
        'jsonb_extract_bool' : [['boolean', 'json', 'varchar'], ['boolean', 'json', 'string']]
        'jsonb_extract_double' : [['double', 'json', 'varchar'], ['double', 'json', 'string']]
        'jsonb_extract_int' : [['integer', 'json', 'varchar'], ['integer', 'json', 'string']]
        'jsonb_extract_isnull' : [['boolean', 'json', 'varchar'], ['boolean', 'json', 'string']]
        'jsonb_extract_string' : [['string', 'json', 'varchar'], ['string', 'json', 'string']]
        'jsonb_parse' : [['json', 'varchar']]
        'jsonb_parse_error_to_invalid' : [['json', 'varchar']]
        'jsonb_parse_error_to_null' : [['json', 'varchar']]
        'jsonb_parse_error_to_value' : [['json', 'varchar', 'varchar']]
        'jsonb_parse_notnull' : [['json', 'varchar']]
        'jsonb_parse_notnull_error_to_invalid' : [['json', 'varchar']]
        'jsonb_parse_notnull_error_to_value' : [['json', 'varchar', 'varchar']]
        'jsonb_parse_nullable' : [['json', 'varchar']]
        'jsonb_parse_nullable_error_to_invalid' : [['json', 'varchar']]
        'jsonb_parse_nullable_error_to_null' : [['json', 'varchar']]
        'jsonb_parse_nullable_error_to_value' : [['json', 'varchar', 'varchar']]
        'jsonb_type' : [['string', 'json', 'string']]
        'last_day' : [['date', 'datetime'], ['date', 'date'], ['date', 'datetime'], ['date', 'date']]
        'least' : [['tinyint.varargs(tinyint'], ['smallint.varargs(smallint'], ['integer.varargs(integer'], ['bigint.varargs(bigint'], ['largeint.varargs(largeint'], ['float.varargs(float'], ['double.varargs(double'], ['datetime.varargs(datetime'], ['decimal.varargs(decimal'], ['varchar.varargs(varchar'], ['string.varargs(string']]
        'left' : [['varchar', 'varchar', 'integer'], ['string', 'string', 'integer']]
        'length' : [['integer', 'varchar'], ['integer', 'string']]
        'ln' : [['double', 'double']]
        'local_time' : [['datetime', ''], ['datetime', 'integer']]
        'local_timestamp' : [['datetime', ''], ['datetime', 'integer']]
        'locate' : [['integer', 'varchar', 'varchar'], ['integer', 'string', 'string'], ['integer'], ['integer']]
        'log' : [['double', 'double', 'double']]
        'log10' : [['double', 'double']]
        'log2' : [['double', 'double']]
        'lower' : [['varchar', 'varchar'], ['string', 'string']]
        'lpad' : [['varchar'], ['string']]
        'ltrim' : [['varchar', 'varchar'], ['string', 'string']]
        'make_date' : [['date', 'integer', 'integer']]
        'mask' : [['varchar.varargs(varchar'], ['string.varargs(string']]
        'mask_first_n' : [['varchar', 'varchar'], ['string', 'string'], ['varchar', 'varchar', 'integer'], ['string', 'string', 'integer']]
        'mask_last_n' : [['varchar', 'varchar'], ['string', 'string'], ['varchar', 'varchar', 'integer'], ['string', 'string', 'integer']]
        'max' : [['return data', 'data;']]
        'max_by' : [['functionsignature.retarg(0', 'anydata', 'anydata']]
        'md5' : [['varchar', 'varchar'], ['varchar', 'string']]
        'md5_sum' : [['varchar.varargs(varchar'], ['varchar.varargs(string']]
        'min' : [['return data', 'data;']]
        'min_by' : [['functionsignature.retarg(0', 'anydata', 'anydata']]
        'minute' : [['integer', 'datetime'], ['integer', 'datetime'], ['integer', 'date']]
        'minute_ceil' : [['datetime', 'datetime'], ['datetime', 'datetime'], ['datetime', 'date'], ['datetime', 'datetime', 'datetime'], ['datetime', 'datetime', 'integer'], ['datetime'], ['datetime'], ['datetime', 'date', 'date'], ['datetime', 'date', 'integer'], ['datetime'], ['datetime'], ['datetime']]
        'minute_floor' : [['datetime', 'datetime'], ['datetime', 'datetime'], ['datetime', 'date'], ['datetime', 'datetime', 'datetime'], ['datetime', 'datetime', 'integer'], ['datetime'], ['datetime'], ['datetime', 'date', 'date'], ['datetime', 'date', 'integer'], ['datetime'], ['datetime'], ['datetime']]
        'minutes_add' : [['datetime', 'datetime', 'integer'], ['datetime'], ['datetime', 'date', 'integer'], ['datetime', 'date', 'integer']]
        'minutes_diff' : [['bigint', 'datetime', 'datetime'], ['bigint'], ['bigint', 'date', 'datetime'], ['bigint', 'datetime', 'date'], ['bigint', 'date', 'date'], ['bigint', 'date', 'datetime'], ['bigint', 'datetime', 'date'], ['bigint', 'datetime', 'datetime'], ['bigint', 'datetime', 'datetime']]
        'minutes_sub' : [['datetime', 'datetime', 'integer'], ['datetime'], ['datetime', 'date', 'integer'], ['datetime', 'date', 'integer']]
        'money_format' : [['varchar', 'bigint'], ['varchar', 'largeint'], ['varchar', 'double'], ['varchar', 'decimal']]
        'month' : [['integer', 'datetime'], ['integer', 'datetime'], ['integer', 'date']]
        'month_ceil' : [['datetime', 'datetime'], ['datetime', 'datetime'], ['date', 'date'], ['datetime', 'datetime', 'datetime'], ['datetime', 'datetime', 'integer'], ['datetime'], ['datetime'], ['date', 'date', 'date'], ['date', 'date', 'integer'], ['datetime'], ['datetime'], ['date']]
        'month_floor' : [['datetime', 'datetime'], ['datetime', 'datetime'], ['date', 'date'], ['datetime', 'datetime', 'datetime'], ['datetime', 'datetime', 'integer'], ['datetime'], ['datetime'], ['date', 'date', 'date'], ['date', 'date', 'integer'], ['datetime'], ['datetime'], ['date']]
        'month_name' : [['varchar', 'datetime'], ['varchar', 'datetime'], ['varchar', 'date']]
        'months_add' : [['datetime', 'datetime', 'integer'], ['datetime'], ['date', 'date', 'integer'], ['date', 'date', 'integer']]
        'months_diff' : [['bigint', 'datetime', 'datetime'], ['bigint'], ['bigint', 'date', 'datetime'], ['bigint', 'datetime', 'date'], ['bigint', 'date', 'date'], ['bigint', 'date', 'datetime'], ['bigint', 'datetime', 'date'], ['bigint', 'datetime', 'datetime'], ['bigint', 'datetime', 'datetime']]
        'months_sub' : [['datetime', 'datetime', 'integer'], ['datetime'], ['date', 'date', 'integer'], ['date', 'date', 'integer']]
        'multi_distinct_count' : [['bigint.varargs(anydata']]
        'multi_distinct_sum' : [['bigint.varargs(bigint'], ['bigint.varargs(double'], ['bigint.varargs(largeint']]
        'murmur_hash332' : [['integer.varargs(varchar'], ['integer.varargs(string']]
        'murmur_hash364' : [['bigint.varargs(varchar'], ['bigint.varargs(string']]
        'ndv' : [['bigint', 'anydata']]
        'negative' : [['bigint', 'bigint'], ['double', 'double'], ['decimal', 'decimal']]
        'not_null_or_empty' : [['boolean', 'varchar'], ['boolean', 'string']]
        'now' : [['datetime', ''], ['datetime', 'integer']]
        'null_if' : [['boolean', 'boolean', 'boolean'], ['tinyint', 'tinyint', 'tinyint'], ['smallint', 'smallint', 'smallint'], ['integer', 'integer', 'integer'], ['bigint', 'bigint', 'bigint'], ['largeint', 'largeint', 'largeint'], ['float', 'float', 'float'], ['double', 'double', 'double'], ['datetime', 'datetime', 'datetime'], ['date', 'date', 'date'], ['datetime'], ['date', 'date', 'date'], ['decimal'], ['varchar'], ['string', 'string', 'string']]
        'null_or_empty' : [['boolean', 'varchar'], ['boolean', 'string']]
        'nvl' : [['boolean', 'boolean', 'boolean'], ['tinyint', 'tinyint', 'tinyint'], ['smallint', 'smallint', 'smallint'], ['integer', 'integer', 'integer'], ['bigint', 'bigint', 'bigint'], ['largeint', 'largeint', 'largeint'], ['float', 'float', 'float'], ['double', 'double', 'double'], ['date', 'date', 'date'], ['datetime', 'datetime', 'datetime'], ['datetime', 'date', 'datetime'], ['datetime', 'datetime', 'date'], ['datetime'], ['datetime'], ['datetime'], ['decimal'], ['bitmap', 'bitmap', 'bitmap'], ['varchar'], ['string', 'string', 'string']]
        'orthogonal_bitmap_union_count' : [['bigint', 'bitmap']]
        'parse_url' : [['varchar'], ['string', 'string', 'string'], ['varchar'], ['string']]
        'percentile' : [['double', 'bigint', 'double']]
        'percentile_approx' : [['double', 'double', 'double'], ['double']]
        'pi' : [['double', '']]
        'pmod' : [['bigint', 'bigint', 'bigint'], ['double', 'double', 'double']]
        'positive' : [['bigint', 'bigint'], ['double', 'double'], ['decimal', 'decimal']]
        'pow' : [['double', 'double', 'double']]
        'power' : [['double', 'double', 'double']]
        'protocol' : [['string', 'string']]
        'quantile_percent' : [['double', 'quantilestate', 'float']]
        'quantile_union' : [['quantilestate', 'quantilestate']]
        'quarter' : [['integer', 'datetime'], ['integer', 'datetime'], ['integer', 'date']]
        'radians' : [['double', 'double']]
        'random' : [['double', ''], ['double', 'bigint']]
        'regexp_extract' : [['varchar'], ['string']]
        'regexp_extract_all' : [['varchar'], ['string', 'string', 'string']]
        'regexp_replace' : [['varchar'], ['string']]
        'regexp_replace_one' : [['varchar'], ['string']]
        'repeat' : [['varchar', 'varchar', 'integer'], ['string', 'string', 'integer']]
        'replace' : [['varchar'], ['string']]
        'reverse' : [['varchar', 'varchar'], ['string', 'string']]
        'right' : [['varchar', 'varchar', 'integer'], ['string', 'string', 'integer']]
        'round' : [['double', 'double'], ['double', 'double', 'integer']]
        'round_bankers' : [['double', 'double'], ['double', 'double', 'integer']]
        'rpad' : [['varchar'], ['string']]
        'rtrim' : [['varchar', 'varchar'], ['string', 'string']]
        'running_difference' : [['smallint', 'tinyint'], ['integer', 'smallint'], ['bigint', 'integer'], ['largeint', 'bigint'], ['largeint', 'largeint'], ['double', 'float'], ['double', 'double'], ['decimal', 'decimal'], ['decimal.defaultdecimal32', 'decimal.defaultdecimal32'], ['decimal.defaultdecimal64', 'decimal.defaultdecimal64'], ['decimal.defaultdecimal128', 'decimal.defaultdecimal128'], ['integer', 'date'], ['integer', 'date'], ['double', 'datetime'], ['double', 'datetime']]
        'second' : [['integer', 'datetime'], ['integer', 'datetime'], ['integer', 'date']]
        'second_ceil' : [['datetime', 'datetime'], ['datetime', 'datetime'], ['datetime', 'date'], ['datetime', 'datetime', 'datetime'], ['datetime', 'datetime', 'integer'], ['datetime'], ['datetime'], ['datetime', 'date', 'date'], ['datetime', 'date', 'integer'], ['datetime'], ['datetime'], ['datetime']]
        'second_floor' : [['datetime', 'datetime'], ['datetime', 'datetime'], ['datetime', 'date'], ['datetime', 'datetime', 'datetime'], ['datetime', 'datetime', 'integer'], ['datetime'], ['datetime'], ['datetime', 'date', 'date'], ['datetime', 'date', 'integer'], ['datetime'], ['datetime'], ['datetime']]
        'seconds_add' : [['datetime', 'datetime', 'integer'], ['datetime'], ['datetime', 'date', 'integer'], ['datetime', 'date', 'integer']]
        'seconds_diff' : [['bigint', 'datetime', 'datetime'], ['bigint'], ['bigint', 'date', 'datetime'], ['bigint', 'datetime', 'date'], ['bigint', 'date', 'date'], ['bigint', 'date', 'datetime'], ['bigint', 'datetime', 'date'], ['bigint', 'datetime', 'datetime'], ['bigint', 'datetime', 'datetime']]
        'seconds_sub' : [['datetime', 'datetime', 'integer'], ['datetime'], ['datetime', 'date', 'integer'], ['datetime', 'date', 'integer']]
        'sequence_count' : [['bigint'], ['bigint'], ['bigint']]
        'sequence_match' : [['boolean'], ['boolean'], ['boolean']]
        'sign' : [['tinyint', 'double']]
        'sin' : [['double', 'double']]
        'sleep' : [['boolean', 'integer']]
        'sm3' : [['varchar', 'varchar'], ['varchar', 'string']]
        'sm3sum' : [['varchar.varargs(varchar'], ['varchar.varargs(string']]
        'sm4_decrypt' : [['varchar'], ['string', 'string', 'string'], ['varchar'], ['string'], ['varchar'], ['string']]
        'sm4_encrypt' : [['varchar'], ['string', 'string', 'string'], ['varchar'], ['string'], ['varchar'], ['string']]
        'space' : [['varchar', 'integer']]
        'split_part' : [['varchar'], ['string']]
        'sqrt' : [['double', 'double']]
        'st_astext' : [['varchar', 'varchar'], ['varchar', 'string']]
        'st_aswkt' : [['varchar', 'varchar'], ['varchar', 'string']]
        'st_circle' : [['varchar']]
        'st_contains' : [['boolean', 'varchar', 'varchar']]
        'st_distance_sphere' : [['double']]
        'st_geometryfromtext' : [['varchar', 'varchar'], ['varchar', 'string']]
        'st_geomfromtext' : [['varchar', 'varchar'], ['varchar', 'string']]
        'st_linefromtext' : [['varchar', 'varchar'], ['varchar', 'string']]
        'st_linestringfromtext' : [['varchar', 'varchar'], ['varchar', 'string']]
        'st_point' : [['varchar', 'double', 'double']]
        'st_polyfromtext' : [['varchar', 'varchar'], ['varchar', 'string']]
        'st_polygon' : [['varchar', 'varchar'], ['varchar', 'string']]
        'st_polygonfromtext' : [['varchar', 'varchar'], ['varchar', 'string']]
        'st_x' : [['double', 'varchar'], ['double', 'string']]
        'st_y' : [['double', 'varchar'], ['double', 'string']]
        'starts_with' : [['boolean', 'varchar', 'varchar'], ['boolean', 'string', 'string']]
        'stddev' : [['double', 'tinyint'], ['double', 'smallint'], ['double', 'integer'], ['double', 'bigint'], ['double', 'float'], ['double', 'double'], ['decimal', 'decimal']]
        'stddev_samp' : [['double', 'tinyint'], ['double', 'smallint'], ['double', 'integer'], ['double', 'bigint'], ['double', 'float'], ['double', 'double'], ['decimal', 'decimal']]
        'str_left' : [['varchar', 'varchar', 'integer'], ['string', 'string', 'integer']]
        'str_right' : [['varchar', 'varchar', 'integer'], ['string', 'string', 'integer']]
        'str_to_date' : [['datetime', 'varchar', 'varchar'], ['datetime', 'string', 'string']]
        'sub_bitmap' : [['bitmap']]
        'sub_replace' : [['varchar'], ['string'], ['varchar'], ['string']]
        'substring' : [['varchar', 'varchar', 'integer'], ['string', 'string', 'integer'], ['varchar'], ['string']]
        'substring_index' : [['varchar'], ['string']]
        'sum' : [['bigint', 'tinyint'], ['bigint', 'smallint'], ['bigint', 'integer'], ['bigint', 'bigint'], ['double', 'double'], ['decimal', 'decimal'], ['decimal.defaultdecimal128', 'decimal.defaultdecimal32'], ['decimal.defaultdecimal128', 'decimal.defaultdecimal64'], ['decimal.defaultdecimal128', 'decimal.defaultdecimal128'], ['largeint', 'largeint']]
        'tan' : [['double', 'double']]
        'time_diff' : [['time', 'datetime', 'datetime'], ['time'], ['time', 'datetime', 'date'], ['time', 'date', 'datetime'], ['time', 'date', 'date'], ['time', 'datetime', 'datetime'], ['time', 'date', 'datetime']]
        'timestamp' : [['datetime', 'datetime'], ['datetime', 'datetime']]
        'to_base64' : [['string', 'string']]
        'to_bitmap' : [['bitmap', 'varchar'], ['bitmap', 'string']]
        'to_bitmap_with_check' : [['bitmap', 'varchar'], ['bitmap', 'string']]
        'to_date' : [['date', 'datetime'], ['date', 'datetime']]
        'to_date_v2' : [['date', 'datetime']]
        'to_days' : [['integer', 'date'], ['integer', 'date']]
        'to_monday' : [['date', 'datetime'], ['date', 'date'], ['date', 'datetime'], ['date', 'date']]
        'to_quantile_state' : [['quantilestate', 'varchar', 'float']]
        'top_n' : [['varchar', 'varchar', 'integer'], ['varchar', 'string', 'integer'], ['varchar'], ['varchar']]
        'top_n_weighted' : [['varchar'], ['varchar']]
        'trim' : [['varchar', 'varchar'], ['string', 'string']]
        'truncate' : [['double', 'double', 'integer']]
        'unhex' : [['varchar', 'varchar'], ['string', 'string']]
        'unix_timestamp' : [['integer', ''], ['integer', 'datetime'], ['integer', 'date'], ['integer', 'datetime'], ['integer', 'date'], ['integer', 'varchar', 'varchar'], ['integer', 'string', 'string']]
        'upper' : [['varchar', 'varchar'], ['string', 'string']]
        'user' : [['varchar', '']]
        'utc_timestamp' : [['datetime', '']]
        'uuid' : [['varchar', '']]
        'variance' : [['double', 'tinyint'], ['double', 'smallint'], ['double', 'integer'], ['double', 'bigint'], ['double', 'float'], ['double', 'double'], ['decimal', 'decimal']]
        'variance_samp' : [['double', 'tinyint'], ['double', 'smallint'], ['double', 'integer'], ['double', 'bigint'], ['double', 'float'], ['double', 'double'], ['decimal', 'decimal']]
        'version' : [['varchar', '']]
        'week' : [['integer', 'datetime'], ['integer', 'datetime'], ['integer', 'date'], ['integer', 'datetime', 'integer'], ['integer', 'datetime', 'integer'], ['integer', 'date', 'integer']]
        'week_ceil' : [['datetime', 'datetime'], ['datetime', 'datetime'], ['date', 'date'], ['datetime', 'datetime', 'datetime'], ['datetime', 'datetime', 'integer'], ['datetime'], ['datetime'], ['date', 'date', 'date'], ['date', 'date', 'integer'], ['datetime'], ['datetime'], ['date']]
        'week_floor' : [['datetime', 'datetime'], ['datetime', 'datetime'], ['date', 'date'], ['datetime', 'datetime', 'datetime'], ['datetime', 'datetime', 'integer'], ['datetime'], ['datetime'], ['date', 'date', 'date'], ['date', 'date', 'integer'], ['datetime'], ['datetime'], ['date']]
        'week_of_year' : [['integer', 'datetime'], ['integer', 'datetime'], ['integer', 'date']]
        'weekday' : [['integer', 'datetime'], ['integer', 'datetime'], ['integer', 'date']]
        'weeks_diff' : [['bigint', 'datetime', 'datetime'], ['bigint'], ['bigint', 'date', 'datetime'], ['bigint', 'datetime', 'date'], ['bigint', 'date', 'date'], ['bigint', 'date', 'datetime'], ['bigint', 'datetime', 'date'], ['bigint', 'datetime', 'datetime'], ['bigint', 'datetime', 'datetime']]
        'window_funnel' : [['integer'], ['varchar']]
        'year' : [['integer', 'date'], ['integer', 'datetime'], ['integer', 'datetime']]
        'year_ceil' : [['datetime', 'datetime'], ['datetime', 'datetime'], ['date', 'date'], ['datetime', 'datetime', 'datetime'], ['datetime', 'datetime', 'integer'], ['datetime'], ['datetime'], ['date', 'date', 'date'], ['date', 'date', 'integer'], ['datetime'], ['datetime'], ['date']]
        'year_floor' : [['datetime', 'datetime'], ['datetime', 'datetime'], ['date', 'date'], ['datetime', 'datetime', 'datetime'], ['datetime', 'datetime', 'integer'], ['datetime'], ['datetime'], ['date', 'date', 'date'], ['date', 'date', 'integer'], ['datetime'], ['datetime'], ['date']]
        'year_week' : [['integer', 'datetime'], ['integer', 'datetime'], ['integer', 'date'], ['integer', 'datetime', 'integer'], ['integer', 'datetime', 'integer'], ['integer', 'date', 'integer']]
        'years_add' : [['datetime', 'datetime', 'integer'], ['datetime'], ['date', 'date', 'integer'], ['date', 'date', 'integer']]
        'years_diff' : [['bigint', 'datetime', 'datetime'], ['bigint'], ['bigint', 'date', 'datetime'], ['bigint', 'datetime', 'date'], ['bigint', 'date', 'date'], ['bigint', 'date', 'datetime'], ['bigint', 'datetime', 'date'], ['bigint', 'datetime', 'datetime'], ['bigint', 'datetime', 'datetime']]
        'years_sub' : [['datetime', 'datetime', 'integer'], ['datetime'], ['date', 'date', 'integer'], ['date', 'date', 'integer']]
    ]
    // function table end

    def args = "arg1, arg2, arg3, ..."
    def fn_str = "fn(arg1, arg2, arg3, ...)"

    // test begin
    sql """
        select fn_str from t1
        select fn_str from t2
        select fn_str from t3
    """
    // test end
}