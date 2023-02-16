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

const_sql = {
    # scalar_function
    'aes_decrypt_Varchar_Varchar_Varchar_Varchar': "select aes_decrypt(kvchrs1, kvchrs1, kvchrs1, 'AES_128_ECB') from ${t} order by kvchrs1, kvchrs1, kvchrs1",
    'aes_decrypt_String_String_String_String': "select aes_decrypt(kstr, kstr, kstr, 'AES_128_ECB') from ${t} order by kstr, kstr, kstr, kstr",
    'aes_encrypt_Varchar_Varchar_Varchar_Varchar': "select aes_encrypt(kvchrs1, kvchrs1, kvchrs1, 'AES_128_ECB') from ${t} order by kvchrs1, kvchrs1, kvchrs1",
    'aes_encrypt_String_String_String_String': "select aes_encrypt(kstr, kstr, kstr, 'AES_128_ECB') from ${t} order by kstr, kstr, kstr, kstr",
    'convert_to_Varchar_Varchar': "select convert_to(kvchrs1, 'gbk') from ${t} order by kvchrs1",
    'convert_tz_DateTime_Varchar_Varchar': "select convert_tz(kdtm, 'Asia/Shanghai', 'Europe/Sofia') from ${t} order by kdtm",
    'convert_tz_DateTimeV2_Varchar_Varchar': "select convert_tz(kdtmv2s1, 'Asia/Shanghai', 'Europe/Sofia') from ${t} order by kdtmv2s1",
    'date_format_DateTime_Varchar': "select date_format(kdtm, '2006-01-02 12:00:00') from ${t} order by kdtm",
    'date_format_Date_Varchar': "select date_format(kdt, '2006-01-02') from ${t} order by kdt",
    'date_format_DateTimeV2_Varchar': "select date_format(kdtmv2s1, '2006-01-02 12:00:00') from ${t} order by kdtmv2s1",
    'date_format_DateV2_Varchar': "select date_format(kdtv2, '2006-01-02') from ${t} order by kdtv2",
    'dround_Double_Integer': "select dround(kdbl, 2) from ${t} order by kdbl",
    'field_TinyInt': "select field(ktint, 1, 2) from ${t} order by ktint",
    'field_SmallInt': "select field(ksint, 1, 2) from ${t} order by ksint",
    'field_Integer': "select field(kint, 1, 2) from ${t} order by kint",
    'field_BigInt': "select field(kbint, 1, 2) from ${t} order by kbint",
    'field_LargeInt': "select field(klint, 1, 2) from ${t} order by klint",
    'field_Float': "select field(kfloat, 1, 2) from ${t} order by kfloat",
    'field_Double': "select field(kdbl, 1, 2) from ${t} order by kdbl",
    'field_DecimalV2': "select field(kdcmls1, 1, 2) from ${t} order by kdcmls1",
    'field_DateV2': "select field(kdtv2, 1, 2) from ${t} order by kdtv2",
    'field_DateTimeV2': "select field(kdtmv2s1, 1, 2) from ${t} order by kdtmv2s1",
    'field_Varchar': "select field(kvchrs1, 1, 2) from ${t} order by kvchrs1",
    'field_String': "select field(kstr, 1, 2) from ${t} order by kstr",
    'from_unixtime_Integer_Varchar': "select from_unixtime(kint, 'varchar') from ${t} order by kint",
    'from_unixtime_Integer_String': "select from_unixtime(kint, 'string') from ${t} order by kint",
    'now_Integer': "select now() from ${t} where kint is not null order by kint",
    'parse_url_Varchar_Varchar': "select parse_url(kvchrs1, 'HOST') from ${t} order by kvchrs1, kvchrs1",
    'parse_url_String_String': "select parse_url(kstr, 'HOST') from ${t} order by kstr, kstr",
    'parse_url_Varchar_Varchar_Varchar': "select parse_url(kvchrs1, 'HOST', 'PROTOCOL') from ${t} order by kvchrs1, kvchrs1, kvchrs1",
    'parse_url_String_String_String': "select parse_url(kstr, 'HOST', 'PROTOCOL') from ${t} order by kstr, kstr, kstr",
    'round_Double_Integer': "select round(kdbl, 2) from ${t} order by kdbl",
    'round_bankers_Double_Integer': "select round_bankers(kdbl, 2) from ${t} order by kdbl",
    'running_difference_DateTime': "select cast(running_difference(kdtm) as string) from ${t} order by kdtm",
    'running_difference_DateTimeV2': "select cast(running_difference(kdtmv2s1) as string) from ${t} order by kdtmv2s1",
    'sleep_Integer': "select sleep(0.1) from ${t} order by kint",
    'sm4_decrypt_Varchar_Varchar_Varchar_Varchar': "select sm4_decrypt(kvchrs1, kvchrs1, kvchrs1, 'SM4_128_ECB') from ${t} order by kvchrs1, kvchrs1, kvchrs1",
    'sm4_decrypt_String_String_String_String': "select sm4_decrypt(kstr, kstr, kstr, 'SM4_128_ECB') from ${t} order by kstr, kstr, kstr",
    'sm4_encrypt_Varchar_Varchar_Varchar_Varchar': "select sm4_encrypt(kvchrs1, kvchrs1, kvchrs1, 'SM4_128_ECB') from ${t} order by kvchrs1, kvchrs1, kvchrs1",
    'sm4_encrypt_String_String_String_String': "select sm4_encrypt(kstr, kstr, kstr, 'SM4_128_ECB') from ${t} order by kstr, kstr, kstr",
    'space_Integer': "select space(10) from ${t} order by kint",
    'split_part_Varchar_Varchar_Integer': "select split_part(kvchrs1, ' ', 1) from ${t} order by kvchrs1",
    'split_part_String_String_Integer': "select split_part(kstr, ' ', 1) from ${t} order by kstr",
    'substring_index_Varchar_Varchar_Integer': "select substring_index(kvchrs1, ' ', 2) from ${t} order by kvchrs1",
    'substring_index_String_String_Integer': "select substring_index(kstr, ' ', 2) from ${t} order by kstr",
    'truncate_Double_Integer': "select truncate(kdbl, 2) from ${t} order by kdbl",
    'random': "select random() from ${t}",
    'random_BigInt': "select random(1000) from ${t} order by kbint",
    'to_quantile_state_Varchar_Float': 'select to_quantile_state(kvchrs1, 2048) from ${t} order by kvchrs1',
    # agg
    'group_concat_Varchar_Varchar_AnyData': 'select group_concat(distinct cast(abs(kint) as varchar) order by abs(ksint), kdt) from ${t}',
    'group_concat_Varchar_AnyData': 'select group_concat(distinct cast(abs(kint) as varchar) order by abs(ksint), kdt) from ${t}',
    'percentile_BigInt_Double': 'select percentile(kbint, 0.6) from ${t}',
    'percentile_approx_Double_Double': 'select percentile_approx(kdbl, 0.6) from ${t}',
    'percentile_approx_Double_Double_Double': 'select percentile_approx(kdbl, 0.6, 4096.0) from ${t}',
    'sequence_count_String_DateV2_Boolean': 'select sequence_count(\'(?1)(?2)\', kdtv2, kint = 1, kint = 2) from ${t}',
    'sequence_count_String_DateTime_Boolean': 'select sequence_count(\'(?1)(?2)\', kdtm, kint = 1, kint = 2) from ${t}',
    'sequence_count_String_DateTimeV2_Boolean': 'select sequence_count(\'(?1)(?2)\', kdtmv2s1, kint = 1, kint = 5) from ${t}',
    'sequence_match_String_DateV2_Boolean': 'select sequence_match(\'(?1)(?2)\', kdtv2, kint = 1, kint = 2) from ${t}',
    'sequence_match_String_DateTime_Boolean': 'select sequence_match(\'(?1)(?2)\', kdtm, kint = 1, kint = 2) from ${t}',
    'sequence_match_String_DateTimeV2_Boolean': 'select sequence_match(\'(?1)(?2)\', kdtmv2s1, kint = 1, kint = 2) from ${t}',
    'topn_Varchar_Integer': 'select topn(kvchrs1, 3) from ${t}',
    'topn_String_Integer': 'select topn(kstr, 3) from ${t}',
    'topn_Varchar_Integer_Integer': 'select topn(kvchrs1, 3, 100) from ${t}',
    'topn_String_Integer_Integer': 'select topn(kstr, 3, 100) from ${t}',
    'window_funnel_BigInt_String_DateTime_Boolean': 'select window_funnel(3600 * 3, \'default\', kdtm, kint = 1, kint = 2) from ${t}',
    'window_funnel_BigInt_String_DateTimeV2_Boolean': 'select window_funnel(3600 * 3, \'default\', kdtmv2s1, kint = 1, kint = 2) from ${t}',
    # gen
    'explode_bitmap_Bitmap': 'select kint, e from (select kint, kbint from ${t}) t lateral view explode_bitmap(to_bitmap(kbint)) lv as e',
    'explode_bitmap_outer_Bitmap': 'select kint, e from (select kint, kbint from ${t}) t lateral view explode_bitmap_outer(to_bitmap(kbint)) lv as e',
}

not_check_result = {
    'aes_decrypt_Varchar_Varchar',
    'aes_decrypt_String_String',
    'aes_decrypt_Varchar_Varchar_Varchar',
    'aes_decrypt_String_String_String',
    'aes_decrypt_Varchar_Varchar_Varchar_Varchar',
    'aes_decrypt_String_String_String_String',
    'aes_encrypt_Varchar_Varchar',
    'aes_encrypt_String_String',
    'aes_encrypt_Varchar_Varchar_Varchar',
    'aes_encrypt_String_String_String',
    'aes_encrypt_Varchar_Varchar_Varchar_Varchar',
    'aes_encrypt_String_String_String_String',
    'connection_id',
    'current_user',
    'database',
    'from_base64_Varchar',
    'from_base64_String',
    'now',
    'now_Integer',
    'parse_url_Varchar_Varchar',
    'parse_url_String_String',
    'parse_url_Varchar_Varchar_Varchar',
    'parse_url_String_String_String',
    'random',
    'random_BigInt',
    'running_difference_TinyInt',
    'running_difference_SmallInt',
    'running_difference_Integer',
    'running_difference_BigInt',
    'running_difference_LargeInt',
    'running_difference_Float',
    'running_difference_Double',
    'running_difference_DecimalV2',
    'running_difference_Date',
    'running_difference_DateV2',
    'running_difference_DateTime',
    'running_difference_DateTimeV2',
    'running_difference_DateTime',
    'running_difference_DateTimeV2',
    'sm4_decrypt_Varchar_Varchar',
    'sm4_decrypt_String_String',
    'sm4_decrypt_Varchar_Varchar_Varchar',
    'sm4_decrypt_String_String_String',
    'sm4_decrypt_Varchar_Varchar_Varchar_Varchar',
    'sm4_decrypt_String_String_String_String',
    'sm4_encrypt_Varchar_Varchar',
    'sm4_encrypt_String_String',
    'sm4_encrypt_Varchar_Varchar_Varchar',
    'sm4_encrypt_String_String_String',
    'sm4_encrypt_Varchar_Varchar_Varchar_Varchar',
    'sm4_encrypt_String_String_String_String',
    'space_Integer',
    'user',
    'unix_timestamp'
}

denied_tag = {
    'esquery',
    'hll_cardinality',
    'hll_union',
    'hll_union_agg',
    'to_quantile_state',
    'quantile_percent',
    'quantile_union'
}

header = '''// Licensed to the Apache Software Foundation (ASF) under one
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

'''
