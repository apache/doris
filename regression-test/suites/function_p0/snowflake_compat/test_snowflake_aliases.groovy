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

suite("test_snowflake_aliases") {

    sql """ set enable_nereids_planner=true; """
    sql """ set enable_fallback_to_original_planner=false; """

    // ===== IFF (alias for IF) =====
    qt_iff_true """ SELECT IFF(1 > 0, 'yes', 'no') """
    qt_iff_false """ SELECT IFF(1 < 0, 'yes', 'no') """

    // ===== LEN (alias for CharacterLength — character count, NOT byte count) =====
    // Critical: LEN must return character count for multi-byte characters
    qt_len_ascii """ SELECT LEN('hello') """
    qt_len_unicode """ SELECT LEN('你好') """
    qt_len_empty """ SELECT LEN('') """
    qt_len_mixed """ SELECT LEN('abc你好') """

    // Verify LEN != LENGTH for multi-byte (LENGTH returns byte count)
    qt_len_vs_length """ SELECT LEN('你好') != LENGTH('你好') """

    // ===== CHARINDEX (alias for Locate — parameter order must match) =====
    qt_charindex_basic """ SELECT CHARINDEX('cd', 'abcdef') """
    qt_charindex_not_found """ SELECT CHARINDEX('xyz', 'abcdef') """
    qt_charindex_with_pos """ SELECT CHARINDEX('cd', 'abcdef', 4) """
    qt_charindex_with_pos2 """ SELECT CHARINDEX('ab', 'abcabc', 2) """

    // ===== ARRAY_CONSTRUCT (alias for Array) =====
    qt_array_construct """ SELECT ARRAY_CONSTRUCT(1, 2, 3) """

    // ===== ARRAY_CAT (alias for ArrayConcat) =====
    qt_array_cat """ SELECT ARRAY_CAT(ARRAY(1, 2), ARRAY(3, 4)) """

    // ===== ARRAY_TO_STRING (alias for ArrayJoin) =====
    qt_array_to_string """ SELECT ARRAY_TO_STRING(ARRAY('a', 'b', 'c'), ',') """

    // ===== PARSE_JSON (alias for JsonbParse) =====
    qt_parse_json """ SELECT PARSE_JSON('{"a":1}') """

    // ===== TRY_PARSE_JSON (alias for JsonbParseErrorToNull) =====
    qt_try_parse_json_valid """ SELECT TRY_PARSE_JSON('{"a":1}') """
    qt_try_parse_json_invalid """ SELECT TRY_PARSE_JSON('invalid json') """

    // ===== CHECK_JSON (alias for JsonbValid) =====
    qt_check_json_valid """ SELECT CHECK_JSON('{"a":1}') """
    qt_check_json_invalid """ SELECT CHECK_JSON('not json') """

    // ===== BASE64_ENCODE (alias for ToBase64) =====
    qt_base64_encode """ SELECT BASE64_ENCODE('hello') """

    // ===== BASE64_DECODE_STRING (alias for FromBase64) =====
    qt_base64_decode_string """ SELECT BASE64_DECODE_STRING(BASE64_ENCODE('hello')) """

    // ===== HEX_ENCODE (alias for Hex) =====
    qt_hex_encode """ SELECT HEX_ENCODE('hello') """

    // ===== HEX_DECODE_STRING (alias for Unhex) =====
    qt_hex_decode_string """ SELECT HEX_DECODE_STRING(HEX_ENCODE('hello')) """

    // ===== STARTSWITH (alias for StartsWith) =====
    qt_startswith_true """ SELECT STARTSWITH('hello', 'he') """
    qt_startswith_false """ SELECT STARTSWITH('hello', 'lo') """

    // ===== ENDSWITH (alias for EndsWith) =====
    qt_endswith_true """ SELECT ENDSWITH('hello', 'lo') """
    qt_endswith_false """ SELECT ENDSWITH('hello', 'he') """

    // ===== GETDATE (alias for Now) =====
    qt_getdate_not_null """ SELECT GETDATE() IS NOT NULL """

    // ===== Verify alias results match native function results =====
    qt_iff_eq_if """ SELECT IFF(true, 1, 2) = IF(true, 1, 2) """
    qt_len_eq_char_length """ SELECT LEN('test') = CHAR_LENGTH('test') """
    qt_charindex_eq_locate """ SELECT CHARINDEX('b', 'abc') = LOCATE('b', 'abc') """
    qt_startswith_eq """ SELECT STARTSWITH('abc', 'ab') = STARTS_WITH('abc', 'ab') """
    qt_endswith_eq """ SELECT ENDSWITH('abc', 'bc') = ENDS_WITH('abc', 'bc') """
}
