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

suite("variant_parse_functions", "p0,nonConcurrent") {
    setFeConfigTemporary([enable_variant_v2: true]) {
    assertTrue(getFeConfig("enable_variant_v2").toBoolean())
    order_qt_valid_json_supported """
        SELECT /*+SET_VAR(enable_fold_constant_by_be=false)*/
            CAST(parse_to_variant('{"object":{"k":1},"array":[true,null],"text":"v"}')['object']['k'] AS INT),
            CAST(parse_to_variant('{"object":{"k":1},"array":[true,null],"text":"v"}')['array'] AS ARRAY<BOOLEAN>),
            CAST(parse_to_variant('{"object":{"k":1},"array":[true,null],"text":"v"}')['text'] AS STRING),
            CAST(parse_to_variant('42') AS INT), CAST(parse_to_variant('"json string"') AS STRING)
    """

    def nullableInput = """SELECT 1 AS id, CAST(NULL AS STRING) AS payload
            UNION ALL SELECT 2 AS id, 'null' AS payload
            UNION ALL SELECT 3 AS id, '{"k":1}' AS payload"""
    order_qt_sql_null_json_null_and_nullable_input_supported """
        SELECT /*+SET_VAR(enable_fold_constant_by_be=false)*/ id,
            parse_to_variant(payload) IS NULL, try_parse_to_variant(payload) IS NULL,
            payload IS NULL,
            CASE WHEN payload = '{"k":1}' THEN CAST(parse_to_variant(payload)['k'] AS INT) END,
            CASE WHEN payload = '{"k":1}' THEN CAST(try_parse_to_variant(payload)['k'] AS INT) END
        FROM (${nullableInput}) t ORDER BY id
    """

    setBeConfigTemporary([variant_throw_exeception_on_invalid_json: true]) {
        test {
            sql """
                SELECT /*+SET_VAR(enable_fold_constant_by_be=false)*/
                    parse_to_variant('{')
            """
            exception "Parse json document failed at row 0, error:"
        }
        test {
            sql """
                SELECT /*+SET_VAR(enable_fold_constant_by_be=false)*/
                    json_parse('{')
            """
            exception "Parse json document failed at row 0, error:"
        }
        order_qt_strict_error_to_null """
            SELECT /*+SET_VAR(enable_fold_constant_by_be=false)*/
                try_parse_to_variant('{') IS NULL,
                try_parse_to_variant('') IS NULL
        """
    }

    setBeConfigTemporary([variant_throw_exeception_on_invalid_json: false]) {
        order_qt_permissive_invalid_json_is_string """
            SELECT /*+SET_VAR(enable_fold_constant_by_be=false)*/
                CAST(parse_to_variant('{') AS STRING),
                parse_to_variant('{') IS NULL,
                CAST(try_parse_to_variant('{') AS STRING),
                try_parse_to_variant('{') IS NULL
        """
    }

    setBeConfigTemporary([
            variant_throw_exeception_on_invalid_json: true,
            variant_max_json_key_length: 3
    ]) {
        test {
            sql """
                SELECT /*+SET_VAR(enable_fold_constant_by_be=false)*/
                    parse_to_variant('{"long":1}')
            """
            exception "Parse json document failed at row 0, error:"
        }
        order_qt_key_too_long_error_to_null """
            SELECT /*+SET_VAR(enable_fold_constant_by_be=false)*/
                try_parse_to_variant('{"long":1}') IS NULL
        """
    }

    setBeConfigTemporary([
            variant_throw_exeception_on_invalid_json: true,
            variant_enable_duplicate_json_path_check: false
    ]) {
        test {
            sql """
                SELECT /*+SET_VAR(enable_fold_constant_by_be=false)*/
                    parse_to_variant('{"dup":1,"dup":2}')
            """
            exception "Parse json document failed at row 0, error:"
        }
        order_qt_duplicate_key_error_to_null """
            SELECT /*+SET_VAR(enable_fold_constant_by_be=false)*/
                try_parse_to_variant('{"dup":1,"dup":2}') IS NULL
        """
    }

    setBeConfigTemporary([
            variant_throw_exeception_on_invalid_json: true,
            variant_enable_duplicate_json_path_check: true
    ]) {
        order_qt_duplicate_key_first_wins """
            SELECT /*+SET_VAR(enable_fold_constant_by_be=false)*/
                CAST(parse_to_variant('{"dup":1,"dup":2}') AS STRING),
                CAST(try_parse_to_variant('{"dup":1,"dup":2}') AS STRING)
        """
    }
    }
}
