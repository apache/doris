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

suite("regexp_position") {
    // S02: each optional argument extends the supported two-to-seven argument interface.
    order_qt_arity_2 "select regexp_position('abcabc', 'b')"
    order_qt_arity_3 "select regexp_position('abcabc', 'b', 3)"
    order_qt_arity_4 "select regexp_position('abcabc', 'b', 1, 2)"
    order_qt_arity_5 "select regexp_position('abcabc', 'b', 1, 2, 1)"
    order_qt_arity_6 "select regexp_position('abcABC', 'b', 1, 2, 0, 'i')"
    order_qt_arity_7 "select regexp_position('abcabc', '(a)(bc)', 1, 2, 0, '', 2)"

    test {
        sql "select regexp_position('abc')"
        exception "Can not found function 'regexp_position'"
    }
    test {
        sql "select regexp_position('abc', 'b', 1, 1, 0, '', 0, 0)"
        exception "Can not found function 'regexp_position'"
    }

    // S03: all public aliases and their case variants resolve to the same function.
    order_qt_aliases """
        select regexp_position('abcabc', 'b', 1, 2),
               regexp_instr('abcabc', 'b', 1, 2),
               regexpPosition('abcabc', 'b', 1, 2),
               regexpInstr('abcabc', 'b', 1, 2)
    """
    order_qt_aliases_case """
        select REGEXP_POSITION('abcabc', 'b', 1, 2),
               ReGeXp_InStR('abcabc', 'b', 1, 2),
               REGEXPPOSITION('abcabc', 'b', 1, 2),
               rEgExPiNsTr('abcabc', 'b', 1, 2)
    """

    // S04: occurrence counts complete, non-overlapping matches.
    order_qt_basic_matches """
        select regexp_position('abcabc', 'bc'),
               regexp_position('abcabc', 'z'),
               regexp_position('abcabc', 'bc', 1, 2),
               regexp_position('abcabc', 'bc', 1, 3)
    """
    order_qt_non_overlapping_occurrence """
        select regexp_position('aaa', 'aa', 1, 2),
               regexp_position('aaaa', 'aa', 1, 2)
    """

    // S05: positions and match ends count UTF-8 bytes for Chinese and emoji.
    order_qt_utf8_byte_positions """
        select regexp_position('a中文b', '中'),
               regexp_position('a中文b', '中', 1, 1, 1),
               regexp_position('a中文b', '文'),
               regexp_position('a中文b', '文', 1, 1, 1),
               regexp_position('a😀b😀', '😀'),
               regexp_position('a😀b😀', '😀', 1, 1, 1),
               regexp_position('a😀b😀', '😀', 1, 2),
               regexp_position('a😀b😀', '😀', 1, 2, 1)
    """

    // S06: a byte start may fall inside UTF-8; the end and out-of-range starts stay distinct.
    order_qt_utf8_internal_start """
        select regexp_position('äbc', '.', 2),
               regexp_position('😀a', '.', 3)
    """
    order_qt_start_at_end """
        select regexp_position('äbc', '.', 4),
               regexp_position('äbc', '.', 5),
               regexp_position('äbc', '.', 6),
               regexp_position('äbc', '', 4),
               regexp_position('äbc', '', 5),
               regexp_position('äbc', '', 6),
               regexp_position('', '', 1),
               regexp_position('', '', 2)
    """
    // S07: NUL and invalid UTF-8 bytes preserve searchable byte positions.
    order_qt_embedded_nul """
        select regexp_position(unhex('610062'), 'b'),
               regexp_position(unhex('610062'), 'b', 1, 1, 1),
               regexp_position(unhex('610062'), unhex('00')),
               regexp_position(unhex('610062'), unhex('00'), 1, 1, 1)
    """
    order_qt_invalid_utf8_haystack """
        select regexp_position(unhex('61ff62'), 'b'),
               regexp_position(unhex('61ff62'), 'b', 1, 1, 1),
               regexp_position(unhex('61ff62'), unhex('ff')),
               regexp_position(unhex('61ff62'), unhex('ff'), 1, 1, 1)
    """

    // S08: flags apply left to right; multiline anchors and dot-newline are independent.
    order_qt_flags_case_order """
        select regexp_position('AbC', 'abc', 1, 1, 0, ''),
               regexp_position('AbC', 'abc', 1, 1, 0, 'i'),
               regexp_position('AbC', 'abc', 1, 1, 0, 'ic'),
               regexp_position('AbC', 'abc', 1, 1, 0, 'ci')
    """
    order_qt_flags_multiline '''
        select regexp_position(unhex('610a620a63'), '^b$', 1, 1, 0, ''),
               regexp_position(unhex('610a620a63'), '^b$', 1, 1, 0, 'm'),
               regexp_position(unhex('610a620a63'), '^b$', 1, 1, 0, 'n'),
               regexp_position(unhex('610a620a63'), '^b$', 1, 1, 0, 's')
    '''
    order_qt_flags_dot_newline """
        select regexp_position(unhex('610a62'), 'a.b', 1, 1, 0, ''),
               regexp_position(unhex('610a62'), 'a.b', 1, 1, 0, 'm'),
               regexp_position(unhex('610a62'), 'a.b', 1, 1, 0, 'n'),
               regexp_position(unhex('610a62'), 'a.b', 1, 1, 0, 's')
    """
    order_qt_flags_inline_modifiers '''
        select regexp_position('AbC', '(?i)abc', 1, 1, 0, 'c'),
               regexp_position('AbC', '(?-i)abc', 1, 1, 0, 'i'),
               regexp_position(unhex('610a620a63'), '(?m)^b$', 1, 1, 0, 's'),
               regexp_position(unhex('610a620a63'), '(?-m)^b$', 1, 1, 0, 'm'),
               regexp_position(unhex('610a62'), '(?s)a.b', 1, 1, 0, 'm'),
               regexp_position(unhex('610a62'), '(?-s)a.b', 1, 1, 0, 's')
    '''
    // S09: a constant NULL haystack suppresses runtime validation of the other arguments.
    order_qt_constant_null_invalid_pattern "select regexp_position(NULL, '[')"
    order_qt_constant_null_invalid_flags "select regexp_position(NULL, 'a', 1, 1, 0, 'z')"
    order_qt_constant_null_invalid_numbers """
        select regexp_position(NULL, 'a', 0),
               regexp_position(NULL, 'a', 1, 0),
               regexp_position(NULL, 'a', 1, 1, 2),
               regexp_position(NULL, 'a', 1, 1, 0, '', -1)
    """

    // S10: every argument propagates NULL before runtime validation of that row.
    order_qt_null_each_argument """
        select regexp_position(NULL, 'b', 1, 1, 0, '', 0),
               regexp_position('abc', NULL, 1, 1, 0, '', 0),
               regexp_position('abc', 'b', NULL, 1, 0, '', 0),
               regexp_position('abc', 'b', 1, NULL, 0, '', 0),
               regexp_position('abc', 'b', 1, 1, NULL, '', 0),
               regexp_position('abc', 'b', 1, 1, 0, NULL, 0),
               regexp_position('abc', 'b', 1, 1, 0, '', NULL)
    """
    // Rows 0..4 each have one NULL and invalid numeric values; rows 5..6 are valid.
    order_qt_mixed_null_row_priority """
        select number,
               regexp_position(if(number = 0, NULL, 'abcabc'), 'b',
                   if(number = 1, NULL, if(number < 5, 0, 1)),
                   if(number = 2, NULL, if(number < 5, 0, if(number = 6, 2, 1))),
                   if(number = 3, NULL, if(number < 5, 2, 0)), '',
                   if(number = 4, NULL, if(number < 5, -1, 0)))
        from numbers("number" = "7")
    """
    test {
        sql """
            select regexp_position(if(number = 0, NULL, 'abc'), 'a', 0)
            from numbers("number" = "2")
        """
        exception "Invalid regexp_position position: expected at least 1, got 0"
    }

    // S11: constant flags and pattern are validated before numeric values in a block.
    test {
        sql """
            select regexp_position(if(number = 0, 'abc', 'def'), '[',
                cast(number as bigint) - 1, 0, 2, 'z', -1)
            from numbers("number" = "2")
        """
        exception "Invalid regexp_position flag (byte 122)"
    }
    test {
        sql """
            select regexp_position(if(number = 0, 'abc', 'def'), '[',
                cast(number as bigint) - 1, 0, 2, '', -1)
            from numbers("number" = "2")
        """
        exception "Invalid regexp_position pattern:"
    }

    // S12: legal BIGINT limits finish without overflow, and the last capture is selectable.
    order_qt_numeric_boundaries """
        select regexp_position('abc', 'a', cast('9223372036854775807' as bigint)),
               regexp_position('abc', 'a', 1, cast('9223372036854775807' as bigint)),
               regexp_position('abc', 'b', 1, 1, 0),
               regexp_position('abc', 'b', 1, 1, 1),
               regexp_position('abc', '(a)(bc)', 1, 1, 0, '', 2),
               regexp_position('abc', '(a)(bc)', 1, 1, 1, '', 2)
    """

    // Every invalid number must fail even when the pattern cannot match or the start is past EOF.
    ["'z'", "'(z)'"].each { pattern ->
        ["0", "-1", "cast('-9223372036854775808' as bigint)"].each { value ->
            test {
                sql "select regexp_position('abc', ${pattern}, ${value})"
                exception "Invalid regexp_position position: expected at least 1, got "
            }
        }
    }
    [1, 5].each { position ->
        ["0", "-1", "cast('-9223372036854775808' as bigint)"].each { value ->
            test {
                sql "select regexp_position('abc', '(z)', ${position}, ${value})"
                exception "Invalid regexp_position occurrence: expected at least 1, got "
            }
        }
        ["-1", "2", "cast('-9223372036854775808' as bigint)",
                "cast('9223372036854775807' as bigint)"].each { value ->
            test {
                sql "select regexp_position('abc', '(z)', ${position}, 1, ${value})"
                exception "Invalid regexp_position return_option: expected 0 or 1, got "
            }
        }
        ["-1", "2", "cast('-9223372036854775808' as bigint)",
                "cast('9223372036854775807' as bigint)"].each { value ->
            test {
                sql "select regexp_position('abc', '(z)', ${position}, 1, 0, '', ${value})"
                exception "Invalid regexp_position subexpression: expected a value from 0 to 1, got "
            }
        }
    }
    // S13: the empty literal pattern advances by byte through the final boundary.
    order_qt_zero_width_empty_literal """
        select regexp_position('abc', '', 1, 1),
               regexp_position('abc', '', 1, 4),
               regexp_position('abc', '', 1, 5)
    """
    // S14: general RE2 zero-width matches return zero.
    order_qt_zero_width_anchors_and_repetition '''
        select regexp_position('abc', '^'),
               regexp_position('abc', '$', 2),
               regexp_position('ba', 'a*', 1, 1),
               regexp_position('ba', 'a*', 1, 2),
               regexp_position('ba', 'a*', 1, 3)
    '''
    // S15: empty and optional captures return zero while a nonempty whole match remains visible.
    order_qt_zero_width_capture_groups '''
        select regexp_position('', '()', 1, 1, 0, '', 1),
               regexp_position('', '(a)?', 1, 1, 0, '', 1),
               regexp_position('a', 'a()', 1, 1, 0, '', 0),
               regexp_position('a', 'a()', 1, 1, 0, '', 1),
               regexp_position('a', 'a()', 1, 1, 1, '', 1)
    '''
    // S16: flags disable the empty-literal shortcut; the final byte boundary is searchable.
    order_qt_zero_width_empty_flags_and_end """
        select regexp_position('abc', '', 1, 2, 0, 'i'),
               regexp_position('abc', '', 1, 4, 1),
               regexp_position('', '', 1, 1),
               regexp_position('', '', 1, 2),
               regexp_position('abc', '', 4, 1),
               regexp_position('abc', '', 5, 1)
    """
    // S17: repeated searches retain the original anchors and word-boundary context.
    order_qt_original_context_anchors_and_word_boundaries """
        select regexp_position('abc', '^.', 1, 2),
               regexp_position('abc', '^b', 2),
               regexp_position('new york is the greatest', '\\\\b\\\\w', 1, 2),
               regexp_position('new york is the greatest', '\\\\b\\\\w', 1, 3)
    """
    // S18: NUL and invalid UTF-8 preserve the byte positions from the ClickHouse golden.
    order_qt_golden_nul_and_invalid_utf8 """
        select regexp_position(unhex('610062'), 'b'),
               regexp_position(unhex('610062'), unhex('00')),
               regexp_position(unhex('61ff62'), 'b'),
               regexp_position(unhex('61ff62'), unhex('ff'))
    """
    // S19: starts inside multibyte characters advance to the next matching byte boundary.
    order_qt_golden_multibyte_internal_start """
        select regexp_position('äbc', '.', 2),
               regexp_position('😀a', '.', 3)
    """
}
