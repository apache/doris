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

suite("test_conv_negative_base_range") {
    order_qt_negative_literals """
        SELECT CONV('-10', 10, 10), CONV(-10, 10, 10),
               CONV('-10', 10, -10), CONV(-10, 10, -10),
               CONV('-10', 2, -10), CONV(-10, 2, -10)
    """
    order_qt_negative_source_prefix_literals """
        SELECT CONV('-121', -2, -10), CONV(-121, -2, -10),
               CONV('-1a1', -10, -10), CONV('-1A1', -10, 10),
               CONV('-1012', -2, -10), CONV(-1012, -2, -10),
               CONV('-123a9', -10, -10)
    """

    sql "DROP TABLE IF EXISTS test_conv_negative_base_range"
    sql """
        CREATE TABLE test_conv_negative_base_range (
            id INT,
            input_str VARCHAR(64),
            input_num BIGINT,
            from_base TINYINT,
            to_base TINYINT
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO test_conv_negative_base_range VALUES
            (1, '-10', -10, 10, 10),
            (2, '-10', -10, 2, 10),
            (3, '-100', -100, 10, 16),
            (4, '-8000000000000000', -8000000000000000, 16, 10),
            (5, '-10', -10, 10, -10),
            (6, '-10', -10, -10, 10),
            (7, '10', 10, 1, 10),
            (8, '10', 10, 37, 10),
            (9, '10', 10, 10, 1),
            (10, '10', 10, 10, 37),
            (11, 'z', NULL, 36, 10),
            (12, '10', 10, 2, 36),
            (13, NULL, NULL, 10, 10),
            (14, '18446744073709551615', NULL, 10, 16),
            (15, '-18446744073709551615', NULL, 10, 10),
            (16, '-18446744073709551616', NULL, 10, 10),
            (17, '-9223372036854775808', -9223372036854775808, 10, 10),
            (18, '-0', 0, 10, 10),
            (19, '  -10  ', -10, 10, 10),
            (20, '10', 10, NULL, 10),
            (21, '10', 10, 10, NULL),
            (22, '-10', -10, -128, 10),
            (23, '-10', -10, 10, -128),
            (24, '10', 10, 0, 10),
            (25, '10', 10, 10, 0),
            (26, '10', 10, 127, 10),
            (27, '10', 10, 10, 127),
            (28, '35', 35, 10, 36),
            (29, '-10', -10, -2, 10),
            (30, '-10', -10, -36, 10),
            (31, '-10', -10, 10, -2),
            (32, '35', 35, 10, -36),
            (33, '10', 10, -1, 10),
            (34, '10', 10, -37, 10),
            (35, '10', 10, 10, -1),
            (36, '10', 10, 10, -37),
            (37, '-18446744073709551616', NULL, 10, -10),
            (38, '-9223372036854775808', -9223372036854775808, 10, -10),
            (39, '-121', -121, -2, -10),
            (40, '-1a1', NULL, -10, -10),
            (41, '  -1A1  ', NULL, -10, 10),
            (42, '-1012', -1012, -2, -10),
            (43, '-123a9', NULL, -10, -10),
            (44, '-9223372036854775808', -9223372036854775808, -10, -10),
            (45, '10', 10, -10, -10),
            (46, '-0', 0, -2, -10)
    """

    order_qt_base_columns """
        SELECT id, CONV(input_str, from_base, to_base), CONV(input_num, from_base, to_base)
        FROM test_conv_negative_base_range ORDER BY id
    """
    order_qt_constant_bases """
        SELECT id, CONV(input_str, 10, 10), CONV(input_num, 10, 10)
        FROM test_conv_negative_base_range ORDER BY id
    """
    order_qt_constant_invalid_bases """
        SELECT id, CONV(input_str, -1, 10), CONV(input_num, 10, -37)
        FROM test_conv_negative_base_range ORDER BY id
    """
    order_qt_constant_signed_bases """
        SELECT id, CONV(input_str, 10, -10), CONV(input_num, 10, -10)
        FROM test_conv_negative_base_range ORDER BY id
    """
    order_qt_constant_negative_source_bases """
        SELECT id, CONV(input_str, -2, -10), CONV(input_num, -2, -10),
               CONV(input_str, -10, -10), CONV(input_num, -10, -10)
        FROM test_conv_negative_base_range ORDER BY id
    """
    order_qt_mixed_bases """
        SELECT id, CONV(input_str, from_base, 10), CONV(input_num, 10, to_base)
        FROM test_conv_negative_base_range ORDER BY id
    """

    check_fold_consistency "conv('-10', 10, 10)"
    check_fold_consistency "conv('-10', 2, 10)"
    check_fold_consistency "conv('-10', 10, -10)"
    check_fold_consistency "conv('-10', 2, -10)"
    check_fold_consistency "conv('-10', 10, -2)"
    check_fold_consistency "conv('35', 10, -36)"
    check_fold_consistency "conv(-10, -10, 10)"
    check_fold_consistency "conv('-121', -2, -10)"
    check_fold_consistency "conv(-121, -2, -10)"
    check_fold_consistency "conv('-1a1', -10, -10)"
    check_fold_consistency "conv('-1A1', -10, 10)"
    check_fold_consistency "conv('-1012', -2, -10)"
    check_fold_consistency "conv('10', 1, 10)"
    check_fold_consistency "conv(10, 10, 37)"
    check_fold_consistency "conv('10', -37, 10)"
    check_fold_consistency "conv(10, 10, -1)"
    check_fold_consistency "conv('-18446744073709551615', 10, 10)"
}
