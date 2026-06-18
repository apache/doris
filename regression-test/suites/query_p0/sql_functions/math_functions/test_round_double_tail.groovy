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

suite("test_round_double_tail") {
    sql "set round_double_returns_decimal_for_const_scale = true"

    qt_round_const           """ select round(23900/293, 2); """
    qt_round_bankers_const   """ select round_bankers(23900/293, 2); """
    qt_ceil_const            """ select ceil(23900/293, 2); """
    qt_floor_const           """ select floor(23900/293, 2); """
    qt_truncate_const        """ select truncate(23900/293, 2); """

    sql """ DROP TABLE IF EXISTS test_round_double_tail_t; """
    sql """
        CREATE TABLE test_round_double_tail_t (
            id INT,
            num BIGINT,
            den BIGINT,
            d DOUBLE
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
    """
    sql """
        INSERT INTO test_round_double_tail_t VALUES
            (1, 239,  293,  cast(239  as double) / cast(293  as double) * 100),
            (2, 458,  486,  cast(458  as double) / cast(486  as double) * 100),
            (3, 1033, 1101, cast(1033 as double) / cast(1101 as double) * 100),
            (4, 1040, 1101, cast(1040 as double) / cast(1101 as double) * 100),
            (5, 140,  172,  cast(140  as double) / cast(172  as double) * 100);
    """

    qt_round_col          """ SELECT id, round(d, 2)         FROM test_round_double_tail_t ORDER BY id; """
    qt_round_bankers_col  """ SELECT id, round_bankers(d, 2) FROM test_round_double_tail_t ORDER BY id; """
    qt_ceil_col           """ SELECT id, ceil(d, 2)          FROM test_round_double_tail_t ORDER BY id; """
    qt_floor_col          """ SELECT id, floor(d, 2)         FROM test_round_double_tail_t ORDER BY id; """
    qt_truncate_col       """ SELECT id, truncate(d, 2)      FROM test_round_double_tail_t ORDER BY id; """

    qt_round_concat_pct """
        SELECT id, concat(round(cast(num as double) * 100 / cast(den as double), 2), '%')
        FROM test_round_double_tail_t
        ORDER BY id;
    """

    qt_round_single_arg      """ SELECT id, round(d)     FROM test_round_double_tail_t ORDER BY id; """
    qt_round_negative_scale  """ SELECT id, round(d, -1) FROM test_round_double_tail_t ORDER BY id; """

    // Scale from a column also stays double.
    sql """ DROP TABLE IF EXISTS test_round_double_tail_scale_col; """
    sql """
        CREATE TABLE test_round_double_tail_scale_col (
            id INT,
            d  DOUBLE,
            n  INT
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
    """
    sql """
        INSERT INTO test_round_double_tail_scale_col VALUES
            (1, cast(239 as double) / cast(293 as double) * 100, 2);
    """
    qt_round_scale_col """
        SELECT id, round(d, n) FROM test_round_double_tail_scale_col ORDER BY id;
    """
    sql "set round_double_returns_decimal_for_const_scale = false"
}
