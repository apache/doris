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

// Regression for the unaligned __int128 dereference fix. Exercises the
// real-runtime paths that previously dereferenced a __int128* through a
// non 16-byte aligned StringRef::data / Slice::data pointer:
//
//   * cast LARGEINT column -> JSON: DataTypeNumberSerDe<TYPE_LARGEINT>::
//     write_one_cell_to_jsonb
//   * LARGEINT / DECIMALV2 / DECIMAL128 round-trip through a table and
//     group-by: column-data-to-string paths used by vexpr literal
//     construction at runtime (e.g. runtime filter min/max push), and
//     by meta_tool.
//
// Under UBSan -fsanitize=alignment or on strict-alignment platforms
// (e.g. aarch64) the previous code could SIGBUS / abort.
//
// Note: FoldConstantExecutor::_get_result is not exercised here because
// it is unreachable under the current Nereids planner with default
// be_exec_version (>= 4); the new BE-side fold path serializes the
// result via DataTypeSerDe::write_column_to_pb which does not contain an
// unaligned __int128 dereference. The fix to _get_result is retained as
// defensive code; the BE unit test (vexpr_unaligned_int128_test.cpp)
// covers the create_texpr_literal_node LARGEINT / DECIMAL128I /
// DECIMALV2 branches directly from a deliberately misaligned buffer.
suite("test_int128_unaligned_access") {
    sql "set enable_sql_cache=false;"

    // Storage path: round-trip largeint / decimal128 / decimalv2 through
    // a table to exercise the column-data-to-string code paths used by
    // vexpr literal construction and meta_tool at runtime.
    sql "drop table if exists test_int128_unaligned"
    sql """
        CREATE TABLE test_int128_unaligned (
            id INT NOT NULL,
            v_largeint LARGEINT NULL,
            v_decimal128 DECIMALV3(38, 10) NULL,
            v_decimalv2 DECIMALV2(27, 9) NULL
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """
        INSERT INTO test_int128_unaligned VALUES
            (1, 170141183460469231731687303715884105727,
                12345678901234567890.1234567890,
                1234567890.123456789),
            (2, -170141183460469231731687303715884105728,
                -12345678901234567890.1234567890,
                -1234567890.123456789),
            (3, 0, 0, 0),
            (4, 1, 0.0000000001, 0.000000001),
            (5, NULL, NULL, NULL)
    """

    order_qt_largeint_select "select id, v_largeint from test_int128_unaligned"
    order_qt_decimal128_select "select id, v_decimal128 from test_int128_unaligned"
    order_qt_decimalv2_select "select id, v_decimalv2 from test_int128_unaligned"

    // Aggregation + group by exercises hash-table key serialization where
    // largeint values are packed into non-aligned byte buffers.
    order_qt_largeint_groupby """
        select v_largeint, count(*) from test_int128_unaligned group by v_largeint
    """
    order_qt_decimal128_groupby """
        select v_decimal128, count(*) from test_int128_unaligned group by v_decimal128
    """

    // JSON serialization path: DataTypeNumberSerDe<TYPE_LARGEINT>::
    // write_one_cell_to_jsonb reads __int128 from StringRef::data.
    order_qt_largeint_to_json """
        select id, cast(v_largeint as JSON) from test_int128_unaligned where v_largeint is not null
    """
}
