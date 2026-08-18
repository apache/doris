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

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>

#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/define_primitive_type.h"
#include "core/value/variant/variant_value.h"

namespace doris {

class VariantBatchBuilder;

// Collects the value one requested path selected on each row and produces a
// Nullable(ColumnVariantV2) with exactly one output row per append.
//
// A path projection normally selects the same scalar kind on every row, so the common result is a
// typed ColumnVariantV2. A CAST or predicate then reads a plain Doris column instead of decoding a
// Variant header per row, and the selected leaf is never re-encoded into owning Variant bytes.
// Whatever a typed identity cannot reproduce exactly - containers, mixed scalar kinds, kinds with
// no exact Doris type, or an integer written wider than its value needs - degrades to canonical
// encoded rows. Degrading replays the values collected so far instead of revisiting the source.
class VariantSelectedValueBuilder {
public:
    explicit VariantSelectedValueBuilder(size_t reserve_rows);
    ~VariantSelectedValueBuilder();

    VariantSelectedValueBuilder(const VariantSelectedValueBuilder&) = delete;
    VariantSelectedValueBuilder& operator=(const VariantSelectedValueBuilder&) = delete;

    // The requested path selects nothing on this row, so the result row is SQL NULL.
    void append_missing();
    // The requested path selected `value`. A Variant null stays SQL non-NULL, exactly as a
    // canonical encoded result would represent it.
    void append_selected(VariantRef value);

    // Produces Nullable(ColumnVariantV2). Call once; the builder is spent afterwards.
    ColumnPtr finish();

    size_t rows() const noexcept { return _rows; }
    // True once a value forced canonical encoded rows.
    bool degraded() const noexcept { return _mode == Mode::ENCODED; }
    // INVALID_TYPE until a scalar decides the typed identity, and after degrading.
    PrimitiveType typed_identity() const noexcept;

private:
    // UNSUPPORTED marks a value that has no exact typed identity, so it always degrades.
    enum class Kind : uint8_t { UNDECIDED, UNSUPPORTED, STRING, BOOLEAN, INTEGER, FLOAT, DOUBLE };
    enum class Mode : uint8_t { TYPED, ENCODED };

    static Kind kind_of(VariantRef value);
    static DataTypePtr identity_type(Kind kind);

    void _append_typed_null();
    // Returns false when the typed column could not reproduce `value` byte for byte.
    bool _try_append_typed(VariantRef value);
    void _append_encoded(VariantRef value);
    void _replay_typed_scalar(size_t row, VariantBatchBuilder* encoded) const;
    void _degrade();
    void _start_encoded();

    size_t _reserve_rows = 0;
    size_t _rows = 0;
    Mode _mode = Mode::TYPED;
    Kind _kind = Kind::UNDECIDED;
    DataTypePtr _type;
    // Rows the requested path did not select. This is the SQL null map of the result column.
    ColumnUInt8::MutablePtr _missing;
    // Rows whose selected value is a Variant null. This is the typed column's own null map, so a
    // Variant null keeps casting to 'null' instead of collapsing into the SQL null map.
    ColumnUInt8::MutablePtr _variant_nulls;
    MutableColumnPtr _values;
    std::unique_ptr<VariantBatchBuilder> _encoded;
};

} // namespace doris
