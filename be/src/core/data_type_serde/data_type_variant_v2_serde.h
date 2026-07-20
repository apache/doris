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

#include <span>

#include "core/data_type_serde/data_type_serde.h"

namespace doris {

// Direct SerDe used when the logical Variant type carries a ColumnVariantV2 physical column.
class DataTypeVariantV2SerDe final : public DataTypeSerDe {
public:
    explicit DataTypeVariantV2SerDe(int nesting_level = 1);

    // Binary block serialization for ColumnVariantV2. The V2 frame is intentionally selected by
    // the BE config rather than an FE/BE compatibility version.
    static Result<size_t> serialized_size(const IColumn& source);
    static Status serialize_binary(const IColumn& source, std::span<uint8_t> exact_frame);
    static Status deserialize_binary(std::span<const uint8_t> exact_frame,
                                     MutableColumnPtr* destination);

    std::string get_name() const override;

    Status serialize_one_cell_to_json(const IColumn& column, int64_t row_num, BufferWritable& bw,
                                      FormatOptions& options) const override;
    Status serialize_column_to_json(const IColumn& column, int64_t start_idx, int64_t end_idx,
                                    BufferWritable& bw, FormatOptions& options) const override;
    Status deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                          const FormatOptions& options) const override;
    Status deserialize_one_cell_from_csv(IColumn& column, Slice& slice,
                                         const FormatOptions& options) const override;
    Status deserialize_column_from_json_vector(IColumn& column, std::vector<Slice>& slices,
                                               uint64_t* num_deserialized,
                                               const FormatOptions& options) const override;

    Status write_column_to_pb(const IColumn& column, PValues& result, int64_t start,
                              int64_t end) const override;
    Status read_column_from_pb(IColumn& column, const PValues& arg) const override;

    void write_one_cell_to_jsonb(const IColumn& column, JsonbWriter& result, Arena& mem_pool,
                                 int32_t col_id, int64_t row_num,
                                 const FormatOptions& options) const override;
    void read_one_cell_from_jsonb(IColumn& column, const JsonbValue* arg) const override;

    Status write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                                 arrow::ArrayBuilder* array_builder, int64_t start, int64_t end,
                                 const cctz::time_zone& ctz) const override;
    Status read_column_from_arrow(IColumn& column, const arrow::Array* arrow_array, int64_t start,
                                  int64_t end, const cctz::time_zone& ctz) const override;
    Status write_column_to_mysql_binary(const IColumn& column, MysqlRowBinaryBuffer& row_buffer,
                                        int64_t row_idx, bool col_const,
                                        const FormatOptions& options) const override;
    Status write_column_to_orc(const std::string& timezone, const IColumn& column,
                               const NullMap* null_map, orc::ColumnVectorBatch* orc_col_batch,
                               int64_t start, int64_t end, Arena& arena,
                               const FormatOptions& options) const override;

    void to_string(const IColumn& column, size_t row_num, BufferWritable& bw,
                   const FormatOptions& options) const override;
};

} // namespace doris
#include <array>
#include <cstring>
#include <limits>

#include "common/exception.h"
#include "core/assert_cast.h"
#include "core/column/column_const.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/column/variant_v2/column_variant_v2_typed_column.h"
#include "core/custom_allocator.h"
#include "core/value/variant/variant_encoding.h"
#include "exprs/function/parse/variant_json.h"

namespace doris::data_type_variant_v2_serde_internal {

class ReadInput {
public:
    explicit ReadInput(const IColumn& source) : ReadInput(resolve(source)) {}

    size_t physical_row(size_t logical_row) const noexcept { return _constant ? 0 : logical_row; }
    const ColumnVariantV2::ReadView& view() const noexcept { return _view; }

    void validate_range(size_t start, size_t end) const {
        if (start > end || end > _logical_size) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Variant SerDe row range [{}, {}) exceeds column size {}", start, end,
                            _logical_size);
        }
    }

private:
    struct Resolved {
        const ColumnVariantV2* column;
        size_t logical_size;
        bool constant;
    };

    static Resolved resolve(const IColumn& source) {
        const IColumn* physical = &source;
        bool constant = false;
        if (const auto* const_column = check_and_get_column<ColumnConst>(source)) {
            physical = &const_column->get_data_column();
            constant = true;
        }
        const auto* variant = check_and_get_column<ColumnVariantV2>(*physical);
        if (variant == nullptr) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Variant V2 SerDe requires ColumnVariantV2, got {}", source.get_name());
        }
        return {.column = variant, .logical_size = source.size(), .constant = constant};
    }

    explicit ReadInput(Resolved resolved)
            : _logical_size(resolved.logical_size),
              _constant(resolved.constant),
              _view(resolved.column->read_view()) {}

    size_t _logical_size;
    bool _constant;
    ColumnVariantV2::ReadView _view;
};

inline size_t checked_row(int64_t row) {
    if (row < 0) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant SerDe row {} is negative", row);
    }
    return static_cast<size_t>(row);
}

class ScalarScratch {
public:
    VariantRef value(VariantScalarEncodingPlan plan) {
        _bytes.resize(plan.size());
        plan.write(_bytes.data(), _bytes.size());
        return {.metadata = {.data = EMPTY_METADATA.data(), .size = EMPTY_METADATA.size()},
                .value = {_bytes.data(), _bytes.size()}};
    }

private:
    static constexpr std::array<char, 3> EMPTY_METADATA {
            static_cast<char>(VARIANT_ENCODING_VERSION | VARIANT_METADATA_SORTED_STRINGS_MASK), 0,
            0};
    DorisVector<char> _bytes;
};

struct CountingWriter {
    void write(const char*, size_t size) {
        if (size > std::numeric_limits<size_t>::max() - count) {
            throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant JSON output is too large");
        }
        count += size;
    }
    size_t count = 0;
};

struct FixedWriter {
    void write(const char* data, size_t size) {
        DCHECK_LE(written, capacity);
        DCHECK_LE(size, capacity - written);
        memcpy(destination + written, data, size);
        written += size;
    }
    char* destination;
    size_t capacity;
    size_t written = 0;
};

template <typename OuterNullCallback, typename ValueCallback>
// Any VariantRef passed to on_value borrows either the source column or the reusable scalar
// scratch and is valid only until that callback returns. Callbacks must not retain it.
void for_each_value(const ReadInput& input, size_t start, size_t end, const NullMap* outer_nulls,
                    OuterNullCallback&& on_outer_null, ValueCallback&& on_value) {
    input.validate_range(start, end);
    if (outer_nulls != nullptr && outer_nulls->size() < end) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant SerDe null map size {} is smaller than row end {}",
                        outer_nulls->size(), end);
    }

    const auto& view = input.view();
    if (!view.is_typed()) {
        for (size_t row = start; row < end; ++row) {
            if (outer_nulls != nullptr && (*outer_nulls)[row] != 0) {
                on_outer_null(row);
                continue;
            }
            on_value(row, view.value_at(input.physical_row(row)));
        }
        return;
    }

    const auto& nullable = assert_cast<const ColumnNullable&>(view.typed_column());
    const PrimitiveType type = view.typed_type()->get_primitive_type();
    const uint32_t scale = view.typed_type()->get_scale();
    DORIS_CHECK_LE(scale, static_cast<uint32_t>(std::numeric_limits<uint8_t>::max()));
    const auto& inner_nulls = nullable.get_null_map_data();
    ScalarScratch scratch;
    column_variant_v2_internal::dispatch_typed_column(
            nullable, type, [&]<PrimitiveType Type>(const auto& nested) {
                for (size_t row = start; row < end; ++row) {
                    if (outer_nulls != nullptr && (*outer_nulls)[row] != 0) {
                        on_outer_null(row);
                        continue;
                    }
                    const size_t physical_row = input.physical_row(row);
                    if (inner_nulls[physical_row] != 0) {
                        on_value(row, scratch.value(VariantScalarEncodingPlan::null_value()));
                        continue;
                    }
                    column_variant_v2_internal::with_typed_scalar<Type>(
                            nested, physical_row, static_cast<uint8_t>(scale),
                            [&](auto&& physical_factory, auto&&, auto) {
                                on_value(row, scratch.value(physical_factory()));
                            });
                }
            });
}

template <typename Writer>
void write_json_value(VariantRef value, Writer& writer,
                      const DataTypeSerDe::FormatOptions& options) {
    VariantJsonFormatOptions json_options {.timezone = options.timezone};
    to_json(value, writer, json_options);
}

} // namespace doris::data_type_variant_v2_serde_internal
