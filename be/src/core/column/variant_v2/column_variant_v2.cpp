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

#include "core/column/variant_v2/column_variant_v2.h"

#include <algorithm>
#include <array>
#include <bit>
#include <cstdint>
#include <limits>
#include <mutex>
#include <string_view>
#include <typeinfo>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/check.h"
#include "common/exception.h"
#include "core/assert_cast.h"
#include "core/column/column_const.h"
#include "core/column/column_decimal.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/column/columns_common.h"
#include "core/column/variant_v2/column_variant_v2_typed_column.h"
#include "core/custom_allocator.h"
#include "core/data_type/data_type_string.h"
#include "core/value/variant/variant_batch_builder.h"
#include "core/value/variant/variant_canonical.h"
#include "core/value/variant/variant_field.h"
#include "core/value/variant/variant_parquet_encoding.h"

namespace doris {
namespace {

using MetaIdsColumn = ColumnVector<TYPE_UINT32>;
constexpr uint32_t UNMAPPED_METADATA_ID = std::numeric_limits<uint32_t>::max();
constexpr std::array<char, 3> EMPTY_OBJECT_VALUE {static_cast<char>(0x02), 0, 0};

uint32_t read_canonical_payload_size(const char* pos) {
    DCHECK(pos != nullptr);
    uint32_t payload_size = 0;
    for (uint8_t byte = 0; byte < VARIANT_CANONICAL_SIZE_PREFIX; ++byte) {
        payload_size |= static_cast<uint32_t>(static_cast<uint8_t>(pos[byte])) << (byte * 8);
    }
    return payload_size;
}

size_t trusted_canonical_cell_size(const char* pos) {
    return VARIANT_CANONICAL_SIZE_PREFIX + read_canonical_payload_size(pos);
}

void check_hash_range(const ColumnVariantV2& column, size_t start, size_t end) {
    DCHECK_LE(start, end);
    DCHECK_LE(end, column.size());
}

template <typename Sink, typename Hash>
void update_canonical_hash_range(const ColumnVariantV2& column, size_t start, size_t end,
                                 Hash& hash, const uint8_t* __restrict null_data) {
    check_hash_range(column, start, end);
    Sink sink(hash);
    for (size_t row = start; row < end; ++row) {
        if (null_data == nullptr || null_data[row] == 0) {
            canonical_hash(column.get_value_ref(row), sink);
        }
    }
    hash = sink.digest();
}

template <typename Sink, typename Hash>
void update_canonical_hashes(const ColumnVariantV2& column, Hash* __restrict hashes,
                             const uint8_t* __restrict null_data) {
    for (size_t row = 0; row < column.size(); ++row) {
        if (null_data == nullptr || null_data[row] == 0) {
            Sink sink(hashes[row]);
            canonical_hash(column.get_value_ref(row), sink);
            hashes[row] = sink.digest();
        }
    }
}

void validate_offsets(StringRef bytes, std::span<const uint32_t> offsets,
                      std::string_view description) {
    DORIS_CHECK(bytes.data != nullptr || bytes.size == 0)
            << description << " bytes have a null pointer";
    DORIS_CHECK(!offsets.empty()) << description << " offsets cannot be empty";
    DORIS_CHECK_EQ(offsets.front(), 0) << description << " offsets must start at zero";
    DORIS_CHECK_EQ(static_cast<size_t>(offsets.back()), bytes.size)
            << description << " offsets must end at the byte size";
    for (size_t index = 1; index < offsets.size(); ++index) {
        DORIS_CHECK_LT(offsets[index - 1], offsets[index])
                << description << " offsets must be strictly increasing";
    }
}

void require_exclusive(const IColumn::WrappedPtr& column, std::string_view description) {
    const auto& immutable = static_cast<const IColumn::Ptr&>(column);
    DORIS_CHECK(immutable->is_exclusive())
            << "ColumnVariantV2 " << description << " must be COW-detached before mutation";
}

void reserve_rows(ColumnString& values, MetaIdsColumn& metadata_ids, size_t value_bytes,
                  size_t rows) {
    const size_t final_value_bytes = values.get_chars().size() + value_bytes;
    ColumnString::check_chars_length(final_value_bytes, values.size() + rows, values.size());
    values.get_chars().reserve(final_value_bytes);
    values.get_offsets().reserve(values.size() + rows);
    metadata_ids.get_data().reserve(metadata_ids.size() + rows);
}

bool byte_ranges_overlap(StringRef left, StringRef right) {
    if (left.size == 0 || right.size == 0) {
        return false;
    }
    DCHECK(left.data != nullptr);
    DCHECK(right.data != nullptr);
    const auto left_begin = reinterpret_cast<uintptr_t>(left.data);
    const auto right_begin = reinterpret_cast<uintptr_t>(right.data);
    // Compare start-address distances instead of forming end pointers, which also avoids integer
    // overflow for a malformed borrowed size.
    return left_begin <= right_begin ? right_begin - left_begin < left.size
                                     : left_begin - right_begin < right.size;
}

size_t validate_selected_indices(const uint32_t* indices_begin, const uint32_t* indices_end,
                                 size_t source_rows) {
    if (indices_begin == indices_end) {
        return 0;
    }
    DORIS_CHECK(indices_begin != nullptr && indices_end != nullptr)
            << "non-empty source indices cannot be null";
    DORIS_CHECK_LT(indices_begin, indices_end) << "source index range is reversed";
    const size_t rows = indices_end - indices_begin;
    DORIS_CHECK_LT(*std::max_element(indices_begin, indices_end), source_rows)
            << "source index is out of range";
    return rows;
}

bool exact_typed_identity(const DataTypePtr& left, const DataTypePtr& right) {
    const PrimitiveType primitive = left->get_primitive_type();
    if (primitive != right->get_primitive_type()) {
        return false;
    }
    if (is_string_type(primitive)) {
        return assert_cast<const DataTypeString&>(*left).len() ==
               assert_cast<const DataTypeString&>(*right).len();
    }
    return left->equals(*right);
}

void validate_typed_decimal_scale(const IColumn& nested, PrimitiveType type, uint32_t scale) {
    uint32_t column_scale = scale;
    switch (type) {
    case TYPE_DECIMALV2:
        column_scale = assert_cast<const ColumnDecimal128V2&>(nested).get_scale();
        break;
    case TYPE_DECIMAL32:
        column_scale = assert_cast<const ColumnDecimal32&>(nested).get_scale();
        break;
    case TYPE_DECIMAL64:
        column_scale = assert_cast<const ColumnDecimal64&>(nested).get_scale();
        break;
    case TYPE_DECIMAL128I:
        column_scale = assert_cast<const ColumnDecimal128V3&>(nested).get_scale();
        break;
    default:
        return;
    }
    DORIS_CHECK_EQ(column_scale, scale) << "typed decimal scale does not match data type scale";
}

template <PrimitiveType Type, typename Column, typename Callback>
void visit_typed_rows(const ColumnNullable& nullable, const Column& column, uint32_t scale,
                      size_t start, size_t end, Callback&& callback) {
    DCHECK_LE(start, end);
    DCHECK_LE(end, nullable.size());
    uint8_t variant_scale = 0;
    if constexpr (Type == TYPE_DECIMALV2 || Type == TYPE_DECIMAL32 || Type == TYPE_DECIMAL64 ||
                  Type == TYPE_DECIMAL128I) {
        DORIS_CHECK_LE(scale, static_cast<uint32_t>(std::numeric_limits<uint8_t>::max()))
                << "typed decimal scale exceeds the Variant scale domain";
        variant_scale = static_cast<uint8_t>(scale);
    }
    const auto& null_map = nullable.get_null_map_data();
    for (size_t row = start; row < end; ++row) {
        if (null_map[row] != 0) {
            callback(row, VariantScalarRef::null_value());
            continue;
        }
        with_variant_typed_scalar<Type>(
                column, row, variant_scale,
                [&](const VariantScalarRef& scalar) { callback(row, scalar); });
    }
}

template <typename Callback>
void visit_typed_scalar_column(const ColumnNullable& nullable, PrimitiveType type, uint32_t scale,
                               size_t start, size_t end, Callback&& callback) {
    dispatch_variant_typed_column(nullable.get_nested_column(), type,
                                  [&]<PrimitiveType Type>(const auto& column) {
                                      visit_typed_rows<Type>(nullable, column, scale, start, end,
                                                             std::forward<Callback>(callback));
                                  });
}

template <typename Sink, typename Hash>
void update_typed_hashes(const ColumnNullable& nullable, PrimitiveType type, uint32_t scale,
                         Hash* __restrict hashes, const uint8_t* __restrict null_data) {
    visit_typed_scalar_column(nullable, type, scale, 0, nullable.size(),
                              [&](size_t row, const VariantScalarRef& scalar) {
                                  if (null_data == nullptr || null_data[row] == 0) {
                                      Sink sink(hashes[row]);
                                      canonical_hash(scalar, sink);
                                      hashes[row] = sink.digest();
                                  }
                              });
}

template <typename Sink, typename Hash>
void update_typed_hash_range(const ColumnNullable& nullable, PrimitiveType type, uint32_t scale,
                             size_t start, size_t end, Hash& hash,
                             const uint8_t* __restrict null_data) {
    Sink sink(hash);
    visit_typed_scalar_column(nullable, type, scale, start, end,
                              [&](size_t row, const VariantScalarRef& scalar) {
                                  if (null_data == nullptr || null_data[row] == 0) {
                                      canonical_hash(scalar, sink);
                                  }
                              });
    hash = sink.digest();
}

struct TypedEncodingResult {
    ColumnString::MutablePtr metadatas;
    MetaIdsColumn::MutablePtr metadata_ids;
    ColumnString::MutablePtr values;
};

template <PrimitiveType Type, typename Column>
TypedEncodingResult encode_typed_column(const ColumnNullable& nullable, const Column& column,
                                        uint32_t scale) {
    TypedEncodingResult result;
    result.metadatas = ColumnString::create();
    result.metadata_ids = MetaIdsColumn::create();
    result.values = ColumnString::create();
    if (!nullable.empty()) {
        result.metadatas->insert_data(VARIANT_EMPTY_METADATA.data(), VARIANT_EMPTY_METADATA.size());
    }
    result.metadata_ids->get_data().resize(nullable.size());
    std::fill(result.metadata_ids->get_data().begin(), result.metadata_ids->get_data().end(), 0);
    auto& chars = result.values->get_chars();
    auto& offsets = result.values->get_offsets();
    offsets.resize(nullable.size());

    visit_typed_rows<Type>(
            nullable, column, scale, 0, nullable.size(),
            [&](size_t row, const VariantScalarRef& scalar) {
                if (scalar.encoded_size() > std::numeric_limits<size_t>::max() - chars.size()) {
                    throw Exception(ErrorCode::INVALID_ARGUMENT,
                                    "Typed Variant values exceed addressable size");
                }
                const size_t old_size = chars.size();
                const size_t new_size = old_size + scalar.encoded_size();
                ColumnString::check_chars_length(new_size, row + 1, row);
                chars.resize(new_size);
                scalar.write_physical(reinterpret_cast<char*>(chars.data()) + old_size,
                                      scalar.encoded_size());
                offsets[row] = static_cast<ColumnString::Offset>(new_size);
            });
    return result;
}

struct ValidatedTypedInput {
    ColumnPtr column;
    DataTypePtr type;
};

ValidatedTypedInput validate_typed_input(ColumnPtr column, DataTypePtr scalar_type) {
    DORIS_CHECK(static_cast<bool>(column)) << "typed ColumnVariantV2 column must not be null";
    DORIS_CHECK(scalar_type != nullptr) << "typed ColumnVariantV2 type must not be null";
    DORIS_CHECK(!scalar_type->is_nullable())
            << "typed ColumnVariantV2 requires a non-nullable scalar type";
    DORIS_CHECK(check_and_get_column<ColumnConst>(column.get()) == nullptr)
            << "typed ColumnVariantV2 input must not be ColumnConst";
    const IColumn* input_column = column.get();
    DORIS_CHECK(typeid(*input_column) == typeid(ColumnNullable))
            << "typed ColumnVariantV2 input must be an exact ColumnNullable";

    const auto& nullable = assert_cast<const ColumnNullable&>(*column);
    const IColumn& nested = nullable.get_nested_column();
    DORIS_CHECK_EQ(nested.size(), nullable.get_null_map_column().size())
            << "typed ColumnVariantV2 null map size does not match nested column size";
    const PrimitiveType type = scalar_type->get_primitive_type();
    DORIS_CHECK(is_supported_variant_typed_identity(type))
            << "unsupported typed identity " << scalar_type->get_name();
    MutableColumnPtr expected = scalar_type->create_column();
    const IColumn* expected_column = expected.get();
    DORIS_CHECK(typeid(nested) == typeid(*expected_column))
            << "typed nested column " << nested.get_name() << " does not match data type "
            << scalar_type->get_name();
    validate_typed_decimal_scale(nested, type, scalar_type->get_scale());

    return {.column = std::move(column), .type = std::move(scalar_type)};
}

[[noreturn]] void throw_unsupported(std::string_view method) {
    throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                    "ColumnVariantV2::{} is intentionally unsupported for Variant values", method);
}

class CompositeVariantShreddedState final : public VariantShreddedState {
public:
    explicit CompositeVariantShreddedState(
            std::vector<std::shared_ptr<VariantShreddedState>> segments)
            : _segments(std::move(segments)) {
        DORIS_CHECK(std::ranges::all_of(_segments, [](const auto& segment) {
            return segment != nullptr;
        })) << "composite Variant shredded segments must not be null";
        for (const auto& segment : _segments) {
            const size_t segment_rows = segment->size();
            DORIS_CHECK_LE(segment_rows, std::numeric_limits<size_t>::max() - _rows)
                    << "composite Variant shredded row count overflows size_t";
            _rows += segment_rows;
        }
    }

    size_t size() const override { return _rows; }

    size_t recompute_size() const {
        size_t rows = 0;
        for (const auto& segment : _segments) {
            const size_t segment_rows = segment->size();
            DORIS_CHECK_LE(segment_rows, std::numeric_limits<size_t>::max() - rows)
                    << "composite Variant shredded row count overflows size_t";
            rows += segment_rows;
        }
        return rows;
    }

    size_t byte_size() const override {
        size_t bytes = 0;
        for (const auto& segment : _segments) {
            bytes += segment->byte_size();
        }
        std::lock_guard lock(_materialization_lock);
        return bytes + (_materialized ? _materialized->byte_size() : 0) +
               (_serialized ? _serialized->byte_size() : 0);
    }

    size_t allocated_bytes() const override {
        size_t bytes = 0;
        for (const auto& segment : _segments) {
            bytes += segment->allocated_bytes();
        }
        std::lock_guard lock(_materialization_lock);
        return bytes + (_materialized ? _materialized->allocated_bytes() : 0) +
               (_serialized ? _serialized->allocated_bytes() : 0);
    }

    void sanity_check() const override {
        // Row count is read in per-row expression loops, so cache it and reserve the full segment
        // walk for invariant checks instead of making extraction quadratic in segment count.
        DORIS_CHECK_EQ(recompute_size(), _rows)
                << "cached composite Variant shredded row count is stale";
        for (const auto& segment : _segments) {
            segment->sanity_check();
        }
    }

    void for_each_subcolumn(const IColumn::ImutableColumnCallback& callback) const override {
        for (const auto& segment : _segments) {
            segment->for_each_subcolumn(callback);
        }
    }

    std::shared_ptr<VariantShreddedState> filter(const IColumn::Filter& filter,
                                                 ssize_t /*result_size_hint*/) const override {
        DORIS_CHECK_EQ(filter.size(), size())
                << "composite Variant shredded filter size does not match row count";
        std::vector<std::shared_ptr<VariantShreddedState>> selected;
        selected.reserve(_segments.size());
        size_t offset = 0;
        for (const auto& segment : _segments) {
            IColumn::Filter segment_filter;
            segment_filter.insert(filter.begin() + offset,
                                  filter.begin() + offset + segment->size());
            auto filtered = segment->filter(segment_filter, -1);
            if (filtered->size() != 0) {
                selected.push_back(std::move(filtered));
            }
            offset += segment->size();
        }
        return pack(std::move(selected));
    }

    std::shared_ptr<VariantShreddedState> select_range(size_t start, size_t length) const override {
        DORIS_CHECK_LE(start, size()) << "composite Variant range starts past source size";
        DORIS_CHECK_LE(length, size() - start) << "composite Variant range exceeds source size";
        std::vector<std::shared_ptr<VariantShreddedState>> selected;
        if (length == 0) {
            return pack(std::move(selected));
        }
        const size_t end = start + length;
        size_t offset = 0;
        for (const auto& segment : _segments) {
            const size_t segment_end = offset + segment->size();
            const size_t overlap_begin = std::max(start, offset);
            const size_t overlap_end = std::min(end, segment_end);
            if (overlap_begin < overlap_end) {
                selected.push_back(
                        segment->select_range(overlap_begin - offset, overlap_end - overlap_begin));
            }
            offset = segment_end;
            if (offset >= end) {
                break;
            }
        }
        return pack(std::move(selected));
    }

    std::shared_ptr<VariantShreddedState> select_indices(
            const uint32_t* indices_begin, const uint32_t* indices_end) const override {
        if (indices_begin == indices_end) {
            return pack({});
        }
        DORIS_CHECK(indices_begin != nullptr && indices_end != nullptr &&
                    indices_begin < indices_end)
                << "composite Variant indices are invalid";

        std::vector<size_t> segment_ends;
        segment_ends.reserve(_segments.size());
        size_t rows = 0;
        for (const auto& segment : _segments) {
            rows += segment->size();
            segment_ends.push_back(rows);
        }

        std::vector<std::shared_ptr<VariantShreddedState>> selected;
        const uint32_t* cursor = indices_begin;
        while (cursor != indices_end) {
            DORIS_CHECK_LT(*cursor, rows) << "composite Variant source index is out of range";
            const size_t segment_index =
                    std::upper_bound(segment_ends.begin(), segment_ends.end(), *cursor) -
                    segment_ends.begin();
            const size_t segment_begin = segment_index == 0 ? 0 : segment_ends[segment_index - 1];
            DorisVector<uint32_t> local_indices;
            while (cursor != indices_end && *cursor >= segment_begin &&
                   *cursor < segment_ends[segment_index]) {
                local_indices.push_back(static_cast<uint32_t>(*cursor - segment_begin));
                ++cursor;
            }
            selected.push_back(_segments[segment_index]->select_indices(
                    local_indices.data(), local_indices.data() + local_indices.size()));
        }
        return pack(std::move(selected));
    }

    bool can_materialize() const override {
        return std::ranges::all_of(_segments,
                                   [](const auto& segment) { return segment->can_materialize(); });
    }

    bool try_append(const VariantShreddedState& source) override {
        const size_t source_rows = source.size();
        DORIS_CHECK_LE(source_rows, std::numeric_limits<size_t>::max() - _rows)
                << "composite Variant shredded row count overflows size_t";
        if (const auto* composite = dynamic_cast<const CompositeVariantShreddedState*>(&source)) {
            for (const auto& segment : composite->_segments) {
                append(segment);
            }
        } else {
            append(source.select_range(0, source.size()));
        }
        std::lock_guard lock(_materialization_lock);
        _materialized.reset();
        _serialized.reset();
        _rows += source_rows;
        return true;
    }

    std::optional<VariantShreddedTypedValue> find_typed_value(
            std::span<const VariantShreddedPathSegment> path) const override {
        if (_segments.empty()) {
            return std::nullopt;
        }
        std::vector<std::optional<VariantShreddedTypedValue>> matches;
        matches.reserve(_segments.size());
        bool all_direct = true;
        for (const auto& segment : _segments) {
            auto match = segment->find_typed_value(path);
            if (!match.has_value()) {
                all_direct = false;
            }
            // Visit every segment even after one requests normalized fallback. Format states use
            // find_typed_value() as their validation boundary, so stopping early would let a later
            // segment publish an unvalidated normalized value and make errors depend on row order.
            matches.push_back(std::move(match));
        }

        const auto& first = matches.front();
        const bool homogeneous = all_direct && first->column && first->type &&
                                 std::ranges::all_of(matches, [&](const auto& match) {
                                     return match->column && match->type && !match->normalized &&
                                            exact_typed_identity(first->type, match->type);
                                 });
        if (homogeneous) {
            MutableColumnPtr combined = first->column->clone_empty();
            for (const auto& match : matches) {
                combined->insert_range_from(*match->column, 0, match->column->size());
            }
            return VariantShreddedTypedValue {
                    .column = std::move(combined), .type = first->type, .normalized = nullptr};
        }

        auto normalized = find_normalized_value(path);
        if (!normalized.has_value()) {
            return std::nullopt;
        }
        return VariantShreddedTypedValue {
                .column = nullptr, .type = nullptr, .normalized = std::move(*normalized)};
    }

    std::optional<ColumnPtr> find_normalized_value(
            std::span<const VariantShreddedPathSegment> path) const override {
        auto values = ColumnVariantV2::create();
        auto nulls = ColumnUInt8::create();
        nulls->reserve(size());
        for (const auto& segment : _segments) {
            auto normalized = segment->find_normalized_value(path);
            if (!normalized.has_value()) {
                return std::nullopt;
            }
            const auto& nullable = assert_cast<const ColumnNullable&>(**normalized);
            const auto& variants =
                    assert_cast<const ColumnVariantV2&>(nullable.get_nested_column());
            values->insert_range_from(variants, 0, variants.size());
            nulls->insert_range_from(nullable.get_null_map_column(), 0, nullable.size());
        }
        return ColumnNullable::create(std::move(values), std::move(nulls));
    }

    const ColumnVariantV2& materialized_column() const override {
        std::lock_guard lock(_materialization_lock);
        if (!_materialized) {
            auto materialized = ColumnVariantV2::create();
            for (const auto& segment : _segments) {
                const ColumnVariantV2& source = segment->materialized_column();
                materialized->insert_range_from(source, 0, source.size());
            }
            _materialized = std::move(materialized);
        }
        return *_materialized;
    }

    const ColumnVariantV2& serialized_column() const override {
        std::lock_guard lock(_materialization_lock);
        if (!_serialized) {
            auto serialized = ColumnVariantV2::create();
            for (const auto& segment : _segments) {
                const ColumnVariantV2& source = segment->serialized_column();
                serialized->insert_range_from(source, 0, source.size());
            }
            _serialized = std::move(serialized);
        }
        return *_serialized;
    }

private:
    static std::shared_ptr<VariantShreddedState> pack(
            std::vector<std::shared_ptr<VariantShreddedState>> segments) {
        if (segments.size() == 1) {
            return std::move(segments.front());
        }
        return std::make_shared<CompositeVariantShreddedState>(std::move(segments));
    }

    void append(std::shared_ptr<VariantShreddedState> source) {
        if (source->size() == 0) {
            return;
        }
        if (const auto* composite =
                    dynamic_cast<const CompositeVariantShreddedState*>(source.get())) {
            _segments.insert(_segments.end(), composite->_segments.begin(),
                             composite->_segments.end());
            return;
        }
        if (!_segments.empty()) {
            auto& tail = _segments.back();
            if (tail.use_count() != 1) {
                tail = tail->select_range(0, tail->size());
            }
            if (tail->try_append(*source)) {
                return;
            }
        }
        _segments.push_back(std::move(source));
    }

    std::vector<std::shared_ptr<VariantShreddedState>> _segments;
    size_t _rows = 0;
    mutable std::mutex _materialization_lock;
    mutable ColumnVariantV2::MutablePtr _materialized;
    mutable ColumnVariantV2::MutablePtr _serialized;
};

std::shared_ptr<VariantShreddedState> combine_shredded_states(
        std::shared_ptr<VariantShreddedState> left, std::shared_ptr<VariantShreddedState> right) {
    auto combined = std::make_shared<CompositeVariantShreddedState>(
            std::vector<std::shared_ptr<VariantShreddedState>> {std::move(left)});
    combined->try_append(*right);
    return combined;
}

} // namespace

#ifdef BE_TEST
void ColumnVariantV2::TestAccess::replace_encoded_subcolumn(ColumnVariantV2& column, size_t index,
                                                            ColumnPtr replacement) {
    DORIS_CHECK(!column._typed);
    switch (index) {
    case 0:
        static_cast<IColumn::Ptr&>(column._metadatas) = std::move(replacement);
        break;
    case 1:
        static_cast<IColumn::Ptr&>(column._meta_ids) = std::move(replacement);
        break;
    case 2:
        static_cast<IColumn::Ptr&>(column._values) = std::move(replacement);
        break;
    default:
        DORIS_CHECK(false) << "ColumnVariantV2 test subcolumn index is out of range";
    }
}
#endif

ColumnVariantV2::ColumnVariantV2()
        : _metadatas(ColumnString::create()),
          _meta_ids(MetaIdsColumn::create()),
          _values(ColumnString::create()) {
    _check_invariants();
}

ColumnVariantV2::ColumnVariantV2(const ColumnVariantV2& other)
        : _metadatas(other._metadatas),
          _meta_ids(other._meta_ids),
          _values(other._values),
          _typed(other._typed),
          _typed_type(other._typed_type),
          _shredded(other._shredded) {}

ColumnVariantV2::MutablePtr ColumnVariantV2::create_typed(ColumnPtr column,
                                                          DataTypePtr scalar_type) {
    ValidatedTypedInput input = validate_typed_input(std::move(column), std::move(scalar_type));
    auto result = ColumnVariantV2::create();
    static_cast<IColumn::Ptr&>(result->_typed) = std::move(input.column);
    result->_typed_type = std::move(input.type);
    result->_check_invariants();
    return result;
}

ColumnVariantV2::MutablePtr ColumnVariantV2::create_shredded(
        std::shared_ptr<VariantShreddedState> state) {
    DORIS_CHECK(state != nullptr) << "shredded ColumnVariantV2 state must not be null";
    auto result = ColumnVariantV2::create();
    result->_shredded = std::move(state);
    result->_check_invariants();
    return result;
}

const IColumn& ColumnVariantV2::typed_column() const {
    DORIS_CHECK(_typed != nullptr) << "typed_column requires ColumnVariantV2 typed state";
    return *_typed;
}

const DataTypePtr& ColumnVariantV2::typed_type() const {
    DORIS_CHECK(_typed_type != nullptr) << "typed_type requires ColumnVariantV2 typed state";
    return _typed_type;
}

std::optional<VariantShreddedTypedValue> ColumnVariantV2::find_shredded_typed_value(
        std::span<const VariantShreddedPathSegment> path) const {
    if (!_shredded) {
        return std::nullopt;
    }
    return _shredded->find_typed_value(path);
}

const ColumnVariantV2& ColumnVariantV2::serialization_column() const {
    if (!_shredded) {
        return *this;
    }
    const ColumnVariantV2& serialized = _shredded->serialized_column();
    DORIS_CHECK(!serialized.is_shredded())
            << "shredded Variant wire materializer returned another shredded column";
    DORIS_CHECK_EQ(serialized.size(), size())
            << "shredded Variant wire materializer changed the row count";
    return serialized;
}

void ColumnVariantV2::ensure_encoded() {
    if (_shredded) {
        _replace_shredded_state_with(_shredded->materialized_column());
    }
    if (!_typed) {
        DCHECK(_typed_type == nullptr);
        return;
    }

    const auto& typed = static_cast<const IColumn::Ptr&>(_typed);
    const auto& nullable = assert_cast<const ColumnNullable&>(*typed);
    const PrimitiveType type = _typed_type->get_primitive_type();
    const uint32_t scale = _typed_type->get_scale();
    TypedEncodingResult encoded;
    dispatch_variant_typed_column(nullable.get_nested_column(), type,
                                  [&]<PrimitiveType Type>(const auto& column) {
                                      encoded = encode_typed_column<Type>(nullable, column, scale);
                                  });

    static_cast<IColumn::Ptr&>(_metadatas) = std::move(encoded.metadatas);
    static_cast<IColumn::Ptr&>(_meta_ids) = std::move(encoded.metadata_ids);
    static_cast<IColumn::Ptr&>(_values) = std::move(encoded.values);
    static_cast<IColumn::Ptr&>(_typed).reset();
    _typed_type.reset();
    _check_invariants();
}

void ColumnVariantV2::_ensure_serialized() {
    if (_shredded) {
        _replace_shredded_state_with(_shredded->serialized_column());
    }
}

std::string ColumnVariantV2::get_name() const {
    if (_shredded) {
        return "variant_v2(shredded)";
    }
    if (_typed) {
        DORIS_CHECK(_typed_type != nullptr);
        return "variant_v2(typed=" + _typed_type->get_name() + ")";
    }
    DCHECK(_typed_type == nullptr);
    return "variant_v2";
}

size_t ColumnVariantV2::size() const {
    if (_shredded) {
        return _shredded->size();
    }
    if (_typed) {
        DCHECK(_typed_type != nullptr);
        DCHECK(_metadatas->empty());
        DCHECK(_meta_ids->empty());
        DCHECK(_values->empty());
        return _typed->size();
    }

    DCHECK(_typed_type == nullptr);
    DCHECK_EQ(_meta_ids->size(), _values->size());
    return _meta_ids->size();
}

size_t ColumnVariantV2::byte_size() const {
    if (_shredded) {
        return _shredded->byte_size();
    }
    if (_typed) {
        DCHECK(_metadatas->empty());
        DCHECK(_meta_ids->empty());
        DCHECK(_values->empty());
        return _typed->byte_size();
    }
    DCHECK_EQ(_meta_ids->size(), _values->size());
    return _metadatas->byte_size() + _meta_ids->byte_size() + _values->byte_size();
}

size_t ColumnVariantV2::allocated_bytes() const {
    if (_shredded) {
        return _shredded->allocated_bytes();
    }
    if (_typed) {
        DCHECK(_metadatas->empty());
        DCHECK(_meta_ids->empty());
        DCHECK(_values->empty());
        return _typed->allocated_bytes();
    }
    DCHECK_EQ(_meta_ids->size(), _values->size());
    return _metadatas->allocated_bytes() + _meta_ids->allocated_bytes() +
           _values->allocated_bytes();
}

bool ColumnVariantV2::has_enough_capacity(const IColumn& src) const {
    const auto& source = assert_cast<const ColumnVariantV2&>(src);
    if (_shredded || source._shredded) {
        return false;
    }
    if (static_cast<bool>(_typed) != static_cast<bool>(source._typed)) {
        return false;
    }
    if (_typed) {
        if (!exact_typed_identity(_typed_type, source._typed_type)) {
            return false;
        }
        return _typed->has_enough_capacity(*source._typed);
    }
    return _metadatas->has_enough_capacity(*source._metadatas) &&
           _meta_ids->has_enough_capacity(*source._meta_ids) &&
           _values->has_enough_capacity(*source._values);
}

bool ColumnVariantV2::structure_equals(const IColumn& rhs) const {
    return typeid(rhs) == typeid(ColumnVariantV2);
}

void ColumnVariantV2::sanity_check() const {
    if (_shredded) {
        _shredded->sanity_check();
        _check_invariants();
        return;
    }
    if (_typed) {
        _typed->sanity_check();
    } else {
        _metadatas->sanity_check();
        _meta_ids->sanity_check();
        _values->sanity_check();
    }
    _check_invariants();
    if (!_typed) {
        const auto& metadatas = assert_cast<const ColumnString&>(*_metadatas);
        const auto& metadata_ids = assert_cast<const MetaIdsColumn&>(*_meta_ids).get_data();
        const auto& values = assert_cast<const ColumnString&>(*_values);
        for (size_t id = 0; id < metadatas.size(); ++id) {
            const StringRef metadata = metadatas.get_data_at(id);
            validate_variant_metadata({.data = metadata.data, .size = metadata.size});
        }
        for (size_t row = 0; row < values.size(); ++row) {
            const uint32_t id = metadata_ids[row];
            DORIS_CHECK_LT(id, metadatas.size()) << "ColumnVariantV2 metadata id is out of range";
            const StringRef metadata = metadatas.get_data_at(id);
            validate_variant_payload({.metadata = {.data = metadata.data, .size = metadata.size},
                                      .value = values.get_data_at(row)});
        }
    }
}

void ColumnVariantV2::for_each_subcolumn(ColumnCallback callback) {
    if (_shredded) {
        // The legacy mutable traversal is used for COW detachment. Shredded state is immutable and
        // reference-counted, so keep a partial leaf projection intact until canonical bytes are needed.
        return;
    }
    if (_typed) {
        callback(_typed);
    } else {
        callback(_metadatas);
        callback(_meta_ids);
        callback(_values);
    }
}

void ColumnVariantV2::clear() {
    if (_shredded) {
        _shredded.reset();
        _check_invariants();
        return;
    }
    if (_typed) {
        mutate_subcolumn(_typed);
        _typed->clear();
    } else {
        auto& metadata_ptr = static_cast<IColumn::Ptr&>(_metadatas);
        if (metadata_ptr->is_exclusive()) {
            _metadatas->clear();
        } else {
            metadata_ptr = ColumnString::create();
        }
        require_exclusive(_meta_ids, "metadata ids");
        require_exclusive(_values, "values");
        _meta_ids->clear();
        _values->clear();
    }
    _check_invariants();
}

// Validate the encoded batch before appending metadata, ids, and values.
void ColumnVariantV2::insert_encoded_rows( // NOLINT(readability-function-size)
        const EncodedDataView& data) {
    validate_offsets(data.metadata_bytes, data.metadata_offsets, "metadata");
    validate_offsets(data.value_bytes, data.value_offsets, "value");

    const size_t metadata_count = data.metadata_offsets.size() - 1;
    const size_t rows = data.value_offsets.size() - 1;
    DORIS_CHECK_LE(metadata_count, std::numeric_limits<uint32_t>::max())
            << "metadata count exceeds the uint32 id domain";
    if (rows == 0) {
        DORIS_CHECK(data.meta_ids.empty()) << "empty encoded batch cannot contain metadata ids";
        return;
    }
    DORIS_CHECK_NE(metadata_count, 0) << "encoded rows require at least one metadata blob";
    if (data.meta_ids.empty()) {
        DORIS_CHECK_EQ(metadata_count, 1)
                << "omitted metadata ids require exactly one metadata blob";
    } else {
        DORIS_CHECK_EQ(data.meta_ids.size(), rows)
                << "metadata id count must match the encoded row count";
    }

    auto metadata_at = [&](uint32_t id) {
        const uint32_t begin = data.metadata_offsets[id];
        const uint32_t end = data.metadata_offsets[id + 1];
        return VariantMetadataRef {.data = data.metadata_bytes.data + begin, .size = end - begin};
    };
    for (uint32_t id = 0; id < metadata_count; ++id) {
        validate_variant_metadata(metadata_at(id));
    }

    // Validate every row before changing a typed destination or appending to an encoded one.
    // This keeps input errors atomic without introducing a second temporary column.
    if (data.meta_ids.empty()) {
        const VariantMetadataRef metadata = metadata_at(0);
        for (size_t row = 0; row < rows; ++row) {
            const uint32_t begin = data.value_offsets[row];
            const uint32_t end = data.value_offsets[row + 1];
            validate_variant_payload(
                    {.metadata = metadata,
                     .value = {data.value_bytes.data + begin, static_cast<size_t>(end - begin)}});
        }
    } else {
        for (size_t row = 0; row < rows; ++row) {
            const uint32_t source_id = data.meta_ids[row];
            DORIS_CHECK_LT(source_id, metadata_count) << "encoded row metadata id is out of range";
            const uint32_t begin = data.value_offsets[row];
            const uint32_t end = data.value_offsets[row + 1];
            validate_variant_payload(
                    {.metadata = metadata_at(source_id),
                     .value = {data.value_bytes.data + begin, static_cast<size_t>(end - begin)}});
        }
    }

    _append_prevalidated_encoded_data(data);
}

struct ColumnVariantV2::EncodedRowsAppender::Impl {
    using MetadataHashHeads =
            std::unordered_map<size_t, uint32_t, std::hash<size_t>, std::equal_to<size_t>,
                               CustomStdAllocator<std::pair<const size_t, uint32_t>>>;

    explicit Impl(ColumnVariantV2& destination) : column(&destination) {}

    ColumnVariantV2* column;
    MetadataHashHeads metadata_hash_heads;
    DorisVector<uint32_t> metadata_hash_next;
    size_t expected_rows = 0;
    bool initialized = false;
#ifdef BE_TEST
    size_t metadata_comparisons = 0;
#endif
};

ColumnVariantV2::EncodedRowsAppender::EncodedRowsAppender(ColumnVariantV2& column)
        : _impl(std::make_unique<Impl>(column)) {}

ColumnVariantV2::EncodedRowsAppender::~EncodedRowsAppender() = default;

ColumnVariantV2::EncodedRowsAppender::EncodedRowsAppender(EncodedRowsAppender&&) noexcept = default;

#ifdef BE_TEST
size_t ColumnVariantV2::EncodedRowsAppender::metadata_comparisons_for_test() const noexcept {
    return _impl == nullptr ? 0 : _impl->metadata_comparisons;
}
#endif

void ColumnVariantV2::EncodedRowsAppender::append( // NOLINT(readability-function-size)
        std::span<const VariantRef> rows) {
    if (rows.empty()) {
        return;
    }
    DORIS_CHECK(_impl != nullptr && _impl->column != nullptr)
            << "Cannot use a moved-from Variant encoded-row appender";
    ColumnVariantV2& column = *_impl->column;

    using MetadataIdMap =
            std::unordered_map<std::string_view, uint32_t, std::hash<std::string_view>,
                               std::equal_to<std::string_view>,
                               CustomStdAllocator<std::pair<const std::string_view, uint32_t>>>;
    MetadataIdMap metadata_ids_by_value;
    DorisVector<VariantMetadataRef> unique_metadatas;
    DorisVector<uint32_t> source_metadata_ids;
    DorisVector<StringRef> source_values(rows.size());
    size_t total_value_bytes = 0;
    for (size_t row = 0; row < rows.size(); ++row) {
        const VariantRef value = rows[row];
        if (value.metadata.data == nullptr && value.metadata.size != 0) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Variant encoded metadata has a null data pointer for {} bytes",
                            value.metadata.size);
        }
        if (value.value.data == nullptr && value.value.size != 0) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Variant encoded value has a null data pointer for {} bytes",
                            value.value.size);
        }

        const std::string_view metadata_key(
                value.metadata.data == nullptr ? "" : value.metadata.data, value.metadata.size);
        uint32_t source_metadata_id = 0;
        if (unique_metadatas.empty()) {
            validate_variant_metadata(value.metadata);
            unique_metadatas.push_back(value.metadata);
        } else if (unique_metadatas.size() == 1 && metadata_ids_by_value.empty() &&
                   StringRef(unique_metadatas.front().data, unique_metadatas.front().size) ==
                           StringRef(value.metadata.data, value.metadata.size)) {
            // Iceberg files normally share one metadata dictionary across a batch. Avoid a hash
            // table and per-row ids until a second distinct dictionary is actually observed.
        } else {
            if (metadata_ids_by_value.empty()) {
                const VariantMetadataRef first = unique_metadatas.front();
                metadata_ids_by_value.emplace(
                        std::string_view(first.data == nullptr ? "" : first.data, first.size), 0);
                source_metadata_ids.resize(rows.size());
            }
            auto metadata_id = metadata_ids_by_value.find(metadata_key);
            if (metadata_id != metadata_ids_by_value.end()) {
                source_metadata_id = metadata_id->second;
            } else {
                if (unique_metadatas.size() == std::numeric_limits<uint32_t>::max()) {
                    throw Exception(
                            ErrorCode::INVALID_ARGUMENT,
                            "Variant encoded metadata dictionary exceeds the uint32 id limit");
                }
                validate_variant_metadata(value.metadata);
                source_metadata_id = static_cast<uint32_t>(unique_metadatas.size());
                unique_metadatas.push_back(value.metadata);
                metadata_ids_by_value.emplace(metadata_key, source_metadata_id);
            }
        }
        if (!source_metadata_ids.empty()) {
            source_metadata_ids[row] = source_metadata_id;
        }
        validate_variant_payload(value);
        source_values[row] = value.value;
        if (value.value.size > std::numeric_limits<size_t>::max() - total_value_bytes) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Variant encoded value bytes exceed the size_t limit");
        }
        total_value_bytes += value.value.size;
    }

    // Validate the complete input before changing a typed/shredded destination. Failed and empty
    // appends must preserve its representation just like the EncodedDataView overload does.
    if (_impl->initialized) {
        DORIS_CHECK(column._typed == nullptr && column._shredded == nullptr)
                << "ColumnVariantV2 changed outside its encoded-row appender";
        DORIS_CHECK_EQ(_impl->expected_rows, column.size())
                << "ColumnVariantV2 changed outside its encoded-row appender";
        const auto& current_metadatas = assert_cast<const ColumnString&>(
                *static_cast<const IColumn::Ptr&>(column._metadatas));
        DORIS_CHECK_EQ(_impl->metadata_hash_next.size(), current_metadatas.size())
                << "ColumnVariantV2 metadata changed outside its encoded-row appender";
    }

    auto aliases_encoded_storage = [&](const ColumnVariantV2& owner) {
        DORIS_CHECK(owner._typed == nullptr && owner._shredded == nullptr)
                << "encoded-row owner must have encoded storage";
        const auto& metadatas = assert_cast<const ColumnString&>(
                *static_cast<const IColumn::Ptr&>(owner._metadatas));
        const auto& values = assert_cast<const ColumnString&>(*owner._values);
        const StringRef metadata_storage(metadatas.get_chars().data(),
                                         metadatas.get_chars().size());
        const StringRef value_storage(values.get_chars().data(), values.get_chars().size());
        for (const VariantRef row : rows) {
            if (byte_ranges_overlap({row.metadata.data, row.metadata.size}, metadata_storage) ||
                byte_ranges_overlap(row.value, value_storage)) {
                return true;
            }
        }
        return false;
    };
    const ColumnVariantV2* encoded_owner = nullptr;
    if (column._shredded) {
        // get_value_ref() on a shredded destination borrows from this cached materialization.
        // Check it before ensure_encoded() drops the state that owns those bytes.
        encoded_owner = &column._shredded->materialized_column();
    } else if (!column._typed) {
        encoded_owner = &column;
    }
    if (encoded_owner != nullptr && aliases_encoded_storage(*encoded_owner)) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant encoded-row input must not alias its destination; use "
                        "insert_range_from() for same-column rows");
    }

    if (column._typed || column._shredded) {
        column.ensure_encoded();
    }
    DORIS_CHECK(column._typed_type == nullptr) << "encoded state cannot retain a typed data type";
    require_exclusive(column._meta_ids, "metadata ids");
    require_exclusive(column._values, "values");
    auto& values = assert_cast<ColumnString&>(*column._values);
    auto& metadata_ids = assert_cast<MetaIdsColumn&>(*column._meta_ids);

    auto current_metadatas = [&]() -> const ColumnString& {
        return assert_cast<const ColumnString&>(
                *static_cast<const IColumn::Ptr&>(column._metadatas));
    };
    if (!_impl->initialized) {
        const auto& metadatas = current_metadatas();
        _impl->metadata_hash_heads.reserve(metadatas.size());
        _impl->metadata_hash_next.reserve(metadatas.size());
        for (size_t index = 0; index < metadatas.size(); ++index) {
            const auto id = static_cast<uint32_t>(index);
            const StringRef metadata = metadatas.get_data_at(id);
            const size_t hash = StringRefHash {}(metadata);
            const auto existing = _impl->metadata_hash_heads.find(hash);
            const uint32_t next = existing == _impl->metadata_hash_heads.end()
                                          ? UNMAPPED_METADATA_ID
                                          : existing->second;
            _impl->metadata_hash_next.push_back(next);
            _impl->metadata_hash_heads[hash] = id;
        }
        _impl->expected_rows = column.size();
        _impl->initialized = true;
    }
    reserve_rows(values, metadata_ids, total_value_bytes, rows.size());
    auto find_or_insert_metadata = [&](VariantMetadataRef metadata) {
        const StringRef metadata_bytes(metadata.data, metadata.size);
        const size_t hash = StringRefHash {}(metadata_bytes);
        auto existing = _impl->metadata_hash_heads.find(hash);
        uint32_t id = existing == _impl->metadata_hash_heads.end() ? UNMAPPED_METADATA_ID
                                                                   : existing->second;
        while (id != UNMAPPED_METADATA_ID) {
#ifdef BE_TEST
            ++_impl->metadata_comparisons;
#endif
            if (current_metadatas().get_data_at(id) == metadata_bytes) {
                return id;
            }
            id = _impl->metadata_hash_next[id];
        }

        const uint32_t new_id = column._append_metadata(metadata_bytes);
        const uint32_t next = existing == _impl->metadata_hash_heads.end() ? UNMAPPED_METADATA_ID
                                                                           : existing->second;
        _impl->metadata_hash_next.push_back(next);
        if (existing == _impl->metadata_hash_heads.end()) {
            _impl->metadata_hash_heads.emplace(hash, new_id);
        } else {
            existing->second = new_id;
        }
        return new_id;
    };

    if (unique_metadatas.size() == 1) {
        const uint32_t id = find_or_insert_metadata(unique_metadatas.front());
        values.insert_many_strings_without_reserve(source_values.data(), source_values.size());
        metadata_ids.insert_many_vals(id, rows.size());
    } else {
        DorisVector<uint32_t> destination_metadata_ids(unique_metadatas.size());
        for (size_t id = 0; id < unique_metadatas.size(); ++id) {
            destination_metadata_ids[id] = find_or_insert_metadata(unique_metadatas[id]);
        }
        values.insert_many_strings_without_reserve(source_values.data(), source_values.size());
        auto& destination_ids = metadata_ids.get_data();
        for (size_t row = 0; row < rows.size(); ++row) {
            destination_ids.push_back(destination_metadata_ids[source_metadata_ids[row]]);
        }
    }

    _impl->expected_rows = column.size();
    DCHECK_EQ(column._meta_ids->size(), column._values->size());
    column._check_invariants();
}

ColumnVariantV2::EncodedRowsAppender ColumnVariantV2::create_encoded_rows_appender() {
    return EncodedRowsAppender(*this);
}

void ColumnVariantV2::insert_encoded_batch(const VariantBatchBuilder& block) {
    const size_t rows = block.num_rows();
    const std::span<const uint32_t> offsets = block.value_offsets();
    DORIS_CHECK_EQ(offsets.size(), rows + 1)
            << "VariantBatchBuilder must be materialized before insertion";
    if (rows == 0) {
        return;
    }

    const VariantMetadataRef metadata = block.metadata_ref();
    const StringRef value_bytes = block.value_bytes();
    DORIS_CHECK_LE(metadata.size, std::numeric_limits<uint32_t>::max())
            << "VariantBatchBuilder metadata exceeds the ColumnString uint32 byte limit";
    DORIS_CHECK_EQ(offsets.front(), 0);
    DORIS_CHECK_EQ(static_cast<size_t>(offsets.back()), value_bytes.size);

    const std::array<uint32_t, 2> metadata_offsets {0, static_cast<uint32_t>(metadata.size)};
    _append_prevalidated_encoded_data({.metadata_bytes = {metadata.data, metadata.size},
                                       .metadata_offsets = metadata_offsets,
                                       .meta_ids = {},
                                       .value_bytes = value_bytes,
                                       .value_offsets = offsets});
}

void ColumnVariantV2::_append_prevalidated_encoded_data(const EncodedDataView& data) {
    DCHECK(!data.metadata_offsets.empty());
    DCHECK(!data.value_offsets.empty());
    const size_t metadata_count = data.metadata_offsets.size() - 1;
    const size_t rows = data.value_offsets.size() - 1;
    if (rows == 0) {
        return;
    }

    DCHECK_NE(metadata_count, 0);
    DCHECK(data.meta_ids.empty() || data.meta_ids.size() == rows);
    auto metadata_at = [&](uint32_t id) {
        DCHECK_LT(id, metadata_count);
        const uint32_t begin = data.metadata_offsets[id];
        const uint32_t end = data.metadata_offsets[id + 1];
        return StringRef(data.metadata_bytes.data + begin, end - begin);
    };

    if (_typed || _shredded) {
        ensure_encoded();
    }
    DORIS_CHECK(_typed_type == nullptr) << "encoded state cannot retain a typed data type";
    require_exclusive(_meta_ids, "metadata ids");
    require_exclusive(_values, "values");
    auto& values = assert_cast<ColumnString&>(*_values);
    auto& metadata_ids = assert_cast<MetaIdsColumn&>(*_meta_ids);
    reserve_rows(values, metadata_ids, data.value_bytes.size, rows);

    if (data.meta_ids.empty()) {
        DCHECK_EQ(metadata_count, 1);
        const uint32_t id = _find_or_insert_metadata(metadata_at(0));
        values.insert_many_continuous_binary_data(data.value_bytes.data, data.value_offsets.data(),
                                                  rows);
        metadata_ids.insert_many_vals(id, rows);
    } else {
        DorisVector<uint32_t> remap(metadata_count, UNMAPPED_METADATA_ID);
        auto& destination_ids = metadata_ids.get_data();
        for (size_t row = 0; row < rows; ++row) {
            const uint32_t source_id = data.meta_ids[row];
            DCHECK_LT(source_id, metadata_count);
            if (remap[source_id] == UNMAPPED_METADATA_ID) {
                remap[source_id] = _find_or_insert_metadata(metadata_at(source_id));
            }
            destination_ids.push_back(remap[source_id]);
        }
        values.insert_many_continuous_binary_data(data.value_bytes.data, data.value_offsets.data(),
                                                  rows);
    }

    DCHECK_EQ(_meta_ids->size(), _values->size());
    _check_invariants();
}

VariantRef ColumnVariantV2::get_value_ref(size_t row) const {
    if (_shredded) {
        return _shredded->materialized_column().get_value_ref(row);
    }
    DCHECK(!_typed);
    DCHECK(_typed_type == nullptr);
    DCHECK_LT(row, size());
    const auto& metadata_ids = assert_cast<const MetaIdsColumn&>(*_meta_ids).get_data();
    const uint32_t metadata_id = metadata_ids[row];
    DCHECK_LT(metadata_id, _metadatas->size());
    const StringRef metadata =
            assert_cast<const ColumnString&>(*_metadatas).get_data_at(metadata_id);
    const StringRef value = assert_cast<const ColumnString&>(*_values).get_data_at(row);
    return {.metadata = {.data = metadata.data, .size = metadata.size}, .value = value};
}

Field ColumnVariantV2::operator[](size_t row) const {
    Field result;
    get(row, result);
    return result;
}

void ColumnVariantV2::get(size_t row, Field& result) const {
    if (UNLIKELY(row >= size())) {
        throw Exception(ErrorCode::OUT_OF_BOUND,
                        "Index ({}) for getting Variant field is out of range for size {}", row,
                        size());
    }

    VariantField value;
    if (_typed) {
        const auto& nullable = assert_cast<const ColumnNullable&>(*_typed);
        visit_typed_scalar_column(nullable, _typed_type->get_primitive_type(),
                                  _typed_type->get_scale(), row, row + 1,
                                  [&](size_t, const VariantScalarRef& scalar) {
                                      value = VariantField::from_scalar(scalar);
                                  });
    } else {
        value = VariantField::from_ref(get_value_ref(row));
    }
    result = Field::create_field<TYPE_VARIANT>(std::move(value));
}

void ColumnVariantV2::insert(const Field& field) {
    VariantField null_value;
    const VariantField* value = nullptr;
    if (field.get_type() == TYPE_NULL) {
        null_value = VariantField::from_scalar(VariantScalarRef::null_value());
        value = &null_value;
    } else if (field.get_type() == TYPE_VARIANT) {
        value = &field.get<TYPE_VARIANT>();
        if (value->is_legacy()) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "ColumnVariantV2 cannot insert a legacy VariantMap Field");
        }
    } else {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "ColumnVariantV2 only accepts Variant or NULL Field values, got {}",
                        field.get_type_name());
    }

    const VariantMetadataRef metadata = value->metadata();
    const StringRef encoded_value = value->value();
    if (metadata.size > std::numeric_limits<uint32_t>::max() ||
        encoded_value.size > std::numeric_limits<uint32_t>::max()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant Field row exceeds ColumnString uint32 limits");
    }
    const std::array<uint32_t, 2> metadata_offsets {0, static_cast<uint32_t>(metadata.size)};
    const std::array<uint32_t, 2> value_offsets {0, static_cast<uint32_t>(encoded_value.size)};
    insert_encoded_rows({.metadata_bytes = {metadata.data, metadata.size},
                         .metadata_offsets = metadata_offsets,
                         .meta_ids = {},
                         .value_bytes = encoded_value,
                         .value_offsets = value_offsets});
}

void ColumnVariantV2::insert_default() {
    insert_many_defaults(1);
}

void ColumnVariantV2::insert_many_defaults(size_t length) {
    if (length == 0) {
        return;
    }

    if (_typed || _shredded) {
        ensure_encoded();
    }

    DORIS_CHECK(_typed_type == nullptr) << "encoded state cannot retain a typed data type";

    DORIS_CHECK_LE(length, std::numeric_limits<size_t>::max() - size())
            << "default row count overflows size_t";
    DORIS_CHECK_LE(length, std::numeric_limits<size_t>::max() / EMPTY_OBJECT_VALUE.size())
            << "default value bytes overflow size_t";
    require_exclusive(_meta_ids, "metadata ids");
    require_exclusive(_values, "values");

    const size_t value_bytes = length * EMPTY_OBJECT_VALUE.size();
    auto& values = assert_cast<ColumnString&>(*_values);
    auto& metadata_ids = assert_cast<MetaIdsColumn&>(*_meta_ids);
    DORIS_CHECK_LE(value_bytes, std::numeric_limits<size_t>::max() - values.get_chars().size())
            << "default value bytes overflow the destination";
    reserve_rows(values, metadata_ids, value_bytes, length);

    const uint32_t metadata_id = _find_or_insert_metadata(
            {VARIANT_EMPTY_METADATA.data(), VARIANT_EMPTY_METADATA.size()});
    auto& chars = values.get_chars();
    auto& offsets = values.get_offsets();
    const size_t old_chars_size = chars.size();
    const size_t old_offsets_size = offsets.size();
    chars.resize(old_chars_size + value_bytes);
    offsets.resize(old_offsets_size + length);
    for (size_t row = 0; row < length; ++row) {
        std::ranges::copy(EMPTY_OBJECT_VALUE,
                          chars.begin() + old_chars_size + row * EMPTY_OBJECT_VALUE.size());
        offsets[old_offsets_size + row] = static_cast<ColumnString::Offset>(
                old_chars_size + (row + 1) * EMPTY_OBJECT_VALUE.size());
    }
    metadata_ids.insert_many_vals(metadata_id, length);
    _check_invariants();
}

void ColumnVariantV2::insert_from(const IColumn& src, size_t row) {
    insert_range_from(src, row, 1);
}

// Range insertion handles typed and encoded state pairs.
void ColumnVariantV2::insert_range_from( // NOLINT(readability-function-size)
        const IColumn& src, size_t start, size_t length) {
    const auto& source = assert_cast<const ColumnVariantV2&>(src);
    DORIS_CHECK_LE(start, source.size()) << "source range starts past source size";
    DORIS_CHECK_LE(length, source.size() - start) << "source range exceeds source size";
    if (length == 0) {
        return;
    }

    if (!_shredded && !_typed && empty() && _metadatas->empty() && source._shredded) {
        // IColumn::cut() inserts into an empty clone. Select the physical tree directly because an
        // incomplete leaf projection cannot be reconstructed merely to copy a row range.
        _shredded = start == 0 && length == source.size()
                            ? source._shredded
                            : source._shredded->select_range(start, length);
        _check_invariants();
        return;
    }
    if (_shredded && source._shredded) {
        auto selected_source = start == 0 && length == source.size()
                                       ? source._shredded
                                       : source._shredded->select_range(start, length);
        // C++20 libc++ removed shared_ptr::unique(); use_count preserves the same COW invariant
        // on every supported toolchain before mutating the format-owned state.
        if (_shredded.use_count() != 1) {
            _shredded = _shredded->select_range(0, size());
        }
        // A partial physical projection has no metadata/value pair to encode. Preserve that
        // invariant by merging compatible scanner batches before the canonical fallback below.
        if (_shredded->try_append(*selected_source)) {
            _check_invariants();
            return;
        }
        if (!_shredded->can_materialize() || !selected_source->can_materialize()) {
            // A projected segment cannot reconstruct omitted root fields. Preserve it beside any
            // complete or projected neighbor so later path extraction can choose per-segment
            // direct or canonical evaluation without forcing the incomplete state to materialize.
            _shredded = combine_shredded_states(std::move(_shredded), std::move(selected_source));
            _check_invariants();
            return;
        }
    }
    if (_shredded) {
        // A merging exchange can combine a local projected state with its remotely serialized
        // peer. Use the wire representation so omitted roots are never requested from either side.
        _ensure_serialized();
    }
    if (source._shredded) {
        insert_range_from(source.serialization_column(), start, length);
        return;
    }

    if (_typed && source._typed && exact_typed_identity(_typed_type, source._typed_type)) {
        mutate_subcolumn(_typed);
        _typed->insert_range_from(*source._typed, start, length);
        _check_invariants();
        return;
    }

    if (_typed) {
        ensure_encoded();
    }

    if (source._typed) {
        MutableColumnPtr selected = source._typed->clone_empty();
        selected->insert_range_from(*source._typed, start, length);
        auto encoded_source = ColumnVariantV2::create();
        static_cast<IColumn::Ptr&>(encoded_source->_typed) = std::move(selected);
        encoded_source->_typed_type = source._typed_type;
        encoded_source->_check_invariants();
        encoded_source->ensure_encoded();
        insert_range_from(*encoded_source, 0, length);
        return;
    }

    if (this == &source) {
        auto snapshot = ColumnVariantV2::create();
        snapshot->insert_range_from(source, start, length);
        insert_range_from(*snapshot, 0, length);
        return;
    }

    require_exclusive(_meta_ids, "metadata ids");
    require_exclusive(_values, "values");
    const auto& source_metadatas = assert_cast<const ColumnString&>(*source._metadatas);
    const auto& source_metadata_ids =
            assert_cast<const MetaIdsColumn&>(*source._meta_ids).get_data();

    const auto& source_values = assert_cast<const ColumnString&>(*source._values);
    const auto& source_offsets = source_values.get_offsets();
    const size_t value_begin = source_offsets[static_cast<ssize_t>(start) - 1];
    const size_t value_end = source_offsets[start + length - 1];
    const bool destination_has_no_metadata = static_cast<const IColumn::Ptr&>(_metadatas)->empty();
    const bool copy_metadata_dictionary = empty() && destination_has_no_metadata;
    const bool already_shared = static_cast<const IColumn::Ptr&>(_metadatas).get() ==
                                static_cast<const IColumn::Ptr&>(source._metadatas).get();
    DorisVector<uint32_t> remap;
    if (!copy_metadata_dictionary && !already_shared) {
        remap.assign(source_metadatas.size(), UNMAPPED_METADATA_ID);
    }
    auto& values = assert_cast<ColumnString&>(*_values);
    auto& metadata_ids = assert_cast<MetaIdsColumn&>(*_meta_ids);
    reserve_rows(values, metadata_ids, value_end - value_begin, length);
    if (copy_metadata_dictionary) {
        static_cast<IColumn::Ptr&>(_metadatas) =
                source._metadatas->clone_resized(source_metadatas.size());
    }
    const bool shared_metadata = static_cast<const IColumn::Ptr&>(_metadatas).get() ==
                                 static_cast<const IColumn::Ptr&>(source._metadatas).get();
    values.insert_range_from(source_values, start, length);
    if (copy_metadata_dictionary || shared_metadata) {
        metadata_ids.insert_range_from(*source._meta_ids, start, length);
    } else {
        auto& destination_ids = metadata_ids.get_data();
        for (size_t row = 0; row < length; ++row) {
            const uint32_t source_id = source_metadata_ids[start + row];
            DORIS_CHECK_LT(source_id, source_metadatas.size())
                    << "source metadata id is out of range";
            if (remap[source_id] == UNMAPPED_METADATA_ID) {
                remap[source_id] =
                        _find_or_insert_metadata(source_metadatas.get_data_at(source_id));
            }
            destination_ids.push_back(remap[source_id]);
        }
    }
    _check_invariants();
}

// Indexed insertion handles typed and encoded state pairs.
void ColumnVariantV2::insert_indices_from( // NOLINT(readability-function-size)
        const IColumn& src, const uint32_t* indices_begin, const uint32_t* indices_end) {
    const auto& source = assert_cast<const ColumnVariantV2&>(src);
    const size_t rows = validate_selected_indices(indices_begin, indices_end, source.size());
    if (rows == 0) {
        return;
    }

    if (!_typed && empty() && _metadatas->empty() && source._shredded) {
        // Gather into the native shredded representation for the same reason as range selection:
        // row selection does not require, and may not have, a complete logical Variant value.
        _shredded = source._shredded->select_indices(indices_begin, indices_end);
        _check_invariants();
        return;
    }
    if (_shredded && source._shredded) {
        auto selected_source = source._shredded->select_indices(indices_begin, indices_end);
        if (_shredded.use_count() != 1) {
            _shredded = _shredded->select_range(0, size());
        }
        if (_shredded->try_append(*selected_source)) {
            _check_invariants();
            return;
        }
        if (!_shredded->can_materialize() || !selected_source->can_materialize()) {
            // Exchange gathers may mix complete files with projected files. Keep their boundaries
            // because only complete segments are allowed to reconstruct the full logical root.
            _shredded = combine_shredded_states(std::move(_shredded), std::move(selected_source));
            _check_invariants();
            return;
        }
    }
    if (_shredded) {
        // Indexed gathers have the same local/remote representation boundary as range gathers.
        _ensure_serialized();
    }
    if (source._shredded) {
        insert_indices_from(source.serialization_column(), indices_begin, indices_end);
        return;
    }

    if (_typed && source._typed && exact_typed_identity(_typed_type, source._typed_type)) {
        mutate_subcolumn(_typed);
        _typed->insert_indices_from(*source._typed, indices_begin, indices_end);
        _check_invariants();
        return;
    }

    if (_typed) {
        ensure_encoded();
    }

    if (source._typed) {
        MutableColumnPtr selected = source._typed->clone_empty();
        selected->insert_indices_from(*source._typed, indices_begin, indices_end);
        auto encoded_source = ColumnVariantV2::create();
        static_cast<IColumn::Ptr&>(encoded_source->_typed) = std::move(selected);
        encoded_source->_typed_type = source._typed_type;
        encoded_source->_check_invariants();
        encoded_source->ensure_encoded();
        insert_range_from(*encoded_source, 0, rows);
        return;
    }

    if (this == &source) {
        auto snapshot = ColumnVariantV2::create();
        snapshot->insert_indices_from(source, indices_begin, indices_end);
        insert_range_from(*snapshot, 0, rows);
        return;
    }

    require_exclusive(_meta_ids, "metadata ids");
    require_exclusive(_values, "values");
    const auto& source_metadatas = assert_cast<const ColumnString&>(*source._metadatas);
    const auto& source_metadata_ids =
            assert_cast<const MetaIdsColumn&>(*source._meta_ids).get_data();
    const auto& source_values = assert_cast<const ColumnString&>(*source._values);

    const bool destination_has_no_metadata = static_cast<const IColumn::Ptr&>(_metadatas)->empty();
    const bool copy_metadata_dictionary = empty() && destination_has_no_metadata;
    const bool already_shared = static_cast<const IColumn::Ptr&>(_metadatas).get() ==
                                static_cast<const IColumn::Ptr&>(source._metadatas).get();
    DorisVector<uint32_t> remap;
    if (!copy_metadata_dictionary && !already_shared) {
        remap.assign(source_metadatas.size(), UNMAPPED_METADATA_ID);
    }
    auto& values = assert_cast<ColumnString&>(*_values);
    auto& metadata_ids = assert_cast<MetaIdsColumn&>(*_meta_ids);
    metadata_ids.get_data().reserve(metadata_ids.size() + rows);
    if (copy_metadata_dictionary) {
        static_cast<IColumn::Ptr&>(_metadatas) =
                source._metadatas->clone_resized(source_metadatas.size());
    }
    const bool shared_metadata = static_cast<const IColumn::Ptr&>(_metadatas).get() ==
                                 static_cast<const IColumn::Ptr&>(source._metadatas).get();
    values.insert_indices_from(source_values, indices_begin, indices_end);
    if (copy_metadata_dictionary || shared_metadata) {
        metadata_ids.insert_indices_from(*source._meta_ids, indices_begin, indices_end);
    } else {
        auto& destination_ids = metadata_ids.get_data();
        for (size_t row = 0; row < rows; ++row) {
            const uint32_t source_id = source_metadata_ids[indices_begin[row]];
            DORIS_CHECK_LT(source_id, source_metadatas.size())
                    << "source metadata id is out of range";
            if (remap[source_id] == UNMAPPED_METADATA_ID) {
                remap[source_id] =
                        _find_or_insert_metadata(source_metadatas.get_data_at(source_id));
            }
            destination_ids.push_back(remap[source_id]);
        }
    }
    _check_invariants();
}

void ColumnVariantV2::pop_back(size_t length) {
    DORIS_CHECK_LE(length, size()) << "pop_back length exceeds the column size";
    if (length == 0) {
        return;
    }
    if (_shredded) {
        ensure_encoded();
    }
    if (_typed) {
        mutate_subcolumn(_typed);
        _typed->pop_back(length);
        _check_invariants();
        return;
    }
    require_exclusive(_meta_ids, "metadata ids");
    require_exclusive(_values, "values");
    _values->pop_back(length);
    _meta_ids->pop_back(length);
    _check_invariants();
}

StringRef ColumnVariantV2::get_data_at(size_t) const {
    throw_unsupported("get_data_at");
}

void ColumnVariantV2::insert_data(const char* pos, size_t length) {
    const VariantRef value = parse_canonical_serialized({pos, length});
    const std::array<uint32_t, 2> metadata_offsets {0, static_cast<uint32_t>(value.metadata.size)};
    const std::array<uint32_t, 2> value_offsets {0, static_cast<uint32_t>(value.value.size)};
    insert_encoded_rows({.metadata_bytes = {value.metadata.data, value.metadata.size},
                         .metadata_offsets = metadata_offsets,
                         .meta_ids = {},
                         .value_bytes = value.value,
                         .value_offsets = value_offsets});
}

StringRef ColumnVariantV2::serialize_value_into_arena(size_t row, Arena& arena,
                                                      const char*& begin) const {
    DCHECK_LT(row, size());
    if (_typed) {
        const auto& nullable = assert_cast<const ColumnNullable&>(*_typed);
        StringRef serialized;
        visit_typed_scalar_column(nullable, _typed_type->get_primitive_type(),
                                  _typed_type->get_scale(), row, row + 1,
                                  [&](size_t, const VariantScalarRef& scalar) {
                                      const CanonicalScalarSerializationPlan plan =
                                              prepare_canonical_serialize(scalar);
                                      char* destination = arena.alloc_continue(plan.size(), begin);
                                      plan.write(destination, plan.size());
                                      serialized = {destination, plan.size()};
                                  });
        return serialized;
    }
    DCHECK(_typed_type == nullptr);
    const CanonicalSerializationPlan plan = prepare_canonical_serialize(get_value_ref(row));
    const size_t cell_size = plan.size();
    char* destination = arena.alloc_continue(cell_size, begin);
    plan.write(destination, cell_size);
    return {destination, cell_size};
}

const char* ColumnVariantV2::deserialize_and_insert_from_arena(const char* pos) {
    const size_t cell_size = deserialize_impl(pos);
    return pos + cell_size;
}

size_t ColumnVariantV2::serialize_size_at(size_t row) const {
    DCHECK_LT(row, size());
    if (_typed) {
        const auto& nullable = assert_cast<const ColumnNullable&>(*_typed);
        size_t serialized_size = 0;
        visit_typed_scalar_column(nullable, _typed_type->get_primitive_type(),
                                  _typed_type->get_scale(), row, row + 1,
                                  [&](size_t, const VariantScalarRef& scalar) {
                                      serialized_size = prepare_canonical_serialize(scalar).size();
                                  });
        return serialized_size;
    }
    DCHECK(_typed_type == nullptr);
    return prepare_canonical_serialize(get_value_ref(row)).size();
}

size_t ColumnVariantV2::serialize_impl(char* pos, size_t row) const {
    DCHECK_LT(row, size());
    if (_typed) {
        const auto& nullable = assert_cast<const ColumnNullable&>(*_typed);
        size_t serialized_size = 0;
        visit_typed_scalar_column(nullable, _typed_type->get_primitive_type(),
                                  _typed_type->get_scale(), row, row + 1,
                                  [&](size_t, const VariantScalarRef& scalar) {
                                      const CanonicalScalarSerializationPlan plan =
                                              prepare_canonical_serialize(scalar);
                                      plan.write(pos, plan.size());
                                      serialized_size = plan.size();
                                  });
        return serialized_size;
    }
    DCHECK(_typed_type == nullptr);
    const CanonicalSerializationPlan plan = prepare_canonical_serialize(get_value_ref(row));
    const size_t cell_size = plan.size();
    plan.write(pos, cell_size);
    return cell_size;
}

size_t ColumnVariantV2::deserialize_impl(const char* pos) {
    const size_t cell_size = trusted_canonical_cell_size(pos);
    insert_data(pos, cell_size);
    return cell_size;
}

size_t ColumnVariantV2::get_max_row_byte_size() const {
    size_t maximum_size = 0;
    if (_typed) {
        const auto& nullable = assert_cast<const ColumnNullable&>(*_typed);
        visit_typed_scalar_column(
                nullable, _typed_type->get_primitive_type(), _typed_type->get_scale(), 0, size(),
                [&](size_t, const VariantScalarRef& scalar) {
                    maximum_size =
                            std::max(maximum_size, prepare_canonical_serialize(scalar).size());
                });
        return maximum_size;
    }
    DCHECK(_typed_type == nullptr);
    for (size_t row = 0; row < size(); ++row) {
        maximum_size =
                std::max(maximum_size, prepare_canonical_serialize(get_value_ref(row)).size());
    }
    return maximum_size;
}

void ColumnVariantV2::serialize_vec(StringRef* keys, size_t num_rows) const {
    DCHECK(keys != nullptr || num_rows == 0);
    DCHECK_LE(num_rows, size());
    if (_typed) {
        const auto& nullable = assert_cast<const ColumnNullable&>(*_typed);
        visit_typed_scalar_column(
                nullable, _typed_type->get_primitive_type(), _typed_type->get_scale(), 0, num_rows,
                [&](size_t row, const VariantScalarRef& scalar) {
                    const CanonicalScalarSerializationPlan plan =
                            prepare_canonical_serialize(scalar);
                    DCHECK(keys[row].data != nullptr);
                    DCHECK_LE(plan.size(), std::numeric_limits<size_t>::max() - keys[row].size);
                    plan.write(const_cast<char*>(keys[row].data) + keys[row].size, plan.size());
                    keys[row].size += plan.size();
                });
        return;
    }
    DCHECK(_typed_type == nullptr);
    for (size_t row = 0; row < num_rows; ++row) {
        const CanonicalSerializationPlan plan = prepare_canonical_serialize(get_value_ref(row));
        const size_t cell_size = plan.size();
        DCHECK(keys[row].data != nullptr);
        DCHECK_LE(cell_size, std::numeric_limits<size_t>::max() - keys[row].size);
        plan.write(const_cast<char*>(keys[row].data) + keys[row].size, cell_size);
        keys[row].size += cell_size;
    }
}

void ColumnVariantV2::deserialize_vec(StringRef* keys, size_t num_rows) {
    DCHECK(keys != nullptr || num_rows == 0);
    for (size_t row = 0; row < num_rows; ++row) {
        DCHECK_GE(keys[row].size, VARIANT_CANONICAL_SIZE_PREFIX);
        DCHECK(keys[row].data != nullptr);
        const size_t cell_size = trusted_canonical_cell_size(keys[row].data);
        DCHECK_LE(cell_size, keys[row].size);
        insert_data(keys[row].data, cell_size);
        keys[row].data += cell_size;
        keys[row].size -= cell_size;
    }
}

void ColumnVariantV2::update_hash_with_value(size_t row, SipHash& hash) const {
    DCHECK_LT(row, size());
    if (_typed) {
        const auto& nullable = assert_cast<const ColumnNullable&>(*_typed);
        visit_typed_scalar_column(
                nullable, _typed_type->get_primitive_type(), _typed_type->get_scale(), row, row + 1,
                [&](size_t, const VariantScalarRef& scalar) { canonical_hash(scalar, hash); });
        return;
    }
    DCHECK(_typed_type == nullptr);
    canonical_hash(get_value_ref(row), hash);
}

// NOLINTNEXTLINE(readability-non-const-parameter) -- IColumn override mutates caller seed array through helper.
void ColumnVariantV2::update_hashes_with_value(uint64_t* __restrict hashes,
                                               const uint8_t* __restrict null_data) const {
    if (_typed) {
        const auto& nullable = assert_cast<const ColumnNullable&>(*_typed);
        update_typed_hashes<VariantXxHashSink>(nullable, _typed_type->get_primitive_type(),
                                               _typed_type->get_scale(), hashes, null_data);
        return;
    }
    DCHECK(_typed_type == nullptr);
    update_canonical_hashes<VariantXxHashSink>(*this, hashes, null_data);
}

void ColumnVariantV2::update_xxHash_with_value(size_t start, size_t end, uint64_t& hash,
                                               const uint8_t* __restrict null_data) const {
    if (_typed) {
        check_hash_range(*this, start, end);
        const auto& nullable = assert_cast<const ColumnNullable&>(*_typed);
        update_typed_hash_range<VariantXxHashSink>(nullable, _typed_type->get_primitive_type(),
                                                   _typed_type->get_scale(), start, end, hash,
                                                   null_data);
        return;
    }
    DCHECK(_typed_type == nullptr);
    update_canonical_hash_range<VariantXxHashSink>(*this, start, end, hash, null_data);
}

// NOLINTNEXTLINE(readability-non-const-parameter) -- IColumn override mutates caller seed array through helper.
void ColumnVariantV2::update_crcs_with_value(uint32_t* __restrict hashes, PrimitiveType,
                                             uint32_t rows, uint32_t,
                                             const uint8_t* __restrict null_data) const {
    DCHECK_EQ(rows, size());
    if (_typed) {
        const auto& nullable = assert_cast<const ColumnNullable&>(*_typed);
        update_typed_hashes<VariantCrc32HashSink>(nullable, _typed_type->get_primitive_type(),
                                                  _typed_type->get_scale(), hashes, null_data);
        return;
    }
    DCHECK(_typed_type == nullptr);
    update_canonical_hashes<VariantCrc32HashSink>(*this, hashes, null_data);
}

void ColumnVariantV2::update_crc_with_value(size_t start, size_t end, uint32_t& hash,
                                            const uint8_t* __restrict null_data) const {
    if (_typed) {
        check_hash_range(*this, start, end);
        const auto& nullable = assert_cast<const ColumnNullable&>(*_typed);
        update_typed_hash_range<VariantCrc32HashSink>(nullable, _typed_type->get_primitive_type(),
                                                      _typed_type->get_scale(), start, end, hash,
                                                      null_data);
        return;
    }
    DCHECK(_typed_type == nullptr);
    update_canonical_hash_range<VariantCrc32HashSink>(*this, start, end, hash, null_data);
}

// NOLINTNEXTLINE(readability-non-const-parameter) -- IColumn override mutates caller seed array through helper.
void ColumnVariantV2::update_crc32c_batch(uint32_t* __restrict hashes,
                                          const uint8_t* __restrict null_map) const {
    if (_typed) {
        const auto& nullable = assert_cast<const ColumnNullable&>(*_typed);
        update_typed_hashes<VariantCrc32cHashSink>(nullable, _typed_type->get_primitive_type(),
                                                   _typed_type->get_scale(), hashes, null_map);
        return;
    }
    DCHECK(_typed_type == nullptr);
    update_canonical_hashes<VariantCrc32cHashSink>(*this, hashes, null_map);
}

void ColumnVariantV2::update_crc32c_single(size_t start, size_t end, uint32_t& hash,
                                           const uint8_t* __restrict null_map) const {
    if (_typed) {
        check_hash_range(*this, start, end);
        const auto& nullable = assert_cast<const ColumnNullable&>(*_typed);
        update_typed_hash_range<VariantCrc32cHashSink>(nullable, _typed_type->get_primitive_type(),
                                                       _typed_type->get_scale(), start, end, hash,
                                                       null_map);
        return;
    }
    DCHECK(_typed_type == nullptr);
    update_canonical_hash_range<VariantCrc32cHashSink>(*this, start, end, hash, null_map);
}

void ColumnVariantV2::replace_column_null_data(const uint8_t* __restrict null_map) {
    if (empty()) {
        return;
    }
    DORIS_CHECK(null_map != nullptr) << "ColumnVariantV2 null map must not be null";
    if (std::none_of(null_map, null_map + size(), [](uint8_t value) { return value != 0; })) {
        return;
    }
    if (_shredded) {
        ensure_encoded();
    }

    // Hash joins serialize the nested value even for a null-safe NULL key. Normalize those hidden
    // values to the canonical Variant default so build and probe keys compare byte-for-byte.
    auto encoded_source = ColumnVariantV2::create();
    encoded_source->insert_range_from(*this, 0, size());
    auto replacement = ColumnVariantV2::create();
    for (size_t begin = 0; begin < size();) {
        const bool is_null = null_map[begin] != 0;
        size_t end = begin + 1;
        while (end < size() && (null_map[end] != 0) == is_null) {
            ++end;
        }
        if (is_null) {
            replacement->insert_many_defaults(end - begin);
        } else {
            replacement->insert_range_from(*encoded_source, begin, end - begin);
        }
        begin = end;
    }
    _adopt_state_from(*replacement);
}

ColumnPtr ColumnVariantV2::filter(const Filter& filter, ssize_t result_size_hint) const {
    column_match_filter_size(size(), filter.size());
    if (_shredded) {
        return ColumnVariantV2::create_shredded(_shredded->filter(filter, result_size_hint));
    }
    if (_typed) {
        ColumnPtr filtered = _typed->filter(filter, result_size_hint);
        auto result = ColumnVariantV2::create();
        static_cast<IColumn::Ptr&>(result->_typed) = std::move(filtered);
        result->_typed_type = _typed_type;
        result->_check_invariants();
        return result;
    }
    ColumnPtr filtered_values =
            static_cast<const IColumn::Ptr&>(_values)->filter(filter, result_size_hint);
    ColumnPtr filtered_metadata_ids =
            static_cast<const IColumn::Ptr&>(_meta_ids)->filter(filter, result_size_hint);
    DORIS_CHECK_EQ(filtered_values->size(), filtered_metadata_ids->size())
            << "filtered encoded row counts differ";

    auto result = ColumnVariantV2::create();
    static_cast<IColumn::Ptr&>(result->_metadatas) = static_cast<const IColumn::Ptr&>(_metadatas);
    static_cast<IColumn::Ptr&>(result->_meta_ids) = std::move(filtered_metadata_ids);
    static_cast<IColumn::Ptr&>(result->_values) = std::move(filtered_values);
    result->_check_invariants();
    return result;
}

size_t ColumnVariantV2::filter(const Filter& filter) {
    column_match_filter_size(size(), filter.size());
    if (_shredded) {
        // Scanner-side compaction is a row-selection operation, not a request for canonical
        // Variant bytes. Keep partial Parquet projections in their physical representation.
        _shredded = _shredded->filter(filter, -1);
        _check_invariants();
        return size();
    }
    if (_typed) {
        ColumnPtr filtered = static_cast<const IColumn::Ptr&>(_typed)->filter(filter, -1);
        const size_t filtered_size = filtered->size();
        static_cast<IColumn::Ptr&>(_typed) = std::move(filtered);
        _check_invariants();
        return filtered_size;
    }
    require_exclusive(_meta_ids, "metadata ids");
    require_exclusive(_values, "values");
    const size_t value_rows = _values->filter(filter);
    const size_t metadata_id_rows = _meta_ids->filter(filter);
    DORIS_CHECK_EQ(value_rows, metadata_id_rows) << "filtered encoded row counts differ";
    _check_invariants();
    return value_rows;
}

MutableColumnPtr ColumnVariantV2::permute(const Permutation& permutation, size_t limit) const {
    const size_t result_size = limit == 0 ? size() : std::min(size(), limit);
    if (permutation.size() < result_size) {
        throw Exception(ErrorCode::INTERNAL_ERROR,
                        "Size of permutation ({}) is less than required ({})", permutation.size(),
                        result_size);
    }
    for (size_t row = 0; row < result_size; ++row) {
        if (permutation[row] >= size()) {
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "Permutation index {} at row {} exceeds column size {}",
                            permutation[row], row, size());
        }
    }

    if (_shredded) {
        DorisVector<uint32_t> selected_indices(result_size);
        for (size_t row = 0; row < result_size; ++row) {
            DORIS_CHECK_LE(permutation[row], std::numeric_limits<uint32_t>::max())
                    << "shredded Variant permutation index exceeds uint32 domain";
            selected_indices[row] = static_cast<uint32_t>(permutation[row]);
        }
        // Local TopN selection may run before exchange and projected states cannot reconstruct
        // omitted root fields, so preserve the native shredded representation while gathering.
        return ColumnVariantV2::create_shredded(_shredded->select_indices(
                selected_indices.data(), selected_indices.data() + selected_indices.size()));
    }

    if (_typed) {
        MutableColumnPtr permuted = _typed->permute(permutation, result_size);
        auto result = ColumnVariantV2::create();
        static_cast<IColumn::Ptr&>(result->_typed) = std::move(permuted);
        result->_typed_type = _typed_type;
        result->_check_invariants();
        return result;
    }

    MutableColumnPtr permuted_values =
            static_cast<const IColumn::Ptr&>(_values)->permute(permutation, result_size);
    MutableColumnPtr permuted_metadata_ids =
            static_cast<const IColumn::Ptr&>(_meta_ids)->permute(permutation, result_size);
    DORIS_CHECK_EQ(permuted_values->size(), permuted_metadata_ids->size())
            << "permuted encoded row counts differ";

    auto result = ColumnVariantV2::create();
    static_cast<IColumn::Ptr&>(result->_metadatas) = static_cast<const IColumn::Ptr&>(_metadatas);
    static_cast<IColumn::Ptr&>(result->_meta_ids) = std::move(permuted_metadata_ids);
    static_cast<IColumn::Ptr&>(result->_values) = std::move(permuted_values);
    result->_check_invariants();
    return result;
}

MutableColumnPtr ColumnVariantV2::clone_resized(size_t new_size) const {
    if (_shredded) {
        if (new_size == 0) {
            // Empty scanner placeholders carry no rows and therefore need no physical shredded
            // state. Avoid forcing a partial leaf projection through full materialization.
            return ColumnVariantV2::create();
        }
        if (new_size == size()) {
            auto result = ColumnVariantV2::create();
            result->_shredded = _shredded;
            result->_check_invariants();
            return result;
        }
        if (new_size < size()) {
            // LIMIT truncation is a row selection and must not require complete Variant roots from
            // a projected scanner state.
            return ColumnVariantV2::create_shredded(_shredded->select_range(0, new_size));
        }
        return _shredded->materialized_column().clone_resized(new_size);
    }
    if (_typed) {
        auto result = ColumnVariantV2::create();
        if (new_size <= size()) {
            static_cast<IColumn::Ptr&>(result->_typed) = _typed->clone_resized(new_size);
            result->_typed_type = _typed_type;
            result->_check_invariants();
            return result;
        }

        static_cast<IColumn::Ptr&>(result->_typed) = _typed->clone_resized(size());
        result->_typed_type = _typed_type;
        result->ensure_encoded();
        result->insert_many_defaults(new_size - size());
        result->_check_invariants();
        return result;
    }
    if (new_size == 0) {
        return ColumnVariantV2::create();
    }

    const size_t copied_rows = std::min(size(), new_size);
    MutableColumnPtr copied_values =
            static_cast<const IColumn::Ptr&>(_values)->clone_resized(copied_rows);
    MutableColumnPtr copied_metadata_ids =
            static_cast<const IColumn::Ptr&>(_meta_ids)->clone_resized(copied_rows);
    auto result = ColumnVariantV2::create();
    static_cast<IColumn::Ptr&>(result->_metadatas) = static_cast<const IColumn::Ptr&>(_metadatas);
    static_cast<IColumn::Ptr&>(result->_meta_ids) = std::move(copied_metadata_ids);
    static_cast<IColumn::Ptr&>(result->_values) = std::move(copied_values);
    if (new_size > copied_rows) {
        result->insert_many_defaults(new_size - copied_rows);
    }
    result->_check_invariants();
    return result;
}

void ColumnVariantV2::resize(size_t new_size) {
    const size_t old_size = size();
    if (_shredded && new_size < old_size) {
        // LIMIT truncation only selects existing rows, so keep it physical: projected states may
        // omit roots that cannot be reconstructed merely to reduce the row count.
        if (new_size == 0) {
            _shredded.reset();
        } else {
            _shredded = _shredded->select_range(0, new_size);
        }
        _check_invariants();
        return;
    }
    if (_shredded && new_size > old_size) {
        ensure_encoded();
    }
    if (_typed) {
        if (new_size == old_size) {
            return;
        }
        if (new_size < old_size) {
            mutate_subcolumn(_typed);
            _typed->pop_back(old_size - new_size);
            _check_invariants();
            return;
        }
        ensure_encoded();
        insert_many_defaults(new_size - old_size);
        return;
    }
    if (new_size < old_size) {
        pop_back(old_size - new_size);
    } else if (new_size > old_size) {
        insert_many_defaults(new_size - old_size);
    }
}

void ColumnVariantV2::get_permutation(bool, size_t, int, HybridSorter&, Permutation&) const {
    throw_unsupported("get_permutation");
}

void ColumnVariantV2::replace_column_data(const IColumn&, size_t, size_t) {
    throw_unsupported("replace_column_data");
}

uint32_t ColumnVariantV2::_find_or_insert_metadata(StringRef metadata) {
    DORIS_CHECK(metadata.data != nullptr || metadata.size == 0)
            << "metadata bytes have a null pointer";
    const auto& current_metadatas =
            assert_cast<const ColumnString&>(*static_cast<const IColumn::Ptr&>(_metadatas));
    for (uint32_t id = 0; id < current_metadatas.size(); ++id) {
        if (current_metadatas.get_data_at(id) == metadata) {
            return id;
        }
    }

    return _append_metadata(metadata);
}

uint32_t ColumnVariantV2::_append_metadata(StringRef metadata) {
    DORIS_CHECK(metadata.data != nullptr || metadata.size == 0)
            << "metadata bytes have a null pointer";
    const auto& current_metadatas =
            assert_cast<const ColumnString&>(*static_cast<const IColumn::Ptr&>(_metadatas));
    if (current_metadatas.size() == std::numeric_limits<uint32_t>::max()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "ColumnVariantV2 metadata dictionary exceeds the uint32 id limit");
    }

    _detach_metadata_for_write();
    auto& metadatas = assert_cast<ColumnString&>(*_metadatas);
    const size_t new_chars_size = metadatas.get_chars().size() + metadata.size;
    ColumnString::check_chars_length(new_chars_size, metadatas.size() + 1, _meta_ids->size());
    metadatas.get_chars().reserve(new_chars_size);
    metadatas.get_offsets().reserve(metadatas.size() + 1);
    const auto id = static_cast<uint32_t>(metadatas.size());
    metadatas.insert_data(metadata.data, metadata.size);
    return id;
}

void ColumnVariantV2::_replace_shredded_state_with(const ColumnVariantV2& replacement) {
    DORIS_CHECK(_shredded != nullptr) << "replacing shredded state requires a shredded destination";
    DORIS_CHECK(!replacement.is_shredded())
            << "shredded state replacement must be a non-shredded column";
    DORIS_CHECK_EQ(replacement.size(), size())
            << "shredded state replacement changed the row count";
    // Format states cache and share materialized columns. Detach every mutable buffer before
    // dropping the state owner so later COW mutations cannot modify a cached representation.
    _metadatas = replacement._metadatas->clone_resized(replacement._metadatas->size());
    _meta_ids = replacement._meta_ids->clone_resized(replacement._meta_ids->size());
    _values = replacement._values->clone_resized(replacement._values->size());
    _typed = replacement._typed == nullptr
                     ? nullptr
                     : replacement._typed->clone_resized(replacement._typed->size());
    _typed_type = replacement._typed_type;
    _shredded.reset();
    _check_invariants();
}

void ColumnVariantV2::_adopt_state_from(ColumnVariantV2& replacement) {
    DORIS_CHECK(this != &replacement) << "cannot adopt ColumnVariantV2 state from itself";
    _metadatas = std::move(replacement._metadatas);
    _meta_ids = std::move(replacement._meta_ids);
    _values = std::move(replacement._values);
    _typed = std::move(replacement._typed);
    _typed_type = std::move(replacement._typed_type);
    _shredded = std::move(replacement._shredded);
    _check_invariants();
}

void ColumnVariantV2::_detach_metadata_for_write() {
    auto& metadata_ptr = static_cast<IColumn::Ptr&>(_metadatas);
    if (!metadata_ptr->is_exclusive()) {
        metadata_ptr = std::move(*metadata_ptr).mutate();
    }
}

void ColumnVariantV2::_check_invariants() const {
    if (_shredded) {
        DORIS_CHECK(_typed == nullptr) << "shredded state cannot contain a typed column";
        DORIS_CHECK(_typed_type == nullptr) << "shredded state cannot retain a typed data type";
        DORIS_CHECK(_metadatas->empty()) << "shredded state cannot contain encoded metadata";
        DORIS_CHECK(_meta_ids->empty()) << "shredded state cannot contain encoded metadata ids";
        DORIS_CHECK(_values->empty()) << "shredded state cannot contain encoded values";
        return;
    }
    if (_typed) {
        DORIS_CHECK(_typed_type != nullptr) << "typed state requires a data type";
        const IColumn* typed_column = static_cast<const IColumn::Ptr&>(_typed).get();
        DORIS_CHECK(typeid(*typed_column) == typeid(ColumnNullable))
                << "typed state requires an exact ColumnNullable";
        const auto& nullable = assert_cast<const ColumnNullable&>(*_typed);
        DORIS_CHECK_EQ(nullable.get_nested_column().size(), nullable.get_null_map_column().size())
                << "typed state null map size does not match nested column size";
        DORIS_CHECK(_metadatas->empty()) << "typed state cannot contain encoded metadata";
        DORIS_CHECK(_meta_ids->empty()) << "typed state cannot contain encoded metadata ids";
        DORIS_CHECK(_values->empty()) << "typed state cannot contain encoded values";
        return;
    }

    DORIS_CHECK(_typed_type == nullptr) << "encoded state cannot retain a typed data type";
    const auto& metadata_ids = assert_cast<const MetaIdsColumn&>(*_meta_ids).get_data();
    const auto& values = assert_cast<const ColumnString&>(*_values);
    DORIS_CHECK_EQ(metadata_ids.size(), values.size())
            << "ColumnVariantV2 encoded row counts differ";
}

} // namespace doris
