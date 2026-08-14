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
#include <optional>
#include <ranges>
#include <string_view>
#include <type_traits>
#include <typeinfo>
#include <utility>

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
#include "core/column/variant_v2/variant_shredded_path.h"
#include "core/custom_allocator.h"
#include "core/data_type/data_type_string.h"
#include "core/memcmp_small.h"
#include "core/value/variant/variant_batch_builder.h"
#include "core/value/variant/variant_canonical.h"
#include "core/value/variant/variant_field.h"
#include "core/value/variant/variant_parquet_encoding.h"
#include "util/utf8_check.h"

namespace doris {
namespace {

using MetaIdsColumn = ColumnVariantV2::MetadataIdsColumn;
constexpr uint32_t UNMAPPED_METADATA_ID = std::numeric_limits<uint32_t>::max();
constexpr std::array<char, 3> EMPTY_OBJECT_VALUE {static_cast<char>(0x02), 0, 0};

PathInData normalize_shredded_path(const PathInData& path) {
    const auto& parts = path.get_parts();
    DORIS_CHECK(!parts.empty()) << "ColumnVariantV2 shredded field path cannot be the root";
    DORIS_CHECK_LE(parts.size(), VARIANT_MAX_NESTING_DEPTH)
            << "ColumnVariantV2 shredded field path exceeds maximum nesting depth "
            << VARIANT_MAX_NESTING_DEPTH;
    for (const auto& part : parts) {
        DORIS_CHECK(!part.is_nested && part.anonymous_array_level == 0)
                << "ColumnVariantV2 shredded fields do not support array paths";
        DORIS_CHECK(part.key.empty() || validate_utf8(part.key.data(), part.key.size()))
                << "ColumnVariantV2 shredded field path key contains invalid UTF-8";
    }
    return PathInData(parts);
}

void validate_shredded_residual_disjoint(const ColumnVariantV2& residual,
                                         const ColumnVariantV2::ShreddedField& field, size_t row) {
    VariantRef current = residual.get_value_ref(row);
    const auto& parts = field.path.get_parts();
    for (size_t depth = 0; depth < parts.size(); ++depth) {
        DORIS_CHECK(current.basic_type() == VariantBasicType::OBJECT)
                << "ColumnVariantV2 residual has a scalar or array ancestor of shredded path "
                << field.path.get_path() << " at row " << row;
        VariantRef child;
        const auto& key = parts[depth].key;
        if (!current.object_find({key.data(), key.size()}, &child)) {
            return;
        }
        DORIS_CHECK(depth + 1 != parts.size())
                << "ColumnVariantV2 residual overlaps a present shredded field at path "
                << field.path.get_path() << " at row " << row;
        current = child;
    }
}

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

template <typename WrappedPtr>
void require_exclusive(const WrappedPtr& column, std::string_view description) {
    DORIS_CHECK(column->is_exclusive())
            << "ColumnVariantV2 " << description << " must be COW-detached before mutation";
}

template <typename ColumnType, typename ColumnPtrType>
typename ColumnType::Ptr cast_column_ptr(ColumnPtrType column) {
    return ColumnType::cast_to_column_ptr(assert_cast<const ColumnType*>(column.get()));
}

template <typename ColumnType>
typename ColumnType::MutablePtr cast_mutable_column(MutableColumnPtr column) {
    auto result = ColumnType::cast_to_column_mutptr(assert_cast<ColumnType*>(column.get()));
    column = nullptr;
    return result;
}

void reserve_rows(ColumnString& values, MetaIdsColumn& metadata_ids, size_t value_bytes,
                  size_t rows) {
    const size_t final_value_bytes = values.get_chars().size() + value_bytes;
    ColumnString::check_chars_length(final_value_bytes, values.size() + rows, values.size());
    values.get_chars().reserve(final_value_bytes);
    values.get_offsets().reserve(values.size() + rows);
    metadata_ids.get_data().reserve(metadata_ids.size() + rows);
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

template <typename Callback>
void visit_typed_scalar_column(const ColumnNullable& nullable, PrimitiveType type, uint32_t scale,
                               size_t start, size_t end, Callback&& callback);

using ActiveShreddedFields = DorisVector<const ColumnVariantV2::ShreddedField*>;

struct ResidualObjectEntry {
    StringRef key;
    VariantRef value;
};

void append_variant_column_row(VariantBatchBuilder::Row& output, const ColumnVariantV2& column,
                               size_t row) {
    DORIS_CHECK(!column.is_shredded()) << "nested shredded ColumnVariantV2 is not supported";
    if (column.is_encoded()) {
        output.add_value(column.get_value_ref(row));
        return;
    }

    const auto& nullable = assert_cast<const ColumnNullable&>(column.typed_column());
    visit_typed_scalar_column(
            nullable, column.typed_type()->get_primitive_type(), column.typed_type()->get_scale(),
            row, row + 1,
            [&](size_t, const VariantScalarRef& scalar) { output.add_scalar(scalar); });
}

void append_merged_shredded_node(VariantBatchBuilder::Row& output,
                                 std::optional<VariantRef> residual,
                                 const ActiveShreddedFields& fields, size_t begin, size_t end,
                                 size_t depth, size_t row) {
    DORIS_CHECK_LT(begin, end);
    const auto& first_parts = fields[begin]->path.get_parts();
    if (first_parts.size() == depth) {
        DORIS_CHECK_EQ(end - begin, 1) << "ColumnVariantV2 shredded paths must be prefix-free";
        DORIS_CHECK(!residual.has_value())
                << "ColumnVariantV2 residual overlaps a present shredded field at path "
                << fields[begin]->path.get_path();
        append_variant_column_row(output,
                                  assert_cast<const ColumnVariantV2&>(*fields[begin]->values), row);
        return;
    }

    if (residual.has_value()) {
        DORIS_CHECK(residual->basic_type() == VariantBasicType::OBJECT)
                << "ColumnVariantV2 residual has a scalar or array ancestor of shredded path "
                << fields[begin]->path.get_path();
    }

    DorisVector<ResidualObjectEntry> residual_entries;
    if (residual.has_value()) {
        const uint32_t count = residual->num_elements();
        residual_entries.reserve(count);
        for (uint32_t index = 0; index < count; ++index) {
            uint32_t field_id = 0;
            const VariantRef value = residual->object_value_at(index, &field_id);
            residual_entries.push_back(
                    {.key = residual->metadata.key_at(field_id), .value = value});
        }
        std::ranges::sort(residual_entries, [](const auto& left, const auto& right) {
            return left.key.compare(right.key) < 0;
        });
    }

    auto object = output.start_object();
    size_t residual_index = 0;
    size_t field_index = begin;
    while (residual_index < residual_entries.size() || field_index < end) {
        size_t field_group_end = field_index;
        StringRef field_key;
        if (field_index < end) {
            const auto& part = fields[field_index]->path.get_parts()[depth];
            field_key = {part.key.data(), part.key.size()};
            field_group_end = field_index + 1;
            while (field_group_end < end &&
                   fields[field_group_end]->path.get_parts()[depth].key == part.key) {
                ++field_group_end;
            }
        }

        const bool has_residual = residual_index < residual_entries.size();
        int comparison;
        if (!has_residual) {
            comparison = 1;
        } else if (field_index == end) {
            comparison = -1;
        } else {
            comparison = residual_entries[residual_index].key.compare(field_key);
        }
        if (comparison < 0) {
            object.add_key(residual_entries[residual_index].key);
            output.add_value(residual_entries[residual_index].value);
            ++residual_index;
            continue;
        }

        object.add_key(field_key);
        if (comparison == 0) {
            append_merged_shredded_node(output, residual_entries[residual_index].value, fields,
                                        field_index, field_group_end, depth + 1, row);
            ++residual_index;
        } else {
            append_merged_shredded_node(output, std::nullopt, fields, field_index, field_group_end,
                                        depth + 1, row);
        }
        field_index = field_group_end;
    }
    object.finish();
}

struct ShreddedRangeSelection {
    size_t start;
    size_t length;

    size_t size() const noexcept { return length; }
    size_t source_row(size_t row) const noexcept { return start + row; }

    void insert_from(IColumn& destination, const IColumn& source) const {
        destination.insert_range_from(source, start, length);
    }

    size_t selected_string_bytes(const ColumnString& source) const {
        DCHECK_GT(length, 0);
        const auto& offsets = source.get_offsets();
        return offsets[start + length - 1] - offsets[static_cast<ssize_t>(start) - 1];
    }
};

struct ShreddedIndicesSelection {
    const uint32_t* begin;
    const uint32_t* end;

    size_t size() const noexcept { return end - begin; }
    size_t source_row(size_t row) const noexcept { return begin[row]; }

    void insert_from(IColumn& destination, const IColumn& source) const {
        destination.insert_indices_from(source, begin, end);
    }

    size_t selected_string_bytes(const ColumnString& source) const {
        size_t bytes = 0;
        for (const uint32_t* index = begin; index != end; ++index) {
            const size_t value_bytes = source.get_data_at(*index).size;
            if (UNLIKELY(value_bytes > std::numeric_limits<size_t>::max() - bytes)) {
                throw Exception(ErrorCode::INVALID_ARGUMENT,
                                "selected typed string bytes overflow size_t");
            }
            bytes += value_bytes;
        }
        return bytes;
    }
};

const ColumnVariantV2::ShreddedField* find_shredded_field(
        const ColumnVariantV2::ShreddedFields& fields, const PathInData& path) {
    const auto candidate = std::ranges::lower_bound(
            fields, path,
            [](const PathInData& left, const PathInData& right) {
                return variant_shredded_path_less(left, right);
            },
            &ColumnVariantV2::ShreddedField::path);
    if (candidate == fields.end() || candidate->path.get_parts() != path.get_parts()) {
        return nullptr;
    }
    return &*candidate;
}

bool conflicts_with_shredded_layout(const ColumnVariantV2::ShreddedFields& fields,
                                    const PathInData& path) {
    return std::ranges::any_of(fields, [&path](const auto& field) {
        if (field.path.get_parts() == path.get_parts()) {
            return false;
        }
        return variant_shredded_path_is_prefix(field.path, path) ||
               variant_shredded_path_is_prefix(path, field.path);
    });
}

struct ShreddedUnionPlan {
    DorisVector<uint8_t> source_active;
    DorisVector<const ColumnVariantV2::ShreddedField*> source_only;
    ActiveShreddedFields source_conflicts;
};

template <typename Selection>
bool has_selected_presence(const ColumnUInt8& presence, const Selection& selection) {
    static_assert(std::is_same_v<Selection, ShreddedIndicesSelection>);
    DCHECK(selection.begin != nullptr);
    DCHECK(selection.end != nullptr);
    DCHECK_LE(selection.begin, selection.end);
    const auto& data = presence.get_data();
    for (size_t output_row = 0; output_row < selection.size(); ++output_row) {
        if (data[selection.source_row(output_row)] != 0) {
            return true;
        }
    }
    return false;
}

bool has_selected_presence(const ColumnUInt8& presence, const ShreddedRangeSelection& selection) {
    const auto& data = presence.get_data();
    DCHECK_LE(selection.start, data.size());
    DCHECK_LE(selection.length, data.size() - selection.start);
    // ColumnUInt8 uses PaddedPODArray, satisfying the helper's 15-byte overread contract.
    return !memory_is_zero_small_allow_overflow15(data.data() + selection.start, selection.length);
}

const ColumnVariantV2::ShreddedField* find_selected_shredded_field(
        const ColumnVariantV2::ShreddedFields& source, const DorisVector<uint8_t>& source_active,
        const PathInData& path) {
    DCHECK_EQ(source.size(), source_active.size());
    const auto* field = find_shredded_field(source, path);
    if (field == nullptr) {
        return nullptr;
    }
    const size_t index = field - source.data();
    DCHECK_LT(index, source_active.size());
    return source_active[index] != 0 ? field : nullptr;
}

bool shredded_children_append_compatible(const ColumnVariantV2::ShreddedField& destination,
                                         const ColumnVariantV2::ShreddedField& source) {
    const auto& destination_values = assert_cast<const ColumnVariantV2&>(*destination.values);
    const auto& source_values = assert_cast<const ColumnVariantV2&>(*source.values);
    if (destination_values.is_typed() || source_values.is_typed()) {
        return destination_values.is_typed() && source_values.is_typed() &&
               exact_typed_identity(destination_values.typed_type(), source_values.typed_type());
    }
    DCHECK(destination_values.is_encoded());
    DCHECK(source_values.is_encoded());
    return true;
}

template <typename Selection>
ShreddedUnionPlan build_shredded_union_plan(const ColumnVariantV2::ShreddedFields& destination,
                                            const ColumnVariantV2::ShreddedFields& source,
                                            size_t source_only_budget, const Selection& selection) {
    ShreddedUnionPlan plan;
    plan.source_active.resize(source.size());
    plan.source_only.reserve(source.size());
    plan.source_conflicts.reserve(source.size());
    for (size_t index = 0; index < source.size(); ++index) {
        const auto& source_field = source[index];
        const auto* destination_field = find_shredded_field(destination, source_field.path);
        // Compatible exact-path children stay on their existing bulk append path. Their presence
        // is copied unchanged, so scanning it cannot improve representation or copy behavior.
        if (destination_field != nullptr &&
            shredded_children_append_compatible(*destination_field, source_field)) {
            plan.source_active[index] = 1;
            continue;
        }
        const bool active = has_selected_presence(*source_field.presence, selection);
        if (destination_field != nullptr) {
            const auto& destination_values =
                    assert_cast<const ColumnVariantV2&>(*destination_field->values);
            if (active && destination_values.is_typed()) {
                // An incompatible exact-path child must not change a typed destination child's
                // physical type. Only selected present values are folded into the encoded
                // residual; missing source rows append a missing value without entering the slow
                // lane.
                plan.source_conflicts.push_back(&source_field);
            } else {
                // An encoded destination child can consume a selected typed child without a
                // representation change. Keep that existing mapped path instead of forcing its
                // rows through the root residual.
                plan.source_active[index] = active;
            }
            continue;
        }
        plan.source_active[index] = active;
        if (!active) {
            continue;
        }
        if (plan.source_only.size() >= source_only_budget ||
            conflicts_with_shredded_layout(destination, source_field.path)) {
            plan.source_conflicts.push_back(&source_field);
        } else {
            plan.source_only.push_back(&source_field);
        }
    }
    return plan;
}

MutableColumnPtr copy_variant_child_with_missing_suffix(const ColumnVariantV2& source,
                                                        size_t missing_rows) {
    if (source.is_typed()) {
        MutableColumnPtr physical = source.typed_column().clone_empty();
        physical->insert_range_from(source.typed_column(), 0, source.size());
        physical->insert_many_defaults(missing_rows);
        return ColumnVariantV2::create_typed(std::move(physical), source.typed_type());
    }
    MutableColumnPtr output = source.clone_empty();
    output->insert_range_from(source, 0, source.size());
    output->insert_many_defaults(missing_rows);
    return output;
}

template <typename Selection>
MutableColumnPtr copy_variant_child_with_missing_prefix(const ColumnVariantV2& source,
                                                        size_t missing_rows,
                                                        const Selection& selection) {
    if (source.is_typed()) {
        MutableColumnPtr physical = source.typed_column().clone_empty();
        physical->insert_many_defaults(missing_rows);
        selection.insert_from(*physical, source.typed_column());
        return ColumnVariantV2::create_typed(std::move(physical), source.typed_type());
    }
    MutableColumnPtr output = source.clone_empty();
    output->insert_many_defaults(missing_rows);
    selection.insert_from(*output, source);
    return output;
}

template <typename Selection>
MutableColumnPtr copy_and_append_variant_child(const ColumnVariantV2& destination,
                                               const ColumnVariantV2& source,
                                               const Selection& selection) {
    MutableColumnPtr output = destination.clone_empty();
    output->insert_range_from(destination, 0, destination.size());
    selection.insert_from(*output, source);
    return output;
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

} // namespace

template <typename Selection>
void ColumnVariantV2::_append_shredded_conflicts_to_residual(
        ColumnVariantV2& residual, const ColumnVariantV2& source,
        const DorisVector<const ShreddedField*>& source_conflicts, const Selection& selection) {
    DORIS_CHECK(residual.is_encoded());
    DorisVector<uint8_t> conflict_rows(selection.size(), 0);
    for (size_t output_row = 0; output_row < selection.size(); ++output_row) {
        const size_t source_row = selection.source_row(output_row);
        for (const auto* field : source_conflicts) {
            if (field->presence->get_data()[source_row] != 0) {
                conflict_rows[output_row] = 1;
                break;
            }
        }
    }

    const auto append_direct_run = [&](size_t run_begin, size_t run_end) {
        if constexpr (std::is_same_v<Selection, ShreddedRangeSelection>) {
            residual._append_encoded_range(source, selection.start + run_begin,
                                           run_end - run_begin);
        } else {
            static_assert(std::is_same_v<Selection, ShreddedIndicesSelection>);
            residual._append_encoded_indices(source, selection.begin + run_begin,
                                             selection.begin + run_end);
        }
    };

#ifdef BE_TEST
    size_t slow_rows = 0;
#endif
    ActiveShreddedFields active_fields;
    active_fields.reserve(source_conflicts.size());
    const auto view = source.read_view();
    for (size_t run_begin = 0; run_begin < selection.size();) {
        const bool has_conflict = conflict_rows[run_begin] != 0;
        size_t run_end = run_begin + 1;
        while (run_end < selection.size() && (conflict_rows[run_end] != 0) == has_conflict) {
            ++run_end;
        }
        if (!has_conflict) {
            append_direct_run(run_begin, run_end);
            run_begin = run_end;
            continue;
        }

        VariantBatchBuilder builder({.rows = run_end - run_begin});
        for (size_t output_row = run_begin; output_row < run_end; ++output_row) {
            const size_t source_row = selection.source_row(output_row);
            active_fields.clear();
            for (const auto* field : source_conflicts) {
                if (field->presence->get_data()[source_row] != 0) {
                    active_fields.push_back(field);
                }
            }
            DORIS_CHECK(!active_fields.empty());
            auto output = builder.begin_row();
            append_merged_shredded_node(output, view.residual_value_at(source_row), active_fields,
                                        0, active_fields.size(), 0, source_row);
            output.finish();
        }
        VariantBatchBuilder encoded = builder.finish_batch();
        residual.insert_encoded_batch(encoded);
#ifdef BE_TEST
        slow_rows += run_end - run_begin;
#endif
        run_begin = run_end;
    }
#ifdef BE_TEST
    _test_shredded_conflict_slow_rows += slow_rows;
#endif
}

template <typename Selection>
void ColumnVariantV2::_append_same_shredded_layout_rows(const ColumnVariantV2& source,
                                                        const Selection& selection) {
    DCHECK(is_shredded());
    DCHECK(source.is_shredded());
    DCHECK_EQ(_shredded_fields.size(), source._shredded_fields.size());
    if constexpr (std::is_same_v<Selection, ShreddedRangeSelection>) {
        _append_encoded_range(source, selection.start, selection.size());
    } else {
        static_assert(std::is_same_v<Selection, ShreddedIndicesSelection>);
        _append_encoded_indices(source, selection.begin, selection.end);
    }
    for (size_t index = 0; index < _shredded_fields.size(); ++index) {
        auto& destination_field = _shredded_fields[index];
        const auto& source_field = source._shredded_fields[index];
        const auto& destination_values =
                assert_cast<const ColumnVariantV2&>(*destination_field.values);
        if (destination_values.is_typed()) {
            DCHECK(shredded_children_append_compatible(destination_field, source_field));
        } else {
            const auto& source_values = assert_cast<const ColumnVariantV2&>(*source_field.values);
            if (source_values.is_typed() &&
                !has_selected_presence(*source_field.presence, selection)) {
                _append_missing_shredded_field(destination_field, selection.size());
                continue;
            }
        }
        mutate_subcolumn(destination_field.values);
        mutate_subcolumn<ColumnUInt8>(destination_field.presence);
        selection.insert_from(*destination_field.values, *source_field.values);
        selection.insert_from(*destination_field.presence, *source_field.presence);
    }
    _shredded_layout_frozen = _shredded_layout_frozen || source._shredded_layout_frozen;
    _check_invariants();
}

template <typename Selection>
void ColumnVariantV2::_append_shredded_mapped_rows(const ColumnVariantV2& source,
                                                   const DorisVector<uint8_t>& source_active,
                                                   const ColumnVariantV2& residual,
                                                   const Selection& selection) {
    _append_encoded_range(residual, 0, selection.size());
    for (auto& destination_field : _shredded_fields) {
        const auto* source_field = find_selected_shredded_field(
                source._shredded_fields, source_active, destination_field.path);
        if (source_field == nullptr) {
            _append_missing_shredded_field(destination_field, selection.size());
            continue;
        }
        mutate_subcolumn(destination_field.values);
        mutate_subcolumn<ColumnUInt8>(destination_field.presence);
        selection.insert_from(*destination_field.values, *source_field->values);
        selection.insert_from(*destination_field.presence, *source_field->presence);
    }
    _shredded_layout_frozen = _shredded_layout_frozen || source._shredded_layout_frozen;
    _check_invariants();
}

template <typename Selection>
// The ownership transaction intentionally keeps preflight, publication, and rollback in one scope
// so raw field pointers cannot escape across helper boundaries.
// NOLINTNEXTLINE(readability-function-size,readability-function-cognitive-complexity)
void ColumnVariantV2::_replace_with_shredded_union(
        MutablePtr residual, const ColumnVariantV2& source,
        const DorisVector<uint8_t>& source_active,
        const DorisVector<const ShreddedField*>& source_only, const Selection& selection) {
    DORIS_CHECK(is_shredded());
    DORIS_CHECK(static_cast<bool>(residual) && residual->is_encoded());
    DORIS_CHECK(!source_only.empty());
    const size_t destination_rows = size();
    const size_t output_rows = destination_rows + selection.size();

    // Build every allocating replacement before borrowing exclusive destination owners. A failed
    // source-only/rebuilt child leaves the destination untouched.
    auto candidate = ColumnVariantV2::create();
    struct DestinationFieldPlan {
        ShreddedField* original;
        const ShreddedField* source;
        bool reuse_values;
        bool reuse_presence;
        ShreddedField* replacement = nullptr;
        size_t nested_rows = 0;
        size_t null_rows = 0;
        size_t presence_rows = 0;
    };
    DorisVector<DestinationFieldPlan> destination_plans;
    destination_plans.reserve(_shredded_fields.size());
    for (auto& destination_field : _shredded_fields) {
        const auto& destination_values_owner =
                static_cast<const IColumn::Ptr&>(destination_field.values);
        const auto& destination_presence_owner =
                static_cast<const ColumnUInt8::Ptr&>(destination_field.presence);
        const auto& destination_values =
                assert_cast<const ColumnVariantV2&>(*destination_values_owner);
        const auto* source_field = find_selected_shredded_field(
                source._shredded_fields, source_active, destination_field.path);
        bool exact_typed_source = false;
        if (destination_values.is_typed()) {
            exact_typed_source = source_field == nullptr || [&] {
                const auto& source_values =
                        assert_cast<const ColumnVariantV2&>(*source_field->values);
                return source_values.is_typed() &&
                       exact_typed_identity(destination_values.typed_type(),
                                            source_values.typed_type());
            }();
        }
        destination_plans.push_back(
                {.original = &destination_field,
                 .source = source_field,
                 .reuse_values = exact_typed_source && destination_values_owner->is_exclusive(),
                 .reuse_presence = destination_presence_owner->is_exclusive()});
    }

    // Reserve before publishing a second owner for reused storage. Fixed-width typed columns only
    // need row capacity. String appends also require an exact chars reservation because
    // ColumnString grows offsets before its overflow check.
    for (auto& field_plan : destination_plans) {
        auto& destination_field = *field_plan.original;
        if (field_plan.reuse_values) {
            auto& destination_values = assert_cast<ColumnVariantV2&>(*destination_field.values);
            auto& destination_nullable = assert_cast<ColumnNullable&>(*destination_values._typed);
            if (is_string_type(destination_values.typed_type()->get_primitive_type())) {
                auto& destination_strings =
                        assert_cast<ColumnString&>(destination_nullable.get_nested_column());
                size_t output_bytes = destination_strings.get_chars().size();
                if (field_plan.source != nullptr) {
                    const auto& source_values =
                            assert_cast<const ColumnVariantV2&>(*field_plan.source->values);
                    const auto& source_nullable =
                            assert_cast<const ColumnNullable&>(source_values.typed_column());
                    const auto& source_strings =
                            assert_cast<const ColumnString&>(source_nullable.get_nested_column());
                    const size_t selected_bytes = selection.selected_string_bytes(source_strings);
                    if (UNLIKELY(selected_bytes >
                                 std::numeric_limits<size_t>::max() - output_bytes)) {
                        throw Exception(ErrorCode::INVALID_ARGUMENT,
                                        "typed string union bytes overflow size_t");
                    }
                    output_bytes += selected_bytes;
                    ColumnString::check_chars_length(output_bytes, output_rows, destination_rows);
                }
                destination_strings.get_offsets().reserve(output_rows);
                destination_nullable.get_null_map_data().reserve(output_rows);
                if (field_plan.source != nullptr) {
                    destination_strings.get_chars().reserve(output_bytes);
                }
            } else {
                destination_nullable.reserve(output_rows);
            }
        }
        if (field_plan.reuse_presence) {
            destination_field.presence->reserve(output_rows);
        }
    }

#ifdef BE_TEST
    size_t existing_child_rows_copied = 0;
    size_t existing_presence_rows_copied = 0;
#endif
    ShreddedFields fields;
    fields.reserve(_shredded_fields.size() + source_only.size());
    for (const auto& field_plan : destination_plans) {
        const auto& destination_field = *field_plan.original;
        const auto& destination_values_owner =
                static_cast<const IColumn::Ptr&>(destination_field.values);
        const auto& destination_presence_owner =
                static_cast<const ColumnUInt8::Ptr&>(destination_field.presence);
        const auto& destination_values =
                assert_cast<const ColumnVariantV2&>(*destination_values_owner);

        ColumnPtr values_owner;
        if (field_plan.reuse_values) {
            values_owner = destination_values_owner;
        } else {
            MutableColumnPtr values;
            if (field_plan.source == nullptr) {
                values = copy_variant_child_with_missing_suffix(destination_values,
                                                                selection.size());
            } else {
                const auto& source_values =
                        assert_cast<const ColumnVariantV2&>(*field_plan.source->values);
                values =
                        copy_and_append_variant_child(destination_values, source_values, selection);
            }
            values_owner = std::move(values);
#ifdef BE_TEST
            existing_child_rows_copied += destination_rows;
#endif
        }

        ColumnUInt8::Ptr presence_owner;
        if (field_plan.reuse_presence) {
            presence_owner = destination_presence_owner;
        } else {
            auto presence = ColumnUInt8::create();
            presence->insert_range_from(*destination_presence_owner, 0, destination_rows);
            if (field_plan.source == nullptr) {
                presence->insert_many_defaults(selection.size());
            } else {
                selection.insert_from(*presence, *field_plan.source->presence);
            }
            presence_owner = std::move(presence);
#ifdef BE_TEST
            existing_presence_rows_copied += destination_rows;
#endif
        }
        fields.push_back(ShreddedField::share(destination_field.path, std::move(values_owner),
                                              std::move(presence_owner)));
    }
    for (const auto* source_field : source_only) {
        const auto& source_values = assert_cast<const ColumnVariantV2&>(*source_field->values);
        MutableColumnPtr values =
                copy_variant_child_with_missing_prefix(source_values, destination_rows, selection);
        auto presence = ColumnUInt8::create();
        presence->insert_many_defaults(destination_rows);
        selection.insert_from(*presence, *source_field->presence);
        fields.emplace_back(source_field->path, std::move(values), std::move(presence));
    }
    std::ranges::sort(fields, [](const auto& left, const auto& right) {
        return variant_shredded_path_less(left.path, right.path);
    });

    for (auto& field_plan : destination_plans) {
        if (!field_plan.reuse_values && !field_plan.reuse_presence) {
            continue;
        }
        auto& original = *field_plan.original;
        const auto replacement = std::ranges::lower_bound(
                fields, original.path,
                [](const PathInData& left, const PathInData& right) {
                    return variant_shredded_path_less(left, right);
                },
                &ShreddedField::path);
        DORIS_CHECK(replacement != fields.end() && replacement->path == original.path);
        field_plan.replacement = &*replacement;
        if (field_plan.reuse_values) {
            const auto& values = assert_cast<const ColumnVariantV2&>(
                    *static_cast<const IColumn::Ptr&>(field_plan.replacement->values));
            const auto& nullable = assert_cast<const ColumnNullable&>(*values._typed);
            field_plan.nested_rows = nullable.get_nested_column().size();
            field_plan.null_rows = nullable.get_null_map_column().size();
        }
        if (field_plan.reuse_presence) {
            field_plan.presence_rows =
                    static_cast<const ColumnUInt8::Ptr&>(field_plan.replacement->presence)->size();
        }
    }

    // Transfer the sole owners into the prebuilt layout. Post-transfer appends use preflighted,
    // reserved storage. The catch path still trims each nullable component independently before
    // restoring the original owners.
    for (auto& field_plan : destination_plans) {
        if (field_plan.reuse_values) {
            static_cast<IColumn::Ptr&>(field_plan.original->values) = nullptr;
            DORIS_CHECK(field_plan.replacement->values->is_exclusive());
        }
        if (field_plan.reuse_presence) {
            static_cast<ColumnUInt8::Ptr&>(field_plan.original->presence) = nullptr;
            DORIS_CHECK(field_plan.replacement->presence->is_exclusive());
        }
    }
    try {
        for (auto& field_plan : destination_plans) {
            if (field_plan.reuse_values) {
                auto& destination_values =
                        assert_cast<ColumnVariantV2&>(*field_plan.replacement->values);
                auto& nullable = assert_cast<ColumnNullable&>(*destination_values._typed);
                if (field_plan.source == nullptr) {
                    nullable.insert_many_defaults(selection.size());
                } else {
                    const auto& source_values =
                            assert_cast<const ColumnVariantV2&>(*field_plan.source->values);
                    selection.insert_from(nullable, source_values.typed_column());
                }
            }
            if (field_plan.reuse_presence) {
                if (field_plan.source == nullptr) {
                    field_plan.replacement->presence->insert_many_defaults(selection.size());
                } else {
                    selection.insert_from(*field_plan.replacement->presence,
                                          *field_plan.source->presence);
                }
            }
        }
    } catch (...) {
        const auto rollback_to = [](IColumn& column, size_t rows) {
            DORIS_CHECK_GE(column.size(), rows);
            if (column.size() > rows) {
                column.pop_back(column.size() - rows);
            }
        };
        for (auto field_plan = destination_plans.rbegin(); field_plan != destination_plans.rend();
             ++field_plan) {
            if (field_plan->reuse_values) {
                auto& values = assert_cast<ColumnVariantV2&>(*field_plan->replacement->values);
                auto& nullable = assert_cast<ColumnNullable&>(*values._typed);
                rollback_to(nullable.get_nested_column(), field_plan->nested_rows);
                rollback_to(nullable.get_null_map_column(), field_plan->null_rows);
                static_cast<IColumn::Ptr&>(field_plan->original->values) =
                        static_cast<const IColumn::Ptr&>(field_plan->replacement->values);
            }
            if (field_plan->reuse_presence) {
                rollback_to(*field_plan->replacement->presence, field_plan->presence_rows);
                static_cast<ColumnUInt8::Ptr&>(field_plan->original->presence) =
                        static_cast<const ColumnUInt8::Ptr&>(field_plan->replacement->presence);
            }
        }
        _check_invariants();
        throw;
    }
    DORIS_CHECK(residual->is_exclusive());
    DORIS_CHECK(std::ranges::all_of(fields, [](const ShreddedField& field) {
        return field.values->is_exclusive() && field.presence->is_exclusive();
    }));
    _set_shredded_from_valid_parts(*candidate, std::move(residual), std::move(fields), true);
    _adopt_state_from(*candidate);
#ifdef BE_TEST
    ++_test_shredded_union_rebuilds;
    _test_shredded_union_existing_child_rows_copied += existing_child_rows_copied;
    _test_shredded_union_existing_presence_rows_copied += existing_presence_rows_copied;
#endif
}

#ifdef BE_TEST
void ColumnVariantV2::TestAccess::replace_metadata_ids(ColumnVariantV2& column,
                                                       MetadataIdsColumn::Ptr replacement) {
    DORIS_CHECK(column.is_encoded());
    static_cast<MetadataIdsColumn::Ptr&>(column._meta_ids) = std::move(replacement);
}

void ColumnVariantV2::TestAccess::replace_values(ColumnVariantV2& column,
                                                 ColumnString::Ptr replacement) {
    DORIS_CHECK(column.is_encoded());
    static_cast<ColumnString::Ptr&>(column._values) = std::move(replacement);
}

size_t ColumnVariantV2::TestAccess::shredded_union_rebuilds(const ColumnVariantV2& column) {
    return column._test_shredded_union_rebuilds;
}

size_t ColumnVariantV2::TestAccess::shredded_conflict_slow_rows(const ColumnVariantV2& column) {
    return column._test_shredded_conflict_slow_rows;
}

size_t ColumnVariantV2::TestAccess::shredded_union_existing_child_rows_copied(
        const ColumnVariantV2& column) {
    return column._test_shredded_union_existing_child_rows_copied;
}

size_t ColumnVariantV2::TestAccess::shredded_union_existing_presence_rows_copied(
        const ColumnVariantV2& column) {
    return column._test_shredded_union_existing_presence_rows_copied;
}

size_t ColumnVariantV2::TestAccess::full_shredded_validations(const ColumnVariantV2& column) {
    return column._test_full_shredded_validations;
}
#endif

ColumnVariantV2::ShreddedField::ShreddedField(PathInData path_, MutableColumnPtr values_,
                                              ColumnUInt8::MutablePtr presence_)
        : path(normalize_shredded_path(path_)),
          values(std::move(values_)),
          presence(std::move(presence_)) {}

ColumnVariantV2::ShreddedField::ShreddedField(PathInData path_, ColumnPtr values_,
                                              ColumnUInt8::Ptr presence_, SharedOwnerTag)
        : path(normalize_shredded_path(path_)),
          values(std::move(values_)),
          presence(std::move(presence_)) {}

ColumnVariantV2::ShreddedField ColumnVariantV2::ShreddedField::share(PathInData path,
                                                                     ColumnPtr values,
                                                                     ColumnUInt8::Ptr presence) {
    return {path, std::move(values), std::move(presence), SharedOwnerTag {}};
}

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
          _shredded_fields(other._shredded_fields),
          _shredded_layout_frozen(other._shredded_layout_frozen)
#ifdef BE_TEST
          ,
          _test_shredded_union_rebuilds(other._test_shredded_union_rebuilds),
          _test_shredded_conflict_slow_rows(other._test_shredded_conflict_slow_rows),
          _test_shredded_union_existing_child_rows_copied(
                  other._test_shredded_union_existing_child_rows_copied),
          _test_shredded_union_existing_presence_rows_copied(
                  other._test_shredded_union_existing_presence_rows_copied),
          _test_full_shredded_validations(other._test_full_shredded_validations)
#endif
{
}

ColumnVariantV2::Representation ColumnVariantV2::representation() const noexcept {
    if (_typed) {
        return Representation::TYPED_SCALAR;
    }
    return _shredded_fields.empty() ? Representation::ENCODED : Representation::SHREDDED;
}

ColumnVariantV2::MutablePtr ColumnVariantV2::create_typed(ColumnPtr column,
                                                          DataTypePtr scalar_type) {
    ValidatedTypedInput input = validate_typed_input(std::move(column), std::move(scalar_type));
    auto result = ColumnVariantV2::create();
    static_cast<IColumn::Ptr&>(result->_typed) = std::move(input.column);
    result->_typed_type = std::move(input.type);
    result->_check_invariants();
    return result;
}

ColumnVariantV2::MutablePtr ColumnVariantV2::create_encoded_from_valid_parts(
        ColumnString::MutablePtr metadatas, MetadataIdsColumn::MutablePtr metadata_ids,
        ColumnString::MutablePtr values) {
    DORIS_CHECK(static_cast<bool>(metadatas));
    DORIS_CHECK(static_cast<bool>(metadata_ids));
    DORIS_CHECK(static_cast<bool>(values));
    DORIS_CHECK_EQ(metadata_ids->size(), values->size());
    DORIS_CHECK(values->empty() || !metadatas->empty());

    auto result = ColumnVariantV2::create();
    static_cast<ColumnString::Ptr&>(result->_metadatas) = std::move(metadatas);
    static_cast<MetadataIdsColumn::Ptr&>(result->_meta_ids) = std::move(metadata_ids);
    static_cast<ColumnString::Ptr&>(result->_values) = std::move(values);
#ifdef BE_TEST
    result->_check_invariants();
#endif
    return result;
}

ColumnVariantV2::MutablePtr ColumnVariantV2::create_shredded(MutablePtr residual,
                                                             ShreddedFields fields) {
    DORIS_CHECK(static_cast<bool>(residual))
            << "shredded ColumnVariantV2 residual must not be null";
    DORIS_CHECK(residual->is_encoded()) << "shredded ColumnVariantV2 residual must be encoded";
    DORIS_CHECK(!fields.empty()) << "shredded ColumnVariantV2 requires at least one field";
    residual->_check_invariants();
    const size_t rows = residual->size();
    for (auto& field : fields) {
        field.path = normalize_shredded_path(field.path);
        DORIS_CHECK(static_cast<bool>(field.values))
                << "shredded ColumnVariantV2 field values must not be null";
        DORIS_CHECK(static_cast<bool>(field.presence))
                << "shredded ColumnVariantV2 field presence must not be null";
        const IColumn* values = static_cast<const IColumn::Ptr&>(field.values).get();
        DORIS_CHECK(typeid(*values) == typeid(ColumnVariantV2))
                << "shredded field values must be an exact ColumnVariantV2";
        const auto& variant_values = assert_cast<const ColumnVariantV2&>(*values);
        variant_values._check_invariants();
        DORIS_CHECK(!variant_values.is_shredded())
                << "nested shredded ColumnVariantV2 fields are not supported";
        DORIS_CHECK_EQ(variant_values.size(), rows)
                << "shredded field values row count differs from residual";
        const auto& presence_column = *static_cast<const ColumnUInt8::Ptr&>(field.presence);
        DORIS_CHECK_EQ(presence_column.size(), rows)
                << "shredded field presence row count differs from residual";
        const auto& presence = presence_column.get_data();
        DORIS_CHECK(std::ranges::all_of(presence, [](uint8_t value) { return value <= 1; }))
                << "shredded field presence values must be zero or one";
    }
    std::ranges::sort(fields, [](const ShreddedField& left, const ShreddedField& right) {
        return variant_shredded_path_less(left.path, right.path);
    });
    for (size_t index = 1; index < fields.size(); ++index) {
        DORIS_CHECK(!variant_shredded_path_is_prefix(fields[index - 1].path, fields[index].path))
                << "shredded field paths must be unique and prefix-free: "
                << fields[index - 1].path.get_path() << " and " << fields[index].path.get_path();
    }

    for (const auto& field : fields) {
        const auto& values = assert_cast<const ColumnVariantV2&>(*field.values);
        const auto& presence = static_cast<const ColumnUInt8::Ptr&>(field.presence)->get_data();
        for (size_t row = 0; row < rows; ++row) {
            if (presence[row] != 0) {
                if (values.is_encoded()) {
                    const VariantBasicType basic_type = values.get_value_ref(row).basic_type();
                    DORIS_CHECK(basic_type != VariantBasicType::OBJECT &&
                                basic_type != VariantBasicType::ARRAY)
                            << "ColumnVariantV2 active shredded field must be scalar at path "
                            << field.path.get_path() << " row " << row;
                }
                validate_shredded_residual_disjoint(*residual, field, row);
            }
        }
    }

    auto result = _create_shredded_from_valid_parts(std::move(residual), std::move(fields));
#ifdef BE_TEST
    result->_test_full_shredded_validations = 1;
#endif
    return result;
}

ColumnVariantV2::MutablePtr ColumnVariantV2::_create_shredded_from_valid_parts(
        MutablePtr residual, ShreddedFields fields, bool layout_frozen) {
    auto result = ColumnVariantV2::create();
    _set_shredded_from_valid_parts(*result, std::move(residual), std::move(fields), layout_frozen);
    return result;
}

void ColumnVariantV2::_set_shredded_from_valid_parts(ColumnVariantV2& result, MutablePtr residual,
                                                     ShreddedFields&& fields, bool layout_frozen) {
    DORIS_CHECK(static_cast<bool>(residual));
    DORIS_CHECK(residual->is_encoded());
    DORIS_CHECK(!fields.empty());
    DORIS_CHECK(result.empty() && result.is_encoded());
    result._metadatas = static_cast<const ColumnString::Ptr&>(residual->_metadatas);
    result._meta_ids = static_cast<const MetadataIdsColumn::Ptr&>(residual->_meta_ids);
    result._values = static_cast<const ColumnString::Ptr&>(residual->_values);
    residual.reset();
    result._shredded_fields.swap(fields);
    result._shredded_layout_frozen = layout_frozen;
    // The S result owns every physical child. Row transforms may return a new child column while
    // retaining its encoded metadata dictionary through COW, so detach recursively at the single
    // publication boundary instead of relying on the caller's tracker lifetime.
    result.mutate_subcolumns();
    result._check_invariants();
}

const IColumn& ColumnVariantV2::typed_column() const {
    DORIS_CHECK(_typed != nullptr) << "typed_column requires ColumnVariantV2 typed state";
    return *_typed;
}

const DataTypePtr& ColumnVariantV2::typed_type() const {
    DORIS_CHECK(_typed_type != nullptr) << "typed_type requires ColumnVariantV2 typed state";
    return _typed_type;
}

const PathInData& ColumnVariantV2::shredded_field_path(size_t index) const {
    DORIS_CHECK(is_shredded()) << "shredded_field_path requires shredded state";
    DORIS_CHECK_LT(index, _shredded_fields.size()) << "shredded field index is out of range";
    return _shredded_fields[index].path;
}

const ColumnVariantV2& ColumnVariantV2::shredded_field_values(size_t index) const {
    DORIS_CHECK(is_shredded()) << "shredded_field_values requires shredded state";
    DORIS_CHECK_LT(index, _shredded_fields.size()) << "shredded field index is out of range";
    return assert_cast<const ColumnVariantV2&>(*_shredded_fields[index].values);
}

const ColumnUInt8& ColumnVariantV2::shredded_field_presence(size_t index) const {
    DORIS_CHECK(is_shredded()) << "shredded_field_presence requires shredded state";
    DORIS_CHECK_LT(index, _shredded_fields.size()) << "shredded field index is out of range";
    return *_shredded_fields[index].presence;
}

ColumnVariantV2::MutablePtr ColumnVariantV2::project_shredded_fields(
        MutablePtr projected_residual, size_t first_field, size_t field_count,
        size_t removed_prefix_parts) const {
    DORIS_CHECK(is_shredded()) << "project_shredded_fields requires shredded state";
    DORIS_CHECK(static_cast<bool>(projected_residual))
            << "projected shredded residual must not be null";
    DORIS_CHECK(projected_residual->is_encoded()) << "projected shredded residual must be encoded";
    DORIS_CHECK_EQ(projected_residual->size(), size())
            << "projected shredded residual row count differs from source";
    DORIS_CHECK_NE(field_count, 0) << "shredded projection requires selected fields";
    DORIS_CHECK_LE(first_field, _shredded_fields.size());
    DORIS_CHECK_LE(field_count, _shredded_fields.size() - first_field)
            << "projected shredded field range is out of bounds";

    ShreddedFields projected_fields;
    projected_fields.reserve(field_count);
    for (size_t field_index = first_field; field_index < first_field + field_count; ++field_index) {
        const ShreddedField& source_field = _shredded_fields[field_index];
        DORIS_CHECK_LT(removed_prefix_parts, source_field.path.get_parts().size())
                << "projected shredded prefix must be a strict field ancestor";
        projected_fields.push_back(
                ShreddedField::share(source_field.path.copy_pop_nfront(removed_prefix_parts),
                                     static_cast<const IColumn::Ptr&>(source_field.values),
                                     static_cast<const ColumnUInt8::Ptr&>(source_field.presence)));
    }

    auto result = ColumnVariantV2::create();
    result->_metadatas = static_cast<const ColumnString::Ptr&>(projected_residual->_metadatas);
    result->_meta_ids = static_cast<const MetadataIdsColumn::Ptr&>(projected_residual->_meta_ids);
    result->_values = static_cast<const ColumnString::Ptr&>(projected_residual->_values);
    projected_residual.reset();
    result->_shredded_fields = std::move(projected_fields);
    result->_shredded_layout_frozen = _shredded_layout_frozen;
    // Strong intrusive owners preserve both allocation lifetime and COW mutation isolation. This
    // is a trusted projection of a validated source layout, so do not recursively detach or rerun
    // publication validation on the query hot path.
#ifdef BE_TEST
    result->_check_invariants();
#endif
    return result;
}

ColumnVariantV2::MutablePtr ColumnVariantV2::materialize_encoded_range(size_t start,
                                                                       size_t length) const {
    DORIS_CHECK_LE(start, size()) << "materialized range starts past source size";
    DORIS_CHECK_LE(length, size() - start) << "materialized range exceeds source size";
    auto result = ColumnVariantV2::create();
    if (length == 0) {
        return result;
    }

    if (is_encoded()) {
        result->_append_encoded_range(*this, start, length);
        result->_check_invariants();
        return result;
    }

    if (is_typed()) {
        const auto& nullable = assert_cast<const ColumnNullable&>(*_typed);
        MutableColumnPtr selected = nullable.clone_empty();
        selected->insert_range_from(nullable, start, length);
        const auto& selected_nullable = assert_cast<const ColumnNullable&>(*selected);
        TypedEncodingResult encoded;
        dispatch_variant_typed_column(
                selected_nullable.get_nested_column(), _typed_type->get_primitive_type(),
                [&]<PrimitiveType Type>(const auto& column) {
                    encoded = encode_typed_column<Type>(selected_nullable, column,
                                                        _typed_type->get_scale());
                });
        static_cast<ColumnString::Ptr&>(result->_metadatas) = std::move(encoded.metadatas);
        static_cast<MetadataIdsColumn::Ptr&>(result->_meta_ids) = std::move(encoded.metadata_ids);
        static_cast<ColumnString::Ptr&>(result->_values) = std::move(encoded.values);
        result->_check_invariants();
        return result;
    }

    VariantBatchBuilder builder(VariantBatchBuilder::ReserveHint {.rows = length});
    ActiveShreddedFields active_fields;
    active_fields.reserve(_shredded_fields.size());
    const ReadView view = read_view();
    for (size_t source_row = start; source_row < start + length; ++source_row) {
        active_fields.clear();
        for (const auto& field : _shredded_fields) {
            const auto& presence = static_cast<const ColumnUInt8::Ptr&>(field.presence)->get_data();
            if (presence[source_row] != 0) {
                active_fields.push_back(&field);
            }
        }

        auto output = builder.begin_row();
        const VariantRef residual = view.residual_value_at(source_row);
        if (active_fields.empty()) {
            output.add_value(residual);
        } else {
            append_merged_shredded_node(output, residual, active_fields, 0, active_fields.size(), 0,
                                        source_row);
        }
        output.finish();
    }
    VariantBatchBuilder encoded = builder.finish_batch();
    result->insert_encoded_batch(encoded);
    result->_check_invariants();
    return result;
}

void ColumnVariantV2::ensure_encoded() {
    if (is_encoded()) {
        return;
    }
    auto replacement = materialize_encoded_range(0, size());
    _adopt_state_from(*replacement);
}

std::string ColumnVariantV2::get_name() const {
    if (_typed) {
        DORIS_CHECK(_typed_type != nullptr);
        return "variant_v2(typed=" + _typed_type->get_name() + ")";
    }
    DCHECK(_typed_type == nullptr);
    return "variant_v2";
}

size_t ColumnVariantV2::size() const {
    if (is_typed()) {
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
    if (is_typed()) {
        DCHECK(_metadatas->empty());
        DCHECK(_meta_ids->empty());
        DCHECK(_values->empty());
        return _typed->byte_size();
    }
    DCHECK_EQ(_meta_ids->size(), _values->size());
    size_t bytes = _metadatas->byte_size() + _meta_ids->byte_size() + _values->byte_size();
    for (const auto& field : _shredded_fields) {
        bytes += field.values->byte_size() + field.presence->byte_size();
    }
    return bytes;
}

size_t ColumnVariantV2::allocated_bytes() const {
    if (is_typed()) {
        DCHECK(_metadatas->empty());
        DCHECK(_meta_ids->empty());
        DCHECK(_values->empty());
        return _typed->allocated_bytes();
    }
    DCHECK_EQ(_meta_ids->size(), _values->size());
    size_t bytes = _metadatas->allocated_bytes() + _meta_ids->allocated_bytes() +
                   _values->allocated_bytes();
    for (const auto& field : _shredded_fields) {
        bytes += field.values->allocated_bytes() + field.presence->allocated_bytes();
    }
    return bytes;
}

bool ColumnVariantV2::has_enough_capacity(const IColumn& src) const {
    const auto& source = assert_cast<const ColumnVariantV2&>(src);
    if (representation() != source.representation()) {
        return false;
    }
    if (is_typed()) {
        if (!exact_typed_identity(_typed_type, source._typed_type)) {
            return false;
        }
        return _typed->has_enough_capacity(*source._typed);
    }
    if (!_metadatas->has_enough_capacity(*source._metadatas) ||
        !_meta_ids->has_enough_capacity(*source._meta_ids) ||
        !_values->has_enough_capacity(*source._values)) {
        return false;
    }
    if (is_encoded()) {
        return true;
    }
    if (!_has_same_shredded_layout(source)) {
        return false;
    }
    for (size_t index = 0; index < _shredded_fields.size(); ++index) {
        if (!_shredded_fields[index].values->has_enough_capacity(
                    *source._shredded_fields[index].values) ||
            !_shredded_fields[index].presence->has_enough_capacity(
                    *source._shredded_fields[index].presence)) {
            return false;
        }
    }
    return true;
}

bool ColumnVariantV2::is_exclusive() const {
    if (!IColumn::is_exclusive()) {
        return false;
    }
    if (is_typed()) {
        return _typed->is_exclusive();
    }
    if (!_metadatas->is_exclusive() || !_meta_ids->is_exclusive() || !_values->is_exclusive()) {
        return false;
    }
    return std::ranges::all_of(_shredded_fields, [](const ShreddedField& field) {
        return field.values->is_exclusive() && field.presence->is_exclusive();
    });
}

bool ColumnVariantV2::structure_equals(const IColumn& rhs) const {
    return typeid(rhs) == typeid(ColumnVariantV2);
}

void ColumnVariantV2::sanity_check() const {
    if (is_typed()) {
        _typed->sanity_check();
    } else {
        _metadatas->sanity_check();
        _meta_ids->sanity_check();
        _values->sanity_check();
        for (const auto& field : _shredded_fields) {
            field.values->sanity_check();
            field.presence->sanity_check();
        }
    }
    _check_invariants();
    if (!is_typed()) {
        const auto& metadatas = *_metadatas;
        const auto& metadata_ids = _meta_ids->get_data();
        const auto& values = *_values;
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

void ColumnVariantV2::for_each_subcolumn(ColumnCallback callback) const {
    if (is_typed()) {
        callback(*static_cast<const IColumn::Ptr&>(_typed));
    } else {
        callback(*_metadatas);
        callback(*_meta_ids);
        callback(*_values);
        for (const auto& field : _shredded_fields) {
            callback(*static_cast<const IColumn::Ptr&>(field.values));
            callback(*static_cast<const ColumnUInt8::Ptr&>(field.presence));
        }
    }
}

// IColumn's COW hook is necessarily non-const even though the wrapped pointers expose mutation
// through their pointees.
// NOLINTNEXTLINE(readability-make-member-function-const)
void ColumnVariantV2::mutate_subcolumns() {
    if (is_typed()) {
        mutate_subcolumn(_typed);
    } else {
        mutate_subcolumn<ColumnString>(_metadatas);
        mutate_subcolumn<MetadataIdsColumn>(_meta_ids);
        mutate_subcolumn<ColumnString>(_values);
        for (auto& field : _shredded_fields) {
            mutate_subcolumn(field.values);
            mutate_subcolumn<ColumnUInt8>(field.presence);
        }
    }
}

void ColumnVariantV2::clear() {
    if (is_typed()) {
        mutate_subcolumn(_typed);
        _typed->clear();
    } else {
        auto& metadata_ptr = static_cast<ColumnString::Ptr&>(_metadatas);
        if (metadata_ptr->is_exclusive()) {
            _metadatas->clear();
        } else {
            metadata_ptr = ColumnString::create();
        }
        require_exclusive(_meta_ids, "metadata ids");
        require_exclusive(_values, "values");
        _meta_ids->clear();
        _values->clear();
        for (auto& field : _shredded_fields) {
            mutate_subcolumn(field.values);
            mutate_subcolumn<ColumnUInt8>(field.presence);
            field.values->clear();
            field.presence->clear();
        }
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

    if (is_shredded()) {
        auto encoded = ColumnVariantV2::create();
        encoded->insert_encoded_rows(data);
        insert_range_from(*encoded, 0, rows);
        return;
    }
    if (is_typed()) {
        ensure_encoded();
    }
    DORIS_CHECK(is_encoded()) << "encoded insertion requires encoded destination state";
    require_exclusive(_meta_ids, "metadata ids");
    require_exclusive(_values, "values");
    auto& values = *_values;
    auto& metadata_ids = *_meta_ids;
    reserve_rows(values, metadata_ids, data.value_bytes.size, rows);

    if (data.meta_ids.empty()) {
        const VariantMetadataRef metadata = metadata_at(0);
        const uint32_t id = _find_or_insert_metadata({metadata.data, metadata.size});
        values.insert_many_continuous_binary_data(data.value_bytes.data, data.value_offsets.data(),
                                                  rows);
        metadata_ids.insert_many_vals(id, rows);
    } else {
        DorisVector<uint32_t> remap(metadata_count, UNMAPPED_METADATA_ID);
        auto& destination_ids = metadata_ids.get_data();
        for (size_t row = 0; row < rows; ++row) {
            const uint32_t source_id = data.meta_ids[row];
            DCHECK_LT(source_id, metadata_count);
            const VariantMetadataRef metadata = metadata_at(source_id);
            if (remap[source_id] == UNMAPPED_METADATA_ID) {
                remap[source_id] = _find_or_insert_metadata({metadata.data, metadata.size});
            }
            destination_ids.push_back(remap[source_id]);
        }
        values.insert_many_continuous_binary_data(data.value_bytes.data, data.value_offsets.data(),
                                                  rows);
    }

    DCHECK_EQ(_meta_ids->size(), _values->size());
    _check_invariants();
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
    DORIS_CHECK_EQ(offsets.front(), 0);
    DORIS_CHECK_EQ(static_cast<size_t>(offsets.back()), value_bytes.size);

    if (is_shredded()) {
        auto encoded = ColumnVariantV2::create();
        encoded->insert_encoded_batch(block);
        insert_range_from(*encoded, 0, rows);
        return;
    }
    if (is_typed()) {
        ensure_encoded();
    }
    DORIS_CHECK(is_encoded()) << "encoded insertion requires encoded destination state";
    require_exclusive(_meta_ids, "metadata ids");
    require_exclusive(_values, "values");
    auto& values = *_values;
    auto& metadata_ids = *_meta_ids;
    reserve_rows(values, metadata_ids, value_bytes.size, rows);

    const uint32_t id = _find_or_insert_metadata({metadata.data, metadata.size});
    values.insert_many_continuous_binary_data(value_bytes.data, offsets.data(), rows);
    metadata_ids.insert_many_vals(id, rows);

    DCHECK_EQ(_meta_ids->size(), _values->size());
    _check_invariants();
}

VariantRef ColumnVariantV2::get_value_ref(size_t row) const {
    DORIS_CHECK(is_encoded()) << "get_value_ref requires ColumnVariantV2 encoded state";
    DORIS_CHECK_LT(row, size()) << "ColumnVariantV2 encoded row is out of range";
    const auto& metadata_ids = _meta_ids->get_data();
    const uint32_t metadata_id = metadata_ids[row];
    DCHECK_LT(metadata_id, _metadatas->size());
    const StringRef metadata = _metadatas->get_data_at(metadata_id);
    const StringRef value = _values->get_data_at(row);
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
    if (is_typed()) {
        const auto& nullable = assert_cast<const ColumnNullable&>(*_typed);
        visit_typed_scalar_column(nullable, _typed_type->get_primitive_type(),
                                  _typed_type->get_scale(), row, row + 1,
                                  [&](size_t, const VariantScalarRef& scalar) {
                                      value = VariantField::from_scalar(scalar);
                                  });
    } else if (is_encoded()) {
        value = VariantField::from_ref(get_value_ref(row));
    } else {
        auto encoded = materialize_encoded_range(row, 1);
        value = VariantField::from_ref(encoded->get_value_ref(0));
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

    if (is_shredded()) {
        auto encoded = ColumnVariantV2::create();
        encoded->insert_many_defaults(length);
        insert_range_from(*encoded, 0, length);
        return;
    }
    if (is_typed()) {
        ensure_encoded();
    }

    DORIS_CHECK(is_encoded()) << "default insertion requires encoded destination state";

    DORIS_CHECK_LE(length, std::numeric_limits<size_t>::max() - size())
            << "default row count overflows size_t";
    DORIS_CHECK_LE(length, std::numeric_limits<size_t>::max() / EMPTY_OBJECT_VALUE.size())
            << "default value bytes overflow size_t";
    require_exclusive(_meta_ids, "metadata ids");
    require_exclusive(_values, "values");

    const size_t value_bytes = length * EMPTY_OBJECT_VALUE.size();
    auto& values = *_values;
    auto& metadata_ids = *_meta_ids;
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

// Range insertion preserves S when the destination already has, or can adopt, its layout.
void ColumnVariantV2::insert_range_from( // NOLINT(readability-function-size)
        const IColumn& src, size_t start, size_t length) {
    const auto& source = assert_cast<const ColumnVariantV2&>(src);
    DORIS_CHECK_LE(start, source.size()) << "source range starts past source size";
    DORIS_CHECK_LE(length, source.size() - start) << "source range exceeds source size";
    if (length == 0) {
        return;
    }

    if (this == &source) {
        MutableColumnPtr snapshot = source.clone_empty();
        snapshot->insert_range_from(source, start, length);
        insert_range_from(*snapshot, 0, length);
        return;
    }

    if (empty() && !is_shredded() && source.is_shredded()) {
        auto residual = ColumnVariantV2::create();
        residual->_append_encoded_range(source, start, length);
        ShreddedFields fields;
        fields.reserve(source._shredded_fields.size());
        for (const auto& source_field : source._shredded_fields) {
            MutableColumnPtr values = source_field.values->clone_empty();
            values->insert_range_from(*source_field.values, start, length);
            auto presence = ColumnUInt8::create();
            presence->insert_range_from(*source_field.presence, start, length);
            fields.emplace_back(source_field.path, std::move(values), std::move(presence));
        }
        auto replacement = _create_shredded_from_valid_parts(std::move(residual), std::move(fields),
                                                             source._shredded_layout_frozen);
        _adopt_state_from(*replacement);
        return;
    }

    if (is_typed() && source.is_typed() && exact_typed_identity(_typed_type, source._typed_type)) {
        mutate_subcolumn(_typed);
        _typed->insert_range_from(*source._typed, start, length);
        _check_invariants();
        return;
    }

    if (is_typed()) {
        ensure_encoded();
    }

    if (is_encoded() && source.is_shredded()) {
        _adopt_shredded_layout_from(source);
    }

    if (is_shredded()) {
        if (source.is_encoded()) {
            _append_encoded_range(source, start, length);
            _append_missing_shredded_fields(length);
            _check_invariants();
            return;
        }

        if (source.is_typed()) {
            auto encoded_source = source.materialize_encoded_range(start, length);
            _append_encoded_range(*encoded_source, 0, length);
            _append_missing_shredded_fields(length);
            _check_invariants();
            return;
        }

        DORIS_CHECK(source.is_shredded());
        const ShreddedRangeSelection selection {.start = start, .length = length};
        if (_has_same_shredded_layout(source)) {
            _append_same_shredded_layout_rows(source, selection);
            return;
        }
        const size_t destination_rows = size();
        const ShreddedUnionPlan plan = build_shredded_union_plan(
                _shredded_fields, source._shredded_fields,
                _shredded_layout_frozen ? 0 : _shredded_fields.size(), selection);
        if (plan.source_only.empty() && plan.source_conflicts.empty()) {
            _append_encoded_range(source, start, length);
            for (auto& destination_field : _shredded_fields) {
                const auto* source_field = find_selected_shredded_field(
                        source._shredded_fields, plan.source_active, destination_field.path);
                if (source_field == nullptr) {
                    _append_missing_shredded_field(destination_field, length);
                    continue;
                }
                mutate_subcolumn(destination_field.values);
                mutate_subcolumn<ColumnUInt8>(destination_field.presence);
                destination_field.values->insert_range_from(*source_field->values, start, length);
                destination_field.presence->insert_range_from(*source_field->presence, start,
                                                              length);
            }
            _shredded_layout_frozen = _shredded_layout_frozen || source._shredded_layout_frozen;
            _check_invariants();
            return;
        }
        if (plan.source_only.empty()) {
            auto residual = ColumnVariantV2::create();
            _append_shredded_conflicts_to_residual(*residual, source, plan.source_conflicts,
                                                   selection);
            _append_shredded_mapped_rows(source, plan.source_active, *residual, selection);
            return;
        }
        auto residual = ColumnVariantV2::create();
        residual->_append_encoded_range(*this, 0, destination_rows);
        if (plan.source_conflicts.empty()) {
            residual->_append_encoded_range(source, start, length);
        } else {
            _append_shredded_conflicts_to_residual(*residual, source, plan.source_conflicts,
                                                   selection);
        }
        _replace_with_shredded_union(std::move(residual), source, plan.source_active,
                                     plan.source_only, selection);
        return;
    }

    DORIS_CHECK(is_encoded());
    if (!source.is_encoded()) {
        auto encoded_source = source.materialize_encoded_range(start, length);
        _append_encoded_range(*encoded_source, 0, length);
        _check_invariants();
        return;
    }
    _append_encoded_range(source, start, length);
    _check_invariants();
}

// Append directly when the representations are compatible. Only aliasing or a real layout/type
// conversion needs an owned selection snapshot.
// NOLINTNEXTLINE(readability-function-size,readability-function-cognitive-complexity): complete E/T/S state dispatch keeps representation transitions explicit.
void ColumnVariantV2::insert_indices_from(const IColumn& src, const uint32_t* indices_begin,
                                          const uint32_t* indices_end) {
    const auto& source = assert_cast<const ColumnVariantV2&>(src);
    const size_t rows = validate_selected_indices(indices_begin, indices_end, source.size());
    if (rows == 0) {
        return;
    }

    if (this == &source) {
        MutableColumnPtr snapshot = source.clone_empty();
        snapshot->insert_indices_from(source, indices_begin, indices_end);
        insert_range_from(*snapshot, 0, rows);
        return;
    }

    if (empty() && !is_shredded() && source.is_shredded()) {
        auto residual = ColumnVariantV2::create();
        residual->_append_encoded_indices(source, indices_begin, indices_end);
        ShreddedFields fields;
        fields.reserve(source._shredded_fields.size());
        for (const auto& source_field : source._shredded_fields) {
            MutableColumnPtr values = source_field.values->clone_empty();
            values->insert_indices_from(*source_field.values, indices_begin, indices_end);
            auto presence = ColumnUInt8::create();
            presence->insert_indices_from(*source_field.presence, indices_begin, indices_end);
            fields.emplace_back(source_field.path, std::move(values), std::move(presence));
        }
        auto replacement = _create_shredded_from_valid_parts(std::move(residual), std::move(fields),
                                                             source._shredded_layout_frozen);
        _adopt_state_from(*replacement);
        return;
    }

    if (is_typed() && source.is_typed() && exact_typed_identity(_typed_type, source._typed_type)) {
        mutate_subcolumn(_typed);
        _typed->insert_indices_from(*source._typed, indices_begin, indices_end);
        _check_invariants();
        return;
    }

    if (is_typed()) {
        ensure_encoded();
    }

    if (is_encoded() && source.is_shredded()) {
        _adopt_shredded_layout_from(source);
    }

    if (is_shredded()) {
        if (source.is_encoded()) {
            _append_encoded_indices(source, indices_begin, indices_end);
            _append_missing_shredded_fields(rows);
            _check_invariants();
            return;
        }
        if (source.is_typed()) {
            MutableColumnPtr selected_values = source._typed->clone_empty();
            selected_values->insert_indices_from(*source._typed, indices_begin, indices_end);
            MutablePtr selected = create_typed(std::move(selected_values), source._typed_type);
            auto encoded = selected->materialize_encoded_range(0, rows);
            _append_encoded_range(*encoded, 0, rows);
            _append_missing_shredded_fields(rows);
            _check_invariants();
            return;
        }
        DORIS_CHECK(source.is_shredded());
        const ShreddedIndicesSelection selection {.begin = indices_begin, .end = indices_end};
        if (_has_same_shredded_layout(source)) {
            _append_same_shredded_layout_rows(source, selection);
            return;
        }
        const size_t destination_rows = size();
        const ShreddedUnionPlan plan = build_shredded_union_plan(
                _shredded_fields, source._shredded_fields,
                _shredded_layout_frozen ? 0 : _shredded_fields.size(), selection);
        if (plan.source_only.empty() && plan.source_conflicts.empty()) {
            _append_encoded_indices(source, indices_begin, indices_end);
            for (auto& destination_field : _shredded_fields) {
                const auto* source_field = find_selected_shredded_field(
                        source._shredded_fields, plan.source_active, destination_field.path);
                if (source_field == nullptr) {
                    _append_missing_shredded_field(destination_field, rows);
                    continue;
                }
                mutate_subcolumn(destination_field.values);
                mutate_subcolumn<ColumnUInt8>(destination_field.presence);
                destination_field.values->insert_indices_from(*source_field->values, indices_begin,
                                                              indices_end);
                destination_field.presence->insert_indices_from(*source_field->presence,
                                                                indices_begin, indices_end);
            }
            _shredded_layout_frozen = _shredded_layout_frozen || source._shredded_layout_frozen;
            _check_invariants();
            return;
        }
        if (plan.source_only.empty()) {
            auto residual = ColumnVariantV2::create();
            _append_shredded_conflicts_to_residual(*residual, source, plan.source_conflicts,
                                                   selection);
            _append_shredded_mapped_rows(source, plan.source_active, *residual, selection);
            return;
        }
        auto residual = ColumnVariantV2::create();
        residual->_append_encoded_range(*this, 0, destination_rows);
        if (plan.source_conflicts.empty()) {
            residual->_append_encoded_indices(source, indices_begin, indices_end);
        } else {
            _append_shredded_conflicts_to_residual(*residual, source, plan.source_conflicts,
                                                   selection);
        }
        _replace_with_shredded_union(std::move(residual), source, plan.source_active,
                                     plan.source_only, selection);
        return;
    } else if (source.is_encoded()) {
        DORIS_CHECK(is_encoded());
        _append_encoded_indices(source, indices_begin, indices_end);
        _check_invariants();
        return;
    }

    // T->E is a genuine representation conversion. Select only the requested rows before
    // materializing so unrelated source rows never enter the temporary E.
    DORIS_CHECK(source.is_typed());
    MutableColumnPtr selected_values = source._typed->clone_empty();
    selected_values->insert_indices_from(*source._typed, indices_begin, indices_end);
    MutablePtr selected = create_typed(std::move(selected_values), source._typed_type);
    auto encoded = selected->materialize_encoded_range(0, rows);
    DORIS_CHECK(encoded->is_encoded());
    if (is_shredded()) {
        _append_encoded_range(*encoded, 0, rows);
        _append_missing_shredded_fields(rows);
    } else {
        DORIS_CHECK(is_encoded());
        _append_encoded_range(*encoded, 0, rows);
    }
    _check_invariants();
}

void ColumnVariantV2::pop_back(size_t length) {
    DORIS_CHECK_LE(length, size()) << "pop_back length exceeds the column size";
    if (length == 0) {
        return;
    }
    if (is_typed()) {
        mutate_subcolumn(_typed);
        _typed->pop_back(length);
        _check_invariants();
        return;
    }
    require_exclusive(_meta_ids, "metadata ids");
    require_exclusive(_values, "values");
    _values->pop_back(length);
    _meta_ids->pop_back(length);
    for (auto& field : _shredded_fields) {
        mutate_subcolumn(field.values);
        mutate_subcolumn<ColumnUInt8>(field.presence);
        field.values->pop_back(length);
        field.presence->pop_back(length);
    }
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
    if (is_shredded()) {
        auto encoded = materialize_encoded_range(row, 1);
        return encoded->serialize_value_into_arena(0, arena, begin);
    }
    if (is_typed()) {
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
    if (is_shredded()) {
        auto encoded = materialize_encoded_range(row, 1);
        return encoded->serialize_size_at(0);
    }
    if (is_typed()) {
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
    if (is_shredded()) {
        auto encoded = materialize_encoded_range(row, 1);
        return encoded->serialize_impl(pos, 0);
    }
    if (is_typed()) {
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
    if (is_shredded()) {
        auto encoded = materialize_encoded_range(0, size());
        return encoded->get_max_row_byte_size();
    }
    if (is_typed()) {
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

void ColumnVariantV2::serialize(StringRef* keys, size_t num_rows) const {
    DCHECK(keys != nullptr || num_rows == 0);
    DCHECK_LE(num_rows, size());
    if (is_shredded()) {
        auto encoded = materialize_encoded_range(0, num_rows);
        encoded->serialize(keys, num_rows);
        return;
    }
    if (is_typed()) {
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

void ColumnVariantV2::deserialize(StringRef* keys, size_t num_rows) {
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
    if (is_shredded()) {
        auto encoded = materialize_encoded_range(row, 1);
        encoded->update_hash_with_value(0, hash);
        return;
    }
    if (is_typed()) {
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
    if (is_shredded()) {
        auto encoded = materialize_encoded_range(0, size());
        encoded->update_hashes_with_value(hashes, null_data);
        return;
    }
    if (is_typed()) {
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
    if (is_shredded()) {
        check_hash_range(*this, start, end);
        auto encoded = materialize_encoded_range(start, end - start);
        encoded->update_xxHash_with_value(0, end - start, hash,
                                          null_data == nullptr ? nullptr : null_data + start);
        return;
    }
    if (is_typed()) {
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
    if (is_shredded()) {
        auto encoded = materialize_encoded_range(0, size());
        encoded->update_crcs_with_value(hashes, TYPE_VARIANT, rows, 0, null_data);
        return;
    }
    if (is_typed()) {
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
    if (is_shredded()) {
        check_hash_range(*this, start, end);
        auto encoded = materialize_encoded_range(start, end - start);
        encoded->update_crc_with_value(0, end - start, hash,
                                       null_data == nullptr ? nullptr : null_data + start);
        return;
    }
    if (is_typed()) {
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
    if (is_shredded()) {
        auto encoded = materialize_encoded_range(0, size());
        encoded->update_crc32c_batch(hashes, null_map);
        return;
    }
    if (is_typed()) {
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
    if (is_shredded()) {
        check_hash_range(*this, start, end);
        auto encoded = materialize_encoded_range(start, end - start);
        encoded->update_crc32c_single(0, end - start, hash,
                                      null_map == nullptr ? nullptr : null_map + start);
        return;
    }
    if (is_typed()) {
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

    if (is_shredded()) {
        auto residual = ColumnVariantV2::create();
        residual->_append_encoded_range(*this, 0, size());
        residual->replace_column_null_data(null_map);
        ShreddedFields fields;
        fields.reserve(_shredded_fields.size());
        for (const auto& field : _shredded_fields) {
            MutableColumnPtr values = field.values->clone_resized(size());
            auto presence = ColumnUInt8::create();
            presence->insert_range_from(*field.presence, 0, size());
            auto& presence_data = presence->get_data();
            for (size_t row = 0; row < size(); ++row) {
                if (null_map[row] != 0) {
                    presence_data[row] = 0;
                }
            }
            fields.emplace_back(field.path, std::move(values), std::move(presence));
        }
        auto replacement = _create_shredded_from_valid_parts(std::move(residual), std::move(fields),
                                                             _shredded_layout_frozen);
        _adopt_state_from(*replacement);
        return;
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
    if (is_typed()) {
        ColumnPtr filtered = _typed->filter(filter, result_size_hint);
        auto result = ColumnVariantV2::create();
        static_cast<IColumn::Ptr&>(result->_typed) = std::move(filtered);
        result->_typed_type = _typed_type;
        result->_check_invariants();
        return result;
    }
    ColumnPtr filtered_values = _values->filter(filter, result_size_hint);
    ColumnPtr filtered_metadata_ids = _meta_ids->filter(filter, result_size_hint);
    DORIS_CHECK_EQ(filtered_values->size(), filtered_metadata_ids->size())
            << "filtered encoded row counts differ";

    auto residual = ColumnVariantV2::create();
    static_cast<ColumnString::Ptr&>(residual->_metadatas) =
            static_cast<const ColumnString::Ptr&>(_metadatas);
    static_cast<MetadataIdsColumn::Ptr&>(residual->_meta_ids) =
            cast_column_ptr<MetadataIdsColumn>(std::move(filtered_metadata_ids));
    static_cast<ColumnString::Ptr&>(residual->_values) =
            cast_column_ptr<ColumnString>(std::move(filtered_values));
    residual->_check_invariants();
    if (is_encoded()) {
        return residual;
    }

    ShreddedFields fields;
    fields.reserve(_shredded_fields.size());
    for (const auto& field : _shredded_fields) {
        ColumnPtr filtered_field = field.values->filter(filter, result_size_hint);
        MutableColumnPtr mutable_field = IColumn::mutate(std::move(filtered_field));
        ColumnPtr filtered_presence = field.presence->filter(filter, result_size_hint);
        MutableColumnPtr mutable_presence = IColumn::mutate(std::move(filtered_presence));
        fields.emplace_back(field.path, std::move(mutable_field),
                            cast_mutable_column<ColumnUInt8>(std::move(mutable_presence)));
    }
    return _create_shredded_from_valid_parts(std::move(residual), std::move(fields),
                                             _shredded_layout_frozen);
}

size_t ColumnVariantV2::filter(const Filter& filter) {
    column_match_filter_size(size(), filter.size());
    if (is_typed()) {
        ColumnPtr filtered = static_cast<const IColumn::Ptr&>(_typed)->filter(filter, -1);
        const size_t filtered_size = filtered->size();
        static_cast<IColumn::Ptr&>(_typed) = std::move(filtered);
        _check_invariants();
        return filtered_size;
    }
    if (is_shredded()) {
        ColumnPtr filtered = static_cast<const ColumnVariantV2&>(*this).filter(filter, -1);
        MutableColumnPtr mutable_filtered = IColumn::mutate(std::move(filtered));
        auto& replacement = assert_cast<ColumnVariantV2&>(*mutable_filtered);
        const size_t filtered_size = replacement.size();
        _adopt_state_from(replacement);
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

    if (is_typed()) {
        MutableColumnPtr permuted = _typed->permute(permutation, result_size);
        auto result = ColumnVariantV2::create();
        static_cast<IColumn::Ptr&>(result->_typed) = std::move(permuted);
        result->_typed_type = _typed_type;
        result->_check_invariants();
        return result;
    }

    MutableColumnPtr permuted_values = _values->permute(permutation, result_size);
    MutableColumnPtr permuted_metadata_ids = _meta_ids->permute(permutation, result_size);
    DORIS_CHECK_EQ(permuted_values->size(), permuted_metadata_ids->size())
            << "permuted encoded row counts differ";

    auto residual = ColumnVariantV2::create();
    static_cast<ColumnString::Ptr&>(residual->_metadatas) =
            static_cast<const ColumnString::Ptr&>(_metadatas);
    static_cast<MetadataIdsColumn::Ptr&>(residual->_meta_ids) =
            cast_column_ptr<MetadataIdsColumn>(std::move(permuted_metadata_ids));
    static_cast<ColumnString::Ptr&>(residual->_values) =
            cast_column_ptr<ColumnString>(std::move(permuted_values));
    residual->_check_invariants();
    if (is_encoded()) {
        return residual;
    }

    ShreddedFields fields;
    fields.reserve(_shredded_fields.size());
    for (const auto& field : _shredded_fields) {
        MutableColumnPtr values = field.values->permute(permutation, result_size);
        MutableColumnPtr presence_base = field.presence->permute(permutation, result_size);
        fields.emplace_back(field.path, std::move(values),
                            cast_mutable_column<ColumnUInt8>(std::move(presence_base)));
    }
    return _create_shredded_from_valid_parts(std::move(residual), std::move(fields),
                                             _shredded_layout_frozen);
}

MutableColumnPtr ColumnVariantV2::clone_resized(size_t new_size) const {
    if (is_typed()) {
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
    if (is_encoded() && new_size == 0) {
        return ColumnVariantV2::create();
    }

    const size_t copied_rows = std::min(size(), new_size);
    MutableColumnPtr copied_values = _values->clone_resized(copied_rows);
    MutableColumnPtr copied_metadata_ids = _meta_ids->clone_resized(copied_rows);
    auto residual = ColumnVariantV2::create();
    static_cast<ColumnString::Ptr&>(residual->_metadatas) =
            static_cast<const ColumnString::Ptr&>(_metadatas);
    static_cast<MetadataIdsColumn::Ptr&>(residual->_meta_ids) =
            cast_column_ptr<MetadataIdsColumn>(std::move(copied_metadata_ids));
    static_cast<ColumnString::Ptr&>(residual->_values) =
            cast_column_ptr<ColumnString>(std::move(copied_values));
    residual->_check_invariants();
    if (is_encoded()) {
        if (new_size > copied_rows) {
            residual->insert_many_defaults(new_size - copied_rows);
        }
        residual->_check_invariants();
        return residual;
    }

    ShreddedFields fields;
    fields.reserve(_shredded_fields.size());
    for (const auto& field : _shredded_fields) {
        MutableColumnPtr values = field.values->clone_resized(copied_rows);
        MutableColumnPtr presence_base = field.presence->clone_resized(copied_rows);
        fields.emplace_back(field.path, std::move(values),
                            cast_mutable_column<ColumnUInt8>(std::move(presence_base)));
    }
    auto result = _create_shredded_from_valid_parts(std::move(residual), std::move(fields),
                                                    _shredded_layout_frozen);
    if (new_size > copied_rows) {
        result->insert_many_defaults(new_size - copied_rows);
    }
    result->_check_invariants();
    return result;
}

void ColumnVariantV2::resize(size_t new_size) {
    const size_t old_size = size();
    if (is_typed()) {
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

void ColumnVariantV2::_append_encoded_range(const ColumnVariantV2& source, size_t start,
                                            size_t length) {
    DORIS_CHECK(!is_typed() && !source.is_typed());
    DORIS_CHECK_LE(start, source.size());
    DORIS_CHECK_LE(length, source.size() - start);
    if (length == 0) {
        return;
    }

    require_exclusive(_meta_ids, "metadata ids");
    require_exclusive(_values, "values");
    const auto& source_metadatas = *source._metadatas;
    const auto& source_metadata_ids = source._meta_ids->get_data();
    const auto& source_values = *source._values;
    const auto& source_offsets = source_values.get_offsets();
    const size_t value_begin = source_offsets[static_cast<ssize_t>(start) - 1];
    const size_t value_end = source_offsets[start + length - 1];
    const bool destination_has_no_metadata =
            static_cast<const ColumnString::Ptr&>(_metadatas)->empty();
    const bool copy_metadata_dictionary = empty() && destination_has_no_metadata;
    const bool already_shared =
            static_cast<const ColumnString::Ptr&>(_metadatas).get() == source._metadatas.get();
    DorisVector<uint32_t> remap;
    if (!copy_metadata_dictionary && !already_shared) {
        remap.assign(source_metadatas.size(), UNMAPPED_METADATA_ID);
    }
    auto& values = *_values;
    auto& metadata_ids = *_meta_ids;
    reserve_rows(values, metadata_ids, value_end - value_begin, length);
    if (copy_metadata_dictionary) {
        static_cast<ColumnString::Ptr&>(_metadatas) = cast_column_ptr<ColumnString>(
                source._metadatas->clone_resized(source_metadatas.size()));
    }
    const bool shared_metadata =
            static_cast<const ColumnString::Ptr&>(_metadatas).get() == source._metadatas.get();
    values.insert_range_from(source_values, start, length);
    if (copy_metadata_dictionary || shared_metadata) {
        metadata_ids.insert_range_from(*source._meta_ids, start, length);
        return;
    }

    auto& destination_ids = metadata_ids.get_data();
    for (size_t row = 0; row < length; ++row) {
        const uint32_t source_id = source_metadata_ids[start + row];
        DORIS_CHECK_LT(source_id, source_metadatas.size()) << "source metadata id is out of range";
        if (remap[source_id] == UNMAPPED_METADATA_ID) {
            remap[source_id] = _find_or_insert_metadata(source_metadatas.get_data_at(source_id));
        }
        destination_ids.push_back(remap[source_id]);
    }
}

void ColumnVariantV2::_append_encoded_indices(const ColumnVariantV2& source,
                                              const uint32_t* indices_begin,
                                              const uint32_t* indices_end) {
    DORIS_CHECK(!is_typed() && !source.is_typed());
    const size_t rows = validate_selected_indices(indices_begin, indices_end, source.size());
    if (rows == 0) {
        return;
    }

    require_exclusive(_meta_ids, "metadata ids");
    require_exclusive(_values, "values");
    const auto& source_metadatas = *source._metadatas;
    const auto& source_metadata_ids = source._meta_ids->get_data();
    const auto& source_values = *source._values;
    const bool destination_has_no_metadata =
            static_cast<const ColumnString::Ptr&>(_metadatas)->empty();
    const bool copy_metadata_dictionary = empty() && destination_has_no_metadata;
    const bool already_shared =
            static_cast<const ColumnString::Ptr&>(_metadatas).get() == source._metadatas.get();
    DorisVector<uint32_t> remap;
    if (!copy_metadata_dictionary && !already_shared) {
        remap.assign(source_metadatas.size(), UNMAPPED_METADATA_ID);
    }
    auto& values = *_values;
    auto& metadata_ids = *_meta_ids;
    metadata_ids.get_data().reserve(metadata_ids.size() + rows);
    if (copy_metadata_dictionary) {
        static_cast<ColumnString::Ptr&>(_metadatas) = cast_column_ptr<ColumnString>(
                source._metadatas->clone_resized(source_metadatas.size()));
    }
    const bool shared_metadata =
            static_cast<const ColumnString::Ptr&>(_metadatas).get() == source._metadatas.get();
    values.insert_indices_from(source_values, indices_begin, indices_end);
    if (copy_metadata_dictionary || shared_metadata) {
        metadata_ids.insert_indices_from(*source._meta_ids, indices_begin, indices_end);
        return;
    }

    auto& destination_ids = metadata_ids.get_data();
    for (size_t row = 0; row < rows; ++row) {
        const uint32_t source_id = source_metadata_ids[indices_begin[row]];
        DORIS_CHECK_LT(source_id, source_metadatas.size()) << "source metadata id is out of range";
        if (remap[source_id] == UNMAPPED_METADATA_ID) {
            remap[source_id] = _find_or_insert_metadata(source_metadatas.get_data_at(source_id));
        }
        destination_ids.push_back(remap[source_id]);
    }
}

void ColumnVariantV2::_append_missing_shredded_field(ShreddedField& field, size_t length) {
    mutate_subcolumn(field.values);
    mutate_subcolumn<ColumnUInt8>(field.presence);
    auto& values = assert_cast<ColumnVariantV2&>(*field.values);
    if (values.is_typed()) {
        mutate_subcolumn(values._typed);
        values._typed->insert_many_defaults(length);
        values._check_invariants();
    } else {
        values.insert_many_defaults(length);
    }
    field.presence->insert_many_defaults(length);
}

// This mutates nested COW columns while preserving the top-level S layout.
// NOLINTNEXTLINE(readability-make-member-function-const)
void ColumnVariantV2::_append_missing_shredded_fields(size_t length) {
    DORIS_CHECK(is_shredded());
    for (auto& field : _shredded_fields) {
        _append_missing_shredded_field(field, length);
    }
}

void ColumnVariantV2::_adopt_shredded_layout_from(const ColumnVariantV2& source) {
    DORIS_CHECK(is_encoded());
    DORIS_CHECK(source.is_shredded());
    const size_t existing_rows = size();
    auto residual = ColumnVariantV2::create();
    residual->_append_encoded_range(*this, 0, existing_rows);
    ShreddedFields fields;
    fields.reserve(source._shredded_fields.size());
    for (const auto& source_field : source._shredded_fields) {
        MutableColumnPtr values = source_field.values->clone_empty();
        auto& variant_values = assert_cast<ColumnVariantV2&>(*values);
        if (variant_values.is_typed()) {
            mutate_subcolumn(variant_values._typed);
            variant_values._typed->insert_many_defaults(existing_rows);
            variant_values._check_invariants();
        } else {
            variant_values.insert_many_defaults(existing_rows);
        }
        auto presence = ColumnUInt8::create();
        presence->insert_many_defaults(existing_rows);
        fields.emplace_back(source_field.path, std::move(values), std::move(presence));
    }
    auto replacement = _create_shredded_from_valid_parts(std::move(residual), std::move(fields),
                                                         source._shredded_layout_frozen);
    _adopt_state_from(*replacement);
}

uint32_t ColumnVariantV2::_find_or_insert_metadata(StringRef metadata) {
    DORIS_CHECK(metadata.data != nullptr || metadata.size == 0)
            << "metadata bytes have a null pointer";
    const auto& current_metadatas = *static_cast<const ColumnString::Ptr&>(_metadatas);
    for (uint32_t id = 0; id < current_metadatas.size(); ++id) {
        if (current_metadatas.get_data_at(id) == metadata) {
            return id;
        }
    }
    if (current_metadatas.size() == std::numeric_limits<uint32_t>::max()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "ColumnVariantV2 metadata dictionary exceeds the uint32 id limit");
    }

    _detach_metadata_for_write();
    auto& metadatas = *_metadatas;
    const size_t new_chars_size = metadatas.get_chars().size() + metadata.size;
    ColumnString::check_chars_length(new_chars_size, metadatas.size() + 1, _meta_ids->size());
    metadatas.get_chars().reserve(new_chars_size);
    metadatas.get_offsets().reserve(metadatas.size() + 1);
    const auto id = static_cast<uint32_t>(metadatas.size());
    metadatas.insert_data(metadata.data, metadata.size);
    return id;
}

void ColumnVariantV2::_adopt_state_from(ColumnVariantV2& replacement) {
    DORIS_CHECK(this != &replacement) << "cannot adopt ColumnVariantV2 state from itself";
    _metadatas = std::move(replacement._metadatas);
    _meta_ids = std::move(replacement._meta_ids);
    _values = std::move(replacement._values);
    _typed = std::move(replacement._typed);
    _typed_type = std::move(replacement._typed_type);
    _shredded_fields = std::move(replacement._shredded_fields);
    _shredded_layout_frozen = replacement._shredded_layout_frozen;
    _check_invariants();
}

void ColumnVariantV2::_detach_metadata_for_write() {
    auto& metadata_ptr = static_cast<ColumnString::Ptr&>(_metadatas);
    if (!metadata_ptr->is_exclusive()) {
        mutate_subcolumn<ColumnString>(_metadatas);
    }
}

bool ColumnVariantV2::_has_same_shredded_layout(const ColumnVariantV2& source) const {
    if (!is_shredded() || !source.is_shredded() ||
        _shredded_fields.size() != source._shredded_fields.size()) {
        return false;
    }
    for (size_t index = 0; index < _shredded_fields.size(); ++index) {
        const auto& destination_field = _shredded_fields[index];
        const auto& source_field = source._shredded_fields[index];
        if (destination_field.path != source_field.path) {
            return false;
        }
        const auto& destination_values =
                assert_cast<const ColumnVariantV2&>(*destination_field.values);
        if (destination_values.is_typed() &&
            !shredded_children_append_compatible(destination_field, source_field)) {
            return false;
        }
    }
    return true;
}

void ColumnVariantV2::_check_invariants() const {
    if (is_typed()) {
        DORIS_CHECK(!_shredded_layout_frozen);
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
        DORIS_CHECK(_shredded_fields.empty()) << "typed state cannot contain shredded fields";
        return;
    }

    DORIS_CHECK(_typed_type == nullptr) << "encoded state cannot retain a typed data type";
    const auto& metadata_ids = _meta_ids->get_data();
    const auto& values = *_values;
    DORIS_CHECK_EQ(metadata_ids.size(), values.size())
            << "ColumnVariantV2 encoded row counts differ";
    if (is_encoded()) {
        DORIS_CHECK(!_shredded_layout_frozen);
        return;
    }

    _check_shredded_invariants();
}

void ColumnVariantV2::_check_shredded_invariants() const {
    DORIS_CHECK(is_shredded());
    DORIS_CHECK(!_shredded_fields.empty()) << "shredded state requires at least one field";
    const size_t rows = _values->size();
    for (size_t index = 0; index < _shredded_fields.size(); ++index) {
        const auto& field = _shredded_fields[index];
        DORIS_CHECK(!field.path.get_parts().empty()) << "shredded field path cannot be the root";
        for (const auto& part : field.path.get_parts()) {
            DORIS_CHECK(!part.is_nested && part.anonymous_array_level == 0)
                    << "shredded fields do not support array paths";
        }
        DORIS_CHECK(static_cast<bool>(field.values)) << "shredded field values must not be null";
        DORIS_CHECK(static_cast<bool>(field.presence))
                << "shredded field presence must not be null";
        const IColumn* field_values = static_cast<const IColumn::Ptr&>(field.values).get();
        DORIS_CHECK(typeid(*field_values) == typeid(ColumnVariantV2))
                << "shredded field values must be an exact ColumnVariantV2";
        const auto& variant_values = assert_cast<const ColumnVariantV2&>(*field_values);
        DORIS_CHECK(!variant_values.is_shredded())
                << "nested shredded ColumnVariantV2 fields are not supported";
        DORIS_CHECK_EQ(variant_values.size(), rows)
                << "shredded field values row count differs from residual";
        DORIS_CHECK_EQ(field.presence->size(), rows)
                << "shredded field presence row count differs from residual";
        if (index != 0) {
            DORIS_CHECK(variant_shredded_path_less(_shredded_fields[index - 1].path, field.path))
                    << "shredded field paths must be strictly sorted";
            DORIS_CHECK(
                    !variant_shredded_path_is_prefix(_shredded_fields[index - 1].path, field.path))
                    << "shredded field paths must be prefix-free";
        }
    }
}

} // namespace doris
