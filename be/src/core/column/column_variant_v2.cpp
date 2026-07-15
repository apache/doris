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

#include "core/column/column_variant_v2.h"

#include <algorithm>
#include <array>
#include <bit>
#include <cstdint>
#include <limits>
#include <string_view>
#include <typeinfo>
#include <utility>

#include "common/check.h"
#include "common/exception.h"
#include "core/assert_cast.h"
#include "core/column/column_const.h"
#include "core/column/column_decimal.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_variant_v2_typed_scalar.h"
#include "core/column/column_vector.h"
#include "core/column/columns_common.h"
#include "core/custom_allocator.h"
#include "util/hash_util.hpp"
#include "util/variant/variant_canonical.h"
#include "util/variant/variant_encoded_block.h"
#include "util/variant/variant_scalar_encoding.h"

namespace doris {
namespace {

using MetaIdsColumn = ColumnVector<TYPE_UINT32>;
using column_variant_v2_internal::TypedFallbackKind;
using column_variant_v2_internal::dispatch_typed_column;
using column_variant_v2_internal::exact_typed_identity;
using column_variant_v2_internal::is_supported_typed_identity;
using column_variant_v2_internal::validate_typed_decimal_scale;
using column_variant_v2_internal::visit_typed_canonical_rows;
using column_variant_v2_internal::visit_typed_rows;
constexpr uint32_t UNMAPPED_METADATA_ID = std::numeric_limits<uint32_t>::max();
constexpr size_t CANONICAL_SIZE_PREFIX = sizeof(uint32_t);
constexpr std::array<char, 3> EMPTY_OBJECT_METADATA {static_cast<char>(0x11), 0, 0};
constexpr std::array<char, 3> EMPTY_OBJECT_VALUE {static_cast<char>(0x02), 0, 0};

uint32_t read_canonical_payload_size(const char* pos) {
    DCHECK(pos != nullptr);
    uint32_t payload_size = 0;
    for (uint8_t byte = 0; byte < CANONICAL_SIZE_PREFIX; ++byte) {
        payload_size |= static_cast<uint32_t>(static_cast<uint8_t>(pos[byte])) << (byte * 8);
    }
    return payload_size;
}

size_t trusted_canonical_cell_size(const char* pos) {
    return CANONICAL_SIZE_PREFIX + read_canonical_payload_size(pos);
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

void append_metadata_ids(MetaIdsColumn& destination, const DorisVector<uint32_t>& ids) {
    auto& data = destination.get_data();
    const size_t old_size = data.size();
    data.resize(old_size + ids.size());
    std::ranges::copy(ids, data.begin() + old_size);
}

struct SelectedRows {
    DorisVector<uint32_t> metadata_ids;
    size_t value_bytes = 0;
};

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

DorisVector<uint32_t> collect_range_metadata_ids(
        const MetaIdsColumn::Container& source_metadata_ids, size_t source_metadata_count,
        size_t start, size_t length) {
    DorisVector<uint32_t> selected(length);
    for (size_t row = 0; row < length; ++row) {
        const uint32_t metadata_id = source_metadata_ids[start + row];
        DCHECK_LT(metadata_id, source_metadata_count) << "source metadata id is out of range";
        selected[row] = metadata_id;
    }
    return selected;
}

SelectedRows collect_selected_rows(const MetaIdsColumn::Container& source_metadata_ids,
                                   const ColumnString& source_metadatas,
                                   const ColumnString& source_values, const uint32_t* indices_begin,
                                   size_t rows) {
    SelectedRows selected;
    selected.metadata_ids.resize(rows);
    for (size_t row = 0; row < rows; ++row) {
        const uint32_t source_row = indices_begin[row];
        const uint32_t metadata_id = source_metadata_ids[source_row];
        DCHECK_LT(metadata_id, source_metadatas.size()) << "source metadata id is out of range";
        selected.metadata_ids[row] = metadata_id;
        const size_t row_bytes = source_values.get_data_at(source_row).size;
        DORIS_CHECK_LE(row_bytes, std::numeric_limits<size_t>::max() - selected.value_bytes)
                << "selected Variant value bytes overflow size_t";
        selected.value_bytes += row_bytes;
    }
    return selected;
}

template <typename Sink, typename Hash>
void update_typed_hashes(const ColumnNullable& nullable, PrimitiveType type, uint32_t scale,
                         Hash* __restrict hashes, const uint8_t* __restrict null_data) {
    dispatch_typed_column(nullable, type, [&]<PrimitiveType Type>(const auto& column) {
        visit_typed_canonical_rows<Type>(nullable, column, scale, 0, nullable.size(),
                                         [&](size_t row, auto&& canonical_factory) {
                                             if (null_data == nullptr || null_data[row] == 0) {
                                                 VariantCanonicalScalarRef canonical =
                                                         canonical_factory();
                                                 Sink sink(hashes[row]);
                                                 canonical_hash(canonical, sink);
                                                 hashes[row] = sink.digest();
                                             }
                                         });
    });
}

template <typename Sink, typename Hash>
void update_typed_hash_range(const ColumnNullable& nullable, PrimitiveType type, uint32_t scale,
                             size_t start, size_t end, Hash& hash,
                             const uint8_t* __restrict null_data) {
    Sink sink(hash);
    dispatch_typed_column(nullable, type, [&]<PrimitiveType Type>(const auto& column) {
        visit_typed_canonical_rows<Type>(
                nullable, column, scale, start, end, [&](size_t row, auto&& canonical_factory) {
                    if (null_data == nullptr || null_data[row] == 0) {
                        VariantCanonicalScalarRef canonical = canonical_factory();
                        canonical_hash(canonical, sink);
                    }
                });
    });
    hash = sink.digest();
}

struct TypedEncodingResult {
    ColumnString::MutablePtr metadatas;
    MetaIdsColumn::MutablePtr metadata_ids;
    ColumnString::MutablePtr values;
    ColumnVariantV2::TypedEncodingStats stats;
};

void count_fallback(TypedFallbackKind fallback, ColumnVariantV2::TypedEncodingStats& stats) {
    switch (fallback) {
    case TypedFallbackKind::NONE:
        return;
    case TypedFallbackKind::LARGEINT:
        ++stats.largeint_string_fallback_rows;
        return;
    case TypedFallbackKind::IP:
        ++stats.ip_string_fallback_rows;
        return;
    }
}

template <PrimitiveType Type, typename Column>
TypedEncodingResult encode_typed_column(const ColumnNullable& nullable, const Column& column,
                                        uint32_t scale) {
    size_t value_bytes = 0;
    TypedEncodingResult result;
    visit_typed_rows<Type>(
            nullable, column, scale, 0, nullable.size(),
            [&](size_t, auto&& physical_factory, auto&&, TypedFallbackKind fallback) {
                const VariantScalarEncodingPlan plan = physical_factory();
                if (plan.size() > std::numeric_limits<size_t>::max() - value_bytes) {
                    throw Exception(ErrorCode::INVALID_ARGUMENT,
                                    "Typed Variant values exceed addressable size");
                }
                value_bytes += plan.size();
                count_fallback(fallback, result.stats);
            });
    ColumnString::check_chars_length(value_bytes, nullable.size(), 0);

    result.metadatas = ColumnString::create();
    result.metadata_ids = MetaIdsColumn::create();
    result.values = ColumnString::create();
    if (!nullable.empty()) {
        result.metadatas->insert_data(EMPTY_OBJECT_METADATA.data(), EMPTY_OBJECT_METADATA.size());
    }
    result.metadata_ids->get_data().resize(nullable.size());
    std::fill(result.metadata_ids->get_data().begin(), result.metadata_ids->get_data().end(), 0);
    auto& chars = result.values->get_chars();
    auto& offsets = result.values->get_offsets();
    chars.resize(value_bytes);
    offsets.resize(nullable.size());

    size_t offset = 0;
    visit_typed_rows<Type>(nullable, column, scale, 0, nullable.size(),
                           [&](size_t row, auto&& physical_factory, auto&&, TypedFallbackKind) {
                               const VariantScalarEncodingPlan plan = physical_factory();
                               plan.write(reinterpret_cast<char*>(chars.data()) + offset,
                                          plan.size());
                               offset += plan.size();
                               offsets[row] = static_cast<ColumnString::Offset>(offset);
                           });
    DCHECK_EQ(offset, value_bytes);
    return result;
}

struct ValidatedTypedInput {
    MutableColumnPtr column;
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
    DORIS_CHECK(is_supported_typed_identity(type))
            << "unsupported typed identity " << scalar_type->get_name();
    MutableColumnPtr expected = scalar_type->create_column();
    const IColumn* expected_column = expected.get();
    DORIS_CHECK(typeid(nested) == typeid(*expected_column))
            << "typed nested column " << nested.get_name() << " does not match data type "
            << scalar_type->get_name();
    validate_typed_decimal_scale(nested, type, scalar_type->get_scale());

    return {.column = IColumn::mutate(std::move(column)), .type = std::move(scalar_type)};
}

[[noreturn]] void throw_deferred(std::string_view method, std::string_view task) {
    throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                    "ColumnVariantV2::{} is deferred until {} is complete", method, task);
}

[[noreturn]] void throw_unsupported(std::string_view method) {
    throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                    "ColumnVariantV2::{} is intentionally unsupported for Variant values", method);
}

} // namespace

struct ColumnVariantV2::MetaDictIndex {
    using Candidates = DorisVector<uint32_t>;
    using Buckets = DorisMap<uint64_t, Candidates>;

    size_t allocated_bytes() const {
        // std::map does not expose node allocation sizes. Count the logical node payload and a
        // conservative four-pointer tree overhead; the actual allocations are independently
        // charged through CustomStdAllocator.
        size_t bytes =
                sizeof(*this) + buckets.size() * (sizeof(Buckets::value_type) + 4 * sizeof(void*));
        for (const auto& entry : buckets) {
            bytes += entry.second.capacity() * sizeof(uint32_t);
        }
        return bytes;
    }

    Buckets buckets;
};

#ifdef BE_TEST
void ColumnVariantV2::TestAccess::reset_metadata_index(ColumnVariantV2& column) {
    column._meta_index.reset();
}

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
    column._meta_index.reset();
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
          _typed_type(other._typed_type) {}

ColumnVariantV2::~ColumnVariantV2() = default;

ColumnVariantV2::MutablePtr ColumnVariantV2::create_typed(ColumnPtr column,
                                                          DataTypePtr scalar_type) {
    ValidatedTypedInput input = validate_typed_input(std::move(column), std::move(scalar_type));
    auto result = ColumnVariantV2::create();
    static_cast<IColumn::Ptr&>(result->_typed) = std::move(input.column);
    result->_typed_type = std::move(input.type);
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

ColumnVariantV2::TypedEncodingStats ColumnVariantV2::ensure_encoded() {
    if (!_typed) {
        DCHECK(_typed_type == nullptr);
        return {};
    }

    const auto& typed = static_cast<const IColumn::Ptr&>(_typed);
    const auto& nullable = assert_cast<const ColumnNullable&>(*typed);
    const PrimitiveType type = _typed_type->get_primitive_type();
    const uint32_t scale = _typed_type->get_scale();
    TypedEncodingResult encoded;
    dispatch_typed_column(nullable, type, [&]<PrimitiveType Type>(const auto& column) {
        encoded = encode_typed_column<Type>(nullable, column, scale);
    });

    static_cast<IColumn::Ptr&>(_metadatas) = std::move(encoded.metadatas);
    static_cast<IColumn::Ptr&>(_meta_ids) = std::move(encoded.metadata_ids);
    static_cast<IColumn::Ptr&>(_values) = std::move(encoded.values);
    static_cast<IColumn::Ptr&>(_typed).reset();
    _typed_type.reset();
    _meta_index.reset();
    _check_invariants();
    return encoded.stats;
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
    if (_typed) {
        DCHECK(_metadatas->empty());
        DCHECK(_meta_ids->empty());
        DCHECK(_values->empty());
        return _typed->allocated_bytes();
    }
    DCHECK_EQ(_meta_ids->size(), _values->size());
    const size_t index_bytes = _meta_index == nullptr ? 0 : _meta_index->allocated_bytes();
    return _metadatas->allocated_bytes() + _meta_ids->allocated_bytes() +
           _values->allocated_bytes() + index_bytes;
}

bool ColumnVariantV2::has_enough_capacity(const IColumn& src) const {
    const auto& source = assert_cast<const ColumnVariantV2&>(src);
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
    if (_typed) {
        _typed->sanity_check();
    } else {
        _metadatas->sanity_check();
        _meta_ids->sanity_check();
        _values->sanity_check();
    }
    _check_invariants();
}

void ColumnVariantV2::for_each_subcolumn(ColumnCallback callback) const {
    if (_typed) {
        callback(*static_cast<const IColumn::Ptr&>(_typed));
    } else {
        callback(*static_cast<const IColumn::Ptr&>(_metadatas));
        callback(*static_cast<const IColumn::Ptr&>(_meta_ids));
        callback(*static_cast<const IColumn::Ptr&>(_values));
    }
}

void ColumnVariantV2::mutate_subcolumns() {
    _meta_index.reset();
    if (_typed) {
        mutate_subcolumn(_typed);
    } else {
        mutate_subcolumn(_metadatas);
        mutate_subcolumn(_meta_ids);
        mutate_subcolumn(_values);
    }
}

void ColumnVariantV2::clear() {
    if (_typed) {
        require_exclusive(_typed, "typed column");
        static_cast<IColumn::Ptr&>(_typed) = _typed->clone_empty();
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
    _meta_index.reset();
    _check_invariants();
}

// Encoded insertion is a transactional state transition that keeps metadata, ids, and values in
// sync; splitting it would obscure the shared rollback boundary.
void ColumnVariantV2::insert_encoded_rows( // NOLINT(readability-function-size)
        const EncodedDataView& data) {
    if (_typed) {
        require_exclusive(_typed, "typed column");
        auto replacement = ColumnVariantV2::create();
        static_cast<IColumn::Ptr&>(replacement->_typed) = _typed->clone_resized(size());
        replacement->_typed_type = _typed_type;
        replacement->ensure_encoded();
        replacement->insert_encoded_rows(data);
        _adopt_state_from(*replacement);
        return;
    }
    DORIS_CHECK(_typed_type == nullptr) << "encoded state cannot retain a typed data type";
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
        for (uint32_t metadata_id : data.meta_ids) {
            DORIS_CHECK_LT(metadata_id, metadata_count)
                    << "encoded row metadata id is out of range";
        }
    }

    // Source metadata ids are local to this batch. Build one source-to-destination remap, append
    // every referenced metadata blob at most once, then publish row ids and values together. The
    // rollback below restores all three encoded subcolumns if validation or allocation throws.
    DorisVector<uint32_t> remap(metadata_count, UNMAPPED_METADATA_ID);
    DorisVector<uint32_t> appended_ids(rows);
    require_exclusive(_meta_ids, "metadata ids");
    require_exclusive(_values, "values");
    auto& values = assert_cast<ColumnString&>(*_values);
    auto& metadata_ids = assert_cast<MetaIdsColumn&>(*_meta_ids);
    reserve_rows(values, metadata_ids, data.value_bytes.size, rows);

    const auto& metadata_ptr = static_cast<const IColumn::Ptr&>(_metadatas);
    const size_t old_metadata_count = metadata_ptr->size();
    IColumn::Ptr original_metadata;
    if (!metadata_ptr->is_exclusive()) {
        original_metadata = metadata_ptr;
    }
    const size_t old_rows = size();
    try {
        if (data.meta_ids.empty()) {
            const StringRef metadata(data.metadata_bytes.data,
                                     data.metadata_offsets[1] - data.metadata_offsets[0]);
            remap[0] = _find_or_insert_metadata(metadata);
            std::ranges::fill(appended_ids, remap[0]);
        } else {
            for (uint32_t source_id : data.meta_ids) {
                DORIS_CHECK_LT(source_id, metadata_count)
                        << "encoded row metadata id is out of range";
                if (remap[source_id] == UNMAPPED_METADATA_ID) {
                    const uint32_t begin = data.metadata_offsets[source_id];
                    const uint32_t end = data.metadata_offsets[source_id + 1];
                    remap[source_id] = _find_or_insert_metadata(
                            {data.metadata_bytes.data + begin, end - begin});
                }
            }
            for (size_t row = 0; row < rows; ++row) {
                appended_ids[row] = remap[data.meta_ids[row]];
            }
        }
        values.insert_many_continuous_binary_data(data.value_bytes.data, data.value_offsets.data(),
                                                  rows);
        append_metadata_ids(metadata_ids, appended_ids);
    } catch (...) {
        if (metadata_ids.size() > old_rows) {
            metadata_ids.pop_back(metadata_ids.size() - old_rows);
        }
        if (values.size() > old_rows) {
            values.pop_back(values.size() - old_rows);
        }
        _rollback_metadata(old_metadata_count, std::move(original_metadata));
        _check_invariants();
        throw;
    }

    DCHECK_EQ(_meta_ids->size(), _values->size());
    _check_invariants();
}

void ColumnVariantV2::insert_encoded_block(VariantEncodedBlockView block) {
    const VariantMetadataRef metadata = block.metadata_ref();
    if (metadata.size > std::numeric_limits<uint32_t>::max()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant block metadata exceeds the ColumnString uint32 byte limit");
    }
    const std::array<uint32_t, 2> metadata_offsets {0, static_cast<uint32_t>(metadata.size)};
    insert_encoded_rows({.metadata_bytes = {metadata.data, metadata.size},
                         .metadata_offsets = metadata_offsets,
                         .meta_ids = {},
                         .value_bytes = block.value_bytes(),
                         .value_offsets = block.value_offsets()});
}

VariantValueRef ColumnVariantV2::get_value_ref(size_t row) const {
    DCHECK(!_typed);
    DCHECK(_typed_type == nullptr);
    DCHECK_LT(row, size());
    const auto& metadata_ids = assert_cast<const MetaIdsColumn&>(*_meta_ids).get_data();
    const uint32_t metadata_id = metadata_ids[row];
    DCHECK_LT(metadata_id, _metadatas->size());
    const StringRef metadata =
            assert_cast<const ColumnString&>(*_metadatas).get_data_at(metadata_id);
    const StringRef value = assert_cast<const ColumnString&>(*_values).get_data_at(row);
    return {.metadata = {.data = metadata.data, .size = metadata.size},
            .data = value.data,
            .size = value.size};
}

Field ColumnVariantV2::operator[](size_t) const {
    throw_deferred("operator[]", "T1.7b Field rebind");
}

void ColumnVariantV2::get(size_t, Field&) const {
    throw_deferred("get", "T1.7b Field rebind");
}

void ColumnVariantV2::insert(const Field&) {
    throw_deferred("insert(Field)", "T1.7b Field rebind");
}

void ColumnVariantV2::insert_default() {
    insert_many_defaults(1);
}

void ColumnVariantV2::insert_many_defaults(size_t length) {
    if (length == 0) {
        return;
    }

    if (_typed) {
        require_exclusive(_typed, "typed column");
        DORIS_CHECK_LE(length, std::numeric_limits<size_t>::max() - size())
                << "default row count overflows size_t";
        // ColumnNullable::insert_many_defaults would append SQL/JSON nulls, while the IColumn
        // default for Variant is the empty object. clone_resized converts the existing typed rows
        // to encoded form before appending that canonical Variant default.
        MutableColumnPtr replacement_base = clone_resized(size() + length);
        auto& replacement = assert_cast<ColumnVariantV2&>(*replacement_base);
        _adopt_state_from(replacement);
        return;
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

    const auto& metadata_ptr = static_cast<const IColumn::Ptr&>(_metadatas);
    const size_t old_metadata_count = metadata_ptr->size();
    IColumn::Ptr original_metadata;
    if (!metadata_ptr->is_exclusive()) {
        original_metadata = metadata_ptr;
    }
    const size_t old_rows = size();
    try {
        const uint32_t metadata_id = _find_or_insert_metadata(
                {EMPTY_OBJECT_METADATA.data(), EMPTY_OBJECT_METADATA.size()});
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
    } catch (...) {
        if (metadata_ids.size() > old_rows) {
            metadata_ids.pop_back(metadata_ids.size() - old_rows);
        }
        if (values.size() > old_rows) {
            values.pop_back(values.size() - old_rows);
        }
        _rollback_metadata(old_metadata_count, std::move(original_metadata));
        _check_invariants();
        throw;
    }
    _check_invariants();
}

void ColumnVariantV2::insert_from(const IColumn& src, size_t row) {
    insert_range_from(src, row, 1);
}

// Range insertion handles typed/encoded state pairs under one rollback boundary.
void ColumnVariantV2::insert_range_from( // NOLINT(readability-function-size)
        const IColumn& src, size_t start, size_t length) {
    const auto& source = assert_cast<const ColumnVariantV2&>(src);
    DORIS_CHECK_LE(start, source.size()) << "source range starts past source size";
    DORIS_CHECK_LE(length, source.size() - start) << "source range exceeds source size";
    if (length == 0) {
        return;
    }

    if (_typed && source._typed && exact_typed_identity(_typed_type, source._typed_type)) {
        require_exclusive(_typed, "typed column");
        MutableColumnPtr candidate = IColumn::mutate(static_cast<const IColumn::Ptr&>(_typed));
        candidate->insert_range_from(*source._typed, start, length);
        static_cast<IColumn::Ptr&>(_typed) = std::move(candidate);
        _check_invariants();
        return;
    }

    if (source._typed) {
        MutableColumnPtr selected = source._typed->clone_empty();
        selected->insert_range_from(*source._typed, start, length);
        auto encoded_source = ColumnVariantV2::create();
        static_cast<IColumn::Ptr&>(encoded_source->_typed) = std::move(selected);
        encoded_source->_typed_type = source._typed_type;
        encoded_source->_check_invariants();
        encoded_source->ensure_encoded();
        if (!_typed) {
            insert_range_from(*encoded_source, 0, length);
            return;
        }

        require_exclusive(_typed, "typed column");
        auto replacement = ColumnVariantV2::create();
        static_cast<IColumn::Ptr&>(replacement->_typed) = _typed->clone_resized(size());
        replacement->_typed_type = _typed_type;
        replacement->ensure_encoded();
        replacement->insert_range_from(*encoded_source, 0, length);
        _adopt_state_from(*replacement);
        return;
    }

    if (_typed) {
        require_exclusive(_typed, "typed column");
        auto replacement = ColumnVariantV2::create();
        static_cast<IColumn::Ptr&>(replacement->_typed) = _typed->clone_resized(size());
        replacement->_typed_type = _typed_type;
        replacement->ensure_encoded();
        replacement->insert_range_from(source, start, length);
        _adopt_state_from(*replacement);
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
    DorisVector<uint32_t> appended_ids =
            collect_range_metadata_ids(source_metadata_ids, source_metadatas.size(), start, length);

    const auto& source_values = assert_cast<const ColumnString&>(*source._values);
    const auto& source_offsets = source_values.get_offsets();
    const size_t value_begin = source_offsets[static_cast<ssize_t>(start) - 1];
    const size_t value_end = source_offsets[start + length - 1];
    const bool destination_has_no_metadata = static_cast<const IColumn::Ptr&>(_metadatas)->empty();
    const bool adopt_metadata = empty() && destination_has_no_metadata;
    const bool already_shared = static_cast<const IColumn::Ptr&>(_metadatas).get() ==
                                static_cast<const IColumn::Ptr&>(source._metadatas).get();
    const bool metadata_may_change = adopt_metadata || !already_shared;
    DorisVector<uint32_t> remap;
    if (!adopt_metadata && !already_shared) {
        remap.assign(source_metadatas.size(), UNMAPPED_METADATA_ID);
    }
    auto& values = assert_cast<ColumnString&>(*_values);
    auto& metadata_ids = assert_cast<MetaIdsColumn&>(*_meta_ids);
    reserve_rows(values, metadata_ids, value_end - value_begin, length);
    const auto& metadata_ptr = static_cast<const IColumn::Ptr&>(_metadatas);
    const size_t old_metadata_count = metadata_ptr->size();
    IColumn::Ptr original_metadata;
    if (adopt_metadata || (!already_shared && !metadata_ptr->is_exclusive())) {
        original_metadata = metadata_ptr;
    }

    const size_t old_rows = size();
    try {
        if (adopt_metadata) {
            _metadatas = source._metadatas;
            _meta_index.reset();
        }
        const bool shared_metadata = static_cast<const IColumn::Ptr&>(_metadatas).get() ==
                                     static_cast<const IColumn::Ptr&>(source._metadatas).get();
        if (!shared_metadata) {
            for (uint32_t& source_id : appended_ids) {
                if (remap[source_id] == UNMAPPED_METADATA_ID) {
                    remap[source_id] =
                            _find_or_insert_metadata(source_metadatas.get_data_at(source_id));
                }
                source_id = remap[source_id];
            }
        }
        values.insert_range_from(source_values, start, length);
        append_metadata_ids(metadata_ids, appended_ids);
    } catch (...) {
        if (metadata_ids.size() > old_rows) {
            metadata_ids.pop_back(metadata_ids.size() - old_rows);
        }
        if (values.size() > old_rows) {
            values.pop_back(values.size() - old_rows);
        }
        if (metadata_may_change) {
            _rollback_metadata(old_metadata_count, std::move(original_metadata));
        }
        _check_invariants();
        throw;
    }
    _check_invariants();
}

// Indexed insertion handles typed/encoded state pairs under one rollback boundary.
void ColumnVariantV2::insert_indices_from( // NOLINT(readability-function-size)
        const IColumn& src, const uint32_t* indices_begin, const uint32_t* indices_end) {
    const auto& source = assert_cast<const ColumnVariantV2&>(src);
    const size_t rows = validate_selected_indices(indices_begin, indices_end, source.size());
    if (rows == 0) {
        return;
    }

    if (_typed && source._typed && exact_typed_identity(_typed_type, source._typed_type)) {
        require_exclusive(_typed, "typed column");
        MutableColumnPtr candidate = IColumn::mutate(static_cast<const IColumn::Ptr&>(_typed));
        candidate->insert_indices_from(*source._typed, indices_begin, indices_end);
        static_cast<IColumn::Ptr&>(_typed) = std::move(candidate);
        _check_invariants();
        return;
    }

    if (source._typed) {
        MutableColumnPtr selected = source._typed->clone_empty();
        selected->insert_indices_from(*source._typed, indices_begin, indices_end);
        auto encoded_source = ColumnVariantV2::create();
        static_cast<IColumn::Ptr&>(encoded_source->_typed) = std::move(selected);
        encoded_source->_typed_type = source._typed_type;
        encoded_source->_check_invariants();
        encoded_source->ensure_encoded();
        if (!_typed) {
            insert_range_from(*encoded_source, 0, rows);
            return;
        }

        require_exclusive(_typed, "typed column");
        auto replacement = ColumnVariantV2::create();
        static_cast<IColumn::Ptr&>(replacement->_typed) = _typed->clone_resized(size());
        replacement->_typed_type = _typed_type;
        replacement->ensure_encoded();
        replacement->insert_range_from(*encoded_source, 0, rows);
        _adopt_state_from(*replacement);
        return;
    }

    if (_typed) {
        require_exclusive(_typed, "typed column");
        auto replacement = ColumnVariantV2::create();
        static_cast<IColumn::Ptr&>(replacement->_typed) = _typed->clone_resized(size());
        replacement->_typed_type = _typed_type;
        replacement->ensure_encoded();
        replacement->insert_indices_from(source, indices_begin, indices_end);
        _adopt_state_from(*replacement);
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
    SelectedRows selected = collect_selected_rows(source_metadata_ids, source_metadatas,
                                                  source_values, indices_begin, rows);
    auto& appended_ids = selected.metadata_ids;

    const bool destination_has_no_metadata = static_cast<const IColumn::Ptr&>(_metadatas)->empty();
    const bool adopt_metadata = empty() && destination_has_no_metadata;
    const bool already_shared = static_cast<const IColumn::Ptr&>(_metadatas).get() ==
                                static_cast<const IColumn::Ptr&>(source._metadatas).get();
    const bool metadata_may_change = adopt_metadata || !already_shared;
    DorisVector<uint32_t> remap;
    if (!adopt_metadata && !already_shared) {
        remap.assign(source_metadatas.size(), UNMAPPED_METADATA_ID);
    }
    auto& values = assert_cast<ColumnString&>(*_values);
    auto& metadata_ids = assert_cast<MetaIdsColumn&>(*_meta_ids);
    reserve_rows(values, metadata_ids, selected.value_bytes, rows);
    const auto& metadata_ptr = static_cast<const IColumn::Ptr&>(_metadatas);
    const size_t old_metadata_count = metadata_ptr->size();
    IColumn::Ptr original_metadata;
    if (adopt_metadata || (!already_shared && !metadata_ptr->is_exclusive())) {
        original_metadata = metadata_ptr;
    }

    const size_t old_rows = size();
    try {
        if (adopt_metadata) {
            _metadatas = source._metadatas;
            _meta_index.reset();
        }
        const bool shared_metadata = static_cast<const IColumn::Ptr&>(_metadatas).get() ==
                                     static_cast<const IColumn::Ptr&>(source._metadatas).get();
        if (!shared_metadata) {
            for (uint32_t& source_id : appended_ids) {
                if (remap[source_id] == UNMAPPED_METADATA_ID) {
                    remap[source_id] =
                            _find_or_insert_metadata(source_metadatas.get_data_at(source_id));
                }
                source_id = remap[source_id];
            }
        }
        values.insert_indices_from(source_values, indices_begin, indices_end);
        append_metadata_ids(metadata_ids, appended_ids);
    } catch (...) {
        if (metadata_ids.size() > old_rows) {
            metadata_ids.pop_back(metadata_ids.size() - old_rows);
        }
        if (values.size() > old_rows) {
            values.pop_back(values.size() - old_rows);
        }
        if (metadata_may_change) {
            _rollback_metadata(old_metadata_count, std::move(original_metadata));
        }
        _check_invariants();
        throw;
    }
    _check_invariants();
}

void ColumnVariantV2::pop_back(size_t length) {
    DORIS_CHECK_LE(length, size()) << "pop_back length exceeds the column size";
    if (length == 0) {
        return;
    }
    if (_typed) {
        require_exclusive(_typed, "typed column");
        static_cast<IColumn::Ptr&>(_typed) = _typed->clone_resized(size() - length);
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
    const VariantValueRef value = parse_canonical_serialized({pos, length});
    const std::array<uint32_t, 2> metadata_offsets {0, static_cast<uint32_t>(value.metadata.size)};
    const std::array<uint32_t, 2> value_offsets {0, static_cast<uint32_t>(value.size)};
    insert_encoded_rows({.metadata_bytes = {value.metadata.data, value.metadata.size},
                         .metadata_offsets = metadata_offsets,
                         .meta_ids = {},
                         .value_bytes = {value.data, value.size},
                         .value_offsets = value_offsets});
}

StringRef ColumnVariantV2::serialize_value_into_arena(size_t row, Arena& arena,
                                                      const char*& begin) const {
    DCHECK_LT(row, size());
    if (_typed) {
        const auto& nullable = assert_cast<const ColumnNullable&>(*_typed);
        StringRef serialized;
        dispatch_typed_column(
                nullable, _typed_type->get_primitive_type(),
                [&]<PrimitiveType Type>(const auto& column) {
                    visit_typed_canonical_rows<Type>(
                            nullable, column, _typed_type->get_scale(), row, row + 1,
                            [&](size_t, auto&& canonical_factory) {
                                const VariantCanonicalScalarRef canonical = canonical_factory();
                                const CanonicalScalarSerializationPlan plan =
                                        prepare_canonical_serialize(canonical);
                                char* destination = arena.alloc_continue(plan.size(), begin);
                                plan.write(destination, plan.size());
                                serialized = {destination, plan.size()};
                            });
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
        dispatch_typed_column(
                nullable, _typed_type->get_primitive_type(),
                [&]<PrimitiveType Type>(const auto& column) {
                    visit_typed_canonical_rows<Type>(
                            nullable, column, _typed_type->get_scale(), row, row + 1,
                            [&](size_t, auto&& canonical_factory) {
                                const VariantCanonicalScalarRef canonical = canonical_factory();
                                serialized_size = prepare_canonical_serialize(canonical).size();
                            });
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
        dispatch_typed_column(nullable, _typed_type->get_primitive_type(),
                              [&]<PrimitiveType Type>(const auto& column) {
                                  visit_typed_canonical_rows<Type>(
                                          nullable, column, _typed_type->get_scale(), row, row + 1,
                                          [&](size_t, auto&& canonical_factory) {
                                              const VariantCanonicalScalarRef canonical =
                                                      canonical_factory();
                                              const CanonicalScalarSerializationPlan plan =
                                                      prepare_canonical_serialize(canonical);
                                              plan.write(pos, plan.size());
                                              serialized_size = plan.size();
                                          });
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
        dispatch_typed_column(
                nullable, _typed_type->get_primitive_type(),
                [&]<PrimitiveType Type>(const auto& column) {
                    visit_typed_canonical_rows<Type>(
                            nullable, column, _typed_type->get_scale(), 0, size(),
                            [&](size_t, auto&& canonical_factory) {
                                const VariantCanonicalScalarRef canonical = canonical_factory();
                                maximum_size =
                                        std::max(maximum_size,
                                                 prepare_canonical_serialize(canonical).size());
                            });
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
    if (_typed) {
        const auto& nullable = assert_cast<const ColumnNullable&>(*_typed);
        dispatch_typed_column(
                nullable, _typed_type->get_primitive_type(),
                [&]<PrimitiveType Type>(const auto& column) {
                    visit_typed_canonical_rows<Type>(
                            nullable, column, _typed_type->get_scale(), 0, num_rows,
                            [&](size_t row, auto&& canonical_factory) {
                                const VariantCanonicalScalarRef canonical = canonical_factory();
                                const CanonicalScalarSerializationPlan plan =
                                        prepare_canonical_serialize(canonical);
                                DCHECK(keys[row].data != nullptr);
                                DCHECK_LE(plan.size(),
                                          std::numeric_limits<size_t>::max() - keys[row].size);
                                plan.write(const_cast<char*>(keys[row].data) + keys[row].size,
                                           plan.size());
                                keys[row].size += plan.size();
                            });
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
        DCHECK_GE(keys[row].size, CANONICAL_SIZE_PREFIX);
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
        dispatch_typed_column(nullable, _typed_type->get_primitive_type(),
                              [&]<PrimitiveType Type>(const auto& column) {
                                  visit_typed_canonical_rows<Type>(
                                          nullable, column, _typed_type->get_scale(), row, row + 1,
                                          [&](size_t, auto&& canonical_factory) {
                                              const VariantCanonicalScalarRef canonical =
                                                      canonical_factory();
                                              canonical_hash(canonical, hash);
                                          });
                              });
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
    if (_typed) {
        require_exclusive(_typed, "typed column");
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
    result->_meta_index.reset();
    result->_check_invariants();
    return result;
}

void ColumnVariantV2::resize(size_t new_size) {
    const size_t old_size = size();
    if (_typed) {
        if (new_size == old_size) {
            return;
        }
        require_exclusive(_typed, "typed column");
        MutableColumnPtr replacement_base = clone_resized(new_size);
        auto& replacement = assert_cast<ColumnVariantV2&>(*replacement_base);
        _adopt_state_from(replacement);
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
    return _find_or_insert_metadata(metadata,
                                    HashUtil::xxHash64WithSeed(metadata.data, metadata.size, 0));
}

uint32_t ColumnVariantV2::_find_or_insert_metadata(StringRef metadata, uint64_t hash) {
    DORIS_CHECK(metadata.data != nullptr || metadata.size == 0)
            << "metadata bytes have a null pointer";
    const auto& current_metadatas =
            assert_cast<const ColumnString&>(*static_cast<const IColumn::Ptr&>(_metadatas));
    if (_meta_index == nullptr) {
        auto index = std::make_unique<MetaDictIndex>();
        for (size_t id = 0; id < current_metadatas.size(); ++id) {
            const StringRef current = current_metadatas.get_data_at(id);
            const uint64_t current_hash = HashUtil::xxHash64WithSeed(current.data, current.size, 0);
            index->buckets[current_hash].push_back(static_cast<uint32_t>(id));
        }
        _meta_index = std::move(index);
    }

    auto& candidates = _meta_index->buckets[hash];
    for (uint32_t id : candidates) {
        DORIS_CHECK_LT(id, current_metadatas.size()) << "metadata index candidate is out of range";
        if (current_metadatas.get_data_at(id) == metadata) {
            return id;
        }
    }
    if (current_metadatas.size() == std::numeric_limits<uint32_t>::max()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "ColumnVariantV2 metadata dictionary exceeds the uint32 id limit");
    }

    candidates.reserve(candidates.size() + 1);
    _detach_metadata_for_write();
    auto& metadatas = assert_cast<ColumnString&>(*_metadatas);
    const size_t new_chars_size = metadatas.get_chars().size() + metadata.size;
    ColumnString::check_chars_length(new_chars_size, metadatas.size() + 1, size());
    metadatas.get_chars().reserve(new_chars_size);
    metadatas.get_offsets().reserve(metadatas.size() + 1);
    const auto id = static_cast<uint32_t>(metadatas.size());
    metadatas.insert_data(metadata.data, metadata.size);
    candidates.push_back(id);
    return id;
}

void ColumnVariantV2::_adopt_state_from(ColumnVariantV2& replacement) {
    DORIS_CHECK(this != &replacement) << "cannot adopt ColumnVariantV2 state from itself";
    _metadatas = std::move(replacement._metadatas);
    _meta_ids = std::move(replacement._meta_ids);
    _values = std::move(replacement._values);
    _typed = std::move(replacement._typed);
    _typed_type = std::move(replacement._typed_type);
    _meta_index.reset();
    _check_invariants();
}

void ColumnVariantV2::_detach_metadata_for_write() {
    auto& metadata_ptr = static_cast<IColumn::Ptr&>(_metadatas);
    if (!metadata_ptr->is_exclusive()) {
        // The index stores hashes and ids only, so a byte-preserving COW detach cannot invalidate it.
        metadata_ptr = std::move(*metadata_ptr).mutate();
    }
}

void ColumnVariantV2::_rollback_metadata(size_t old_count, IColumn::Ptr original_metadata) {
    if (original_metadata) {
        static_cast<IColumn::Ptr&>(_metadatas) = std::move(original_metadata);
    } else {
        auto& metadatas = assert_cast<ColumnString&>(*_metadatas);
        DORIS_CHECK_GE(metadatas.size(), old_count) << "metadata dictionary shrank during append";
        if (metadatas.size() > old_count) {
            metadatas.pop_back(metadatas.size() - old_count);
        }
    }
    _meta_index.reset();
}

void ColumnVariantV2::_check_invariants() const {
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
    const auto& metadatas = assert_cast<const ColumnString&>(*_metadatas);
    const auto& metadata_ids = assert_cast<const MetaIdsColumn&>(*_meta_ids).get_data();
    const auto& values = assert_cast<const ColumnString&>(*_values);
    DORIS_CHECK_EQ(metadata_ids.size(), values.size())
            << "ColumnVariantV2 encoded row counts differ";
    for (uint32_t metadata_id : metadata_ids) {
        DORIS_CHECK_LT(metadata_id, metadatas.size())
                << "ColumnVariantV2 metadata id is out of range";
    }
}

} // namespace doris
