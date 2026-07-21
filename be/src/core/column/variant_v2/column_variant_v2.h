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

#include <array>
#include <limits>
#include <span>
#include <string>

#include "common/exception.h"
#include "core/assert_cast.h"
#include "core/column/column.h"
#include "core/column/column_const.h"
#include "core/column/variant_v2/column_variant_v2_typed_column.h"
#include "core/custom_allocator.h"
#include "core/data_type/data_type.h"
#include "core/value/variant/variant_parquet_encoding.h"
#include "core/value/variant/variant_value.h"

namespace doris {

class DataTypeVariantV2SerDe;
class VariantBatchBuilder;

// ColumnVariantV2 stores a whole column in exactly one state: encoded Variant bytes or one nullable
// typed scalar column. Mixed operations materialize the typed state as encoded bytes on demand.
class ColumnVariantV2 final : public COWHelper<IColumn, ColumnVariantV2> {
public:
    struct EncodedDataView {
        StringRef metadata_bytes;
        std::span<const uint32_t> metadata_offsets;
        std::span<const uint32_t> meta_ids;
        StringRef value_bytes;
        std::span<const uint32_t> value_offsets;
    };

    // Borrowed immutable adapter for whole-column E/T readers. The source column owns every
    // referenced column, type, and byte; any structural mutation invalidates this view. Encoded
    // bytes have already been validated at their insertion or deserialization boundary.
    class ReadView {
    public:
        bool is_typed() const noexcept { return _typed_state; }
        size_t size() const noexcept;
        size_t metadata_count() const noexcept;
        uint32_t metadata_id_at(size_t row) const;
        VariantMetadataRef metadata_at(uint32_t id) const;
        VariantRef value_at(size_t row) const;
        const IColumn& typed_column() const;
        const DataTypePtr& typed_type() const;

    private:
        friend class ColumnVariantV2;
        ReadView(const IColumn* metadatas, const IColumn* metadata_ids, const IColumn* values);
        ReadView(const IColumn* typed, const DataTypePtr* typed_type);

        bool _typed_state = false;
        const IColumn* _metadatas = nullptr;
        const IColumn* _metadata_ids = nullptr;
        const IColumn* _values = nullptr;
        const IColumn* _typed = nullptr;
        const DataTypePtr* _typed_type = nullptr;
    };

#ifdef BE_TEST
    // Narrow unit-test seam for encoded-state invariant coverage.
    struct TestAccess {
        static void replace_encoded_subcolumn(ColumnVariantV2& column, size_t index,
                                              ColumnPtr replacement);
    };
#endif

    // The input must be an exact, non-Const ColumnNullable whose nested column matches the
    // non-nullable supported scalar type.
    static MutablePtr create_typed(ColumnPtr column, DataTypePtr scalar_type);

    bool is_typed() const noexcept { return _typed != nullptr; }
    const IColumn& typed_column() const;
    const DataTypePtr& typed_type() const;
    void ensure_encoded();
    ReadView read_view() const;

    std::string get_name() const override;
    size_t size() const override;
    size_t byte_size() const override;
    size_t allocated_bytes() const override;
    bool has_enough_capacity(const IColumn& src) const override;
    bool is_variable_length() const override { return true; }
    bool structure_equals(const IColumn& rhs) const override;

    void sanity_check() const override;
    void for_each_subcolumn(ColumnCallback callback) const override;
    void clear() override;
    void finalize() override {}

    // Validates the borrowed buffer/offset/id structure, then appends codec-validated encoded rows
    // without retaining any input pointer. Offsets use the ColumnString uint32 domain and start at
    // zero. Empty meta_ids is the compact representation for a batch whose rows all use its single
    // metadata blob. Input buffers must not alias this column; use insert_range_from for that case.
    void insert_encoded_rows(const EncodedDataView& data);

    // Direct trusted codec adapter. VariantBatchBuilder already produces canonical metadata,
    // validated values, and ColumnString-compatible uint32 offsets, so this path copies its buffers
    // without validating the encoded tree a second time.
    void insert_encoded_batch(const VariantBatchBuilder& block);

    // The returned view borrows this column's metadata and value buffers. Any structural mutation,
    // including insert, clear, COW mutation, or future row transformations, may invalidate it.
    VariantRef get_value_ref(size_t row) const;

    Field operator[](size_t row) const override;
    void get(size_t row, Field& result) const override;
    void insert(const Field& field) override;
    void insert_default() override;
    void insert_many_defaults(size_t length) override;

    void insert_from(const IColumn& src, size_t row) override;
    void insert_range_from(const IColumn& src, size_t start, size_t length) override;
    void insert_indices_from(const IColumn& src, const uint32_t* indices_begin,
                             const uint32_t* indices_end) override;
    void pop_back(size_t length) override;

    StringRef get_data_at(size_t row) const override;
    void insert_data(const char* pos, size_t length) override;
    StringRef serialize_value_into_arena(size_t row, Arena& arena,
                                         const char*& begin) const override;
    const char* deserialize_and_insert_from_arena(const char* pos) override;
    size_t serialize_size_at(size_t row) const override;
    size_t serialize_impl(char* pos, size_t row) const override;
    size_t deserialize_impl(const char* pos) override;
    size_t get_max_row_byte_size() const override;
    void serialize(StringRef* keys, size_t num_rows) const override;
    void deserialize(StringRef* keys, size_t num_rows) override;

    void update_hash_with_value(size_t row, SipHash& hash) const override;
    void update_hashes_with_value(uint64_t* __restrict hashes,
                                  const uint8_t* __restrict null_data) const override;
    void update_xxHash_with_value(size_t start, size_t end, uint64_t& hash,
                                  const uint8_t* __restrict null_data) const override;
    void update_crcs_with_value(uint32_t* __restrict hashes, PrimitiveType type, uint32_t rows,
                                uint32_t offset,
                                const uint8_t* __restrict null_data) const override;
    void update_crc_with_value(size_t start, size_t end, uint32_t& hash,
                               const uint8_t* __restrict null_data) const override;
    void update_crc32c_batch(uint32_t* __restrict hashes,
                             const uint8_t* __restrict null_map) const override;
    void update_crc32c_single(size_t start, size_t end, uint32_t& hash,
                              const uint8_t* __restrict null_map) const override;
    void replace_column_null_data(const uint8_t* __restrict null_map) override;

    ColumnPtr filter(const Filter& filter, ssize_t result_size_hint) const override;
    size_t filter(const Filter& filter) override;
    MutableColumnPtr permute(const Permutation& permutation, size_t limit) const override;
    MutableColumnPtr clone_resized(size_t size) const override;
    void resize(size_t size) override;

    void get_permutation(bool reverse, size_t limit, int nan_direction_hint, HybridSorter& sorter,
                         Permutation& result) const override;
    void replace_column_data(const IColumn& rhs, size_t row, size_t self_row = 0) override;

private:
    friend class COWHelper<IColumn, ColumnVariantV2>;
    friend class DataTypeVariantV2SerDe;

    ColumnVariantV2();
    ColumnVariantV2(const ColumnVariantV2& other);

    uint32_t _find_or_insert_metadata(StringRef metadata);
    void _adopt_state_from(ColumnVariantV2& replacement);
    void _detach_metadata_for_write();
    void _check_invariants() const;
    void mutate_subcolumns() override;

    // Encoded state: each row owns a value and references one deduplicated metadata blob. The
    // uint32 id costs four bytes per encoded row, but avoids repeating object-key metadata and
    // gives canonical comparison, hashing, subpath lookup, and binary SerDe O(1) schema access.
    // It is required because valid external Variant rows may use different metadata dictionaries.
    IColumn::WrappedPtr _metadatas;
    IColumn::WrappedPtr _meta_ids;
    IColumn::WrappedPtr _values;

    // A non-null _typed always means all encoded buffers are empty and the entire column has the
    // single type described by _typed_type.
    IColumn::WrappedPtr _typed;
    DataTypePtr _typed_type;
};

namespace column_variant_v2_internal {

using ForcedNulls = std::span<const NullMap::value_type>;

template <typename NullCallback, typename ValueCallback>
// Any VariantRef passed to on_value borrows either the source column or the reusable local buffer
// and is valid only until that callback returns. Callbacks must not retain it.
void visit_variant_values(const IColumn& source, size_t start, size_t end, ForcedNulls outer_nulls,
                          NullCallback&& on_null, ValueCallback&& on_value) {
    const IColumn* physical = &source;
    bool constant = false;
    if (const auto* const_column = check_and_get_column<ColumnConst>(source)) {
        physical = &const_column->get_data_column();
        constant = true;
    }
    const auto* variant = check_and_get_column<ColumnVariantV2>(*physical);
    if (variant == nullptr) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant reader requires ColumnVariantV2, got {}", source.get_name());
    }
    if (start > end || end > source.size()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant row range [{}, {}) exceeds column size {}", start, end,
                        source.size());
    }
    if (!outer_nulls.empty() && outer_nulls.size() < end) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant null map size {} is smaller than row end {}", outer_nulls.size(),
                        end);
    }

    const auto view = variant->read_view();
    if (!view.is_typed()) {
        for (size_t row = start; row < end; ++row) {
            if (!outer_nulls.empty() && outer_nulls[row] != 0) {
                on_null(row);
            } else {
                on_value(row, view.value_at(constant ? 0 : row));
            }
        }
        return;
    }

    const auto& nullable = assert_cast<const ColumnNullable&>(view.typed_column());
    const uint32_t scale = view.typed_type()->get_scale();
    DORIS_CHECK_LE(scale, static_cast<uint32_t>(std::numeric_limits<uint8_t>::max()));
    const auto& inner_nulls = nullable.get_null_map_data();
    static constexpr std::array<char, 3> EMPTY_METADATA {
            static_cast<char>(VARIANT_ENCODING_VERSION | VARIANT_METADATA_SORTED_STRINGS_MASK), 0,
            0};
    DorisVector<char> scratch;
    auto emit = [&](size_t row, VariantScalarEncodingPlan plan) {
        scratch.resize(plan.size());
        plan.write(scratch.data(), scratch.size());
        on_value(row, VariantRef {.metadata = {.data = EMPTY_METADATA.data(),
                                               .size = EMPTY_METADATA.size()},
                                  .value = {scratch.data(), scratch.size()}});
    };
    dispatch_typed_column(nullable, view.typed_type()->get_primitive_type(),
                          [&]<PrimitiveType Type>(const auto& nested) {
                              for (size_t row = start; row < end; ++row) {
                                  if (!outer_nulls.empty() && outer_nulls[row] != 0) {
                                      on_null(row);
                                      continue;
                                  }
                                  const size_t physical_row = constant ? 0 : row;
                                  if (inner_nulls[physical_row] != 0) {
                                      emit(row, VariantScalarEncodingPlan::null_value());
                                      continue;
                                  }
                                  with_typed_scalar<Type>(nested, physical_row,
                                                          static_cast<uint8_t>(scale),
                                                          [&](auto&& physical_factory, auto&&) {
                                                              emit(row, physical_factory());
                                                          });
                              }
                          });
}

} // namespace column_variant_v2_internal

} // namespace doris
