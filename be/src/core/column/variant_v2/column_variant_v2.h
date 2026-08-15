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
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2_typed_column.h"
#include "core/custom_allocator.h"
#include "core/data_type/data_type.h"
#include "core/value/variant/variant_parquet_encoding.h"
#include "core/value/variant/variant_value.h"
#include "util/json/path_in_data.h"

namespace doris {

class DataTypeVariantV2SerDe;
class VariantBatchBuilder;
class VariantElementV2ResultBuilder;
class VariantShreddedColumnBuilder;

// ColumnVariantV2 stores a whole column in exactly one representation. The shredded representation
// keeps encoded residual rows plus dense shredded fields whose values are themselves non-shredded
// ColumnVariantV2 columns.
class ColumnVariantV2 final : public COWHelper<IColumn, ColumnVariantV2> {
public:
    using MetadataIdsColumn = ColumnVector<TYPE_UINT32>;

    enum class Representation : uint8_t {
        ENCODED,
        TYPED_SCALAR,
        SHREDDED,
    };

    struct ShreddedField {
        ShreddedField(PathInData path, MutableColumnPtr values, ColumnUInt8::MutablePtr presence);

        PathInData path;
        IColumn::WrappedPtr values;
        ColumnUInt8::WrappedPtr presence;

    private:
        friend class ColumnVariantV2;

        // Keeps immutable COW owners for row-preserving projections. Any later mutation of the
        // containing ColumnVariantV2 recursively detaches these shared children first.
        static ShreddedField share(PathInData path, ColumnPtr values, ColumnUInt8::Ptr presence);
        struct SharedOwnerTag {};
        ShreddedField(PathInData path, ColumnPtr values, ColumnUInt8::Ptr presence, SharedOwnerTag);
    };

    using ShreddedFields = DorisVector<ShreddedField>;

    struct EncodedDataView {
        StringRef metadata_bytes;
        std::span<const uint32_t> metadata_offsets;
        std::span<const uint32_t> meta_ids;
        StringRef value_bytes;
        std::span<const uint32_t> value_offsets;
    };

    // Borrowed immutable adapter for whole-column E/T/S readers. The source column owns every
    // referenced column, type, path, and byte; any structural mutation invalidates this view.
    class ReadView {
    public:
        Representation representation() const noexcept { return _representation; }
        bool is_encoded() const noexcept { return _representation == Representation::ENCODED; }
        bool is_typed() const noexcept { return _representation == Representation::TYPED_SCALAR; }
        bool is_shredded() const noexcept { return _representation == Representation::SHREDDED; }
        size_t size() const noexcept;
        size_t metadata_count() const noexcept;
        uint32_t metadata_id_at(size_t row) const;
        VariantMetadataRef metadata_at(uint32_t id) const;
        VariantRef value_at(size_t row) const;
        const IColumn& typed_column() const;
        const DataTypePtr& typed_type() const;
        size_t shredded_field_count() const noexcept;
        const PathInData& shredded_field_path(size_t index) const;
        const ColumnVariantV2& shredded_field_values(size_t index) const;
        const ColumnUInt8& shredded_field_presence(size_t index) const;
        size_t residual_metadata_count() const noexcept;
        uint32_t residual_metadata_id_at(size_t row) const;
        VariantMetadataRef residual_metadata_at(uint32_t id) const;
        VariantRef residual_value_at(size_t row) const;

    private:
        friend class ColumnVariantV2;
        ReadView(const ColumnString* metadatas, const MetadataIdsColumn* metadata_ids,
                 const ColumnString* values);
        ReadView(const IColumn* typed, const DataTypePtr* typed_type);
        ReadView(const ColumnString* metadatas, const MetadataIdsColumn* metadata_ids,
                 const ColumnString* values, const ShreddedFields* shredded_fields);

        Representation _representation = Representation::ENCODED;
        const ColumnString* _metadatas = nullptr;
        const MetadataIdsColumn* _metadata_ids = nullptr;
        const ColumnString* _values = nullptr;
        const IColumn* _typed = nullptr;
        const DataTypePtr* _typed_type = nullptr;
        const ShreddedFields* _shredded_fields = nullptr;
    };

#ifdef BE_TEST
    // Narrow unit-test seam for encoded-state invariant coverage.
    struct TestAccess {
        static void replace_metadata_ids(ColumnVariantV2& column,
                                         MetadataIdsColumn::Ptr replacement);
        static void replace_values(ColumnVariantV2& column, ColumnString::Ptr replacement);
        static size_t shredded_conflict_slow_rows(const ColumnVariantV2& column);
        static size_t full_shredded_validations(const ColumnVariantV2& column);
        static size_t encoded_range_materializations(const ColumnVariantV2& column);
        static void ensure_encoded(ColumnVariantV2& column);
    };
#endif

    // The input must be an exact, non-Const ColumnNullable whose nested column matches the
    // non-nullable supported scalar type.
    static MutablePtr create_typed(ColumnPtr column, DataTypePtr scalar_type);
    // residual must be encoded. Every field must own an exact non-shredded ColumnVariantV2 and a
    // dense 0/1 presence column with the same row count. presence[row] == 1 means the child uniquely
    // owns that logical path (including a Variant JSON null); presence[row] == 0 makes the child
    // slot padding and allows residual to own only an object/array at the exact path or a structural
    // ancestor conflict. Outer SQL NULL remains the surrounding ColumnNullable's responsibility.
    // Active child rows must be scalars. Paths are normalized, sorted, unique, and prefix-free
    // before publication. Ownership is checked without materializing an encoded copy.
    static MutablePtr create_shredded(MutablePtr residual, ShreddedFields fields);

    Representation representation() const noexcept;
    bool is_encoded() const noexcept { return representation() == Representation::ENCODED; }
    bool is_typed() const noexcept { return representation() == Representation::TYPED_SCALAR; }
    bool is_shredded() const noexcept { return representation() == Representation::SHREDDED; }
    const IColumn& typed_column() const;
    const DataTypePtr& typed_type() const;
    size_t shredded_field_count() const noexcept { return _shredded_fields.size(); }
    const PathInData& shredded_field_path(size_t index) const;
    const ColumnVariantV2& shredded_field_values(size_t index) const;
    const ColumnUInt8& shredded_field_presence(size_t index) const;
    // Builds a row-preserving S projection with a newly owned encoded residual and immutable COW
    // owners for a contiguous source field range whose paths are descendants of the removed
    // prefix. The output remains safe after the source Block is released; mutation goes through
    // mutate_subcolumns() and detaches shared children.
    MutablePtr project_shredded_fields(MutablePtr projected_residual, size_t first_field,
                                       size_t field_count, size_t removed_prefix_parts) const;
    MutablePtr materialize_encoded_range(size_t start, size_t length) const;
    ReadView read_view() const;

    std::string get_name() const override;
    size_t size() const override;
    size_t byte_size() const override;
    size_t allocated_bytes() const override;
    bool has_enough_capacity(const IColumn& src) const override;
    bool is_exclusive() const override;
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
    friend class VariantElementV2ResultBuilder;
    friend class VariantShreddedColumnBuilder;

    ColumnVariantV2();
    ColumnVariantV2(const ColumnVariantV2& other);

    // Publishes row transformations of an already validated S layout. Callers must preserve
    // scalar/disjoint semantics; structural invariants and recursive ownership are still checked.
    static MutablePtr _create_shredded_from_valid_parts(MutablePtr residual, ShreddedFields fields);
    static void _set_shredded_from_valid_parts(ColumnVariantV2& result, MutablePtr residual,
                                               ShreddedFields&& fields);
    // Publishes the extraction builder's already validated encoded columns without revalidating or
    // copying their bytes. Only VariantElementV2ResultBuilder can cross this trusted boundary.
    static MutablePtr create_encoded_from_valid_parts(ColumnString::MutablePtr metadatas,
                                                      MetadataIdsColumn::MutablePtr metadata_ids,
                                                      ColumnString::MutablePtr values);
    // Internal representation fallback. Consumers should use materialize_encoded_range() so a
    // read does not silently rewrite the source column's physical state.
    void _ensure_encoded();
    uint32_t _find_or_insert_metadata(StringRef metadata);
    void _append_encoded_range(const ColumnVariantV2& source, size_t start, size_t length);
    void _append_encoded_indices(const ColumnVariantV2& source, const uint32_t* indices_begin,
                                 const uint32_t* indices_end);
    void _append_missing_shredded_field(ShreddedField& field, size_t length);
    void _append_missing_shredded_fields(size_t length);
    template <typename Selection>
    void _insert_selected_from(const ColumnVariantV2& source, const Selection& selection);
    template <typename Selection>
    void _append_fixed_shredded_rows(const ColumnVariantV2& source, const Selection& selection);
    template <typename Selection>
    void _append_source_fields_to_residual(ColumnVariantV2& residual, const ColumnVariantV2& source,
                                           const DorisVector<size_t>& fields_to_residual,
                                           const Selection& selection);
    void _adopt_shredded_layout_from(const ColumnVariantV2& source);
    void _adopt_state_from(ColumnVariantV2& replacement);
    void _detach_metadata_for_write();
    void _check_invariants() const;
    void _check_shredded_invariants() const;
    bool _has_same_shredded_layout(const ColumnVariantV2& source) const;
    void mutate_subcolumns() override;

    // Encoded state: each row owns a value and references one deduplicated metadata blob. The
    // uint32 id costs four bytes per encoded row, but avoids repeating object-key metadata and
    // gives canonical comparison, hashing, subpath lookup, and binary SerDe O(1) schema access.
    // It is required because valid external Variant rows may use different metadata dictionaries.
    ColumnString::WrappedPtr _metadatas;
    MetadataIdsColumn::WrappedPtr _meta_ids;
    ColumnString::WrappedPtr _values;

    // A non-null _typed always means all encoded buffers are empty and the entire column has the
    // single type described by _typed_type.
    IColumn::WrappedPtr _typed;
    DataTypePtr _typed_type;

    // In S, the encoded members above hold every unshredded portion of each row. A present child
    // exclusively owns its logical path, while an absent child is only padding and contributes no
    // value. Thus missing, Variant JSON null, and the surrounding SQL NULL remain distinct.
    ShreddedFields _shredded_fields;
#ifdef BE_TEST
    size_t _test_shredded_conflict_slow_rows = 0;
    size_t _test_full_shredded_validations = 0;
    mutable size_t _test_encoded_range_materializations = 0;
#endif
};

template <typename NullCallback, typename ValueCallback>
// Any VariantRef passed to on_value borrows either the source column or the reusable local buffer
// and is valid only until that callback returns. Callbacks must not retain it.
void visit_variant_v2_values(const IColumn& source, size_t start, size_t end,
                             std::span<const NullMap::value_type> outer_nulls,
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
    if (view.is_encoded()) {
        for (size_t row = start; row < end; ++row) {
            if (!outer_nulls.empty() && outer_nulls[row] != 0) {
                on_null(row);
            } else {
                on_value(row, view.value_at(constant ? 0 : row));
            }
        }
        return;
    }

    if (view.is_shredded()) {
        const size_t physical_start = constant ? 0 : start;
        const size_t physical_rows = constant ? static_cast<size_t>(start != end) : end - start;
        auto encoded = variant->materialize_encoded_range(physical_start, physical_rows);
        for (size_t row = start; row < end; ++row) {
            if (!outer_nulls.empty() && outer_nulls[row] != 0) {
                on_null(row);
            } else {
                on_value(row, encoded->get_value_ref(constant ? 0 : row - start));
            }
        }
        return;
    }

    const auto& nullable = assert_cast<const ColumnNullable&>(view.typed_column());
    const uint32_t scale = view.typed_type()->get_scale();
    DORIS_CHECK_LE(scale, static_cast<uint32_t>(std::numeric_limits<uint8_t>::max()));
    const auto& inner_nulls = nullable.get_null_map_data();
    DorisVector<char> scratch;
    auto emit = [&](size_t row, const VariantScalarRef& scalar) {
        scratch.resize(scalar.encoded_size());
        scalar.write_physical(scratch.data(), scratch.size());
        on_value(row, VariantRef {.metadata = {.data = VARIANT_EMPTY_METADATA.data(),
                                               .size = VARIANT_EMPTY_METADATA.size()},
                                  .value = {scratch.data(), scratch.size()}});
    };
    dispatch_variant_typed_column(
            nullable.get_nested_column(), view.typed_type()->get_primitive_type(),
            [&]<PrimitiveType Type>(const auto& nested) {
                for (size_t row = start; row < end; ++row) {
                    if (!outer_nulls.empty() && outer_nulls[row] != 0) {
                        on_null(row);
                        continue;
                    }
                    const size_t physical_row = constant ? 0 : row;
                    if (inner_nulls[physical_row] != 0) {
                        emit(row, VariantScalarRef::null_value());
                        continue;
                    }
                    with_variant_typed_scalar<Type>(
                            nested, physical_row, static_cast<uint8_t>(scale),
                            [&](const VariantScalarRef& scalar) { emit(row, scalar); });
                }
            });
}

} // namespace doris
