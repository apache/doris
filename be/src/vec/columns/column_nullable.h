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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Columns/ColumnNullable.h
// and modified by Doris

#pragma once

#include <functional>
#include <ostream>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "olap/olap_common.h"
#include "runtime/define_primitive_type.h"
#include "vec/columns/column.h"
#include "vec/columns/column_impl.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/cow.h"
#include "vec/common/string_ref.h"
#include "vec/common/typeid_cast.h"
#include "vec/core/field.h"
#include "vec/core/types.h"

class SipHash;

namespace doris::vectorized {
class Arena;
class ColumnSorter;

using NullMap = ColumnUInt8::Container;
using ConstNullMapPtr = const NullMap*;

/// Class that specifies nullable columns. A nullable column represents
/// a column, which may have any type, provided with the possibility of
/// storing NULL values. For this purpose, a ColumnNullable object stores
/// an ordinary column along with a special column, namely a byte map,
/// whose type is ColumnUInt8. The latter column indicates whether the
/// value of a given row is a NULL or not. Such a design is preferred
/// over a bitmap because columns are usually stored on disk as compressed
/// files. In this regard, using a bitmap instead of a byte map would
/// greatly complicate the implementation with little to no benefits.
class ColumnNullable final : public COWHelper<IColumn, ColumnNullable> {
private:
    friend class COWHelper<IColumn, ColumnNullable>;

    ColumnNullable(MutableColumnPtr&& nested_column_, MutableColumnPtr&& null_map_);
    ColumnNullable(const ColumnNullable&) = default;

public:
    /** Create immutable column using immutable arguments. This arguments may be shared with other columns.
      * Use IColumn::mutate in order to make mutable column and mutate shared nested columns.
      */
    using Base = COWHelper<IColumn, ColumnNullable>;
    static Ptr create(const ColumnPtr& nested_column_, const ColumnPtr& null_map_) {
        return ColumnNullable::create(nested_column_->assume_mutable(),
                                      null_map_->assume_mutable());
    }

    template <typename... Args,
              typename = typename std::enable_if<IsMutableColumns<Args...>::value>::type>
    static MutablePtr create(Args&&... args) {
        return Base::create(std::forward<Args>(args)...);
    }

    MutableColumnPtr get_shrinked_column() override;

    const char* get_family_name() const override { return "Nullable"; }
    std::string get_name() const override { return "Nullable(" + nested_column->get_name() + ")"; }
    MutableColumnPtr clone_resized(size_t size) const override;
    size_t size() const override { return nested_column->size(); }
    bool is_null_at(size_t n) const override {
        return assert_cast<const ColumnUInt8&>(*null_map).get_data()[n] != 0;
    }
    Field operator[](size_t n) const override;
    void get(size_t n, Field& res) const override;
    bool get_bool(size_t n) const override {
        return is_null_at(n) ? false : nested_column->get_bool(n);
    }
    // column must be nullable(uint8)
    bool get_bool_inline(size_t n) const {
        return is_null_at(n) ? false
                             : assert_cast<const ColumnUInt8*>(nested_column.get())->get_bool(n);
    }
    UInt64 get64(size_t n) const override { return nested_column->get64(n); }
    Float64 get_float64(size_t n) const override { return nested_column->get_float64(n); }
    StringRef get_data_at(size_t n) const override;

    /// Will insert null value if pos=nullptr
    void insert_data(const char* pos, size_t length) override;

    void insert_many_strings(const StringRef* strings, size_t num) override;

    StringRef serialize_value_into_arena(size_t n, Arena& arena, char const*& begin) const override;
    const char* deserialize_and_insert_from_arena(const char* pos) override;
    size_t get_max_row_byte_size() const override;
    void serialize_vec(std::vector<StringRef>& keys, size_t num_rows,
                       size_t max_row_byte_size) const override;

    void deserialize_vec(std::vector<StringRef>& keys, size_t num_rows) override;

    void insert_range_from(const IColumn& src, size_t start, size_t length) override;
    void insert_indices_from(const IColumn& src, const uint32_t* __restrict indices_begin,
                             const uint32_t* __restrict indices_end) override;
    void insert(const Field& x) override;
    void insert_from(const IColumn& src, size_t n) override;

    template <typename ColumnType>
    void insert_from_with_type(const IColumn& src, size_t n) {
        const auto& src_concrete = assert_cast<const ColumnNullable&>(src);
        assert_cast<ColumnType*>(nested_column.get())
                ->insert_from(src_concrete.get_nested_column(), n);
        auto is_null = src_concrete.get_null_map_data()[n];
        _has_null |= is_null;
        _get_null_map_data().push_back(is_null);
    }

    void insert_from_not_nullable(const IColumn& src, size_t n);
    void insert_range_from_not_nullable(const IColumn& src, size_t start, size_t length);
    void insert_many_from_not_nullable(const IColumn& src, size_t position, size_t length);

    void insert_many_fix_len_data(const char* pos, size_t num) override {
        _get_null_map_column().fill(0, num);
        get_nested_column().insert_many_fix_len_data(pos, num);
    }

    void insert_many_raw_data(const char* pos, size_t num) override {
        _get_null_map_column().fill(0, num);
        get_nested_column().insert_many_raw_data(pos, num);
    }

    void insert_many_dict_data(const int32_t* data_array, size_t start_index, const StringRef* dict,
                               size_t data_num, uint32_t dict_num) override {
        _get_null_map_column().fill(0, data_num);
        get_nested_column().insert_many_dict_data(data_array, start_index, dict, data_num,
                                                  dict_num);
    }

    void insert_many_continuous_binary_data(const char* data, const uint32_t* offsets,
                                            const size_t num) override {
        if (UNLIKELY(num == 0)) {
            return;
        }
        _get_null_map_column().fill(0, num);
        get_nested_column().insert_many_continuous_binary_data(data, offsets, num);
    }

    void insert_many_binary_data(char* data_array, uint32_t* len_array,
                                 uint32_t* start_offset_array, size_t num) override {
        _get_null_map_column().fill(0, num);
        get_nested_column().insert_many_binary_data(data_array, len_array, start_offset_array, num);
    }

    void insert_default() override {
        get_nested_column().insert_default();
        _get_null_map_data().push_back(1);
        _has_null = true;
    }

    void insert_many_defaults(size_t length) override {
        get_nested_column().insert_many_defaults(length);
        _get_null_map_data().resize_fill(_get_null_map_data().size() + length, 1);
        _has_null = true;
    }

    void insert_not_null_elements(size_t num) {
        get_nested_column().insert_many_defaults(num);
        _get_null_map_column().fill(0, num);
        _has_null = false;
    }

    void insert_null_elements(int num) {
        get_nested_column().insert_many_defaults(num);
        _get_null_map_column().fill(1, num);
        _has_null = true;
    }

    void pop_back(size_t n) override;
    ColumnPtr filter(const Filter& filt, ssize_t result_size_hint) const override;

    size_t filter(const Filter& filter) override;

    Status filter_by_selector(const uint16_t* sel, size_t sel_size, IColumn* col_ptr) override;
    ColumnPtr permute(const Permutation& perm, size_t limit) const override;
    //    ColumnPtr index(const IColumn & indexes, size_t limit) const override;
    int compare_at(size_t n, size_t m, const IColumn& rhs_, int null_direction_hint) const override;
    void get_permutation(bool reverse, size_t limit, int null_direction_hint,
                         Permutation& res) const override;
    void reserve(size_t n) override;
    void resize(size_t n) override;
    size_t byte_size() const override;
    size_t allocated_bytes() const override;
    ColumnPtr replicate(const Offsets& replicate_offsets) const override;
    void replicate(const uint32_t* counts, size_t target_size, IColumn& column) const override;
    void update_xxHash_with_value(size_t start, size_t end, uint64_t& hash,
                                  const uint8_t* __restrict null_data) const override;
    void update_crc_with_value(size_t start, size_t end, uint32_t& hash,
                               const uint8_t* __restrict null_data) const override;

    void update_hash_with_value(size_t n, SipHash& hash) const override;
    void update_hashes_with_value(std::vector<SipHash>& hashes,
                                  const uint8_t* __restrict null_data) const override;
    void update_crcs_with_value(uint32_t* __restrict hash, PrimitiveType type, uint32_t rows,
                                uint32_t offset,
                                const uint8_t* __restrict null_data) const override;
    void update_hashes_with_value(uint64_t* __restrict hashes,
                                  const uint8_t* __restrict null_data) const override;

    MutableColumns scatter(ColumnIndex num_columns, const Selector& selector) const override {
        return scatter_impl<ColumnNullable>(num_columns, selector);
    }

    void append_data_by_selector(MutableColumnPtr& res,
                                 const IColumn::Selector& selector) const override {
        append_data_by_selector_impl<ColumnNullable>(res, selector);
    }

    //    void gather(ColumnGathererStream & gatherer_stream) override;

    void for_each_subcolumn(ColumnCallback callback) override {
        callback(nested_column);
        callback(null_map);
    }

    bool structure_equals(const IColumn& rhs) const override {
        if (const auto* rhs_nullable = typeid_cast<const ColumnNullable*>(&rhs)) {
            return nested_column->structure_equals(*rhs_nullable->nested_column);
        }
        return false;
    }

    bool is_date_type() const override { return get_nested_column().is_date_type(); }
    bool is_datetime_type() const override { return get_nested_column().is_datetime_type(); }
    void set_date_type() override { get_nested_column().set_date_type(); }
    void set_datetime_type() override { get_nested_column().set_datetime_type(); }

    bool is_nullable() const override { return true; }
    bool is_bitmap() const override { return get_nested_column().is_bitmap(); }
    bool is_hll() const override { return get_nested_column().is_hll(); }
    bool is_column_decimal() const override { return get_nested_column().is_column_decimal(); }
    bool is_column_string() const override { return get_nested_column().is_column_string(); }
    bool is_column_array() const override { return get_nested_column().is_column_array(); }
    bool is_column_map() const override { return get_nested_column().is_column_map(); }
    bool is_column_struct() const override { return get_nested_column().is_column_struct(); }
    bool is_fixed_and_contiguous() const override { return false; }
    bool values_have_fixed_size() const override { return nested_column->values_have_fixed_size(); }

    bool is_exclusive() const override {
        return IColumn::is_exclusive() && nested_column->is_exclusive() && null_map->is_exclusive();
    }

    size_t size_of_value_if_fixed() const override {
        return null_map->size_of_value_if_fixed() + nested_column->size_of_value_if_fixed();
    }
    bool only_null() const override { return nested_column->is_dummy(); }

    // used in schema change
    void change_nested_column(ColumnPtr& other) { ((ColumnPtr&)nested_column) = other; }

    /// Return the column that represents values.
    IColumn& get_nested_column() { return *nested_column; }
    const IColumn& get_nested_column() const { return *nested_column; }

    const ColumnPtr& get_nested_column_ptr() const { return nested_column; }

    MutableColumnPtr get_nested_column_ptr() { return nested_column->assume_mutable(); }

    /// Return the column that represents the byte map.
    const ColumnPtr& get_null_map_column_ptr() const { return null_map; }

    MutableColumnPtr get_null_map_column_ptr() {
        _need_update_has_null = true;
        return null_map->assume_mutable();
    }

    ColumnUInt8& get_null_map_column() {
        _need_update_has_null = true;
        return assert_cast<ColumnUInt8&>(*null_map);
    }
    const ColumnUInt8& get_null_map_column() const {
        return assert_cast<const ColumnUInt8&>(*null_map);
    }

    void clear() override {
        null_map->clear();
        nested_column->clear();
        _has_null = false;
    }

    NullMap& get_null_map_data() { return get_null_map_column().get_data(); }

    const NullMap& get_null_map_data() const { return get_null_map_column().get_data(); }

    /// Apply the null byte map of a specified nullable column onto the
    /// null byte map of the current column by performing an element-wise OR
    /// between both byte maps. This method is used to determine the null byte
    /// map of the result column of a function taking one or more nullable
    /// columns.
    void apply_null_map(const ColumnNullable& other);
    void apply_null_map(const ColumnUInt8& map);
    void apply_negated_null_map(const ColumnUInt8& map);

    /// Check that size of null map equals to size of nested column.
    void check_consistency() const;

    bool has_null() const override {
        if (UNLIKELY(_need_update_has_null)) {
            const_cast<ColumnNullable*>(this)->_update_has_null();
        }
        return _has_null;
    }

    bool has_null(size_t size) const override;

    void replace_column_data(const IColumn& rhs, size_t row, size_t self_row = 0) override {
        DCHECK(size() > self_row);
        const auto& nullable_rhs = assert_cast<const ColumnNullable&>(rhs);
        null_map->replace_column_data(*nullable_rhs.null_map, row, self_row);

        if (!nullable_rhs.is_null_at(row)) {
            nested_column->replace_column_data(*nullable_rhs.nested_column, row, self_row);
        } else {
            nested_column->replace_column_data_default(self_row);
        }
    }

    void replace_column_data_default(size_t self_row = 0) override {
        LOG(FATAL) << "should not call the method in column nullable";
    }

    MutableColumnPtr convert_to_predicate_column_if_dictionary() override {
        nested_column = get_nested_column().convert_to_predicate_column_if_dictionary();
        return get_ptr();
    }

    void convert_dict_codes_if_necessary() override {
        get_nested_column().convert_dict_codes_if_necessary();
    }

    void initialize_hash_values_for_runtime_filter() override {
        get_nested_column().initialize_hash_values_for_runtime_filter();
    }

    void sort_column(const ColumnSorter* sorter, EqualFlags& flags, IColumn::Permutation& perms,
                     EqualRange& range, bool last_column) const override;

    void set_rowset_segment_id(std::pair<RowsetId, uint32_t> rowset_segment_id) override {
        nested_column->set_rowset_segment_id(rowset_segment_id);
    }

    std::pair<RowsetId, uint32_t> get_rowset_segment_id() const override {
        return nested_column->get_rowset_segment_id();
    }
    void get_indices_of_non_default_rows(Offsets64& indices, size_t from,
                                         size_t limit) const override {
        get_indices_of_non_default_rows_impl<ColumnNullable>(indices, from, limit);
    }

    ColumnPtr index(const IColumn& indexes, size_t limit) const override;

private:
    // the two functions will not update `_need_update_has_null`
    ColumnUInt8& _get_null_map_column() { return assert_cast<ColumnUInt8&>(*null_map); }
    NullMap& _get_null_map_data() { return _get_null_map_column().get_data(); }

    WrappedPtr nested_column;
    WrappedPtr null_map;

    bool _need_update_has_null = true;
    bool _has_null;

    void _update_has_null();
    template <bool negative>
    void apply_null_map_impl(const ColumnUInt8& map);
};

ColumnPtr make_nullable(const ColumnPtr& column, bool is_nullable = false);
ColumnPtr remove_nullable(const ColumnPtr& column);
// check if argument column is nullable. If so, extract its concrete column and set null_map.
//TODO: use this to replace inner usages.
// is_single: whether null_map is null map of a ColumnConst
void check_set_nullable(ColumnPtr&, ColumnVector<UInt8>::MutablePtr& null_map, bool is_single);
} // namespace doris::vectorized
