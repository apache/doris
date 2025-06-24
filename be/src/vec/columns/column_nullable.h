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

#include <cstddef>
#include <functional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "olap/olap_common.h"
#include "runtime/define_primitive_type.h"
#include "util/simd/bits.h"
#include "vec/columns/column.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_common.h"
#include "vec/common/assert_cast.h"
#include "vec/common/cow.h"
#include "vec/common/pod_array_fwd.h"
#include "vec/common/string_ref.h"
#include "vec/common/typeid_cast.h"
#include "vec/core/field.h"
#include "vec/core/types.h"

class SipHash;

namespace doris::vectorized {
#include "common/compile_check_begin.h"
class Arena;
class ColumnSorter;

class NullMap : public PaddedPODArray<UInt8> {
    using Base = PaddedPODArray<UInt8>;

public:
    // can't init without initial value!
    NullMap() = delete;
    NullMap(size_t n) = delete;
    NullMap(size_t n, const UInt8& x) : Base(n, x) {}

    NullMap(const_iterator from_begin, const_iterator from_end) : Base(from_begin, from_end) {}
    NullMap(std::initializer_list<UInt8> il) : Base(std::begin(il), std::end(il)) {}
    NullMap(const NullMap& other) { this->assign(other.begin(), other.end()); }
    NullMap(NullMap&& other) { Base::swap(other); }

    NullMap& operator=(NullMap&& other) {
        Base::swap(other);
        return *this;
    }

    NullMap& operator=(const NullMap& other) {
        if (this != &other) {
            this->assign(other.begin(), other.end());
        }
        return *this;
    }

    /// add functions same with ColumnVector:
    void insert_range_from(const NullMap& src, size_t start, size_t length) {
        if (src.size() < start + length) {
            throw doris::Exception(doris::Status::InternalError("insert_range_from out of range"));
        }
        auto* it = begin() + start;
        auto* end_it = it + length;
        insert(it, end_it);
    }

    void insert_indices_from(const NullMap& src, const uint32_t* indices_begin,
                             const uint32_t* indices_end) {
        auto origin_size = size();
        auto new_size = indices_end - indices_begin;
        resize(origin_size + new_size);

        auto copy = [](const value_type* __restrict src, value_type* __restrict dest,
                       const uint32_t* __restrict begin, const uint32_t* __restrict end) {
            for (const auto* it = begin; it != end; ++it) {
                *dest = src[*it];
                ++dest;
            }
        };
        copy(reinterpret_cast<const value_type*>(data()), data() + origin_size, indices_begin,
             indices_end);
    }

    void insert_many_from(const NullMap& src, size_t position, size_t length) {
        auto old_size = size();
        resize(old_size + length);
        std::fill(data() + old_size, data() + old_size + length, src[position]);
    }

    void insert_many_vals(value_type val, size_t n) {
        resize_fill(size() + n, val);
    }

    void erase(size_t start, size_t length) {
        auto* it = begin() + start;
        auto* end_it = it + length;
        Base::erase(it, end_it);
    }

    void pop_back(size_t n) { this->c_end -= byte_size(n); }

    using Base::byte_size;
    size_t byte_size() const { return this->c_end - this->c_start; }

    bool has_enough_capacity(const NullMap& src) const { return capacity() - size() > src.size(); }

    void append_data_by_selector(NullMap& res, const IColumn::Selector& selector, size_t begin,
                                 size_t end) const {
        size_t num_rows = size();

        if (num_rows < selector.size()) {
            throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                   "Size of selector: {} is larger than size of column: {}",
                                   selector.size(), num_rows);
        }
        DCHECK_GE(end, begin);
        DCHECK_LE(end, selector.size());
        // here wants insert some value from this column, and the nums is (end - begin)
        // and many be this column num_rows is 4096, but only need insert num is (1 - 0) = 1
        // so can't call res->reserve(num_rows), it's will be too mush waste memory
        res.reserve(res.size() + (end - begin));

        for (size_t i = begin; i < end; ++i) {
            res.push_back(this[selector[i]]);
        }
    }

    NullMap filter(const IColumn::Filter& filt, ssize_t result_size_hint) const {
        size_t size = this->size();
        column_match_filter_size(size, filt.size());

        NullMap res(0, false);
        res.reserve(result_size_hint > 0 ? result_size_hint : size);

        const UInt8* filt_pos = filt.data();
        const UInt8* filt_end = filt_pos + size;
        const value_type* data_pos = data();

        /** A slightly more optimized version.
            * Based on the assumption that often pieces of consecutive values
            *  completely pass or do not pass the filter.
            * Therefore, we will optimistically check the parts of `SIMD_BYTES` values.
            */
        static constexpr size_t SIMD_BYTES = simd::bits_mask_length();
        const UInt8* filt_end_sse = filt_pos + size / SIMD_BYTES * SIMD_BYTES;

        while (filt_pos < filt_end_sse) {
            auto mask = simd::bytes_mask_to_bits_mask(filt_pos);
            if (0 == mask) {
                //pass
            } else if (simd::bits_mask_all() == mask) {
                res.insert(data_pos, data_pos + SIMD_BYTES);
            } else {
                simd::iterate_through_bits_mask(
                        [&](const size_t idx) { res.push_back_without_reserve(data_pos[idx]); },
                        mask);
            }

            filt_pos += SIMD_BYTES;
            data_pos += SIMD_BYTES;
        }

        while (filt_pos < filt_end) {
            if (*filt_pos) {
                res.push_back_without_reserve(*data_pos);
            }

            ++filt_pos;
            ++data_pos;
        }

        return res;
    }

    size_t filter(const IColumn::Filter& filter) {
        size_t size = this->size();
        column_match_filter_size(size, filter.size());

        const UInt8* filter_pos = filter.data();
        const UInt8* filter_end = filter_pos + size;
        value_type* data_pos = data();
        value_type* result_data = data_pos;

        /** A slightly more optimized version.
            * Based on the assumption that often pieces of consecutive values
            *  completely pass or do not pass the filter.
            * Therefore, we will optimistically check the parts of `SIMD_BYTES` values.
            */
        static constexpr size_t SIMD_BYTES = simd::bits_mask_length();
        const UInt8* filter_end_sse = filter_pos + size / SIMD_BYTES * SIMD_BYTES;

        while (filter_pos < filter_end_sse) {
            auto mask = simd::bytes_mask_to_bits_mask(filter_pos);
            if (0 == mask) {
                //pass
            } else if (simd::bits_mask_all() == mask) {
                memmove(result_data, data_pos, sizeof(value_type) * SIMD_BYTES);
                result_data += SIMD_BYTES;
            } else {
                simd::iterate_through_bits_mask(
                        [&](const size_t idx) {
                            *result_data = data_pos[idx];
                            ++result_data;
                        },
                        mask);
            }

            filter_pos += SIMD_BYTES;
            data_pos += SIMD_BYTES;
        }

        while (filter_pos < filter_end) {
            if (*filter_pos) {
                *result_data = *data_pos;
                ++result_data;
            }

            ++filter_pos;
            ++data_pos;
        }

        const auto new_size = result_data - data();
        resize(new_size);

        return new_size;
    }

    NullMap permute(const IColumn::Permutation& perm, size_t limit) const {
        size_t size = this->size();

        if (limit == 0) {
            limit = size;
        } else {
            limit = std::min(size, limit);
        }

        if (perm.size() < limit) {
            throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR,
                                   "Size of permutation ({}) is less than required ({})",
                                   perm.size(), limit);
        }

        auto res = NullMap(limit, 0);
        for (size_t i = 0; i < limit; ++i) {
            res[i] = data()[perm[i]];
        }

        return res;
    }

    NullMap replicate(const IColumn::Offsets& offsets) const {
        size_t size = this->size();
        column_match_offsets_size(size, offsets.size());

        NullMap res(0, false);
        if (0 == size) {
            return res;
        }

        res.reserve(offsets.back());

        // vectorized this code to speed up
        auto counts_uptr = std::unique_ptr<IColumn::Offset[]>(new IColumn::Offset[size]);
        IColumn::Offset* counts = counts_uptr.get();
        for (ssize_t i = 0; i < size; ++i) {
            counts[i] = offsets[i] - offsets[i - 1];
        }

        for (size_t i = 0; i < size; ++i) {
            res.resize_fill(res.size() + counts[i], data()[i]);
        }

        return res;
    }

private:
    /// allow to use all public methods of PODArray, except:
    template <typename... TAllocatorParams>
    void resize(size_t n, TAllocatorParams&&... allocator_params) {
        Base::resize(n, std::forward<TAllocatorParams>(allocator_params)...);
    }
    /// we can use resize_fill instead of it as public interface.
};

/// use this to avoid directly access null_map forgetting modify _need_update_has_null. see more in inner comments
class NullMapProvider {
public:
    NullMapProvider() = delete;
    NullMapProvider(NullMap&& null_map) : _null_map(std::move(null_map)) {}
    NullMapProvider(const NullMap& null_map) : _null_map(null_map) {}
    void reset_null_map(NullMap&& null_map) { _null_map = std::move(null_map); }

    NullMap& get_null_map_data(bool may_change = true) {
        if (may_change) {
            _need_update_has_null = true;
        }
        return _null_map;
    }
    const NullMap& get_null_map_data() const { return _null_map; }

    void clear_null_map() { _null_map.clear(); }

    void update_has_null(bool new_value) {
        _has_null = new_value;
        _need_update_has_null = false;
    }

protected:
    /**
    * Here we have three variables which serve for `has_null()` judgement. If we have known the nullity of object, no need
    *  to check through the `null_map` to get the answer until the next time we modify it. Here `_has_null` is just the answer
    *  we cached. `_need_update_has_null` indicates there's modification or not since we got `_has_null()` last time. So in 
    *  `_has_null()` we can check the two vars to know if there's need to update `has_null` or not.
    * If you just want QUERY BUT NOT MODIFY, make sure the caller is const. There will be no perf overhead for const overload.
    *  Otherwise, this class, as the base class, will make it no possible to directly visit `null_map` forgetting to change the
    *  protected flags. Just call the interface is ok.
    */
    bool _need_update_has_null = true;
    bool _has_null = true;

private:
    NullMap _null_map;
};

/// Class that specifies nullable columns. A nullable column represents
/// a column, which may have any type, provided with the possibility of
/// storing NULL values. For this purpose, a ColumnNullable object stores
/// an ordinary column along with a special column, namely a byte map,
/// whose type is ColumnUInt8. The latter column indicates whether the
/// value of a given row is a NULL or not. Such a design is preferred
/// over a bitmap because columns are usually stored on disk as compressed
/// files. In this regard, using a bitmap instead of a byte map would
/// greatly complicate the implementation with little to no benefits.
class ColumnNullable final : public COWHelper<IColumn, ColumnNullable>, public NullMapProvider {
private:
    friend class COWHelper<IColumn, ColumnNullable>;

    ColumnNullable(MutableColumnPtr&& nested_column_, NullMap&& null_map_);
    ColumnNullable(const ColumnNullable&) = default;

public:
    /** Create immutable column using immutable arguments. This arguments may be shared with other columns.
      * Use IColumn::mutate in order to make mutable column and mutate shared nested columns.
      */
    using Base = COWHelper<IColumn, ColumnNullable>;
    static MutablePtr create(const ColumnPtr& nested_column_, const NullMap& null_map_) {
        return Base::create(nested_column_->assume_mutable(), null_map_);
    }

    template <typename... Args, typename = std::enable_if_t<IsMutableColumns<Args...>::value>>
    static MutablePtr create(Args&&... args) {
        return Base::create(std::forward<Args>(args)...);
    }

    void shrink_padding_chars() override;

    bool is_variable_length() const override { return nested_column->is_variable_length(); }

    std::string get_name() const override { return "Nullable(" + nested_column->get_name() + ")"; }
    MutableColumnPtr clone_resized(size_t size) const override;
    size_t size() const override { return get_null_map_data().size(); }
    PURE bool is_null_at(size_t n) const override { return get_null_map_data()[n] != 0; }
    Field operator[](size_t n) const override;
    void get(size_t n, Field& res) const override;
    bool get_bool(size_t n) const override {
        return is_null_at(n) ? false : nested_column->get_bool(n);
    }
    // column must be nullable(uint8)
    bool get_bool_inline(size_t n) const {
        return is_null_at(n) ? false
                             : assert_cast<const ColumnUInt8*, TypeCheckOnRelease::DISABLE>(
                                       nested_column.get())
                                       ->get_bool(n);
    }
    StringRef get_data_at(size_t n) const override;

    /// Will insert null value if pos=nullptr
    void insert_data(const char* pos, size_t length) override;

    void insert_many_strings(const StringRef* strings, size_t num) override;

    StringRef serialize_value_into_arena(size_t n, Arena& arena, char const*& begin) const override;
    const char* deserialize_and_insert_from_arena(const char* pos) override;
    size_t get_max_row_byte_size() const override;

    void serialize_vec(StringRef* keys, size_t num_rows, size_t max_row_byte_size) const override;

    void deserialize_vec(StringRef* keys, size_t num_rows) override;

    void insert_range_from(const IColumn& src, size_t start, size_t length) override;

    void insert_range_from_ignore_overflow(const IColumn& src, size_t start,
                                           size_t length) override;

    void insert_indices_from(const IColumn& src, const uint32_t* indices_begin,
                             const uint32_t* indices_end) override;
    void insert_indices_from_not_has_null(const IColumn& src, const uint32_t* indices_begin,
                                          const uint32_t* indices_end);

    void insert(const Field& x) override;
    void insert_from(const IColumn& src, size_t n) override;

    void insert_many_from(const IColumn& src, size_t position, size_t length) override;

    void append_data_by_selector(IColumn::MutablePtr& res,
                                 const IColumn::Selector& selector) const override;

    void append_data_by_selector(IColumn::MutablePtr& res, const IColumn::Selector& selector,
                                 size_t begin, size_t end) const override;

    template <typename ColumnType>
    void insert_from_with_type(const IColumn& src, size_t n) {
        const auto& src_concrete =
                assert_cast<const ColumnNullable&, TypeCheckOnRelease::DISABLE>(src);
        assert_cast<ColumnType*, TypeCheckOnRelease::DISABLE>(nested_column.get())
                ->insert_from(src_concrete.get_nested_column(), n);
        auto is_null = src_concrete.get_null_map_data()[n];
        if (is_null) {
            get_null_map_data().push_back(1);
            _has_null = true;
            _need_update_has_null = false;
        } else {
            _push_false_to_nullmap(1);
        }
    }

    void insert_range_from_not_nullable(const IColumn& src, size_t start, size_t length);

    void insert_many_fix_len_data(const char* pos, size_t num) override {
        _push_false_to_nullmap(num);
        get_nested_column().insert_many_fix_len_data(pos, num);
    }

    void insert_many_raw_data(const char* pos, size_t num) override {
        DCHECK(pos);
        _push_false_to_nullmap(num);
        get_nested_column().insert_many_raw_data(pos, num);
    }

    void insert_many_dict_data(const int32_t* data_array, size_t start_index, const StringRef* dict,
                               size_t data_num, uint32_t dict_num) override {
        _push_false_to_nullmap(data_num);
        get_nested_column().insert_many_dict_data(data_array, start_index, dict, data_num,
                                                  dict_num);
    }

    void insert_many_continuous_binary_data(const char* data, const uint32_t* offsets,
                                            const size_t num) override {
        if (UNLIKELY(num == 0)) {
            return;
        }
        _push_false_to_nullmap(num);
        get_nested_column().insert_many_continuous_binary_data(data, offsets, num);
    }

    // Default value in `ColumnNullable` is null
    void insert_default() override {
        get_nested_column().insert_default();
        get_null_map_data().push_back(1);
        _has_null = true;
        _need_update_has_null = false;
    }

    void insert_many_defaults(size_t length) override {
        get_nested_column().insert_many_defaults(length);
        get_null_map_data().resize_fill(get_null_map_data().size() + length, 1);
        _has_null = true;
        _need_update_has_null = false;
    }

    void insert_not_null_elements(size_t num) {
        get_nested_column().insert_many_defaults(num);
        _push_false_to_nullmap(num);
    }

    void pop_back(size_t n) override;
    ColumnPtr filter(const Filter& filt, ssize_t result_size_hint) const override;

    size_t filter(const Filter& filter) override;

    Status filter_by_selector(const uint16_t* sel, size_t sel_size, IColumn* col_ptr) override;
    MutableColumnPtr permute(const Permutation& perm, size_t limit) const override;
    //    ColumnPtr index(const IColumn & indexes, size_t limit) const override;
    int compare_at(size_t n, size_t m, const IColumn& rhs_, int null_direction_hint) const override;

    void compare_internal(size_t rhs_row_id, const IColumn& rhs, int nan_direction_hint,
                          int direction, std::vector<uint8>& cmp_res,
                          uint8* __restrict filter) const override;
    void get_permutation(bool reverse, size_t limit, int null_direction_hint,
                         Permutation& res) const override;
    void reserve(size_t n) override;
    void resize(size_t n) override;
    size_t byte_size() const override;
    size_t allocated_bytes() const override;
    bool has_enough_capacity(const IColumn& src) const override;
    ColumnPtr replicate(const Offsets& replicate_offsets) const override;
    void update_xxHash_with_value(size_t start, size_t end, uint64_t& hash,
                                  const uint8_t* __restrict null_data) const override;
    void update_crc_with_value(size_t start, size_t end, uint32_t& hash,
                               const uint8_t* __restrict null_data) const override;

    void update_hash_with_value(size_t n, SipHash& hash) const override;
    void update_crcs_with_value(uint32_t* __restrict hash, PrimitiveType type, uint32_t rows,
                                uint32_t offset,
                                const uint8_t* __restrict null_data) const override;
    void update_hashes_with_value(uint64_t* __restrict hashes,
                                  const uint8_t* __restrict null_data) const override;

    ColumnPtr convert_column_if_overflow() override {
        nested_column = nested_column->convert_column_if_overflow();
        return get_ptr();
    }

    void for_each_subcolumn(ColumnCallback callback) override { callback(nested_column); }

    bool structure_equals(const IColumn& rhs) const override {
        if (const auto* rhs_nullable = typeid_cast<const ColumnNullable*>(&rhs)) {
            return nested_column->structure_equals(*rhs_nullable->nested_column);
        }
        return false;
    }

    bool is_nullable() const override { return true; }
    bool is_concrete_nullable() const override { return true; }
    bool is_column_string() const override { return get_nested_column().is_column_string(); }

    bool is_exclusive() const override {
        return IColumn::is_exclusive() && nested_column->is_exclusive();
    }

    bool only_null() const override { return size() == 1 && is_null_at(0); }

    // used in schema change
    void change_nested_column(ColumnPtr& other) { ((ColumnPtr&)nested_column) = other; }

    /// Return the column that represents values.
    IColumn& get_nested_column() { return *nested_column; }
    const IColumn& get_nested_column() const { return *nested_column; }

    const ColumnPtr& get_nested_column_ptr() const { return nested_column; }

    MutableColumnPtr get_nested_column_ptr() { return nested_column->assume_mutable(); }

    void clear() override {
        clear_null_map();
        nested_column->clear();
        _has_null = false;
    }

    /// Apply the null byte map of a specified nullable column onto the
    /// null byte map of the current column by performing an element-wise OR
    /// between both byte maps. This method is used to determine the null byte
    /// map of the result column of a function taking one or more nullable
    /// columns.
    void apply_null_map(const NullMap& other);
    void apply_negated_null_map(const NullMap& map);

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
        const auto& nullable_rhs =
                assert_cast<const ColumnNullable&, TypeCheckOnRelease::DISABLE>(rhs);
        get_null_map_data()[self_row] =
                nullable_rhs.get_null_map_data()[row]; // copy null map value

        if (!nullable_rhs.is_null_at(row)) {
            nested_column->replace_column_data(*nullable_rhs.nested_column, row, self_row);
        }
    }

    MutableColumnPtr convert_to_predicate_column_if_dictionary() override {
        nested_column = get_nested_column().convert_to_predicate_column_if_dictionary();
        return get_ptr();
    }

    double get_ratio_of_default_rows(double sample_ratio) const override {
        if (sample_ratio <= 0.0 || sample_ratio > 1.0) {
            throw doris::Exception(ErrorCode::INTERNAL_ERROR, "invalid sample_ratio {}",
                                   sample_ratio);
        }
        static constexpr auto MAX_NUMBER_OF_ROWS_FOR_FULL_SEARCH = 1000;
        size_t num_rows = size();
        size_t num_sampled_rows = std::min(
                static_cast<size_t>(static_cast<double>(num_rows) * sample_ratio), num_rows);
        size_t num_checked_rows = 0;
        size_t res = 0;
        if (num_sampled_rows == num_rows || num_rows <= MAX_NUMBER_OF_ROWS_FOR_FULL_SEARCH) {
            for (size_t i = 0; i < num_rows; ++i) {
                res += is_null_at(i);
            }
            num_checked_rows = num_rows;
        } else if (num_sampled_rows != 0) {
            for (size_t i = 0; i < num_rows; ++i) {
                if (num_checked_rows * num_rows <= i * num_sampled_rows) {
                    res += is_null_at(i);
                    ++num_checked_rows;
                }
            }
        }
        if (num_checked_rows == 0) {
            return 0.0;
        }
        return static_cast<double>(res) / static_cast<double>(num_checked_rows);
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

    void finalize() override { get_nested_column().finalize(); }

    void erase(size_t start, size_t length) override {
        get_nested_column().erase(start, length);
        get_null_map_data().erase(start, length);
    }

private:
    void _update_has_null();

    template <bool negative>
    void apply_null_map_impl(const NullMap& map);

    // push not null value wouldn't change the nullity. no need to update _has_null
    void _push_false_to_nullmap(size_t num) {
        get_null_map_data(false).insert_many_vals(false, num);
    }

    WrappedPtr nested_column;
};

ColumnPtr make_nullable(const ColumnPtr& column, bool is_nullable = false);
ColumnPtr remove_nullable(const ColumnPtr& column);
} // namespace doris::vectorized
#include "common/compile_check_end.h"
