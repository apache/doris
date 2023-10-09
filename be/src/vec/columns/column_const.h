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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Columns/ColumnConst.h
// and modified by Doris

#pragma once

#include <glog/logging.h>
#include <stdint.h>
#include <sys/types.h>

#include <concepts>
#include <cstddef>
#include <functional>
#include <initializer_list>
#include <ostream>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "runtime/define_primitive_type.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/assert_cast.h"
#include "vec/common/cow.h"
#include "vec/common/string_ref.h"
#include "vec/common/typeid_cast.h"
#include "vec/core/column_numbers.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"

class SipHash;

namespace doris {
namespace vectorized {
class Arena;
class Block;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

/** ColumnConst contains another column with single element,
  *  but looks like a column with arbitrary amount of same elements.
  */
class ColumnConst final : public COWHelper<IColumn, ColumnConst> {
private:
    friend class COWHelper<IColumn, ColumnConst>;

    WrappedPtr data;
    size_t s;

    ColumnConst(const ColumnPtr& data, size_t s_);
    ColumnConst(const ColumnConst& src) = default;

public:
    ColumnPtr convert_to_full_column() const;

    ColumnPtr convert_to_full_column_if_const() const override { return convert_to_full_column(); }

    ColumnPtr remove_low_cardinality() const;

    std::string get_name() const override { return "Const(" + data->get_name() + ")"; }

    const char* get_family_name() const override { return "Const"; }

    void resize(size_t new_size) override { s = new_size; }

    MutableColumnPtr clone_resized(size_t new_size) const override {
        return ColumnConst::create(data, new_size);
    }

    size_t size() const override { return s; }

    Field operator[](size_t) const override { return (*data)[0]; }

    void get(size_t, Field& res) const override { data->get(0, res); }

    StringRef get_data_at(size_t) const override { return data->get_data_at(0); }

    TypeIndex get_data_type() const override { return data->get_data_type(); }

    UInt64 get64(size_t) const override { return data->get64(0); }

    UInt64 get_uint(size_t) const override { return data->get_uint(0); }

    Int64 get_int(size_t) const override { return data->get_int(0); }

    bool get_bool(size_t) const override { return data->get_bool(0); }

    Float64 get_float64(size_t) const override { return data->get_float64(0); }

    bool is_null_at(size_t) const override { return data->is_null_at(0); }

    void insert_range_from(const IColumn&, size_t /*start*/, size_t length) override {
        s += length;
    }

    void insert_indices_from(const IColumn& src, const int* indices_begin,
                             const int* indices_end) override {
        s += (indices_end - indices_begin);
    }

    void insert(const Field&) override { ++s; }

    void insert_data(const char*, size_t) override { ++s; }

    void insert_from(const IColumn&, size_t) override { ++s; }

    void clear() override { s = 0; }

    void insert_default() override { ++s; }

    void pop_back(size_t n) override { s -= n; }

    void get_indices_of_non_default_rows(Offsets64& indices, size_t from,
                                         size_t limit) const override;

    ColumnPtr index(const IColumn& indexes, size_t limit) const override;

    StringRef serialize_value_into_arena(size_t, Arena& arena, char const*& begin) const override {
        return data->serialize_value_into_arena(0, arena, begin);
    }

    const char* deserialize_and_insert_from_arena(const char* pos) override {
        auto res = data->deserialize_and_insert_from_arena(pos);
        data->pop_back(1);
        ++s;
        return res;
    }

    size_t get_max_row_byte_size() const override { return data->get_max_row_byte_size(); }

    void serialize_vec(std::vector<StringRef>& keys, size_t num_rows,
                       size_t max_row_byte_size) const override {
        data->serialize_vec(keys, num_rows, max_row_byte_size);
    }

    void update_xxHash_with_value(size_t start, size_t end, uint64_t& hash,
                                  const uint8_t* __restrict null_data) const override {
        auto real_data = data->get_data_at(0);
        if (real_data.data == nullptr) {
            hash = HashUtil::xxHash64NullWithSeed(hash);
        } else {
            hash = HashUtil::xxHash64WithSeed(real_data.data, real_data.size, hash);
        }
    }

    void update_crc_with_value(size_t start, size_t end, uint64_t& hash,
                               const uint8_t* __restrict null_data) const override {
        get_data_column_ptr()->update_crc_with_value(start, end, hash, nullptr);
    }

    void serialize_vec_with_null_map(std::vector<StringRef>& keys, size_t num_rows,
                                     const uint8_t* null_map,
                                     size_t max_row_byte_size) const override {
        data->serialize_vec_with_null_map(keys, num_rows, null_map, max_row_byte_size);
    }

    void update_hash_with_value(size_t, SipHash& hash) const override {
        data->update_hash_with_value(0, hash);
    }

    void update_hashes_with_value(std::vector<SipHash>& hashes,
                                  const uint8_t* __restrict null_data) const override;

    // (TODO.Amory) here may not use column_const update hash, and PrimitiveType is not used.
    void update_crcs_with_value(std::vector<uint64_t>& hashes, PrimitiveType type,
                                const uint8_t* __restrict null_data) const override;

    void update_hashes_with_value(uint64_t* __restrict hashes,
                                  const uint8_t* __restrict null_data) const override;

    ColumnPtr filter(const Filter& filt, ssize_t result_size_hint) const override;
    size_t filter(const Filter& filter) override;

    ColumnPtr replicate(const Offsets& offsets) const override;
    void replicate(const uint32_t* indexs, size_t target_size, IColumn& column) const override;
    ColumnPtr permute(const Permutation& perm, size_t limit) const override;
    // ColumnPtr index(const IColumn & indexes, size_t limit) const override;
    void get_permutation(bool reverse, size_t limit, int nan_direction_hint,
                         Permutation& res) const override;

    size_t byte_size() const override { return s > 0 ? data->byte_size() + sizeof(s) : 0; }

    size_t allocated_bytes() const override { return data->allocated_bytes() + sizeof(s); }

    int compare_at(size_t, size_t, const IColumn& rhs, int nan_direction_hint) const override {
        auto rhs_const_column = assert_cast<const ColumnConst&>(rhs);

        auto* this_nullable = check_and_get_column<ColumnNullable>(data.get());
        auto* rhs_nullable = check_and_get_column<ColumnNullable>(rhs_const_column.data.get());
        if (this_nullable && rhs_nullable) {
            return data->compare_at(0, 0, *rhs_const_column.data, nan_direction_hint);
        } else if (this_nullable) {
            auto rhs_nullable_column = make_nullable(rhs_const_column.data, false);
            return this_nullable->compare_at(0, 0, *rhs_nullable_column, nan_direction_hint);
        } else if (rhs_nullable) {
            auto this_nullable_column = make_nullable(data, false);
            return this_nullable_column->compare_at(0, 0, *rhs_const_column.data,
                                                    nan_direction_hint);
        } else {
            return data->compare_at(0, 0, *rhs_const_column.data, nan_direction_hint);
        }
    }

    MutableColumns scatter(ColumnIndex num_columns, const Selector& selector) const override;

    void append_data_by_selector(MutableColumnPtr& res,
                                 const IColumn::Selector& selector) const override {
        LOG(FATAL) << "append_data_by_selector is not supported in ColumnConst!";
    }

    void for_each_subcolumn(ColumnCallback callback) override { callback(data); }

    bool structure_equals(const IColumn& rhs) const override {
        if (auto rhs_concrete = typeid_cast<const ColumnConst*>(&rhs))
            return data->structure_equals(*rhs_concrete->data);
        return false;
    }

    //    bool is_nullable() const override { return is_column_nullable(*data); }
    bool only_null() const override { return data->is_null_at(0); }
    bool is_numeric() const override { return data->is_numeric(); }
    bool is_fixed_and_contiguous() const override { return data->is_fixed_and_contiguous(); }
    bool values_have_fixed_size() const override { return data->values_have_fixed_size(); }
    size_t size_of_value_if_fixed() const override { return data->size_of_value_if_fixed(); }
    StringRef get_raw_data() const override { return data->get_raw_data(); }

    /// Not part of the common interface.

    IColumn& get_data_column() { return *data; }
    const IColumn& get_data_column() const { return *data; }
    const ColumnPtr& get_data_column_ptr() const { return data; }

    Field get_field() const { return get_data_column()[0]; }

    template <typename T>
    T get_value() const {
        return get_field().safe_get<NearestFieldType<T>>();
    }

    void replace_column_data(const IColumn& rhs, size_t row, size_t self_row = 0) override {
        DCHECK(size() > self_row);
        data->replace_column_data(rhs, row, self_row);
    }

    void replace_column_data_default(size_t self_row = 0) override {
        DCHECK(size() > self_row);
        LOG(FATAL) << "should not call the method in column const";
    }
};

/*
 * @return first : pointer to column itself if it's not ColumnConst, else to column's data column.
 *         second : zero if column is ColumnConst, else itself.
*/
std::pair<ColumnPtr, size_t> check_column_const_set_readability(const IColumn& column,
                                                                const size_t row_num) noexcept;

/*
 * @warning use this function sometimes cause performance problem in GCC.
*/
template <typename T>
    requires std::is_integral_v<T>
T index_check_const(T arg, bool constancy) noexcept {
    return constancy ? 0 : arg;
}

/*
 * @return first : data_column_ptr for ColumnConst, itself otherwise.
 *         second : whether it's ColumnConst.
*/
std::pair<const ColumnPtr&, bool> unpack_if_const(const ColumnPtr&) noexcept;

/*
 * For the functions that some columns of arguments are almost but not completely always const, we use this function to preprocessing its parameter columns
 * (which are not data columns). When we have two or more columns which only provide parameter, use this to deal with corner case. So you can specialize you
 * implementations for all const or all parameters const, without considering some of parameters are const.
 
 * Do the transformation only for the columns whose arg_indexes in parameters.
*/
void default_preprocess_parameter_columns(ColumnPtr* columns, const bool* col_const,
                                          const std::initializer_list<size_t>& parameters,
                                          Block& block, const ColumnNumbers& arg_indexes) noexcept;
} // namespace doris::vectorized
