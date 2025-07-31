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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Columns/ColumnArray.h
// and modified by Doris

#pragma once

#include <glog/logging.h>
#include <sys/types.h>

#include <cstdint>
#include <functional>
#include <string>
#include <type_traits>
#include <utility>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "vec/columns/column.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/common/cow.h"
#include "vec/common/string_ref.h"
#include "vec/core/field.h"
#include "vec/core/types.h"

class SipHash;

//TODO: use marcos below to decouple array function calls
#define ALL_COLUMNS_NUMBER                                                                       \
    ColumnUInt8, ColumnInt8, ColumnInt16, ColumnInt32, ColumnInt64, ColumnInt128, ColumnFloat32, \
            ColumnFloat64, ColumnDecimal32, ColumnDecimal64, ColumnDecimal128V3,                 \
            ColumnDecimal128V2, ColumnDecimal256
#define ALL_COLUMNS_TIME ColumnDate, ColumnDateTime, ColumnDateV2, ColumnDateTimeV2
#define ALL_COLUMNS_NUMERIC ALL_COLUMNS_NUMBER, ALL_COLUMNS_TIME
#define ALL_COLUMNS_SIMPLE ALL_COLUMNS_NUMERIC, ColumnString

namespace doris::vectorized {

class Arena;

/** Obtaining array as Field can be slow for large arrays and consume vast amount of memory.
  * Just don't allow to do it.
  * You can increase the limit if the following query:
  *  SELECT range(10000000)
  * will take less than 500ms on your machine.
  */
static constexpr size_t max_array_size_as_field = 1000000;
/** A column of array values.
  * In memory, it is represented as one column of a nested type, whose size is equal to the sum of the sizes of all arrays,
  *  and as an array of offsets in it, which allows you to get each element.
  */
class ColumnArray final : public COWHelper<IColumn, ColumnArray> {
private:
    friend class COWHelper<IColumn, ColumnArray>;

    /** Create an array column with specified values and offsets. */
    ColumnArray(MutableColumnPtr&& nested_column, MutableColumnPtr&& offsets_column);

    /** Create an empty column of arrays with the type of values as in the column `nested_column` */
    explicit ColumnArray(MutableColumnPtr&& nested_column);

    ColumnArray(const ColumnArray&) = default;

    ColumnArray() = default;

public:
    // offsets of array is 64bit wise
    using Offset64 = IColumn::Offset64;
    using Offsets64 = IColumn::Offsets64;

private:
    // please use IColumn::Offset if we really need 32bit offset, otherwise use ColumnArray::Offset64
    using Offset [[deprecated("ColumnArray::Offset64 for Array, IColumn::Offset for String")]] =
            Offset64;
    using Offsets [[deprecated("ColumnArray::Offsets64 for Array, IColumn::Offsets for String")]] =
            Offsets64;

public:
    /** Create immutable column using immutable arguments. This arguments may be shared with other columns.
      * Use IColumn::mutate in order to make mutable column and mutate shared nested columns.
      */
    using Base = COWHelper<IColumn, ColumnArray>;

    static MutablePtr create(const ColumnPtr& nested_column, const ColumnPtr& offsets_column) {
        return ColumnArray::create(nested_column->assume_mutable(),
                                   offsets_column->assume_mutable());
    }

    static MutablePtr create(const ColumnPtr& nested_column) {
        return ColumnArray::create(nested_column->assume_mutable());
    }

    template <typename... Args,
              typename = typename std::enable_if<IsMutableColumns<Args...>::value>::type>
    static MutablePtr create(Args&&... args) {
        return Base::create(std::forward<Args>(args)...);
    }

    void sanity_check() const override {
        data->sanity_check();
        offsets->sanity_check();
    }

    void shrink_padding_chars() override;

    /** On the index i there is an offset to the beginning of the i + 1 -th element. */
    using ColumnOffsets = ColumnOffset64;

    std::string get_name() const override;
    bool is_variable_length() const override { return true; }

    bool is_exclusive() const override {
        return IColumn::is_exclusive() && data->is_exclusive() && offsets->is_exclusive();
    }

    MutableColumnPtr clone_resized(size_t size) const override;
    size_t size() const override;
    void resize(size_t n) override;
    Field operator[](size_t n) const override;
    void get(size_t n, Field& res) const override;
    bool is_default_at(size_t n) const;
    StringRef serialize_value_into_arena(size_t n, Arena& arena, char const*& begin) const override;
    const char* deserialize_and_insert_from_arena(const char* pos) override;
    void update_hash_with_value(size_t n, SipHash& hash) const override;
    void update_xxHash_with_value(size_t start, size_t end, uint64_t& hash,
                                  const uint8_t* __restrict null_data) const override;
    void update_crc_with_value(size_t start, size_t end, uint32_t& hash,
                               const uint8_t* __restrict null_data) const override;

    void update_hashes_with_value(uint64_t* __restrict hashes,
                                  const uint8_t* __restrict null_data = nullptr) const override;

    void update_crcs_with_value(uint32_t* __restrict hash, PrimitiveType type, uint32_t rows,
                                uint32_t offset = 0,
                                const uint8_t* __restrict null_data = nullptr) const override;

    void insert_range_from(const IColumn& src, size_t start, size_t length) override;
    void insert_range_from_ignore_overflow(const IColumn& src, size_t start,
                                           size_t length) override;
    void insert(const Field& x) override;
    void insert_from(const IColumn& src_, size_t n) override;
    void insert_default() override;
    void pop_back(size_t n) override;
    ColumnPtr filter(const Filter& filt, ssize_t result_size_hint) const override;
    size_t filter(const Filter& filter) override;
    MutableColumnPtr permute(const Permutation& perm, size_t limit) const override;
    int compare_at(size_t n, size_t m, const IColumn& rhs_, int nan_direction_hint) const override;
    void reserve(size_t n) override;
    size_t byte_size() const override;
    size_t allocated_bytes() const override;
    bool has_enough_capacity(const IColumn& src) const override;
    void insert_many_from(const IColumn& src, size_t position, size_t length) override;
    void get_permutation(bool reverse, size_t limit, int nan_direction_hint,
                         IColumn::Permutation& res) const override;
    void sort_column(const ColumnSorter* sorter, EqualFlags& flags, IColumn::Permutation& perms,
                     EqualRange& range, bool last_column) const override;
    void deserialize_vec(StringRef* keys, const size_t num_rows) override;
    size_t get_max_row_byte_size() const override;
    void serialize_vec(StringRef* keys, size_t num_rows) const override;
    /** More efficient methods of manipulation */
    IColumn& get_data() { return *data; }
    const IColumn& get_data() const { return *data; }

    IColumn& get_offsets_column() { return *offsets; }
    const IColumn& get_offsets_column() const { return *offsets; }

    Offsets64& ALWAYS_INLINE get_offsets() {
        return assert_cast<ColumnOffsets&, TypeCheckOnRelease::DISABLE>(*offsets).get_data();
    }

    const Offsets64& ALWAYS_INLINE get_offsets() const {
        return assert_cast<const ColumnOffsets&, TypeCheckOnRelease::DISABLE>(*offsets).get_data();
    }

    bool has_equal_offsets(const ColumnArray& other) const;

    const ColumnPtr& get_data_ptr() const { return data; }
    ColumnPtr& get_data_ptr() { return data; }

    const ColumnPtr& get_offsets_ptr() const { return offsets; }
    ColumnPtr& get_offsets_ptr() { return offsets; }

    size_t ALWAYS_INLINE offset_at(ssize_t i) const { return get_offsets()[i - 1]; }
    size_t ALWAYS_INLINE size_at(ssize_t i) const {
        return get_offsets()[i] - get_offsets()[i - 1];
    }

    void for_each_subcolumn(ColumnCallback callback) override {
        callback(offsets);
        callback(data);
    }

    ColumnPtr convert_column_if_overflow() override {
        data = data->convert_column_if_overflow();
        return IColumn::convert_column_if_overflow();
    }

    void insert_indices_from(const IColumn& src, const uint32_t* indices_begin,
                             const uint32_t* indices_end) override;

    void replace_column_data(const IColumn& rhs, size_t row, size_t self_row = 0) override {
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "Method replace_column_data is not supported for " + get_name());
    }

    void clear() override {
        data->clear();
        offsets->clear();
    }

    size_t get_number_of_dimensions() const {
        const auto* nested_array = check_and_get_column<ColumnArray>(*data);
        if (!nested_array) {
            return 1;
        }
        return 1 +
               nested_array
                       ->get_number_of_dimensions(); /// Every modern C++ compiler optimizes tail recursion.
    }

    void erase(size_t start, size_t length) override;

    size_t serialize_impl(char* pos, const size_t row) const override;
    size_t deserialize_impl(const char* pos) override;
    size_t serialize_size_at(size_t row) const override;
    template <bool positive>
    struct less;

private:
    // [2,1,5,9,1]\n[1,2,4] --> data column [2,1,5,9,1,1,2,4], offset[-1] = 0, offset[0] = 5, offset[1] = 8
    // [[2,1,5],[9,1]]\n[[1,2]] --> data column [3 column array], offset[-1] = 0, offset[0] = 2, offset[1] = 3
    WrappedPtr data;
    WrappedPtr offsets;
};

} // namespace doris::vectorized
