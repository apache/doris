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

#include "vec/columns/column.h"
#include "vec/columns/column_impl.h"
#include "vec/columns/column_vector.h"
#include "vec/common/arena.h"
#include "vec/common/assert_cast.h"
#include "vec/core/types.h"

namespace doris::vectorized {

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

    static Ptr create(const ColumnPtr& nested_column, const ColumnPtr& offsets_column) {
        return ColumnArray::create(nested_column->assume_mutable(),
                                   offsets_column->assume_mutable());
    }

    static Ptr create(const ColumnPtr& nested_column) {
        return ColumnArray::create(nested_column->assume_mutable());
    }

    template <typename... Args,
              typename = typename std::enable_if<IsMutableColumns<Args...>::value>::type>
    static MutablePtr create(Args&&... args) {
        return Base::create(std::forward<Args>(args)...);
    }

    MutableColumnPtr get_shrinked_column() override;

    /** On the index i there is an offset to the beginning of the i + 1 -th element. */
    using ColumnOffsets = ColumnVector<Offset64>;

    std::string get_name() const override;
    const char* get_family_name() const override { return "Array"; }
    bool is_column_array() const override { return true; }
    bool can_be_inside_nullable() const override { return true; }
    TypeIndex get_data_type() const { return TypeIndex::Array; }
    MutableColumnPtr clone_resized(size_t size) const override;
    size_t size() const override;
    Field operator[](size_t n) const override;
    void get(size_t n, Field& res) const override;
    StringRef get_data_at(size_t n) const override;
    bool is_default_at(size_t n) const override;
    void insert_data(const char* pos, size_t length) override;
    StringRef serialize_value_into_arena(size_t n, Arena& arena, char const*& begin) const override;
    const char* deserialize_and_insert_from_arena(const char* pos) override;
    void update_hash_with_value(size_t n, SipHash& hash) const override;
    void insert_range_from(const IColumn& src, size_t start, size_t length) override;
    void insert(const Field& x) override;
    void insert_from(const IColumn& src_, size_t n) override;
    void insert_default() override;
    void pop_back(size_t n) override;
    ColumnPtr filter(const Filter& filt, ssize_t result_size_hint) const override;
    ColumnPtr permute(const Permutation& perm, size_t limit) const override;
    //ColumnPtr index(const IColumn & indexes, size_t limit) const;
    //template <typename Type> ColumnPtr index_impl(const PaddedPODArray<Type> & indexes, size_t limit) const;
    [[noreturn]] int compare_at(size_t n, size_t m, const IColumn& rhs_,
                                int nan_direction_hint) const override {
        LOG(FATAL) << "compare_at not implemented";
    }
    [[noreturn]] void get_permutation(bool reverse, size_t limit, int nan_direction_hint,
                                      Permutation& res) const override {
        LOG(FATAL) << "get_permutation not implemented";
    }
    void reserve(size_t n) override;
    size_t byte_size() const override;
    size_t allocated_bytes() const override;
    void protect() override;
    ColumnPtr replicate(const IColumn::Offsets& replicate_offsets) const override;
    void replicate(const uint32_t* counts, size_t target_size, IColumn& column, size_t begin = 0,
                   int count_sz = -1) const override;
    ColumnPtr convert_to_full_column_if_const() const override;
    void get_extremes(Field& min, Field& max) const override {
        LOG(FATAL) << "get_extremes not implemented";
    }

    /** More efficient methods of manipulation */
    IColumn& get_data() { return *data; }
    const IColumn& get_data() const { return *data; }

    IColumn& get_offsets_column() { return *offsets; }
    const IColumn& get_offsets_column() const { return *offsets; }

    Offsets64& ALWAYS_INLINE get_offsets() {
        return assert_cast<ColumnOffsets&>(*offsets).get_data();
    }

    const Offsets64& ALWAYS_INLINE get_offsets() const {
        return assert_cast<const ColumnOffsets&>(*offsets).get_data();
    }

    const ColumnPtr& get_data_ptr() const { return data; }
    ColumnPtr& get_data_ptr() { return data; }

    const ColumnPtr& get_offsets_ptr() const { return offsets; }
    ColumnPtr& get_offsets_ptr() { return offsets; }

    MutableColumns scatter(ColumnIndex num_columns, const Selector& selector) const override {
        return scatter_impl<ColumnArray>(num_columns, selector);
    }

    void append_data_by_selector(MutableColumnPtr& res,
                                 const IColumn::Selector& selector) const override {
        return append_data_by_selector_impl<ColumnArray>(res, selector);
    }

    void for_each_subcolumn(ColumnCallback callback) override {
        callback(offsets);
        callback(data);
    }

    void insert_indices_from(const IColumn& src, const int* indices_begin,
                             const int* indices_end) override;

    void replace_column_data(const IColumn&, size_t row, size_t self_row = 0) override {
        LOG(FATAL) << "replace_column_data not implemented";
    }
    void replace_column_data_default(size_t self_row = 0) override {
        LOG(FATAL) << "replace_column_data_default not implemented";
    }
    void clear() override {
        data->clear();
        offsets->clear();
    }

private:
    WrappedPtr data;
    WrappedPtr offsets;

    size_t ALWAYS_INLINE offset_at(ssize_t i) const { return get_offsets()[i - 1]; }
    size_t ALWAYS_INLINE size_at(ssize_t i) const {
        return get_offsets()[i] - get_offsets()[i - 1];
    }

    /// Multiply values if the nested column is ColumnVector<T>.
    template <typename T>
    ColumnPtr replicate_number(const IColumn::Offsets& replicate_offsets) const;

    /// Multiply the values if the nested column is ColumnString. The code is too complicated.
    ColumnPtr replicate_string(const IColumn::Offsets& replicate_offsets) const;

    /** Non-constant arrays of constant values are quite rare.
      * Most functions can not work with them, and does not create such columns as a result.
      * An exception is the function `replicate` (see FunctionsMiscellaneous.h), which has service meaning for the implementation of lambda functions.
      * Only for its sake is the implementation of the `replicate` method for ColumnArray(ColumnConst).
      */
    ColumnPtr replicate_const(const IColumn::Offsets& replicate_offsets) const;

    /** The following is done by simply replicating of nested columns.
      */
    ColumnPtr replicate_nullable(const IColumn::Offsets& replicate_offsets) const;
    ColumnPtr replicate_generic(const IColumn::Offsets& replicate_offsets) const;

    /// Specializations for the filter function.
    template <typename T>
    ColumnPtr filter_number(const Filter& filt, ssize_t result_size_hint) const;

    ColumnPtr filter_string(const Filter& filt, ssize_t result_size_hint) const;
    ColumnPtr filter_nullable(const Filter& filt, ssize_t result_size_hint) const;
    ColumnPtr filter_generic(const Filter& filt, ssize_t result_size_hint) const;
};

} // namespace doris::vectorized
