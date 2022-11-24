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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Columns/ColumnTuple.h
// and modified by Doris

/********************************************************************************
// doris/core/be/src/vec/core/field.h
class Field;
using FieldVector = std::vector<Field>;

/// Array and Tuple use the same storage type -- FieldVector, but we declare
/// distinct types for them, so that the caller can choose whether it wants to
/// construct a Field of Array or a Tuple type. An alternative approach would be
/// to construct both of these types from FieldVector, and have the caller
/// specify the desired Field type explicitly.

#define DEFINE_FIELD_VECTOR(X)          \
    struct X : public FieldVector {     \
        using FieldVector::FieldVector; \
    }

DEFINE_FIELD_VECTOR(Array);
DEFINE_FIELD_VECTOR(Tuple);

#undef DEFINE_FIELD_VECTOR

// defination of some pointer
using WrappedPtr = chameleon_ptr<Derived>;

using Ptr = immutable_ptr<Derived>;
using ColumnPtr = IColumn::Ptr;
using Columns = std::vector<ColumnPtr>;
using MutablePtr = mutable_ptr<Derived>;
using MutableColumnPtr = IColumn::MutablePtr;
using MutableColumns = std::vector<MutableColumnPtr>;
****************************************************************************/

#pragma once

#include "vec/columns/column.h"
#include "vec/columns/column_impl.h"
#include "vec/columns/column_vector.h"
#include "vec/common/arena.h"
#include "vec/common/assert_cast.h"
#include "vec/common/typeid_cast.h"
#include "vec/core/field.h"
#include "vec/core/types.h"

namespace doris::vectorized {

/** Column, that is just group of few another columns.
  *
  * For constant Tuples, see ColumnConst.
  * Mixed constant/non-constant columns is prohibited in tuple
  *  for implementation simplicity.
  */
class ColumnStruct final : public COWHelper<IColumn, ColumnStruct> {
private:
    friend class COWHelper<IColumn, ColumnStruct>;

    using TupleColumns = std::vector<WrappedPtr>;
    TupleColumns columns;

    template <bool positive>
    struct Less;

    ColumnStruct(Columns&& columns);
    ColumnStruct(TupleColumns&& tuple_columns);
    explicit ColumnStruct(MutableColumns&& mutable_columns);
    ColumnStruct(const ColumnStruct&) = default;

public:
    /** Create immutable column using immutable arguments. This arguments may be shared with other columns.
      * Use IColumn::mutate in order to make mutable column and mutate shared nested columns.
      */
    using Base = COWHelper<IColumn, ColumnStruct>;
    static Ptr create(Columns& columns);
    static Ptr create(MutableColumns& columns);
    static Ptr create(TupleColumns& columns);
    static Ptr create(Columns&& arg) { return create(arg); }

    template <typename... Args>
    static MutablePtr create(Args&&... args) {
        return Base::create(std::forward<Args>(args)...);
    }

    std::string get_name() const override;
    const char* get_family_name() const override { return "Struct"; }
    TypeIndex get_data_type() const { return TypeIndex::Struct; }

    MutableColumnPtr clone_empty() const override;
    MutableColumnPtr clone_resized(size_t size) const override;

    size_t size() const override { return columns.at(0)->size(); }

    Field operator[](size_t n) const override;
    void get(size_t n, Field& res) const override;

    bool is_default_at(size_t n) const override;
    StringRef get_data_at(size_t n) const override;
    void insert_data(const char* pos, size_t length) override;
    void insert(const Field& x) override;
    void insert_from(const IColumn& src_, size_t n) override;
    void insert_default() override;
    void pop_back(size_t n) override;
    StringRef serialize_value_into_arena(size_t n, Arena& arena, char const*& begin) const override;
    const char* deserialize_and_insert_from_arena(const char* pos) override;
    void update_hash_with_value(size_t n, SipHash& hash) const override;

    // const char * skip_serialized_in_arena(const char * pos) const override;
    // void update_weak_hash32(WeakHash32 & hash) const override;
    // void update_hash_fast(SipHash & hash) const override;

    void insert_indices_from(const IColumn& src, const int* indices_begin,
                             const int* indices_end) override {
        LOG(FATAL) << "insert_indices_from not implemented";
    }

    void get_permutation(bool reverse, size_t limit, int nan_direction_hint,
                         Permutation& res) const override {
        LOG(FATAL) << "get_permutation not implemented";
    }
    void append_data_by_selector(MutableColumnPtr& res, const Selector& selector) const override {
        return append_data_by_selector_impl<ColumnStruct>(res, selector);
    }
    void replace_column_data(const IColumn&, size_t row, size_t self_row = 0) override {
        LOG(FATAL) << "replace_column_data not implemented";
    }
    void replace_column_data_default(size_t self_row = 0) override {
        LOG(FATAL) << "replace_column_data_default not implemented";
    }

    void insert_range_from(const IColumn& src, size_t start, size_t length) override;
    ColumnPtr filter(const Filter& filt, ssize_t result_size_hint) const override;
    ColumnPtr permute(const Permutation& perm, size_t limit) const override;
    ColumnPtr replicate(const Offsets& offsets) const override;
    MutableColumns scatter(ColumnIndex num_columns, const Selector& selector) const override;

    // ColumnPtr index(const IColumn & indexes, size_t limit) const override;
    // void expand(const Filter & mask, bool inverted) override;
    // void gather(ColumnGathererStream & gatherer_stream) override;
    // bool has_equal_values() const override;

    // void compare_column(const IColumn& rhs, size_t rhs_row_num, PaddedPODArray<UInt64>* row_indexes,
    //                     PaddedPODArray<Int8>& compare_results, int direction,
    //                     int nan_direction_hint) const override;
    // int compare_at_with_collation(size_t n, size_t m, const IColumn& rhs, int nan_direction_hint,
    //                               const Collator& collator) const override;

    int compare_at(size_t n, size_t m, const IColumn& rhs, int nan_direction_hint) const override;
    void get_extremes(Field& min, Field& max) const override;

    // void get_permutation(IColumn::PermutationSortDirection direction,
    //                      IColumn::PermutationSortStability stability, size_t limit,
    //                      int nan_direction_hint, IColumn::Permutation& res) const override;
    // void update_permutation(IColumn::PermutationSortDirection direction,
    //                         IColumn::PermutationSortStability stability, size_t limit,
    //                         int nan_direction_hint, IColumn::Permutation& res,
    //                         EqualRanges& equal_ranges) const override;
    // void get_permutation_with_collation(const Collator& collator,
    //                                     IColumn::PermutationSortDirection direction,
    //                                     IColumn::PermutationSortStability stability, size_t limit,
    //                                     int nan_direction_hint,
    //                                     IColumn::Permutation& res) const override;
    // void update_permutation_with_collation(const Collator& collator,
    //                                        IColumn::PermutationSortDirection direction,
    //                                        IColumn::PermutationSortStability stability,
    //                                        size_t limit, int nan_direction_hint,
    //                                        IColumn::Permutation& res,
    //                                        EqualRanges& equal_ranges) const override;

    void reserve(size_t n) override;
    size_t byte_size() const override;

    // size_t byte_size_at(size_t n) const override;
    // void ensure_ownership() override;

    size_t allocated_bytes() const override;
    void protect() override;
    void for_each_subcolumn(ColumnCallback callback) override;
    bool structure_equals(const IColumn& rhs) const override;

    // void for_each_subcolumn_recursively(ColumnCallback callback) override;
    // bool is_collation_supported() const override;
    // ColumnPtr compress() const override;
    // double get_ratio_of_default_rows(double sample_ratio) const override;
    // void get_indices_of_nondefault_rows(Offsets & indices, size_t from, size_t limit) const override;
    // void finalize() override;
    // bool is_finalized() const override;

    size_t tuple_size() const { return columns.size(); }

    const IColumn& get_column(size_t idx) const { return *columns[idx]; }
    IColumn& get_column(size_t idx) { return *columns[idx]; }

    const TupleColumns& get_columns() const { return columns; }
    Columns get_columns_copy() const { return {columns.begin(), columns.end()}; }

    const ColumnPtr& get_column_ptr(size_t idx) const { return columns[idx]; }
    ColumnPtr& get_column_ptr(size_t idx) { return columns[idx]; }

private:
    int compare_at_impl(size_t n, size_t m, const IColumn& rhs, int nan_direction_hint) const;

    // void get_permutation_impl(IColumn::PermutationSortDirection direction,
    //                           IColumn::PermutationSortStability stability, size_t limit,
    //                           int nan_direction_hint, Permutation& res,
    //                           const Collator* collator) const;

    // void update_permutation_impl(IColumn::PermutationSortDirection direction,
    //                              IColumn::PermutationSortStability stability, size_t limit,
    //                              int nan_direction_hint, IColumn::Permutation& res,
    //                              EqualRanges& equal_ranges,
    //                              const Collator* collator = nullptr) const;
};

} // namespace doris::vectorized