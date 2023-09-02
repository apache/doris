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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Columns/ColumnObject.h
// and modified by Doris

#pragma once
#include <glog/logging.h>
#include <sys/types.h>

#include <algorithm>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/status.h"
#include "vec/columns/column.h"
#include "vec/columns/subcolumn_tree.h"
#include "vec/common/cow.h"
#include "vec/common/string_ref.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/json/path_in_data.h"

class SipHash;

namespace doris {
namespace vectorized {
class Arena;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

/// Info that represents a scalar or array field in a decomposed view.
/// It allows to recreate field with different number
/// of dimensions or nullability.
struct FieldInfo {
    /// The common type of of all scalars in field.
    DataTypePtr scalar_type;
    /// Do we have NULL scalar in field.
    bool have_nulls;
    /// If true then we have scalars with different types in array and
    /// we need to convert scalars to the common type.
    bool need_convert;
    /// Number of dimension in array. 0 if field is scalar.
    size_t num_dimensions;
};
void get_field_info(const Field& field, FieldInfo* info);
/** A column that represents object with dynamic set of subcolumns.
 *  Subcolumns are identified by paths in document and are stored in
 *  a trie-like structure. ColumnObject is not suitable for writing into tables
 *  and it should be converted to Tuple with fixed set of subcolumns before that.
 */
class ColumnObject final : public COWHelper<IColumn, ColumnObject> {
public:
    /** Class that represents one subcolumn.
     * It stores values in several parts of column
     * and keeps current common type of all parts.
     * We add a new column part with a new type, when we insert a field,
     * which can't be converted to the current common type.
     * After insertion of all values subcolumn should be finalized
     * for writing and other operations.
     */
    class Subcolumn {
    public:
        Subcolumn() = default;

        Subcolumn(size_t size_, bool is_nullable_);

        Subcolumn(MutableColumnPtr&& data_, bool is_nullable_);

        size_t size() const;

        size_t byteSize() const;

        size_t allocatedBytes() const;

        bool is_finalized() const;

        const DataTypePtr& get_least_common_type() const { return least_common_type.get(); }

        const DataTypePtr& get_least_common_typeBase() const { return least_common_type.getBase(); }

        size_t get_dimensions() const { return least_common_type.get_dimensions(); }

        /// Checks the consistency of column's parts stored in @data.
        void checkTypes() const;

        /// Inserts a field, which scalars can be arbitrary, but number of
        /// dimensions should be consistent with current common type.
        /// throws InvalidArgument when meet conflict types
        void insert(Field field);

        void insert(Field field, FieldInfo info);

        void insertDefault();

        void insertManyDefaults(size_t length);

        void insertRangeFrom(const Subcolumn& src, size_t start, size_t length);

        void pop_back(size_t n);

        /// Converts all column's parts to the common type and
        /// creates a single column that stores all values.
        void finalize();

        /// Returns last inserted field.
        Field get_last_field() const;

        /// Recreates subcolumn with default scalar values and keeps sizes of arrays.
        /// Used to create columns of type Nested with consistent array sizes.
        Subcolumn recreate_with_default_values(const FieldInfo& field_info) const;

        /// Returns single column if subcolumn in finalizes.
        /// Otherwise -- undefined behaviour.
        IColumn& get_finalized_column();

        const IColumn& get_finalized_column() const;

        const ColumnPtr& get_finalized_column_ptr() const;

        void remove_nullable();

        friend class ColumnObject;

    private:
        class LeastCommonType {
        public:
            LeastCommonType() = default;

            explicit LeastCommonType(DataTypePtr type_);

            const DataTypePtr& get() const { return type; }

            const DataTypePtr& getBase() const { return base_type; }

            size_t get_dimensions() const { return num_dimensions; }

            void remove_nullable() { type = doris::vectorized::remove_nullable(type); }

        private:
            DataTypePtr type;
            DataTypePtr base_type;
            size_t num_dimensions = 0;
        };
        void add_new_column_part(DataTypePtr type);

        /// Current least common type of all values inserted to this subcolumn.
        LeastCommonType least_common_type;
        /// If true then common type type of subcolumn is Nullable
        /// and default values are NULLs.
        bool is_nullable = false;
        /// Parts of column. Parts should be in increasing order in terms of subtypes/supertypes.
        /// That means that the least common type for i-th prefix is the type of i-th part
        /// and it's the supertype for all type of column from 0 to i-1.
        std::vector<WrappedPtr> data;
        /// Until we insert any non-default field we don't know further
        /// least common type and we count number of defaults in prefix,
        /// which will be converted to the default type of final common type.
        size_t num_of_defaults_in_prefix = 0;
    };
    using Subcolumns = SubcolumnsTree<Subcolumn>;

private:
    /// If true then all subcolumns are nullable.
    const bool is_nullable;
    Subcolumns subcolumns;
    size_t num_rows;

public:
    static constexpr auto COLUMN_NAME_DUMMY = "_dummy";

    explicit ColumnObject(bool is_nullable_);

    ColumnObject(Subcolumns&& subcolumns_, bool is_nullable_);

    ~ColumnObject() override = default;

    bool can_be_inside_nullable() const override { return true; }

    /// Checks that all subcolumns have consistent sizes.
    void check_consistency() const;

    bool has_subcolumn(const PathInData& key) const;

    // return null if not found
    const Subcolumn* get_subcolumn(const PathInData& key) const;

    /** More efficient methods of manipulation */
    [[noreturn]] IColumn& get_data() { LOG(FATAL) << "Not implemented method get_data()"; }
    [[noreturn]] const IColumn& get_data() const {
        LOG(FATAL) << "Not implemented method get_data()";
    }

    // return null if not found
    Subcolumn* get_subcolumn(const PathInData& key);

    void incr_num_rows() { ++num_rows; }

    void incr_num_rows(size_t n) { num_rows += n; }

    void set_num_rows(size_t n) { num_rows = n; }

    size_t rows() const { return num_rows; }

    /// Adds a subcolumn from existing IColumn.
    bool add_sub_column(const PathInData& key, MutableColumnPtr&& subcolumn);

    /// Adds a subcolumn of specific size with default values.
    bool add_sub_column(const PathInData& key, size_t new_size);

    /// Adds a subcolumn of type Nested of specific size with default values.
    /// It cares about consistency of sizes of Nested arrays.
    bool add_nested_subcolumn(const PathInData& key, const FieldInfo& field_info, size_t new_size);

    const Subcolumns& get_subcolumns() const { return subcolumns; }

    Subcolumns& get_subcolumns() { return subcolumns; }

    PathsInData getKeys() const;

    std::string get_keys_str() const {
        std::stringstream ss;
        bool first = true;
        for (auto& k : getKeys()) {
            if (first) {
                first = false;
            } else {
                ss << ", ";
            }
            ss << k.get_path();
        }

        return ss.str();
    }

    void remove_subcolumns(const std::unordered_set<std::string>& keys);

    /// Finalizes all subcolumns.
    void finalize();

    bool is_finalized() const;

    /// Part of interface
    const char* get_family_name() const override { return "Variant"; }

    TypeIndex get_data_type() const override { return TypeIndex::VARIANT; }

    size_t size() const override;

    MutableColumnPtr clone_resized(size_t new_size) const override;

    size_t byte_size() const override;

    size_t allocated_bytes() const override;

    void for_each_subcolumn(ColumnCallback callback) override;

    // Do nothing, call try_insert instead
    void insert(const Field& field) override { try_insert(field); }

    void insert_range_from(const IColumn& src, size_t start, size_t length) override;

    void append_data_by_selector(MutableColumnPtr& res,
                                 const IColumn::Selector& selector) const override;

    void insert_indices_from(const IColumn& src, const int* indices_begin,
                             const int* indices_end) override;

    // May throw execption
    void try_insert(const Field& field);

    void try_insert_from(const IColumn& src, size_t n);

    void try_insert_range_from(const IColumn& src, size_t start, size_t length);

    void insert_default() override;

    // Revise this column to specified num_rows
    void revise_to(int num_rows);

    [[noreturn]] ColumnPtr replicate(const Offsets& offsets) const override {
        LOG(FATAL) << "should not call the method replicate in column object";
    }

    void pop_back(size_t length) override;

    Field operator[](size_t n) const override;

    void get(size_t n, Field& res) const override;

    /// All other methods throw exception.
    StringRef get_data_at(size_t) const override {
        LOG(FATAL) << "should not call the method in column object";
        return StringRef();
    }

    Status try_insert_indices_from(const IColumn& src, const int* indices_begin,
                                   const int* indices_end);

    StringRef serialize_value_into_arena(size_t n, Arena& arena,
                                         char const*& begin) const override {
        LOG(FATAL) << "should not call the method in column object";
        return StringRef();
    }

    const char* deserialize_and_insert_from_arena(const char* pos) override {
        LOG(FATAL) << "should not call the method in column object";
        return nullptr;
    }

    void update_hash_with_value(size_t n, SipHash& hash) const override {
        LOG(FATAL) << "should not call the method in column object";
    }

    void insert_data(const char* pos, size_t length) override {
        LOG(FATAL) << "should not call the method in column object";
    }

    ColumnPtr filter(const Filter&, ssize_t) const override;

    size_t filter(const Filter&) override;

    ColumnPtr permute(const Permutation&, size_t) const override {
        LOG(FATAL) << "should not call the method in column object";
        return nullptr;
    }

    int compare_at(size_t n, size_t m, const IColumn& rhs, int nan_direction_hint) const override {
        LOG(FATAL) << "should not call the method in column object";
        return 0;
    }

    void get_permutation(bool reverse, size_t limit, int nan_direction_hint,
                         Permutation& res) const override {
        LOG(FATAL) << "should not call the method in column object";
    }

    MutableColumns scatter(ColumnIndex, const Selector&) const override {
        LOG(FATAL) << "should not call the method in column object";
        return {};
    }

    void replace_column_data(const IColumn&, size_t row, size_t self_row) override {
        LOG(FATAL) << "should not call the method in column object";
    }

    void replace_column_data_default(size_t self_row) override {
        LOG(FATAL) << "should not call the method in column object";
    }

    void get_extremes(Field& min, Field& max) const override {
        LOG(FATAL) << "should not call the method in column object";
    }

    void get_indices_of_non_default_rows(Offsets64&, size_t, size_t) const override {
        LOG(FATAL) << "should not call the method in column object";
    }

    void replicate(const uint32_t* indexs, size_t target_size, IColumn& column) const override {
        LOG(FATAL) << "not support";
    }

    template <typename Func>
    ColumnPtr apply_for_subcolumns(Func&& func, std::string_view func_name) const;

    ColumnPtr index(const IColumn& indexes, size_t limit) const override;

    void strip_outer_array();

    bool empty() const;
};
} // namespace doris::vectorized
