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

#include "vec/columns/column.h"
#include "vec/common/assert_cast.h"
#include "vec/common/exception.h"
#include "vec/common/typeid_cast.h"
#include "vec/core/field.h"

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

    MutableColumnPtr clone_resized(size_t new_size) const override {
        return ColumnConst::create(data, new_size);
    }

    size_t size() const override { return s; }

    Field operator[](size_t) const override { return (*data)[0]; }

    void get(size_t, Field& res) const override { data->get(0, res); }

    StringRef get_data_at(size_t) const override { return data->get_data_at(0); }

    StringRef get_data_at_with_terminating_zero(size_t) const override {
        return data->get_data_at_with_terminating_zero(0);
    }

    UInt64 get64(size_t) const override { return data->get64(0); }

    UInt64 get_uint(size_t) const override { return data->get_uint(0); }

    Int64 get_int(size_t) const override { return data->get_int(0); }

    bool get_bool(size_t) const override { return data->get_bool(0); }

    Float64 get_float64(size_t) const override { return data->get_float64(0); }

    bool is_null_at(size_t) const override { return data->is_null_at(0); }

    void insert_range_from(const IColumn&, size_t /*start*/, size_t length) override {
        s += length;
    }

    void insert(const Field&) override { ++s; }

    void insert_data(const char*, size_t) override { ++s; }

    void insert_from(const IColumn&, size_t) override { ++s; }

    void insert_default() override { ++s; }

    void pop_back(size_t n) override { s -= n; }

    StringRef serialize_value_into_arena(size_t, Arena& arena, char const*& begin) const override {
        return data->serialize_value_into_arena(0, arena, begin);
    }

    const char* deserialize_and_insert_from_arena(const char* pos) override {
        auto res = data->deserialize_and_insert_from_arena(pos);
        data->pop_back(1);
        ++s;
        return res;
    }

    void update_hash_with_value(size_t, SipHash& hash) const override {
        data->update_hash_with_value(0, hash);
    }

    ColumnPtr filter(const Filter& filt, ssize_t result_size_hint) const override;
    ColumnPtr replicate(const Offsets& offsets) const override;
    ColumnPtr permute(const Permutation& perm, size_t limit) const override;
    // ColumnPtr index(const IColumn & indexes, size_t limit) const override;
    void get_permutation(bool reverse, size_t limit, int nan_direction_hint,
                         Permutation& res) const override;

    size_t byte_size() const override { return data->byte_size() + sizeof(s); }

    size_t allocated_bytes() const override { return data->allocated_bytes() + sizeof(s); }

    int compare_at(size_t, size_t, const IColumn& rhs, int nan_direction_hint) const override {
        return data->compare_at(0, 0, *assert_cast<const ColumnConst&>(rhs).data,
                                nan_direction_hint);
    }

    MutableColumns scatter(ColumnIndex num_columns, const Selector& selector) const override;

    void get_extremes(Field& min, Field& max) const override { data->get_extremes(min, max); }

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
};

} // namespace doris::vectorized
