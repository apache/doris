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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Columns/ColumnVector.h
// and modified by Doris

#pragma once

#include <cmath>
#include <type_traits>

#include "olap/uint24.h"
#include "vec/columns/column.h"
#include "vec/columns/column_impl.h"
#include "vec/columns/column_vector_helper.h"
#include "vec/common/unaligned.h"
#include "vec/core/field.h"

namespace doris::vectorized {

/** Stuff for comparing numbers.
  * Integer values are compared as usual.
  * Floating-point numbers are compared this way that NaNs always end up at the end
  *  (if you don't do this, the sort would not work at all).
  */
template <typename T>
struct CompareHelper {
    static bool less(T a, T b, int /*nan_direction_hint*/) { return a < b; }
    static bool greater(T a, T b, int /*nan_direction_hint*/) { return a > b; }

    /** Compares two numbers. Returns a number less than zero, equal to zero, or greater than zero if a < b, a == b, a > b, respectively.
      * If one of the values is NaN, then
      * - if nan_direction_hint == -1 - NaN are considered less than all numbers;
      * - if nan_direction_hint == 1 - NaN are considered to be larger than all numbers;
      * Essentially: nan_direction_hint == -1 says that the comparison is for sorting in descending order.
      */
    static int compare(T a, T b, int /*nan_direction_hint*/) {
        return a > b ? 1 : (a < b ? -1 : 0);
    }
};

template <typename T>
struct FloatCompareHelper {
    static bool less(T a, T b, int nan_direction_hint) {
        bool isnan_a = std::isnan(a);
        bool isnan_b = std::isnan(b);

        if (isnan_a && isnan_b) return false;
        if (isnan_a) return nan_direction_hint < 0;
        if (isnan_b) return nan_direction_hint > 0;

        return a < b;
    }

    static bool greater(T a, T b, int nan_direction_hint) {
        bool isnan_a = std::isnan(a);
        bool isnan_b = std::isnan(b);

        if (isnan_a && isnan_b) return false;
        if (isnan_a) return nan_direction_hint > 0;
        if (isnan_b) return nan_direction_hint < 0;

        return a > b;
    }

    static int compare(T a, T b, int nan_direction_hint) {
        bool isnan_a = std::isnan(a);
        bool isnan_b = std::isnan(b);
        if (UNLIKELY(isnan_a || isnan_b)) {
            if (isnan_a && isnan_b) return 0;

            return isnan_a ? nan_direction_hint : -nan_direction_hint;
        }

        return (T(0) < (a - b)) - ((a - b) < T(0));
    }
};

template <>
struct CompareHelper<Float32> : public FloatCompareHelper<Float32> {};
template <>
struct CompareHelper<Float64> : public FloatCompareHelper<Float64> {};

/** A template for columns that use a simple array to store.
 */
template <typename T>
class ColumnVector final : public COWHelper<ColumnVectorHelper, ColumnVector<T>> {
    static_assert(!IsDecimalNumber<T>);

private:
    using Self = ColumnVector;
    friend class COWHelper<ColumnVectorHelper, Self>;

    struct less;
    struct greater;

public:
    using value_type = T;
    using Container = PaddedPODArray<value_type>;

private:
    ColumnVector() {}
    ColumnVector(const size_t n) : data(n) {}
    ColumnVector(const size_t n, const value_type x) : data(n, x) {}
    ColumnVector(const ColumnVector& src) : data(src.data.begin(), src.data.end()) {}

    /// Sugar constructor.
    ColumnVector(std::initializer_list<T> il) : data {il} {}

    void insert_res_column(const uint16_t* sel, size_t sel_size,
                           vectorized::ColumnVector<T>* res_ptr) {
        auto& res_data = res_ptr->data;
        DCHECK(res_data.empty());
        res_data.reserve(sel_size);
        T* t = (T*)res_data.get_end_ptr();
        for (size_t i = 0; i < sel_size; i++) {
            t[i] = T(data[sel[i]]);
        }
        res_data.set_end_ptr(t + sel_size);
    }

    void insert_many_default_type(const char* data_ptr, size_t num) {
        T* input_val_ptr = (T*)data_ptr;
        T* res_val_ptr = (T*)data.get_end_ptr();
        for (int i = 0; i < num; i++) {
            res_val_ptr[i] = input_val_ptr[i];
        }
        res_val_ptr += num;
        data.set_end_ptr(res_val_ptr);
    }

public:
    bool is_numeric() const override { return IsNumber<T>; }

    size_t size() const override { return data.size(); }

    StringRef get_data_at(size_t n) const override {
        return StringRef(reinterpret_cast<const char*>(&data[n]), sizeof(data[n]));
    }

    void insert_from(const IColumn& src, size_t n) override {
        data.push_back(static_cast<const Self&>(src).get_data()[n]);
    }

    void insert_data(const char* pos, size_t /*length*/) override {
        data.push_back(unaligned_load<T>(pos));
    }

    // note(wb) type of data_ptr element should be same with current column_vector's T
    void insert_many_in_copy_way(const char* data_ptr, size_t num) {
        char* res_ptr = (char*)data.get_end_ptr();
        memcpy(res_ptr, data_ptr, num * sizeof(T));
        res_ptr += num * sizeof(T);
        data.set_end_ptr(res_ptr);
    }

    void insert_date_column(const char* data_ptr, size_t num) {
        size_t value_size = sizeof(uint24_t);
        for (int i = 0; i < num; i++) {
            const char* cur_ptr = data_ptr + value_size * i;
            uint64_t value = 0;
            value = *(unsigned char*)(cur_ptr + 2);
            value <<= 8;
            value |= *(unsigned char*)(cur_ptr + 1);
            value <<= 8;
            value |= *(unsigned char*)(cur_ptr);
            vectorized::VecDateTimeValue date = VecDateTimeValue::create_from_olap_date(value);
            this->insert_data(reinterpret_cast<char*>(&date), 0);
        }
    }

    void insert_datetime_column(const char* data_ptr, size_t num) {
        size_t value_size = sizeof(uint64_t);
        for (int i = 0; i < num; i++) {
            const char* cur_ptr = data_ptr + value_size * i;
            uint64_t value = *reinterpret_cast<const uint64_t*>(cur_ptr);
            vectorized::VecDateTimeValue datetime =
                    VecDateTimeValue::create_from_olap_datetime(value);
            this->insert_data(reinterpret_cast<char*>(&datetime), 0);
        }
    }

    /*
        use by date, datetime, basic type
    */
    void insert_many_fix_len_data(const char* data_ptr, size_t num) override {
        if constexpr (std::is_same_v<T, vectorized::Int128>) {
            insert_many_in_copy_way(data_ptr, num);
        } else if (IColumn::is_date) {
            insert_date_column(data_ptr, num);
        } else if (IColumn::is_date_time) {
            insert_datetime_column(data_ptr, num);
        } else {
            insert_many_default_type(data_ptr, num);
        }
    }

    void insert_default() override { data.push_back(T()); }

    void insert_many_defaults(size_t length) override {
        size_t old_size = data.size();
        data.resize(old_size + length);
        memset(data.data() + old_size, 0, length * sizeof(data[0]));
    }

    void pop_back(size_t n) override { data.resize_assume_reserved(data.size() - n); }

    StringRef serialize_value_into_arena(size_t n, Arena& arena, char const*& begin) const override;

    const char* deserialize_and_insert_from_arena(const char* pos) override;

    void update_hash_with_value(size_t n, SipHash& hash) const override;

    size_t byte_size() const override { return data.size() * sizeof(data[0]); }

    size_t allocated_bytes() const override { return data.allocated_bytes(); }

    void protect() override { data.protect(); }

    void insert_value(const T value) { data.push_back(value); }

    /// This method implemented in header because it could be possibly devirtualized.
    int compare_at(size_t n, size_t m, const IColumn& rhs_, int nan_direction_hint) const override {
        return CompareHelper<T>::compare(data[n], static_cast<const Self&>(rhs_).data[m],
                                         nan_direction_hint);
    }

    void get_permutation(bool reverse, size_t limit, int nan_direction_hint,
                         IColumn::Permutation& res) const override;

    void reserve(size_t n) override { data.reserve(n); }

    void resize(size_t n) override { data.resize(n); }

    const char* get_family_name() const override;

    MutableColumnPtr clone_resized(size_t size) const override;

    Field operator[](size_t n) const override { return data[n]; }

    void get(size_t n, Field& res) const override { res = (*this)[n]; }

    UInt64 get64(size_t n) const override;

    Float64 get_float64(size_t n) const override;

    void clear() override { data.clear(); }

    UInt64 get_uint(size_t n) const override { return UInt64(data[n]); }

    bool get_bool(size_t n) const override { return bool(data[n]); }

    Int64 get_int(size_t n) const override { return Int64(data[n]); }

    void insert(const Field& x) override {
        data.push_back(doris::vectorized::get<NearestFieldType<T>>(x));
    }

    void insert_range_from(const IColumn& src, size_t start, size_t length) override;

    void insert_indices_from(const IColumn& src, const int* indices_begin,
                             const int* indices_end) override;

    void fill(const value_type& element, size_t num) {
        auto old_size = data.size();
        auto new_size = old_size + num;
        data.resize(new_size);
        if constexpr (sizeof(value_type) == 1) {
            memset(&data[old_size], element, sizeof(value_type) * num);
        } else {
            for (size_t i = 0; i < num; ++i) {
                data[old_size + i] = element;
            }
        }
    }

    void insert_zeroed_elements(size_t num) {
        auto old_size = data.size();
        auto new_size = old_size + num;
        data.resize(new_size);
        memset(&data[old_size], 0, sizeof(value_type) * num);
    }

    ColumnPtr filter(const IColumn::Filter& filt, ssize_t result_size_hint) const override;

    // note(wb) this method is only used in storage layer now
    Status filter_by_selector(const uint16_t* sel, size_t sel_size, IColumn* col_ptr) override {
        insert_res_column(sel, sel_size, reinterpret_cast<vectorized::ColumnVector<T>*>(col_ptr));
        return Status::OK();
    }

    ColumnPtr permute(const IColumn::Permutation& perm, size_t limit) const override;

    //    ColumnPtr index(const IColumn & indexes, size_t limit) const override;

    template <typename Type>
    ColumnPtr index_impl(const PaddedPODArray<Type>& indexes, size_t limit) const;

    ColumnPtr replicate(const IColumn::Offsets& offsets) const override;

    void replicate(const uint32_t* counts, size_t target_size, IColumn& column) const override;

    void get_extremes(Field& min, Field& max) const override;

    MutableColumns scatter(IColumn::ColumnIndex num_columns,
                           const IColumn::Selector& selector) const override {
        return this->template scatter_impl<Self>(num_columns, selector);
    }

    //    void gather(ColumnGathererStream & gatherer_stream) override;

    bool can_be_inside_nullable() const override { return true; }

    bool is_fixed_and_contiguous() const override { return true; }
    size_t size_of_value_if_fixed() const override { return sizeof(T); }
    StringRef get_raw_data() const override {
        return StringRef(reinterpret_cast<const char*>(data.data()), data.size());
    }

    bool structure_equals(const IColumn& rhs) const override {
        return typeid(rhs) == typeid(ColumnVector<T>);
    }

    /** More efficient methods of manipulation - to manipulate with data directly. */
    Container& get_data() { return data; }

    const Container& get_data() const { return data; }

    const T& get_element(size_t n) const { return data[n]; }

    T& get_element(size_t n) { return data[n]; }

    void replace_column_data(const IColumn& rhs, size_t row, size_t self_row = 0) override {
        DCHECK(size() > self_row);
        data[self_row] = static_cast<const Self&>(rhs).data[row];
    }

    void replace_column_data_default(size_t self_row = 0) override {
        DCHECK(size() > self_row);
        data[self_row] = T();
    }

protected:
    Container data;
};

template <typename T>
template <typename Type>
ColumnPtr ColumnVector<T>::index_impl(const PaddedPODArray<Type>& indexes, size_t limit) const {
    size_t size = indexes.size();

    if (limit == 0)
        limit = size;
    else
        limit = std::min(size, limit);

    auto res = this->create(limit);
    typename Self::Container& res_data = res->get_data();
    for (size_t i = 0; i < limit; ++i) res_data[i] = data[indexes[i]];

    return res;
}

} // namespace doris::vectorized
