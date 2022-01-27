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
#include "vec/columns/column_impl.h"

#include "runtime/string_value.h"
#include "olap/decimal12.h"
#include "olap/uint24.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_vector.h"
#include "vec/core/types.h"

namespace doris::vectorized {

/**
 * used to keep predicate column in storage layer
 * 
 *  T = predicate column type
 */
template <typename T>
class PredicateColumnType final : public COWHelper<IColumn, PredicateColumnType<T>> {
private:
    PredicateColumnType() {}
    PredicateColumnType(const size_t n) : data(n) {}
    friend class COWHelper<IColumn, PredicateColumnType<T>>;

    PredicateColumnType(const PredicateColumnType& src) : data(src.data.begin(), src.data.end()) {}

    uint64_t get_date_at(uint16_t idx) {
        const T val = data[idx];
        const char* val_ptr = reinterpret_cast<const char*>(&val);
        uint64_t value = 0;
        value = *(unsigned char*)(val_ptr + 2);
        value <<= 8;
        value |= *(unsigned char*)(val_ptr + 1);
        value <<= 8;
        value |= *(unsigned char*)(val_ptr);
        return value;
    }

    void insert_date_to_res_column(const uint16_t* sel, size_t sel_size, vectorized::ColumnVector<Int64>* res_ptr) {
        for (size_t i = 0; i < sel_size; i++) {
            VecDateTimeValue date;
            date.from_olap_date(get_date_at(sel[i]));
            res_ptr->insert_data(reinterpret_cast<char*>(&date), 0);
        }
    }

    void insert_datetime_to_res_column(const uint16_t* sel, size_t sel_size, vectorized::ColumnVector<Int64>* res_ptr) {
        for (size_t i = 0; i < sel_size; i++) {
            uint64_t value = data[sel[i]];
            vectorized::VecDateTimeValue date(value);
            res_ptr->insert_data(reinterpret_cast<char*>(&date), 0);
        }
    }

    void insert_string_to_res_column(const uint16_t* sel, size_t sel_size, vectorized::ColumnString* res_ptr) {
        for (size_t i = 0; i < sel_size; i++) {
            uint16_t n = sel[i];
            auto& sv = reinterpret_cast<StringValue&>(data[n]);
            res_ptr->insert_data(sv.ptr, sv.len);
        }
    }

    void insert_decimal_to_res_column(const uint16_t* sel, size_t sel_size, vectorized::ColumnDecimal<Decimal128>* res_ptr) {
        for (size_t i = 0; i < sel_size; i++) {
            uint16_t n = sel[i];
            auto& dv = reinterpret_cast<const decimal12_t&>(data[n]);
            DecimalV2Value dv_data(dv.integer, dv.fraction);
            res_ptr->insert_data(reinterpret_cast<char*>(&dv_data), 0);
        }
    }

    template <typename Y>
    void insert_default_value_res_column(const uint16_t* sel, size_t sel_size, vectorized::ColumnVector<Y>* res_ptr) {
        auto& res_data = res_ptr->get_data();
        DCHECK(res_data.empty());
        res_data.reserve(sel_size);
        Y* y = (Y*)res_data.get_end_ptr();
        for (size_t i = 0; i < sel_size; i++) {
            y[i] = T(data[sel[i]]);
        }
        res_data.set_end_ptr(y + sel_size);
    }

    void insert_byte_to_res_column(const uint16_t* sel, size_t sel_size, vectorized::IColumn* res_ptr) {
        for (size_t i = 0; i < sel_size; i++) {
            uint16_t n = sel[i];
            char* ch_val = reinterpret_cast<char*>(&data[n]);
            res_ptr->insert_data(ch_val, 0);
        }
    }

    template <typename Y>
    ColumnPtr filter_default_type_by_selector(const uint16_t* sel, size_t sel_size, ColumnPtr* ptr = nullptr) {
        static_assert(std::is_same_v<T, Y>);
        // todo(wb) the operation which create a new column maybe should move to other place
        if (ptr == nullptr) {
            auto res = vectorized::ColumnVector<Y>::create();
            if (sel_size == 0) {
                return res;
            }
            insert_default_value_res_column(sel, sel_size, res.get());
            return res;
        } else {
            if (sel_size != 0) {
                MutableColumnPtr ptr_res = (*std::move(*ptr)).assume_mutable();
                insert_default_value_res_column(sel, sel_size, reinterpret_cast<vectorized::ColumnVector<Y>*>(ptr_res.get()));
            }
            return *ptr;
        }
    }

public:
    using Self = PredicateColumnType;
    using value_type = T;
    using Container = PaddedPODArray<value_type>;

    bool is_numeric() const override { return false; }

    bool is_predicate_column() const override { return true; }

    size_t size() const override { return data.size(); }

   [[noreturn]]  StringRef get_data_at(size_t n) const override {
         LOG(FATAL) << "get_data_at not supported in PredicateColumnType";
    }

    void insert_from(const IColumn& src, size_t n) override {
         LOG(FATAL) << "insert_from not supported in PredicateColumnType";
    }

    void insert_range_from(const IColumn& src, size_t start, size_t length) override {
         LOG(FATAL) << "insert_range_from not supported in PredicateColumnType";
    }

    void insert_indices_from(const IColumn& src, const int* indices_begin, const int* indices_end) override {
         LOG(FATAL) << "insert_indices_from not supported in PredicateColumnType";
    }

    void pop_back(size_t n) override {
        LOG(FATAL) << "pop_back not supported in PredicateColumnType";
    }

    void update_hash_with_value(size_t n, SipHash& hash) const override {
         LOG(FATAL) << "update_hash_with_value not supported in PredicateColumnType";
    }

    void insert_string_value(char* data_ptr, size_t length) {
        StringValue sv(data_ptr, length);
        data.push_back_without_reserve(sv);
    }

    void insert_decimal_value(char* data_ptr, size_t length) {
        decimal12_t dc12_value;
        dc12_value.integer = *(int64_t*)(data_ptr);
        dc12_value.fraction = *(int32_t*)(data_ptr + sizeof(int64_t));
        data.push_back_without_reserve(dc12_value);
    }

    // used for int128
    void insert_in_copy_way(char* data_ptr, size_t length) {
        T val {};
        memcpy(&val, data_ptr, sizeof(val));
        data.push_back_without_reserve(val);
    }
    
    void insert_default_type(char* data_ptr, size_t length) {
        T* val = (T*)data_ptr;
        data.push_back_without_reserve(*val);
    }

    void insert_data(const char* data_ptr, size_t length) override {
        char* ch = const_cast<char*>(data_ptr);
        if constexpr (std::is_same_v<T, StringValue>) {
            insert_string_value(ch, length);
         } else if constexpr (std::is_same_v<T, decimal12_t>) {
            insert_decimal_value(ch, length);
         } else if constexpr (std::is_same_v<T, doris::vectorized::Int128>) {
            insert_in_copy_way(ch, length);
         } else {
            insert_default_type(ch, length);
         }
    }

    void insert_default() override { 
        data.push_back(T()); 
    }

    void clear() override { data.clear(); }

    size_t byte_size() const override { 
         return data.size() * sizeof(T);
    }

    size_t allocated_bytes() const override { return byte_size(); }

    void protect() override {}

    void get_permutation(bool reverse, size_t limit, int nan_direction_hint,
                                      IColumn::Permutation& res) const override {
        LOG(FATAL) << "get_permutation not supported in PredicateColumnType";
    }

    void reserve(size_t n) override { 
        data.reserve(n); 
    }

    [[noreturn]] const char* get_family_name() const override { 
        LOG(FATAL) << "get_family_name not supported in PredicateColumnType";
    }

   [[noreturn]] MutableColumnPtr clone_resized(size_t size) const override {
        LOG(FATAL) << "clone_resized not supported in PredicateColumnType";
    }

    void insert(const Field& x) override {
        LOG(FATAL) << "insert not supported in PredicateColumnType";
    }

    [[noreturn]] Field operator[](size_t n) const override {
        LOG(FATAL) << "operator[] not supported in PredicateColumnType";
    }

    void get(size_t n, Field& res) const override {
        LOG(FATAL) << "get field not supported in PredicateColumnType";
    }

    [[noreturn]] UInt64 get64(size_t n) const override {
        LOG(FATAL) << "get field not supported in PredicateColumnTyped";
    }

    [[noreturn]] Float64 get_float64(size_t n) const override {
        LOG(FATAL) << "get field not supported in PredicateColumnType";
    }

    [[noreturn]] UInt64 get_uint(size_t n) const override {
        LOG(FATAL) << "get field not supported in PredicateColumnType";
    }

    [[noreturn]] bool get_bool(size_t n) const override {
        LOG(FATAL) << "get field not supported in PredicateColumnType";
    }

    [[noreturn]] Int64 get_int(size_t n) const override {
        LOG(FATAL) << "get field not supported in PredicateColumnType";
    }

    // it's impossable to use ComplexType as key , so we don't have to implemnt them
    [[noreturn]] StringRef serialize_value_into_arena(size_t n, Arena& arena,
                                                      char const*& begin) const {
        LOG(FATAL) << "serialize_value_into_arena not supported in PredicateColumnType";
    }

    [[noreturn]] const char* deserialize_and_insert_from_arena(const char* pos) {
        LOG(FATAL) << "deserialize_and_insert_from_arena not supported in PredicateColumnType";
    }

    [[noreturn]] int compare_at(size_t n, size_t m, const IColumn& rhs,
                                int nan_direction_hint) const {
        LOG(FATAL) << "compare_at not supported in PredicateColumnType";
    }

    void get_extremes(Field& min, Field& max) const {
        LOG(FATAL) << "get_extremes not supported in PredicateColumnType";
    }

    bool can_be_inside_nullable() const override { return true; }

    bool is_fixed_and_contiguous() const override { return true; }
    size_t size_of_value_if_fixed() const override { return sizeof(T); }

    [[noreturn]] StringRef get_raw_data() const override {
        LOG(FATAL) << "get_raw_data not supported in PredicateColumnType";
    }

    [[noreturn]] bool structure_equals(const IColumn& rhs) const override {
         LOG(FATAL) << "structure_equals not supported in PredicateColumnType";
    }

    [[noreturn]] ColumnPtr filter(const IColumn::Filter& filt, ssize_t result_size_hint) const override {
         LOG(FATAL) << "filter not supported in PredicateColumnType";
    };

    [[noreturn]] ColumnPtr permute(const IColumn::Permutation& perm, size_t limit) const override { 
         LOG(FATAL) << "permute not supported in PredicateColumnType";
    };

    Container& get_data() { return data; }

    const Container& get_data() const { return data; }

    [[noreturn]] ColumnPtr replicate(const IColumn::Offsets& replicate_offsets) const override {
        LOG(FATAL) << "replicate not supported in PredicateColumnType";
    };

    [[noreturn]] MutableColumns scatter(IColumn::ColumnIndex num_columns,
                                        const IColumn::Selector& selector) const override {
        LOG(FATAL) << "scatter not supported in PredicateColumnType";
    }

    ColumnPtr filter_decimal_by_selector(const uint16_t* sel, size_t sel_size, ColumnPtr* ptr = nullptr) {
        if (ptr == nullptr) {
            auto res = vectorized::ColumnDecimal<Decimal128>::create(0, 9); // todo(wb) need a global variable to stand for scale
            if (sel_size == 0) {
                return res;
            }
            insert_decimal_to_res_column(sel, sel_size, res.get());
            return res;
        } else {
            if (sel_size != 0) {
                MutableColumnPtr res_ptr = (*std::move(*ptr)).assume_mutable();
                insert_decimal_to_res_column(sel, sel_size, reinterpret_cast<vectorized::ColumnDecimal<Decimal128>*>(res_ptr.get()));
            }
            return *ptr;
        }
    }
    
    ColumnPtr filter_date_by_selector(const uint16_t* sel, size_t sel_size, ColumnPtr* ptr = nullptr) {
        if (ptr == nullptr) {
            auto res = vectorized::ColumnVector<Int64>::create();
            if (sel_size == 0) {
                return res;
            }
            insert_date_to_res_column(sel, sel_size, res.get());
            return res;
        } else {
            if (sel_size != 0) {
                MutableColumnPtr res_ptr = (*std::move(*ptr)).assume_mutable();
                insert_date_to_res_column(sel, sel_size, reinterpret_cast<vectorized::ColumnVector<Int64>*>(res_ptr.get()));
            }
            return *ptr;
        }
    }

    ColumnPtr filter_date_time_by_selector(const uint16_t* sel, size_t sel_size, ColumnPtr* ptr = nullptr) {
        if (ptr == nullptr) {
            auto res = vectorized::ColumnVector<Int64>::create();
            if (sel_size == 0) {
                return res;
            }

            insert_datetime_to_res_column(sel, sel_size, res.get());
            return res;
        } else {
            if (sel_size != 0) {
                MutableColumnPtr res_ptr = (*std::move(*ptr)).assume_mutable();
                insert_datetime_to_res_column(sel, sel_size, reinterpret_cast<vectorized::ColumnVector<Int64>*>(res_ptr.get()));
            }
        }
        return *ptr;
    }

    ColumnPtr filter_string_value_by_selector(const uint16_t* sel, size_t sel_size, ColumnPtr* ptr = nullptr) {
        if (ptr == nullptr) {
            auto res = vectorized::ColumnString::create();
            if (sel_size == 0) {
                return res;
            }
            res->reserve(sel_size);
            insert_string_to_res_column(sel, sel_size, res.get());
            return res;
        } else {
            if (sel_size != 0) {
                MutableColumnPtr ptr_res = (*std::move(*ptr)).assume_mutable();
                insert_string_to_res_column(sel, sel_size, reinterpret_cast<vectorized::ColumnString*>(ptr_res.get()));
            }
        }
        return *ptr;
    }

    ColumnPtr filter_bool_by_selector(const uint16_t* sel, size_t sel_size, ColumnPtr* ptr = nullptr) {
        if (ptr == nullptr) {
            auto res = vectorized::ColumnVector<vectorized::UInt8>::create();
            if (sel_size == 0) {
                return res;
            }
            res->reserve(sel_size);
            insert_byte_to_res_column(sel, sel_size, res.get());
        } else {
            if (sel_size != 0) {
                MutableColumnPtr ptr_res = (*std::move(*ptr)).assume_mutable();
                insert_byte_to_res_column(sel, sel_size, ptr_res.get());
            }
        }
        return *ptr;
    }

    //todo(wb) need refactor this method, using return status to check unexpect args instead of LOG(FATAL)
    ColumnPtr filter_by_selector(const uint16_t* sel, size_t sel_size, ColumnPtr* ptr = nullptr) override {
        if constexpr (std::is_same_v<T, StringValue>) {
            return filter_string_value_by_selector(sel, sel_size, ptr);
        } else if constexpr (std::is_same_v<T, decimal12_t>) {
            return filter_decimal_by_selector(sel, sel_size, ptr);
        } else if constexpr (std::is_same_v<T, doris::vectorized::Int8>) {
            return filter_default_type_by_selector<doris::vectorized::Int8>(sel, sel_size, ptr);
        } else if constexpr (std::is_same_v<T, doris::vectorized::Int16>) {
            return filter_default_type_by_selector<doris::vectorized::Int16>(sel, sel_size, ptr);
        } else if constexpr (std::is_same_v<T, doris::vectorized::Int32>) {
            return filter_default_type_by_selector<doris::vectorized::Int32>(sel, sel_size, ptr);
        } else if constexpr (std::is_same_v<T, doris::vectorized::Int64>) {
            return filter_default_type_by_selector<doris::vectorized::Int64>(sel, sel_size, ptr);
        } else if constexpr (std::is_same_v<T, doris::vectorized::Float32>) {
            return filter_default_type_by_selector<doris::vectorized::Float32>(sel, sel_size, ptr);
        } else if constexpr (std::is_same_v<T, doris::vectorized::Float64>) {
            return filter_default_type_by_selector<doris::vectorized::Float64>(sel, sel_size, ptr);
        } else if constexpr (std::is_same_v<T, uint64_t>) {
            return filter_date_time_by_selector(sel, sel_size, ptr);
        } else if constexpr (std::is_same_v<T, uint24_t>) {
            return filter_date_by_selector(sel, sel_size, ptr);
        } else if constexpr (std::is_same_v<T, doris::vectorized::Int128>) {
            return filter_default_type_by_selector<doris::vectorized::Int128>(sel, sel_size, ptr);
        } else if (std::is_same_v<T, bool>) {
            return filter_bool_by_selector(sel, sel_size, ptr);
        } else {
            LOG(FATAL) << "unexpected type in predicate column";
        }
    }

    void replace_column_data(const IColumn&, size_t row, size_t self_row = 0) override {
        LOG(FATAL) << "should not call replace_column_data in predicate column";
    }

    void replace_column_data_default(size_t self_row = 0) override {
        LOG(FATAL) << "should not call replace_column_data_default in predicate column";
    }

private:
    Container data;
};
using ColumnStringValue = PredicateColumnType<StringValue>;

} // namespace
