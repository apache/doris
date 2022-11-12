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

#include "olap/decimal12.h"
#include "olap/uint24.h"
#include "runtime/mem_pool.h"
#include "runtime/primitive_type.h"
#include "runtime/string_value.h"
#include "vec/columns/column.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/core/types.h"

namespace doris::vectorized {

/**
 * used to keep predicate column in storage layer
 *
 *  T = predicate column type
 */
template <PrimitiveType Type>
class PredicateColumnType final : public COWHelper<IColumn, PredicateColumnType<Type>> {
private:
    PredicateColumnType() {}
    PredicateColumnType(const size_t n) : data(n) {}
    friend class COWHelper<IColumn, PredicateColumnType<Type>>;
    using T = typename PredicatePrimitiveTypeTraits<Type>::PredicateFieldType;

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

    void insert_date_to_res_column(const uint16_t* sel, size_t sel_size,
                                   vectorized::ColumnVector<Int64>* res_ptr) {
        for (size_t i = 0; i < sel_size; i++) {
            VecDateTimeValue date = VecDateTimeValue::create_from_olap_date(get_date_at(sel[i]));
            res_ptr->insert_data(reinterpret_cast<char*>(&date), 0);
        }
    }

    void insert_date32_to_res_column(const uint16_t* sel, size_t sel_size,
                                     vectorized::ColumnVector<Int64>* res_ptr) {
        res_ptr->reserve(sel_size);
        auto& res_data = res_ptr->get_data();

        for (size_t i = 0; i < sel_size; i++) {
            uint64_t val = data[sel[i]];
            VecDateTimeValue date;
            date.set_olap_date(val);
            res_data.push_back_without_reserve(
                    unaligned_load<Int64>(reinterpret_cast<char*>(&date)));
        }
    }

    void insert_datetime_to_res_column(const uint16_t* sel, size_t sel_size,
                                       vectorized::ColumnVector<Int64>* res_ptr) {
        for (size_t i = 0; i < sel_size; i++) {
            uint64_t value = data[sel[i]];
            vectorized::VecDateTimeValue datetime =
                    VecDateTimeValue::create_from_olap_datetime(value);
            res_ptr->insert_data(reinterpret_cast<char*>(&datetime), 0);
        }
    }

    void insert_string_to_res_column(const uint16_t* sel, size_t sel_size,
                                     vectorized::ColumnString* res_ptr) {
        StringRef refs[sel_size];
        size_t length = 0;
        for (size_t i = 0; i < sel_size; i++) {
            uint16_t n = sel[i];
            auto& sv = reinterpret_cast<StringValue&>(data[n]);
            refs[i].data = sv.ptr;
            refs[i].size = sv.len;
            length += sv.len;
        }
        res_ptr->get_offsets().reserve(sel_size + res_ptr->get_offsets().size());
        res_ptr->get_chars().reserve(length + res_ptr->get_chars().size());
        res_ptr->insert_many_strings_without_reserve(refs, sel_size);
    }

    void insert_decimal_to_res_column(const uint16_t* sel, size_t sel_size,
                                      vectorized::ColumnDecimal<Decimal128>* res_ptr) {
        for (size_t i = 0; i < sel_size; i++) {
            uint16_t n = sel[i];
            auto& dv = reinterpret_cast<const decimal12_t&>(data[n]);
            DecimalV2Value dv_data(dv.integer, dv.fraction);
            res_ptr->insert_data(reinterpret_cast<char*>(&dv_data), 0);
        }
    }

    template <typename Y>
    void insert_default_value_res_column(const uint16_t* sel, size_t sel_size,
                                         vectorized::ColumnVector<Y>* res_ptr) {
        static_assert(std::is_same_v<T, Y>);
        auto& res_data = res_ptr->get_data();
        DCHECK(res_data.empty());
        res_data.reserve(sel_size);
        Y* y = (Y*)res_data.get_end_ptr();
        for (size_t i = 0; i < sel_size; i++) {
            y[i] = T(data[sel[i]]);
        }
        res_data.set_end_ptr(y + sel_size);
    }

    void insert_byte_to_res_column(const uint16_t* sel, size_t sel_size,
                                   vectorized::IColumn* res_ptr) {
        for (size_t i = 0; i < sel_size; i++) {
            uint16_t n = sel[i];
            char* ch_val = reinterpret_cast<char*>(&data[n]);
            res_ptr->insert_data(ch_val, 0);
        }
    }

    void insert_many_default_type(const char* data_ptr, size_t num) {
        auto old_size = data.size();
        data.resize(old_size + num);
        memcpy(data.data() + old_size, data_ptr, num * sizeof(T));
    }

    void insert_many_in_copy_way(const char* data_ptr, size_t num) {
        auto old_size = data.size();
        data.resize(old_size + num);
        memcpy(data.data() + old_size, data_ptr, num * sizeof(T));
    }

public:
    using Self = PredicateColumnType;
    using value_type = T;
    using Container = PaddedPODArray<value_type>;

    bool is_numeric() const override { return false; }

    bool is_predicate_column() const override { return true; }

    size_t size() const override { return data.size(); }

    [[noreturn]] StringRef get_data_at(size_t n) const override {
        LOG(FATAL) << "get_data_at not supported in PredicateColumnType";
    }

    void insert_from(const IColumn& src, size_t n) override {
        LOG(FATAL) << "insert_from not supported in PredicateColumnType";
    }

    void insert_range_from(const IColumn& src, size_t start, size_t length) override {
        LOG(FATAL) << "insert_range_from not supported in PredicateColumnType";
    }

    void insert_indices_from(const IColumn& src, const int* indices_begin,
                             const int* indices_end) override {
        LOG(FATAL) << "insert_indices_from not supported in PredicateColumnType";
    }

    void pop_back(size_t n) override {
        LOG(FATAL) << "pop_back not supported in PredicateColumnType";
    }

    void update_hash_with_value(size_t n, SipHash& hash) const override {
        LOG(FATAL) << "update_hash_with_value not supported in PredicateColumnType";
    }

    void insert_string_value(const char* data_ptr, size_t length) {
        StringValue sv((char*)data_ptr, length);
        data.push_back_without_reserve(sv);
    }

    void insert_decimal_value(const char* data_ptr, size_t length) {
        decimal12_t dc12_value;
        dc12_value.integer = *(int64_t*)(data_ptr);
        dc12_value.fraction = *(int32_t*)(data_ptr + sizeof(int64_t));
        data.push_back_without_reserve(dc12_value);
    }

    // used for int128
    void insert_in_copy_way(const char* data_ptr, size_t length) {
        T val {};
        memcpy(&val, data_ptr, sizeof(val));
        data.push_back_without_reserve(val);
    }

    void insert_default_type(const char* data_ptr, size_t length) {
        T* val = (T*)data_ptr;
        data.push_back_without_reserve(*val);
    }

    void insert_data(const char* data_ptr, size_t length) override {
        if constexpr (std::is_same_v<T, StringValue>) {
            insert_string_value(data_ptr, length);
        } else if constexpr (std::is_same_v<T, decimal12_t>) {
            insert_decimal_value(data_ptr, length);
        } else if constexpr (std::is_same_v<T, doris::vectorized::Int128>) {
            insert_in_copy_way(data_ptr, length);
        } else {
            insert_default_type(data_ptr, length);
        }
    }

    void insert_many_date(const char* data_ptr, size_t num) {
        size_t intput_type_size = sizeof(uint24_t);
        size_t res_type_size = sizeof(uint32_t);
        char* input_data_ptr = const_cast<char*>(data_ptr);

        char* res_ptr = (char*)data.get_end_ptr();
        memset(res_ptr, 0, res_type_size * num);
        for (int i = 0; i < num; i++) {
            memcpy(res_ptr, input_data_ptr, intput_type_size);
            res_ptr += res_type_size;
            input_data_ptr += intput_type_size;
        }
        data.set_end_ptr(res_ptr);
    }

    void insert_many_fix_len_data(const char* data_ptr, size_t num) override {
        if constexpr (std::is_same_v<T, decimal12_t>) {
            insert_many_in_copy_way(data_ptr, num);
        } else if constexpr (std::is_same_v<T, doris::vectorized::Int128>) {
            insert_many_in_copy_way(data_ptr, num);
        } else if constexpr (std::is_same_v<T, StringValue>) {
            // here is unreachable, just for compilation to be able to pass
        } else if constexpr (Type == TYPE_DATE) {
            insert_many_date(data_ptr, num);
        } else {
            insert_many_default_type(data_ptr, num);
        }
    }

    void insert_many_dict_data(const int32_t* data_array, size_t start_index, const StringRef* dict,
                               size_t num, uint32_t /*dict_num*/) override {
        if constexpr (std::is_same_v<T, StringValue>) {
            for (size_t end_index = start_index + num; start_index < end_index; ++start_index) {
                int32_t codeword = data_array[start_index];
                insert_string_value(dict[codeword].data, dict[codeword].size);
            }
        }
    }

    void insert_many_continuous_binary_data(const char* data_, const uint32_t* offsets,
                                            const size_t num) override {
        if (UNLIKELY(num == 0)) {
            return;
        }
        if constexpr (std::is_same_v<T, StringValue>) {
            if (_pool == nullptr) {
                _pool.reset(new MemPool());
            }
            const auto total_mem_size = offsets[num] - offsets[0];
            char* destination = (char*)_pool->allocate(total_mem_size);
            memcpy(destination, data_ + offsets[0], total_mem_size);
            size_t org_elem_num = data.size();
            data.resize(org_elem_num + num);

            auto* data_ptr = &data[org_elem_num];
            for (size_t i = 0; i != num; ++i) {
                data_ptr[i].ptr = destination + offsets[i] - offsets[0];
                data_ptr[i].len = offsets[i + 1] - offsets[i];
            }
            DCHECK(data_ptr[num - 1].ptr + data_ptr[num - 1].len == destination + total_mem_size);
        }
    }

    void insert_many_binary_data(char* data_array, uint32_t* len_array,
                                 uint32_t* start_offset_array, size_t num) override {
        if (num == 0) {
            return;
        }
        if constexpr (std::is_same_v<T, StringValue>) {
            if (_pool == nullptr) {
                _pool.reset(new MemPool());
            }

            size_t total_mem_size = 0;
            for (size_t i = 0; i < num; i++) {
                total_mem_size += len_array[i];
            }

            char* destination = (char*)_pool->allocate(total_mem_size);
            char* org_dst = destination;
            size_t org_elem_num = data.size();
            data.resize(org_elem_num + num);
            uint32_t fragment_start_offset = start_offset_array[0];
            size_t fragment_len = 0;
            for (size_t i = 0; i < num; i++) {
                data[org_elem_num + i].ptr = destination + fragment_len;
                data[org_elem_num + i].len = len_array[i];
                fragment_len += len_array[i];
                // Compute the largest continuous memcpy block and copy them.
                // If this is the last element in data array, then should copy the current memory block.
                if (i == num - 1 ||
                    start_offset_array[i + 1] != start_offset_array[i] + len_array[i]) {
                    memcpy(destination, data_array + fragment_start_offset, fragment_len);
                    destination += fragment_len;
                    fragment_start_offset = (i == num - 1 ? 0 : start_offset_array[i + 1]);
                    fragment_len = 0;
                }
            }
            CHECK(destination - org_dst == total_mem_size)
                    << "Copied size not equal to expected size";
        } else {
            LOG(FATAL) << "Method insert_many_binary_data is not supported";
        }
    }

    void insert_default() override { data.push_back(T()); }

    void clear() override {
        data.clear();
        if (_pool != nullptr) {
            _pool->clear();
        }
    }

    size_t byte_size() const override { return data.size() * sizeof(T); }

    size_t allocated_bytes() const override { return byte_size(); }

    void protect() override {}

    void get_permutation(bool reverse, size_t limit, int nan_direction_hint,
                         IColumn::Permutation& res) const override {
        LOG(FATAL) << "get_permutation not supported in PredicateColumnType";
    }

    void reserve(size_t n) override { data.reserve(n); }

    const char* get_family_name() const override { return TypeName<T>::get(); }

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

    // it's impossible to use ComplexType as key , so we don't have to implement them
    [[noreturn]] StringRef serialize_value_into_arena(size_t n, Arena& arena,
                                                      char const*& begin) const override {
        LOG(FATAL) << "serialize_value_into_arena not supported in PredicateColumnType";
    }

    [[noreturn]] const char* deserialize_and_insert_from_arena(const char* pos) override {
        LOG(FATAL) << "deserialize_and_insert_from_arena not supported in PredicateColumnType";
    }

    [[noreturn]] int compare_at(size_t n, size_t m, const IColumn& rhs,
                                int nan_direction_hint) const override {
        LOG(FATAL) << "compare_at not supported in PredicateColumnType";
    }

    void get_extremes(Field& min, Field& max) const override {
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

    [[noreturn]] ColumnPtr filter(const IColumn::Filter& filt,
                                  ssize_t result_size_hint) const override {
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

    void append_data_by_selector(MutableColumnPtr& res,
                                 const IColumn::Selector& selector) const override {
        LOG(FATAL) << "append_data_by_selector is not supported in PredicateColumnType!";
    }

    Status filter_by_selector(const uint16_t* sel, size_t sel_size, IColumn* col_ptr) override {
        if constexpr (std::is_same_v<T, StringValue>) {
            insert_string_to_res_column(sel, sel_size,
                                        reinterpret_cast<vectorized::ColumnString*>(col_ptr));
        } else if constexpr (std::is_same_v<T, decimal12_t>) {
            insert_decimal_to_res_column(
                    sel, sel_size,
                    reinterpret_cast<vectorized::ColumnDecimal<Decimal128>*>(col_ptr));
        } else if constexpr (std::is_same_v<T, doris::vectorized::Int8>) {
            insert_default_value_res_column(
                    sel, sel_size,
                    reinterpret_cast<vectorized::ColumnVector<doris::vectorized::Int8>*>(col_ptr));
        } else if constexpr (std::is_same_v<T, doris::vectorized::Int16>) {
            insert_default_value_res_column(
                    sel, sel_size,
                    reinterpret_cast<vectorized::ColumnVector<doris::vectorized::Int16>*>(col_ptr));
        } else if constexpr (std::is_same_v<T, doris::vectorized::Int32>) {
            insert_default_value_res_column(
                    sel, sel_size,
                    reinterpret_cast<vectorized::ColumnVector<doris::vectorized::Int32>*>(col_ptr));
        } else if constexpr (std::is_same_v<T, doris::vectorized::Int64>) {
            insert_default_value_res_column(
                    sel, sel_size,
                    reinterpret_cast<vectorized::ColumnVector<doris::vectorized::Int64>*>(col_ptr));
        } else if constexpr (std::is_same_v<T, doris::vectorized::Float32>) {
            insert_default_value_res_column(
                    sel, sel_size,
                    reinterpret_cast<vectorized::ColumnVector<doris::vectorized::Float32>*>(
                            col_ptr));
        } else if constexpr (std::is_same_v<T, doris::vectorized::Float64>) {
            insert_default_value_res_column(
                    sel, sel_size,
                    reinterpret_cast<vectorized::ColumnVector<doris::vectorized::Float64>*>(
                            col_ptr));
        } else if constexpr (std::is_same_v<T, uint64_t>) {
            if (const vectorized::ColumnVector<UInt64>* date_col =
                        check_and_get_column<vectorized::ColumnVector<UInt64>>(
                                const_cast<const IColumn*>(col_ptr))) {
                insert_default_value_res_column(
                        sel, sel_size, const_cast<vectorized::ColumnVector<UInt64>*>(date_col));
            } else {
                insert_datetime_to_res_column(
                        sel, sel_size, reinterpret_cast<vectorized::ColumnVector<Int64>*>(col_ptr));
            }
        } else if constexpr (std::is_same_v<T, uint24_t>) {
            insert_date_to_res_column(sel, sel_size,
                                      reinterpret_cast<vectorized::ColumnVector<Int64>*>(col_ptr));
        } else if constexpr (std::is_same_v<T, uint32_t>) {
            if (const vectorized::ColumnVector<Int64>* date_col =
                        check_and_get_column<vectorized::ColumnVector<Int64>>(
                                const_cast<const IColumn*>(col_ptr))) {
                // a trick type judge, need refactor it.
                insert_date32_to_res_column(sel, sel_size,
                                            const_cast<vectorized::ColumnVector<Int64>*>(date_col));
            } else {
                insert_default_value_res_column(
                        sel, sel_size,
                        reinterpret_cast<vectorized::ColumnVector<doris::vectorized::UInt32>*>(
                                col_ptr));
            }
        } else if constexpr (std::is_same_v<T, doris::vectorized::Int128>) {
            insert_default_value_res_column(
                    sel, sel_size,
                    reinterpret_cast<vectorized::ColumnVector<doris::vectorized::Int128>*>(
                            col_ptr));
        } else if (std::is_same_v<T, bool>) {
            insert_byte_to_res_column(sel, sel_size, col_ptr);
        } else {
            return Status::NotSupported("not supported output type in predicate_column");
        }
        return Status::OK();
    }

    void replace_column_data(const IColumn&, size_t row, size_t self_row = 0) override {
        LOG(FATAL) << "should not call replace_column_data in predicate column";
    }

    void replace_column_data_default(size_t self_row = 0) override {
        LOG(FATAL) << "should not call replace_column_data_default in predicate column";
    }

private:
    Container data;
    // manages the memory for slice's data(For string type)
    std::unique_ptr<MemPool> _pool;
};

} // namespace doris::vectorized
