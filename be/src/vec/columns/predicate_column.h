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

#include <optional>

#include "olap/decimal12.h"
#include "olap/uint24.h"
#include "runtime/define_primitive_type.h"
#include "runtime/primitive_type.h"
#include "vec/columns/column.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/common/arena.h"
#include "vec/common/string_ref.h"
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
    PredicateColumnType() = default;
    PredicateColumnType(const size_t n) : data(n) {}
    PredicateColumnType(const PredicateColumnType& src) : data(src.data.begin(), src.data.end()) {}
    friend class COWHelper<IColumn, PredicateColumnType<Type>>;
    using T = typename PrimitiveTypeTraits<Type>::CppType;
    using ColumnType = typename PrimitiveTypeTraits<Type>::ColumnType;

    void insert_string_to_res_column(const uint16_t* sel, size_t sel_size, ColumnString* res_ptr) {
        _refs.resize(sel_size);
        size_t length = 0;
        for (size_t i = 0; i < sel_size; i++) {
            uint16_t n = sel[i];
            auto& sv = reinterpret_cast<StringRef&>(data[n]);
            _refs[i].data = sv.data;
            _refs[i].size = sv.size;
            length += sv.size;
        }
        res_ptr->get_offsets().reserve(sel_size + res_ptr->get_offsets().size());
        res_ptr->get_chars().reserve(length + res_ptr->get_chars().size());
        res_ptr->insert_many_strings_without_reserve(_refs.data(), sel_size);
    }

    template <typename Y, template <typename> typename ColumnContainer>
    void insert_default_value_res_column(const uint16_t* sel, size_t sel_size,
                                         ColumnContainer<Y>* res_ptr) {
        static_assert(std::is_same_v<ColumnContainer<Y>, ColumnType>);
        auto& res_data = res_ptr->get_data();
        DCHECK(res_data.empty());
        res_data.reserve(sel_size);
        Y* y = (Y*)res_data.get_end_ptr();
        for (size_t i = 0; i < sel_size; i++) {
            if constexpr (std::is_same_v<Y, T>) {
                y[i] = data[sel[i]];
            } else {
                static_assert(sizeof(Y) == sizeof(T));
                memcpy(reinterpret_cast<void*>(&y[i]), reinterpret_cast<void*>(&data[sel[i]]),
                       sizeof(Y));
            }
        }
        res_data.set_end_ptr(y + sel_size);
    }

    void insert_byte_to_res_column(const uint16_t* sel, size_t sel_size, IColumn* res_ptr) {
        for (size_t i = 0; i < sel_size; i++) {
            uint16_t n = sel[i];
            char* ch_val = reinterpret_cast<char*>(&data[n]);
            res_ptr->insert_data(ch_val, 0);
        }
    }

    void insert_many_default_type(const char* data_ptr, size_t num) {
        auto old_size = data.size();
        data.resize(old_size + num);
        memcpy(reinterpret_cast<void*>(data.data() + old_size), data_ptr, num * sizeof(T));
    }

public:
    using Self = PredicateColumnType;
    using value_type = T;
    using Container = PaddedPODArray<value_type>;

    bool is_numeric() const override { return false; }

    size_t size() const override { return data.size(); }

    StringRef get_data_at(size_t n) const override {
        if constexpr (std::is_same_v<T, StringRef>) {
            auto res = reinterpret_cast<const StringRef&>(data[n]);
            if constexpr (Type == TYPE_CHAR) {
                res.size = strnlen(res.data, res.size);
            }
            return res;
        } else {
            throw doris::Exception(
                    ErrorCode::INTERNAL_ERROR,
                    "should not call get_data_at in predicate column except for string type");
            __builtin_unreachable();
        }
    }

    void insert_from(const IColumn& src, size_t n) override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "should not call insert_from in predicate column");
        __builtin_unreachable();
    }

    void insert_range_from(const IColumn& src, size_t start, size_t length) override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "should not call insert_range_from in predicate column");
        __builtin_unreachable();
    }

    void insert_indices_from(const IColumn& src, const uint32_t* indices_begin,
                             const uint32_t* indices_end) override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "should not call insert_indices_from in predicate column");
        __builtin_unreachable();
    }

    void pop_back(size_t n) override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "should not call pop_back in predicate column");
        __builtin_unreachable();
    }

    void update_hash_with_value(size_t n, SipHash& hash) const override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "should not call update_hash_with_value in predicate column");
        __builtin_unreachable();
    }

    void insert_string_value(const char* data_ptr, size_t length) {
        StringRef sv((char*)data_ptr, length);
        data.push_back_without_reserve(sv);
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
        if constexpr (std::is_same_v<T, StringRef>) {
            insert_string_value(data_ptr, length);
        } else if constexpr (std::is_same_v<T, Int128>) {
            insert_in_copy_way(data_ptr, length);
        } else {
            insert_default_type(data_ptr, length);
        }
    }

    void insert_many_date(const char* data_ptr, size_t num) {
        constexpr size_t input_type_size = sizeof(PrimitiveTypeTraits<TYPE_DATE>::StorageFieldType);
        static_assert(input_type_size == sizeof(uint24_t));
        const auto* input_data_ptr = reinterpret_cast<const uint24_t*>(data_ptr);

        auto* res_ptr = reinterpret_cast<VecDateTimeValue*>(data.get_end_ptr());
        for (int i = 0; i < num; i++) {
            res_ptr[i].set_olap_date(unaligned_load<uint24_t>(&input_data_ptr[i]));
        }
        data.set_end_ptr(res_ptr + num);
    }

    void insert_many_datetime(const char* data_ptr, size_t num) {
        constexpr size_t input_type_size =
                sizeof(PrimitiveTypeTraits<TYPE_DATETIME>::StorageFieldType);
        static_assert(input_type_size == sizeof(uint64_t));
        const auto* input_data_ptr = reinterpret_cast<const uint64_t*>(data_ptr);

        auto* res_ptr = reinterpret_cast<VecDateTimeValue*>(data.get_end_ptr());
        for (int i = 0; i < num; i++) {
            res_ptr[i].from_olap_datetime(input_data_ptr[i]);
        }
        data.set_end_ptr(res_ptr + num);
    }

    // The logic is same to ColumnDecimal::insert_many_fix_len_data
    void insert_many_decimalv2(const char* data_ptr, size_t num) {
        size_t old_size = data.size();
        data.resize(old_size + num);

        auto* target = (DecimalV2Value*)(data.data() + old_size);
        for (int i = 0; i < num; i++) {
            const char* cur_ptr = data_ptr + sizeof(decimal12_t) * i;
            auto int_value = unaligned_load<int64_t>(cur_ptr);
            int32_t frac_value = *(int32_t*)(cur_ptr + sizeof(int64_t));
            target[i].from_olap_decimal(int_value, frac_value);
        }
    }

    void insert_many_fix_len_data(const char* data_ptr, size_t num) override {
        if constexpr (Type == TYPE_DECIMALV2) {
            // DecimalV2 is special, its storage is <int64, int32>, but its compute type is <int64,int64>
            // should convert here, but it may have some performance lost
            insert_many_decimalv2(data_ptr, num);
        } else if constexpr (std::is_same_v<T, StringRef>) {
            // here is unreachable, just for compilation to be able to pass
        } else if constexpr (Type == TYPE_DATE) {
            // Datev1 is special, its storage is uint24, but its compute type is actual int64.
            insert_many_date(data_ptr, num);
        } else if constexpr (Type == TYPE_DATETIME) {
            insert_many_datetime(data_ptr, num);
        } else {
            insert_many_default_type(data_ptr, num);
        }
    }

    void insert_many_dict_data(const int32_t* data_array, size_t start_index, const StringRef* dict,
                               size_t num, uint32_t /*dict_num*/) override {
        if constexpr (std::is_same_v<T, StringRef>) {
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
        if constexpr (std::is_same_v<T, StringRef>) {
            if (_arena == nullptr) {
                _arena.reset(new Arena());
            }
            const auto total_mem_size = offsets[num] - offsets[0];
            char* destination = _arena->alloc(total_mem_size);
            memcpy(destination, data_ + offsets[0], total_mem_size);
            size_t org_elem_num = data.size();
            data.resize(org_elem_num + num);

            auto* data_ptr = &data[org_elem_num];
            for (size_t i = 0; i != num; ++i) {
                data_ptr[i].data = destination + offsets[i] - offsets[0];
                data_ptr[i].size = offsets[i + 1] - offsets[i];
            }
            DCHECK(data_ptr[num - 1].data + data_ptr[num - 1].size == destination + total_mem_size);
        }
    }

    void insert_many_binary_data(char* data_array, uint32_t* len_array,
                                 uint32_t* start_offset_array, size_t num) override {
        if (num == 0) {
            return;
        }
        if constexpr (std::is_same_v<T, StringRef>) {
            if (_arena == nullptr) {
                _arena.reset(new Arena());
            }

            size_t total_mem_size = 0;
            for (size_t i = 0; i < num; i++) {
                total_mem_size += len_array[i];
            }

            char* destination = _arena->alloc(total_mem_size);
            char* org_dst = destination;
            size_t org_elem_num = data.size();
            data.resize(org_elem_num + num);
            uint32_t fragment_start_offset = start_offset_array[0];
            size_t fragment_len = 0;
            for (size_t i = 0; i < num; i++) {
                data[org_elem_num + i].data = destination + fragment_len;
                data[org_elem_num + i].size = len_array[i];
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
            throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                   "Method insert_many_binary_data is not supported");
            __builtin_unreachable();
        }
    }

    void insert_default() override { data.push_back(T()); }

    void clear() override {
        data.clear();
        if (_arena != nullptr) {
            _arena->clear();
        }
    }

    size_t byte_size() const override { return data.size() * sizeof(T); }

    size_t allocated_bytes() const override { return byte_size(); }

    void reserve(size_t n) override { data.reserve(n); }

    const char* get_family_name() const override { return TypeName<T>::get(); }

    MutableColumnPtr clone_resized(size_t size) const override {
        DCHECK(size == 0);
        return this->create();
    }

    void insert(const Field& x) override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "insert not supported in PredicateColumnType");
        __builtin_unreachable();
    }

    [[noreturn]] Field operator[](size_t n) const override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "operator[] not supported in PredicateColumnType");
        __builtin_unreachable();
    }

    void get(size_t n, Field& res) const override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "get field not supported in PredicateColumnType");
    }

    [[noreturn]] bool get_bool(size_t n) const override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "get field not supported in PredicateColumnType");
        __builtin_unreachable();
    }

    [[noreturn]] Int64 get_int(size_t n) const override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "get field not supported in PredicateColumnType");
        __builtin_unreachable();
    }

    // it's impossible to use ComplexType as key , so we don't have to implement them
    [[noreturn]] StringRef serialize_value_into_arena(size_t n, Arena& arena,
                                                      char const*& begin) const override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "serialize_value_into_arena not supported in PredicateColumnType");
        __builtin_unreachable();
    }

    [[noreturn]] const char* deserialize_and_insert_from_arena(const char* pos) override {
        throw doris::Exception(
                ErrorCode::INTERNAL_ERROR,
                "deserialize_and_insert_from_arena not supported in PredicateColumnType");
        __builtin_unreachable();
    }

    bool is_fixed_and_contiguous() const override { return true; }
    size_t size_of_value_if_fixed() const override { return sizeof(T); }

    [[noreturn]] StringRef get_raw_data() const override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "get_raw_data not supported in PredicateColumnType");
        __builtin_unreachable();
    }

    [[noreturn]] bool structure_equals(const IColumn& rhs) const override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "structure_equals not supported in PredicateColumnType");
        __builtin_unreachable();
    }

    [[noreturn]] ColumnPtr filter(const IColumn::Filter& filt,
                                  ssize_t result_size_hint) const override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "filter not supported in PredicateColumnType");
        __builtin_unreachable();
    }

    [[noreturn]] size_t filter(const IColumn::Filter&) override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "filter not supported in PredicateColumnType");
        __builtin_unreachable();
    }

    [[noreturn]] ColumnPtr permute(const IColumn::Permutation& perm, size_t limit) const override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "permute not supported in PredicateColumnType");
        __builtin_unreachable();
    }

    Container& get_data() { return data; }

    const Container& get_data() const { return data; }

    [[noreturn]] ColumnPtr replicate(const IColumn::Offsets& replicate_offsets) const override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "replicate not supported in PredicateColumnType");
        __builtin_unreachable();
    }

    void append_data_by_selector(MutableColumnPtr& res,
                                 const IColumn::Selector& selector) const override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "append_data_by_selector is not supported in PredicateColumnType!");
        __builtin_unreachable();
    }
    void append_data_by_selector(MutableColumnPtr& res, const IColumn::Selector& selector,
                                 size_t begin, size_t end) const override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "append_data_by_selector is not supported in PredicateColumnType!");
        __builtin_unreachable();
    }

    Status filter_by_selector(const uint16_t* sel, size_t sel_size, IColumn* col_ptr) override {
        ColumnType* column = assert_cast<ColumnType*>(col_ptr);
        if constexpr (std::is_same_v<T, StringRef>) {
            insert_string_to_res_column(sel, sel_size, column);
        } else if constexpr (std::is_same_v<T, bool>) {
            insert_byte_to_res_column(sel, sel_size, col_ptr);
        } else {
            insert_default_value_res_column(sel, sel_size, column);
        }
        return Status::OK();
    }

    void replace_column_data(const IColumn&, size_t row, size_t self_row = 0) override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "should not call replace_column_data in predicate column");
        __builtin_unreachable();
    }

private:
    Container data;
    // manages the memory for slice's data(For string type)
    std::unique_ptr<Arena> _arena;
    std::vector<StringRef> _refs;
};

} // namespace doris::vectorized
