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

#include <parallel_hashmap/phmap.h>

#include <algorithm>

#include "gutil/hash/string_hash.h"
#include "olap/decimal12.h"
#include "olap/uint24.h"
#include "runtime/string_value.h"
#include "util/slice.h"
#include "vec/columns/column.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_impl.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/predicate_column.h"
#include "vec/core/types.h"
#include "vec/common/typeid_cast.h"
#include "olap/column_predicate.h"
#include "olap/comparison_predicate.h"
#include "olap/in_list_predicate.h"

namespace doris::vectorized {

/**
 * For low cardinality string columns, using ColumnDictionary can reduce memory
 * usage and improve query efficiency.
 * For equal predicate comparisons, convert the predicate constant to encodings
 * according to the dictionary, so that encoding comparisons are used instead
 * of string comparisons to improve performance.
 * For range comparison predicates, it is necessary to sort the dictionary
 * contents, convert the encoding column, and then compare the encoding directly.
 * If the read data page contains plain-encoded data pages, the dictionary
 * columns are converted into PredicateColumn for processing.
 * Currently ColumnDictionary is only used for storage layer.
 */
template <typename T>
class ColumnDictionary final : public COWHelper<IColumn, ColumnDictionary<T>> {
    static_assert(IsNumber<T>);
private:
    friend class COWHelper<IColumn, ColumnDictionary>;

    ColumnDictionary() {}
    ColumnDictionary(const size_t n) : _codes(n) {}
    ColumnDictionary(const ColumnDictionary& src) : _codes(src._codes.begin(), src._codes.end()) {}

public:
    using Self = ColumnDictionary;
    using value_type = T;
    using Container = PaddedPODArray<value_type>;
    using DictContainer = PaddedPODArray<StringValue>;

    bool is_column_dictionary() const override { return true; }

    size_t size() const override { return _codes.size(); }

    [[noreturn]] StringRef get_data_at(size_t n) const override {
        LOG(FATAL) << "get_data_at not supported in ColumnDictionary";
    }

    void insert_from(const IColumn& src, size_t n) override {
        LOG(FATAL) << "insert_from not supported in ColumnDictionary";
    }

    void insert_range_from(const IColumn& src, size_t start, size_t length) override {
        LOG(FATAL) << "insert_range_from not supported in ColumnDictionary";
    }

    void insert_indices_from(const IColumn& src, const int* indices_begin,
                             const int* indices_end) override {
        LOG(FATAL) << "insert_indices_from not supported in ColumnDictionary";
    }

    void pop_back(size_t n) override { LOG(FATAL) << "pop_back not supported in ColumnDictionary"; }

    void update_hash_with_value(size_t n, SipHash& hash) const override {
        LOG(FATAL) << "update_hash_with_value not supported in ColumnDictionary";
    }

    void insert_data(const char* pos, size_t /*length*/) override {
        _codes.push_back(unaligned_load<T>(pos));
    }

    void insert_data(const T value) { _codes.push_back(value); }

    void insert_default() override { _codes.push_back(T()); }

    void clear() override {
        _codes.clear();
        _dict_code_converted = false;
    }

    // TODO: Make dict memory usage more precise
    size_t byte_size() const override { return _codes.size() * sizeof(_codes[0]); }

    size_t allocated_bytes() const override { return byte_size(); }

    void protect() override {}

    void get_permutation(bool reverse, size_t limit, int nan_direction_hint,
                         IColumn::Permutation& res) const override {
        LOG(FATAL) << "get_permutation not supported in ColumnDictionary";
    }

    void reserve(size_t n) override { _codes.reserve(n); }

    const char* get_family_name() const override { return "ColumnDictionary"; }

    [[noreturn]] MutableColumnPtr clone_resized(size_t size) const override {
        LOG(FATAL) << "clone_resized not supported in ColumnDictionary";
    }

    void insert(const Field& x) override {
        LOG(FATAL) << "insert not supported in ColumnDictionary";
    }

    Field operator[](size_t n) const override { return _codes[n]; }

    void get(size_t n, Field& res) const override { res = (*this)[n]; }

    Container& get_data() { return _codes; }

    const Container& get_data() const { return _codes; }

    // it's impossable to use ComplexType as key , so we don't have to implemnt them
    [[noreturn]] StringRef serialize_value_into_arena(size_t n, Arena& arena,
                                                      char const*& begin) const override {
        LOG(FATAL) << "serialize_value_into_arena not supported in ColumnDictionary";
    }

    [[noreturn]] const char* deserialize_and_insert_from_arena(const char* pos) override {
        LOG(FATAL) << "deserialize_and_insert_from_arena not supported in ColumnDictionary";
    }

    [[noreturn]] int compare_at(size_t n, size_t m, const IColumn& rhs,
                                int nan_direction_hint) const override {
        LOG(FATAL) << "compare_at not supported in ColumnDictionary";
    }

    void get_extremes(Field& min, Field& max) const override {
        LOG(FATAL) << "get_extremes not supported in ColumnDictionary";
    }

    bool can_be_inside_nullable() const override { return true; }

    bool is_fixed_and_contiguous() const override { return true; }

    size_t size_of_value_if_fixed() const override { return sizeof(T); }

    [[noreturn]] StringRef get_raw_data() const override {
        LOG(FATAL) << "get_raw_data not supported in ColumnDictionary";
    }

    [[noreturn]] bool structure_equals(const IColumn& rhs) const override {
        LOG(FATAL) << "structure_equals not supported in ColumnDictionary";
    }

    [[noreturn]] ColumnPtr filter(const IColumn::Filter& filt,
                                  ssize_t result_size_hint) const override {
        LOG(FATAL) << "filter not supported in ColumnDictionary";
    };

    [[noreturn]] ColumnPtr permute(const IColumn::Permutation& perm, size_t limit) const override {
        LOG(FATAL) << "permute not supported in ColumnDictionary";
    };

    [[noreturn]] ColumnPtr replicate(const IColumn::Offsets& replicate_offsets) const override {
        LOG(FATAL) << "replicate not supported in ColumnDictionary";
    };

    [[noreturn]] MutableColumns scatter(IColumn::ColumnIndex num_columns,
                                        const IColumn::Selector& selector) const override {
        LOG(FATAL) << "scatter not supported in ColumnDictionary";
    }

    Status filter_by_selector(const uint16_t* sel, size_t sel_size, IColumn* col_ptr) override {
        auto* res_col = reinterpret_cast<vectorized::ColumnString*>(col_ptr);
        for (size_t i = 0; i < sel_size; i++) {
            uint16_t n = sel[i];
            auto& code = reinterpret_cast<T&>(_codes[n]);
            auto value = _dict.get_value(code);
            res_col->insert_data(value.ptr, value.len);
        }
        return Status::OK();
    }

    void replace_column_data(const IColumn&, size_t row, size_t self_row = 0) override {
        LOG(FATAL) << "should not call replace_column_data in ColumnDictionary";
    }

    void replace_column_data_default(size_t self_row = 0) override {
        LOG(FATAL) << "should not call replace_column_data_default in ColumnDictionary";
    }

    void insert_many_dict_data(const int32_t* data_array, size_t start_index,
                               const StringRef* dict_array, size_t data_num,
                               uint32_t dict_num) override {
        if (!is_dict_inited()) {
            _dict.reserve(dict_num);
            for (uint32_t i = 0; i < dict_num; ++i) {
                auto value = StringValue(dict_array[i].data, dict_array[i].size);
                _dict.insert_value(value);
            }
            _dict_inited = true;
        }

        char* end_ptr = (char*)_codes.get_end_ptr();
        memcpy(end_ptr, data_array + start_index, data_num * sizeof(T));
        end_ptr += data_num * sizeof(T);
        _codes.set_end_ptr(end_ptr);
    }

    void convert_dict_codes_if_necessary() override {
        if (!is_dict_sorted()) {
            _dict.sort();
            _dict_sorted = true;
        }

        if (!is_dict_code_converted()) {
            for (size_t i = 0; i < size(); ++i) {
                _codes[i] = _dict.convert_code(_codes[i]);
            }
            _dict_code_converted = true;
        }
    }

    int32_t find_code(const StringValue& value) const { return _dict.find_code(value); }

    int32_t find_code_by_bound(const StringValue& value, bool lower, bool eq) const {
        return _dict.find_code_by_bound(value, lower, eq);
    }

    phmap::flat_hash_set<int32_t> find_codes(
            const phmap::flat_hash_set<StringValue>& values) const {
        return _dict.find_codes(values);
    }

    bool is_dict_inited() const { return _dict_inited; }

    bool is_dict_sorted() const { return _dict_sorted; }

    bool is_dict_code_converted() const { return _dict_code_converted; }

    ColumnPtr convert_to_predicate_column_if_dictionary() override {
        auto res = vectorized::PredicateColumnType<StringValue>::create();
        size_t size = _codes.size();
        res->reserve(size);
        for (size_t i = 0; i < size; ++i) {
            auto& code = reinterpret_cast<T&>(_codes[i]);
            auto value = _dict.get_value(code);
            res->insert_data(value.ptr, value.len);
        }
        _dict.clear();
        return res;
    }

    class Dictionary {
    public:
        Dictionary() = default;

        void reserve(size_t n) {
            _dict_data.reserve(n);
            _inverted_index.reserve(n);
        }

        inline void insert_value(StringValue& value) {
            _dict_data.push_back_without_reserve(value);
            _inverted_index[value] = _inverted_index.size();
        }

        inline int32_t find_code(const StringValue& value) const {
            auto it = _inverted_index.find(value);
            if (it != _inverted_index.end()) {
                return it->second;
            }
            return -1;
        }

        inline int32_t find_code_by_bound(const StringValue& value, bool lower, bool eq) const {
            auto code = find_code(value);
            if (code >= 0) {
                return code;
            }

            if (lower) {
                return std::lower_bound(_dict_data.begin(), _dict_data.end(), value) -
                       _dict_data.begin() - eq;
            } else {
                return std::upper_bound(_dict_data.begin(), _dict_data.end(), value) -
                       _dict_data.begin() + eq;
            }
        }

        inline phmap::flat_hash_set<int32_t> find_codes(
                const phmap::flat_hash_set<StringValue>& values) const {
            phmap::flat_hash_set<int32_t> code_set;
            for (const auto& value : values) {
                auto it = _inverted_index.find(value);
                if (it != _inverted_index.end()) {
                    code_set.insert(it->second);
                }
            }
            return code_set;
        }

        inline StringValue& get_value(T code) { return _dict_data[code]; }

        void clear() {
            _dict_data.clear();
            _inverted_index.clear();
            _code_convert_map.clear();
        }

        void sort() {
            size_t dict_size = _dict_data.size();
            std::sort(_dict_data.begin(), _dict_data.end(), _comparator);
            for (size_t i = 0; i < dict_size; ++i) {
                _code_convert_map[_inverted_index.find(_dict_data[i])->second] = (T)i;
                _inverted_index[_dict_data[i]] = (T)i;
            }
        }

        inline T convert_code(const T& code) const { return _code_convert_map.find(code)->second; }

        size_t byte_size() { return _dict_data.size() * sizeof(_dict_data[0]); }

    private:
        StringValue::Comparator _comparator;
        // dict code -> dict value
        DictContainer _dict_data;
        // dict value -> dict code
        phmap::flat_hash_map<StringValue, T, StringValue::HashOfStringValue> _inverted_index;
        // data page code -> sorted dict code, only used for range comparison predicate
        phmap::flat_hash_map<T, T> _code_convert_map;
    };

private:
    bool _dict_inited = false;
    bool _dict_sorted = false;
    bool _dict_code_converted = false;
    Dictionary _dict;
    Container _codes;
};

template class ColumnDictionary<uint8_t>;
template class ColumnDictionary<uint16_t>;
template class ColumnDictionary<uint32_t>;
template class ColumnDictionary<int32_t>;

} // namespace doris::vectorized
