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

#include <algorithm>
#include <parallel_hashmap/phmap.h>

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

namespace doris::vectorized {

/**
 * For low cardinality string columns, using ColumnDictionary can reducememory
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
private:
    friend class COWHelper<IColumn, ColumnDictionary>;

    ColumnDictionary() {}
    ColumnDictionary(const size_t n) : codes(n) {}
    ColumnDictionary(const ColumnDictionary& src) : codes(src.codes.begin(), src.codes.end()) {}

public:
    using Self = ColumnDictionary;
    using value_type = T;
    using Container = PaddedPODArray<value_type>;
    using DictContainer = PaddedPODArray<StringValue>;

    bool is_numeric() const override { return false; }

    bool is_predicate_column() const override { return false; }

    bool is_column_dictionary() const override { return true; }

    size_t size() const override { return codes.size(); }

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
        codes.push_back(unaligned_load<T>(pos));
    }

    void insert_data(const T value) { codes.push_back(value); }

    void insert_default() override { codes.push_back(T()); }

    void clear() override { codes.clear(); }

    // TODO: Make dict memory usage more precise
    size_t byte_size() const override { return codes.size() * sizeof(codes[0]); }

    size_t allocated_bytes() const override { return byte_size(); }

    void protect() override {}

    void get_permutation(bool reverse, size_t limit, int nan_direction_hint,
                         IColumn::Permutation& res) const override {
        LOG(FATAL) << "get_permutation not supported in ColumnDictionary";
    }

    void reserve(size_t n) override { codes.reserve(n); }

    [[noreturn]] const char* get_family_name() const override {
        LOG(FATAL) << "get_family_name not supported in ColumnDictionary";
    }

    [[noreturn]] MutableColumnPtr clone_resized(size_t size) const override {
        LOG(FATAL) << "clone_resized not supported in ColumnDictionary";
    }

    void insert(const Field& x) override {
        LOG(FATAL) << "insert not supported in ColumnDictionary";
    }

    Field operator[](size_t n) const override { return codes[n]; }

    void get(size_t n, Field& res) const override { res = (*this)[n]; }

    [[noreturn]] UInt64 get64(size_t n) const override {
        LOG(FATAL) << "get field not supported in ColumnDictionary";
    }

    [[noreturn]] Float64 get_float64(size_t n) const override {
        LOG(FATAL) << "get field not supported in ColumnDictionary";
    }

    [[noreturn]] UInt64 get_uint(size_t n) const override {
        LOG(FATAL) << "get field not supported in ColumnDictionary";
    }

    [[noreturn]] bool get_bool(size_t n) const override {
        LOG(FATAL) << "get field not supported in ColumnDictionary";
    }

    [[noreturn]] Int64 get_int(size_t n) const override {
        LOG(FATAL) << "get field not supported in ColumnDictionary";
    }

    Container& get_data() { return codes; }

    const Container& get_data() const { return codes; }

    T find_code(const StringValue& value) const { return dict.find_code(value); }

    T find_bound_code(const StringValue& value, bool lower, bool eq) const {
        return dict.find_bound_code(value, lower, eq);
    }

    phmap::flat_hash_set<T> find_codes(const phmap::flat_hash_set<StringValue>& values) const {
        return dict.find_codes(values);
    }

    // it's impossable to use ComplexType as key , so we don't have to implemnt them
    [[noreturn]] StringRef serialize_value_into_arena(size_t n, Arena& arena,
                                                      char const*& begin) const {
        LOG(FATAL) << "serialize_value_into_arena not supported in ColumnDictionary";
    }

    [[noreturn]] const char* deserialize_and_insert_from_arena(const char* pos) {
        LOG(FATAL) << "deserialize_and_insert_from_arena not supported in ColumnDictionary";
    }

    [[noreturn]] int compare_at(size_t n, size_t m, const IColumn& rhs,
                                int nan_direction_hint) const {
        LOG(FATAL) << "compare_at not supported in ColumnDictionary";
    }

    void get_extremes(Field& min, Field& max) const {
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
            auto& code = reinterpret_cast<T&>(codes[n]);
            auto value = dict.get_value(code);
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
            dict.reserve(dict_num);
            for (uint32_t i = 0; i < dict_num; ++i) {
                auto value = StringValue(dict_array[i].data, dict_array[i].size);
                dict.insert_value(value);
            }
            dict_inited = true;
        }

        for (int i = 0; i < data_num; i++, start_index++) {
            int32_t code = data_array[start_index];
            insert_data(code);
        }
    }

    bool is_dict_inited() const { return dict_inited; }

    bool is_dict_sorted() const { return dict_sorted; }

    bool is_dict_code_converted() const { return dict_code_converted; }

    ColumnPtr convert_to_predicate_column() {
        auto res = vectorized::PredicateColumnType<StringValue>::create();
        size_t size = codes.size();
        res->reserve(size);
        for (size_t i = 0; i < size; ++i) {
            auto& code = reinterpret_cast<T&>(codes[i]);
            auto value = dict.get_value(code);
            res->insert_data(value.ptr, value.len);
        }
        dict.clear();
        return res;
    }

    void convert_dict_codes() {
        if (!is_dict_sorted()) {
            sort_dict();
        }

        if (!is_dict_code_converted()) {
            for (size_t i = 0; i < size(); ++i) {
                codes[i] = dict.convert_code(codes[i]);
            }
            dict_code_converted = true;
        }
    }

    void sort_dict() {
        dict.sort();
        dict_sorted = true;
    }

    class Dictionary {
    public:
        Dictionary() = default;

        void reserve(size_t n) {
            dict_data.reserve(n);
            inverted_index.reserve(n);
        }

        inline void insert_value(StringValue& value) {
            dict_data.push_back_without_reserve(value);
            inverted_index[value] = inverted_index.size();
        }

        inline T find_code(const StringValue& value) const {
            auto it = inverted_index.find(value);
            if (it != inverted_index.end()) {
                return it->second;
            }
            return -1;
        }

        inline T find_bound_code(const StringValue& value, bool lower, bool eq) const {
            if (lower) {
                return std::lower_bound(dict_data.begin(), dict_data.end(), value) - dict_data.begin() - eq;
            } else {
                return std::upper_bound(dict_data.begin(), dict_data.end(), value) - dict_data.begin() + eq;
            }
        }

        inline phmap::flat_hash_set<T> find_codes(const phmap::flat_hash_set<StringValue>& values) const {
            phmap::flat_hash_set<T> code_set;
            for (const auto& value : values) {
                auto it = inverted_index.find(value);
                if (it != inverted_index.end()) {
                    code_set.insert(it->second);
                }
            }
            return code_set;
        }

        inline StringValue& get_value(T code) { return dict_data[code]; }

        void clear() {
            dict_data.clear();
            inverted_index.clear();
            code_convert_map.clear();
        }

        void sort() {
            size_t dict_size = dict_data.size();
            std::sort(dict_data.begin(), dict_data.end(), comparator);
            for (size_t i = 0; i < dict_size; ++i) {
                code_convert_map[inverted_index.find(dict_data[i])->second] = (T)i;
                inverted_index[dict_data[i]] = (T)i;
            }
        }

        inline T convert_code(const T& code) const { return code_convert_map.find(code)->second; }

        size_t byte_size() { return dict_data.size() * sizeof(dict_data[0]); }

    private:
        struct HashOfStringValue {
            size_t operator()(const StringValue& value) const {
                return HashStringThoroughly(value.ptr, value.len);
            }
        };

        StringValue::Comparator comparator;
        // dict code -> dict value
        DictContainer dict_data;
        // dict value -> dict code
        phmap::flat_hash_map<StringValue, T, HashOfStringValue> inverted_index;
        // data page code -> sorted dict code, only used for range comparison predicate
        phmap::flat_hash_map<T, T> code_convert_map;
    };

private:
    bool dict_inited = false;
    bool dict_sorted = false;
    bool dict_code_converted = false;
    Dictionary dict;
    Container codes;
};

} // namespace doris::vectorized