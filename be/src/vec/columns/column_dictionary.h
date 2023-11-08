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

#include "vec/columns/column.h"
#include "vec/columns/column_string.h"
#include "vec/columns/predicate_column.h"
#include "vec/common/pod_array.h"
#include "vec/common/string_ref.h"
#include "vec/core/types.h"

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
    ColumnDictionary(FieldType type) : _type(type) {}

public:
    using Self = ColumnDictionary;
    using value_type = T;
    using Container = PaddedPODArray<value_type>;
    using DictContainer = PaddedPODArray<StringRef>;
    using HashValueContainer = PaddedPODArray<uint32_t>; // used for bloom filter

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
        LOG(FATAL) << "insert_data not supported in ColumnDictionary";
    }

    void insert_default() override { _codes.push_back(_dict.get_null_code()); }

    void clear() override {
        _codes.clear();
        _dict_code_converted = false;
        _dict.clear_hash_values();
    }

    // TODO: Make dict memory usage more precise
    size_t byte_size() const override { return _codes.size() * sizeof(_codes[0]); }

    size_t allocated_bytes() const override { return byte_size(); }

    void get_permutation(bool reverse, size_t limit, int nan_direction_hint,
                         IColumn::Permutation& res) const override {
        LOG(FATAL) << "get_permutation not supported in ColumnDictionary";
    }

    void reserve(size_t n) override { _codes.reserve(n); }

    const char* get_family_name() const override { return "ColumnDictionary"; }

    MutableColumnPtr clone_resized(size_t size) const override {
        DCHECK(size == 0);
        return this->create();
    }

    void insert(const Field& x) override {
        LOG(FATAL) << "insert not supported in ColumnDictionary";
    }

    Field operator[](size_t n) const override { return _codes[n]; }

    void get(size_t n, Field& res) const override { res = (*this)[n]; }

    Container& get_data() { return _codes; }

    const Container& get_data() const { return _codes; }

    // it's impossible to use ComplexType as key , so we don't have to implement them
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

    bool is_fixed_and_contiguous() const override { return true; }

    void get_indices_of_non_default_rows(IColumn::Offsets64& indices, size_t from,
                                         size_t limit) const override {
        LOG(FATAL) << "get_indices_of_non_default_rows not supported in ColumnDictionary";
    }

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
    }

    [[noreturn]] size_t filter(const IColumn::Filter&) override {
        LOG(FATAL) << "filter not supported in ColumnDictionary";
    }

    [[noreturn]] ColumnPtr permute(const IColumn::Permutation& perm, size_t limit) const override {
        LOG(FATAL) << "permute not supported in ColumnDictionary";
    }

    [[noreturn]] ColumnPtr replicate(const IColumn::Offsets& replicate_offsets) const override {
        LOG(FATAL) << "replicate not supported in ColumnDictionary";
    }

    [[noreturn]] MutableColumns scatter(IColumn::ColumnIndex num_columns,
                                        const IColumn::Selector& selector) const override {
        LOG(FATAL) << "scatter not supported in ColumnDictionary";
    }

    void append_data_by_selector(MutableColumnPtr& res,
                                 const IColumn::Selector& selector) const override {
        LOG(FATAL) << "append_data_by_selector is not supported in ColumnDictionary!";
    }

    [[noreturn]] ColumnPtr index(const IColumn& indexes, size_t limit) const override {
        LOG(FATAL) << "index not implemented";
    }

    Status filter_by_selector(const uint16_t* sel, size_t sel_size, IColumn* col_ptr) override {
        auto* res_col = reinterpret_cast<vectorized::ColumnString*>(col_ptr);
        StringRef strings[sel_size];
        size_t length = 0;
        for (size_t i = 0; i != sel_size; ++i) {
            auto& value = _dict.get_value(_codes[sel[i]]);
            strings[i].data = value.data;
            strings[i].size = value.size;
            length += value.size;
        }
        res_col->get_offsets().reserve(sel_size + res_col->get_offsets().size());
        res_col->get_chars().reserve(length + res_col->get_chars().size());
        res_col->insert_many_strings_without_reserve(strings, sel_size);
        return Status::OK();
    }

    void replace_column_data(const IColumn&, size_t row, size_t self_row = 0) override {
        LOG(FATAL) << "should not call replace_column_data in ColumnDictionary";
    }

    void replace_column_data_default(size_t self_row = 0) override {
        LOG(FATAL) << "should not call replace_column_data_default in ColumnDictionary";
    }

    /**
     * Just insert dictionary data items, the items will append into _dict.
     */
    void insert_many_dict_data(const StringRef* dict_array, uint32_t dict_num) {
        _dict.reserve(_dict.size() + dict_num);
        for (uint32_t i = 0; i < dict_num; ++i) {
            auto value = StringRef(dict_array[i].data, dict_array[i].size);
            _dict.insert_value(value);
        }
    }

    void insert_many_dict_data(const int32_t* data_array, size_t start_index,
                               const StringRef* dict_array, size_t data_num,
                               uint32_t dict_num) override {
        if (_dict.empty()) {
            _dict.reserve(dict_num);
            for (uint32_t i = 0; i < dict_num; ++i) {
                auto value = StringRef(dict_array[i].data, dict_array[i].size);
                _dict.insert_value(value);
            }
        }

        char* end_ptr = (char*)_codes.get_end_ptr();
        memcpy(end_ptr, data_array + start_index, data_num * sizeof(T));
        end_ptr += data_num * sizeof(T);
        _codes.set_end_ptr(end_ptr);
    }

    void convert_dict_codes_if_necessary() override {
        // Avoid setting `_dict_sorted` to true when `_dict` is empty.
        // Because `_dict` maybe keep empty after inserting some null rows.
        if (_dict.empty()) {
            return;
        }

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

    int32_t find_code(const StringRef& value) const { return _dict.find_code(value); }

    int32_t find_code_by_bound(const StringRef& value, bool greater, bool eq) const {
        return _dict.find_code_by_bound(value, greater, eq);
    }

    void initialize_hash_values_for_runtime_filter() override {
        _dict.initialize_hash_values_for_runtime_filter();
    }

    uint32_t get_hash_value(uint32_t idx) const { return _dict.get_hash_value(_codes[idx], _type); }
    uint32_t get_crc32_hash_value(uint32_t idx) const {
        return _dict.get_crc32_hash_value(_codes[idx], _type);
    }
    template <typename HybridSetType>
    void find_codes(const HybridSetType* values, std::vector<vectorized::UInt8>& selected) const {
        return _dict.find_codes(values, selected);
    }

    void set_rowset_segment_id(std::pair<RowsetId, uint32_t> rowset_segment_id) override {
        _rowset_segment_id = rowset_segment_id;
    }

    std::pair<RowsetId, uint32_t> get_rowset_segment_id() const override {
        return _rowset_segment_id;
    }

    void replicate(const uint32_t* indexs, size_t target_size, IColumn& column) const override {
        LOG(FATAL) << "not support";
    }

    bool is_dict_sorted() const { return _dict_sorted; }

    bool is_dict_empty() const { return _dict.empty(); }

    bool is_dict_code_converted() const { return _dict_code_converted; }

    MutableColumnPtr convert_to_predicate_column_if_dictionary() override {
        if (is_dict_sorted() && !is_dict_code_converted()) {
            convert_dict_codes_if_necessary();
        }
        // if type is OLAP_FIELD_TYPE_CHAR, we need to construct TYPE_CHAR PredicateColumnType,
        // because the string length will different from varchar and string which needed to be processed after.
        auto create_column = [this]() -> MutableColumnPtr {
            if (_type == FieldType::OLAP_FIELD_TYPE_CHAR) {
                return vectorized::PredicateColumnType<TYPE_CHAR>::create();
            }
            return vectorized::PredicateColumnType<TYPE_STRING>::create();
        };

        auto res = create_column();
        res->reserve(_codes.capacity());
        for (size_t i = 0; i < _codes.size(); ++i) {
            auto& code = reinterpret_cast<T&>(_codes[i]);
            auto value = _dict.get_value(code);
            res->insert_data(value.data, value.size);
        }
        clear();
        _dict.clear();
        return res;
    }

    inline const StringRef& get_value(value_type code) const { return _dict.get_value(code); }

    inline StringRef get_shrink_value(value_type code) const {
        StringRef result = _dict.get_value(code);
        if (_type == FieldType::OLAP_FIELD_TYPE_CHAR) {
            result.size = strnlen(result.data, result.size);
        }
        return result;
    }

    size_t dict_size() const { return _dict.size(); }

    std::string dict_debug_string() const { return _dict.debug_string(); }

    class Dictionary {
    public:
        Dictionary() : _dict_data(new DictContainer()), _total_str_len(0) {}

        void reserve(size_t n) { _dict_data->reserve(n); }

        void insert_value(const StringRef& value) {
            _dict_data->push_back_without_reserve(value);
            _total_str_len += value.size;
        }

        int32_t find_code(const StringRef& value) const {
            for (size_t i = 0; i < _dict_data->size(); i++) {
                if ((*_dict_data)[i] == value) {
                    return i;
                }
            }
            return -2; // -1 is null code
        }

        T get_null_code() const { return -1; }

        inline StringRef& get_value(T code) { return (*_dict_data)[code]; }

        inline const StringRef& get_value(T code) const { return (*_dict_data)[code]; }

        // The function is only used in the runtime filter feature
        inline void initialize_hash_values_for_runtime_filter() {
            if (_hash_values.empty()) {
                _hash_values.resize(_dict_data->size());
                _compute_hash_value_flags.resize(_dict_data->size());
                _compute_hash_value_flags.assign(_dict_data->size(), 0);
            }
        }

        inline uint32_t get_hash_value(T code, FieldType type) const {
            if (_compute_hash_value_flags[code]) {
                return _hash_values[code];
            } else {
                auto& sv = (*_dict_data)[code];
                // The char data is stored in the disk with the schema length,
                // and zeros are filled if the length is insufficient

                // When reading data, use shrink_char_type_column_suffix_zero(_char_type_idx)
                // Remove the suffix 0
                // When writing data, use the CharField::consume function to fill in the trailing 0.

                // For dictionary data of char type, sv.size is the schema length,
                // so use strnlen to remove the 0 at the end to get the actual length.
                int32_t len = sv.size;
                if (type == FieldType::OLAP_FIELD_TYPE_CHAR) {
                    len = strnlen(sv.data, sv.size);
                }
                uint32_t hash_val = HashUtil::murmur_hash3_32(sv.data, len, 0);
                _hash_values[code] = hash_val;
                _compute_hash_value_flags[code] = 1;
                return _hash_values[code];
            }
        }

        inline uint32_t get_crc32_hash_value(T code, FieldType type) const {
            if (_compute_hash_value_flags[code]) {
                return _hash_values[code];
            } else {
                auto& sv = (*_dict_data)[code];
                // The char data is stored in the disk with the schema length,
                // and zeros are filled if the length is insufficient

                // When reading data, use shrink_char_type_column_suffix_zero(_char_type_idx)
                // Remove the suffix 0
                // When writing data, use the CharField::consume function to fill in the trailing 0.

                // For dictionary data of char type, sv.size is the schema length,
                // so use strnlen to remove the 0 at the end to get the actual length.
                int32_t len = sv.size;
                if (type == FieldType::OLAP_FIELD_TYPE_CHAR) {
                    len = strnlen(sv.data, sv.size);
                }
                uint32_t hash_val = HashUtil::crc_hash(sv.data, len, 0);
                _hash_values[code] = hash_val;
                _compute_hash_value_flags[code] = 1;
                return _hash_values[code];
            }
        }

        // For > , code takes upper_bound - 1; For >= , code takes upper_bound
        // For < , code takes upper_bound; For <=, code takes upper_bound - 1
        // For example a sorted dict: <'b',0> <'c',1> <'d',2>
        // Now the predicate value is 'ccc', 'ccc' is not in the dict, 'ccc' is between 'c' and 'd'.
        // std::upper_bound(..., 'ccc') - begin, will return the encoding of 'd', which is 2
        // If the predicate is col > 'ccc' and the value of upper_bound-1 is 1,
        //  then evaluate code > 1 and the result is 'd'.
        // If the predicate is col < 'ccc' and the value of upper_bound is 2,
        //  evaluate code < 2, and the return result is 'b'.
        // If the predicate is col >= 'ccc' and the value of upper_bound is 2,
        //  evaluate code >= 2, and the return result is 'd'.
        // If the predicate is col <= 'ccc' and the value of upper_bound-1 is 1,
        //  evaluate code <= 1, and the returned result is 'b'.
        // If the predicate is col < 'a', 'a' is also not in the dict, and 'a' is less than 'b',
        //  so upper_bound is the code 0 of b, then evaluate code < 0 and returns empty
        // If the predicate is col <= 'a' and upper_bound-1 is -1,
        //  then evaluate code <= -1 and returns empty
        int32_t find_code_by_bound(const StringRef& value, bool greater, bool eq) const {
            auto code = find_code(value);
            if (code >= 0) {
                return code;
            }
            auto bound = std::upper_bound(_dict_data->begin(), _dict_data->end(), value) -
                         _dict_data->begin();
            return greater ? bound - greater + eq : bound - eq;
        }

        template <typename HybridSetType>
        void find_codes(const HybridSetType* values,
                        std::vector<vectorized::UInt8>& selected) const {
            size_t dict_word_num = _dict_data->size();
            selected.resize(dict_word_num);
            selected.assign(dict_word_num, false);
            for (size_t i = 0; i < _dict_data->size(); i++) {
                if (values->find(&((*_dict_data)[i]))) {
                    selected[i] = true;
                }
            }
        }

        void clear() {
            _dict_data->clear();
            _code_convert_table.clear();
            _hash_values.clear();
            _compute_hash_value_flags.clear();
        }

        void clear_hash_values() {
            _hash_values.clear();
            _compute_hash_value_flags.clear();
        }

        void sort() {
            size_t dict_size = _dict_data->size();

            _code_convert_table.reserve(dict_size);
            _perm.resize(dict_size);
            for (size_t i = 0; i < dict_size; ++i) {
                _perm[i] = i;
            }

            std::sort(_perm.begin(), _perm.end(),
                      [&dict_data = *_dict_data, &comparator = _comparator](const size_t a,
                                                                            const size_t b) {
                          return comparator(dict_data[a], dict_data[b]);
                      });

            auto new_dict_data = new DictContainer(dict_size);
            for (size_t i = 0; i < dict_size; ++i) {
                _code_convert_table[_perm[i]] = (T)i;
                (*new_dict_data)[i] = (*_dict_data)[_perm[i]];
            }
            _dict_data.reset(new_dict_data);
        }

        T convert_code(const T& code) const {
            if (get_null_code() == code) {
                return code;
            }
            return _code_convert_table[code];
        }

        size_t byte_size() { return _dict_data->size() * sizeof((*_dict_data)[0]); }

        bool empty() const { return _dict_data->empty(); }

        size_t avg_str_len() { return empty() ? 0 : _total_str_len / _dict_data->size(); }

        size_t size() const {
            if (!_dict_data) {
                return 0;
            }
            return _dict_data->size();
        }

        std::string debug_string() const {
            std::string str = "[";
            if (_dict_data) {
                for (size_t i = 0; i < _dict_data->size(); i++) {
                    if (i) {
                        str += ',';
                    }
                    str += (*_dict_data)[i].to_string();
                }
            }
            str += ']';
            return str;
        }

    private:
        StringRef::Comparator _comparator;
        // dict code -> dict value
        std::unique_ptr<DictContainer> _dict_data;
        std::vector<T> _code_convert_table;
        // hash value of origin string , used for bloom filter
        // It's a trade-off of space for performance
        // But in TPC-DS 1GB q60,we see no significant improvement.
        // This may because the magnitude of the data is not large enough(in q60, only about 80k rows data is filtered for largest table)
        // So we may need more test here.
        mutable HashValueContainer _hash_values;
        mutable std::vector<uint8> _compute_hash_value_flags;
        IColumn::Permutation _perm;
        size_t _total_str_len;
    };

private:
    size_t _reserve_size;
    bool _dict_sorted = false;
    bool _dict_code_converted = false;
    Dictionary _dict;
    Container _codes;
    FieldType _type;
    std::pair<RowsetId, uint32_t> _rowset_segment_id;
};

template class ColumnDictionary<int32_t>;

using ColumnDictI32 = vectorized::ColumnDictionary<doris::vectorized::Int32>;

} // namespace doris::vectorized
