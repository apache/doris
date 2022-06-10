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

#include "runtime/string_value.h"
#include "vec/common/pod_array.h"
#include "vec/common/pod_array_fwd.h"

namespace doris {
namespace vectorized {

//T could be int8/int16/int32
template <typename T>
class Dict {
public:
    Dict() = default;
    Dict(const std::vector<std::string>& data) {
        int n = data.size();
        _dict_data.reserve(n);
        _inverted_index.reserve(n);
    }
    void reserve(size_t n) {
        _dict_data.reserve(n);
        _inverted_index.reserve(n);
    };

    void insert_value(StringValue& value) {
        _dict_data.push_back_without_reserve(value);
        _inverted_index[value] = _inverted_index.size();
    }

    int32_t find_code(const StringValue& value) const {
        auto it = _inverted_index.find(value);
        if (it != _inverted_index.end()) {
            return it->second;
        }
        return -1;
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
    int32_t find_code_by_bound(const StringValue& value, bool greater, bool eq) const {
        auto code = find_code(value);
        if (code >= 0) {
            return code;
        }
        auto bound =
                std::upper_bound(_dict_data.begin(), _dict_data.end(), value) - _dict_data.begin();
        return greater ? bound - greater + eq : bound - eq;
    }

    phmap::flat_hash_set<int32_t> find_codes(
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

    StringValue& get_value(T code) { return _dict_data[code]; }

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

    T convert_code(const T& code) const { return _code_convert_map.find(code)->second; }

    size_t byte_size() { return _dict_data.size() * sizeof(_dict_data[0]); }

    size_t dict_value_num() const { return _dict_data.size(); }

protected:
    size_t cardinality() {
        // cardinality() is planned to used to decide dict code type, which are int8_t, int16_t or int32_t.
        // for simplicity, fe and be always use int16_t as global dict code type.
        return INT16_MAX - 1;
        //return _dict_data.size();
    }

private:
    StringValue::Comparator _comparator;

    PaddedPODArray<StringValue> _dict_data;
    // dict value -> dict code
    phmap::flat_hash_map<StringValue, T, StringValue::HashOfStringValue> _inverted_index;
    // data page code -> sorted dict code, only used for range comparison predicate
    phmap::flat_hash_map<T, T> _code_convert_map;
};

} // namespace vectorized
} // namespace doris
