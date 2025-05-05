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
#include <cmath>
#include <unordered_map>
#include <vector>

#include "udf/udf.h"

namespace doris {

template <typename T>
class Counts {
public:
    Counts() = default;

    inline void merge(const Counts* other) {
        if (other == nullptr || other->_counts.empty()) {
            return;
        }

        for (auto& cell : other->_counts) {
            increment(cell.first, cell.second);
        }
    }

    void increment(T key, uint32_t i) {
        auto item = _counts.find(key);
        if (item != _counts.end()) {
            item->second += i;
        } else {
            _counts.emplace(std::make_pair(key, i));
        }
    }

    uint32_t serialized_size() const {
        return sizeof(uint32_t) + sizeof(T) * _counts.size() + sizeof(uint32_t) * _counts.size();
    }

    void serialize(uint8_t* writer) const {
        uint32_t size = _counts.size();
        memcpy(writer, &size, sizeof(uint32_t));
        writer += sizeof(uint32_t);
        for (auto& cell : _counts) {
            memcpy(writer, &cell.first, sizeof(T));
            writer += sizeof(T);
            memcpy(writer, &cell.second, sizeof(uint32_t));
            writer += sizeof(uint32_t);
        }
    }

    void unserialize(const uint8_t* type_reader) {
        uint32_t size;
        memcpy(&size, type_reader, sizeof(uint32_t));
        type_reader += sizeof(uint32_t);
        for (uint32_t i = 0; i < size; ++i) {
            T key;
            uint32_t count;
            memcpy(&key, type_reader, sizeof(T));
            type_reader += sizeof(T);
            memcpy(&count, type_reader, sizeof(uint32_t));
            type_reader += sizeof(uint32_t);
            _counts.emplace(std::make_pair(key, count));
        }
    }

    double get_percentile(std::vector<std::pair<T, uint32_t>>& counts, double position) const {
        long lower = long(std::floor(position));
        long higher = long(std::ceil(position));

        auto iter = counts.begin();
        for (; iter != counts.end() && iter->second < lower + 1; ++iter)
            ;

        T lower_key = iter->first;
        if (higher == lower) {
            return lower_key;
        }

        if (iter->second < higher + 1) {
            iter++;
        }

        T higher_key = iter->first;
        if (lower_key == higher_key) {
            return lower_key;
        }

        return (higher - position) * lower_key + (position - lower) * higher_key;
    }

    double terminate(double quantile) const {
        if (_counts.empty()) {
            // Although set null here, but the value is 0.0 and the call method just
            // get val in aggregate_function_percentile_approx.h
            return 0.0;
        }

        std::vector<std::pair<T, uint32_t>> elems(_counts.begin(), _counts.end());
        sort(elems.begin(), elems.end(),
             [](const std::pair<T, uint32_t> l, const std::pair<T, uint32_t> r) {
                 return l.first < r.first;
             });

        long total = 0;
        for (auto& cell : elems) {
            total += cell.second;
            cell.second = total;
        }

        long max_position = total - 1;
        double position = max_position * quantile;
        return get_percentile(elems, position);
    }

private:
    std::unordered_map<T, uint32_t> _counts;
};

} // namespace doris
