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

#ifndef DORIS_BE_SRC_UTIL_COUNTS_H_
#define DORIS_BE_SRC_UTIL_COUNTS_H_

#include <algorithm>
#include <cmath>
#include <unordered_map>
#include <vector>

#include "udf/udf.h"

namespace doris {

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

    void increment(int64_t key, uint32_t i) {
        auto item = _counts.find(key);
        if (item != _counts.end()) {
            item->second += i;
        } else {
            _counts.emplace(std::make_pair(key, i));
        }
    }

    uint32_t serialized_size() const {
        return sizeof(uint32_t) + sizeof(int64_t) * _counts.size() +
               sizeof(uint32_t) * _counts.size();
    }

    void serialize(uint8_t* writer) const {
        uint32_t size = _counts.size();
        memcpy(writer, &size, sizeof(uint32_t));
        writer += sizeof(uint32_t);
        for (auto& cell : _counts) {
            memcpy(writer, &cell.first, sizeof(int64_t));
            writer += sizeof(int64_t);
            memcpy(writer, &cell.second, sizeof(uint32_t));
            writer += sizeof(uint32_t);
        }
    }

    void unserialize(const uint8_t* type_reader) {
        uint32_t size;
        memcpy(&size, type_reader, sizeof(uint32_t));
        type_reader += sizeof(uint32_t);
        for (uint32_t i = 0; i < size; ++i) {
            int64_t key;
            uint32_t count;
            memcpy(&key, type_reader, sizeof(int64_t));
            type_reader += sizeof(int64_t);
            memcpy(&count, type_reader, sizeof(uint32_t));
            type_reader += sizeof(uint32_t);
            _counts.emplace(std::make_pair(key, count));
        }
    }

    double get_percentile(std::vector<std::pair<int64_t, uint32_t>>& counts, double position) const {
        long lower = std::floor(position);
        long higher = std::ceil(position);

        auto iter = counts.begin();
        for (; iter != counts.end() && iter->second < lower + 1; ++iter)
            ;

        int64_t lower_key = iter->first;
        if (higher == lower) {
            return lower_key;
        }

        if (iter->second < higher + 1) {
            iter++;
        }

        int64_t higher_key = iter->first;
        if (lower_key == higher_key) {
            return lower_key;
        }

        return (higher - position) * lower_key + (position - lower) * higher_key;
    }

    doris_udf::DoubleVal terminate(double quantile) const {
        if (_counts.empty()) {
            return doris_udf::DoubleVal::null();
        }

        std::vector<std::pair<int64_t, uint32_t>> elems(_counts.begin(), _counts.end());
        sort(elems.begin(), elems.end(),
             [](const std::pair<int64_t, uint32_t> l, const std::pair<int64_t, uint32_t> r) {
                 return l.first < r.first;
             });

        long total = 0;
        for (auto& cell : elems) {
            total += cell.second;
            cell.second = total;
        }

        long max_position = total - 1;
        double position = max_position * quantile;
        return doris_udf::DoubleVal(get_percentile(elems, position));
    }

private:
    std::unordered_map<int64_t, uint32_t> _counts;
};

} // namespace doris

#endif // DORIS_BE_SRC_UTIL_COUNTS_H_
