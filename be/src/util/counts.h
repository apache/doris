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

#include <pdqsort.h>

#include <algorithm>
#include <cmath>
#include <queue>

#include "udf/udf.h"
#include "vec/common/pod_array.h"
#include "vec/common/string_buffer.hpp"
#include "vec/io/io_helper.h"

namespace doris {

class OldCounts {
public:
    OldCounts() = default;

    inline void merge(const OldCounts* other) {
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

    double get_percentile(std::vector<std::pair<int64_t, uint32_t>>& counts,
                          double position) const {
        long lower = long(std::floor(position));
        long higher = long(std::ceil(position));

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

    double terminate(double quantile) const {
        if (_counts.empty()) {
            // Although set null here, but the value is 0.0 and the call method just
            // get val in aggregate_function_percentile_approx.h
            return 0.0;
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
        return get_percentile(elems, position);
    }

private:
    std::unordered_map<int64_t, uint32_t> _counts;
};
template <typename Ty>
class Counts {
public:
    Counts() = default;

    void merge(Counts* other) {
        if (other != nullptr && !other->_nums.empty()) {
            _sorted_nums_vec.emplace_back(std::move(other->_nums));
        }
    }

    void increment(Ty key, uint32_t i) {
        auto old_size = _nums.size();
        _nums.resize(_nums.size() + i);
        for (uint32_t j = 0; j < i; ++j) {
            _nums[old_size + j] = key;
        }
    }

    void serialize(vectorized::BufferWritable& buf) {
        if (!_nums.empty()) {
            pdqsort(_nums.begin(), _nums.end());
            size_t size = _nums.size();
            write_binary(size, buf);
            buf.write(reinterpret_cast<const char*>(_nums.data()), sizeof(Ty) * size);
        } else {
            // convert _sorted_nums_vec to _nums and do seiralize again
            _convert_sorted_num_vec_to_nums();
            serialize(buf);
        }
    }

    void unserialize(vectorized::BufferReadable& buf) {
        size_t size;
        read_binary(size, buf);
        _nums.resize(size);
        auto buff = buf.read(sizeof(Ty) * size);
        memcpy(_nums.data(), buff.data, buff.size);
    }

    double terminate(double quantile) {
        if (_sorted_nums_vec.size() <= 1) {
            if (_sorted_nums_vec.size() == 1) {
                _nums = std::move(_sorted_nums_vec[0]);
            }

            if (_nums.empty()) {
                // Although set null here, but the value is 0.0 and the call method just
                // get val in aggregate_function_percentile_approx.h
                return 0.0;
            }
            if (quantile == 1 || _nums.size() == 1) {
                return _nums.back();
            }
            if (UNLIKELY(!std::is_sorted(_nums.begin(), _nums.end()))) {
                pdqsort(_nums.begin(), _nums.end());
            }

            double u = (_nums.size() - 1) * quantile;
            auto index = static_cast<uint32_t>(u);
            return _nums[index] +
                   (u - static_cast<double>(index)) * (_nums[index + 1] - _nums[index]);
        } else {
            DCHECK(_nums.empty());
            size_t rows = 0;
            for (const auto& i : _sorted_nums_vec) {
                rows += i.size();
            }
            const bool reverse = quantile > 0.5 && rows > 2;
            double u = (rows - 1) * quantile;
            auto index = static_cast<uint32_t>(u);
            // if reverse, the step of target should start 0 like not reverse
            // so here rows need to minus index + 2
            // eg: rows = 10, index = 5
            // if not reverse, so the first number loc is 5, the second number loc is 6
            // if reverse, so the second number is 3, the first number is 4
            // 5 + 4 = 3 + 6 = 9 = rows - 1.
            // the rows must GE 2 beacuse `_sorted_nums_vec` size GE 2
            size_t target = reverse ? rows - index - 2 : index;
            if (quantile == 1) {
                target = 0;
            }
            auto [first_number, second_number] = _merge_sort_and_get_numbers(target, reverse);
            if (quantile == 1) {
                return second_number;
            }
            return first_number + (u - static_cast<double>(index)) * (second_number - first_number);
        }
    }

private:
    struct Node {
        Ty value;
        int array_index;
        int64_t element_index;

        std::strong_ordering operator<=>(const Node& other) const { return value <=> other.value; }
    };

    void _convert_sorted_num_vec_to_nums() {
        size_t rows = 0;
        for (const auto& i : _sorted_nums_vec) {
            rows += i.size();
        }
        _nums.resize(rows);
        size_t count = 0;

        std::priority_queue<Node, std::vector<Node>, std::greater<Node>> min_heap;
        for (int i = 0; i < _sorted_nums_vec.size(); ++i) {
            if (!_sorted_nums_vec[i].empty()) {
                min_heap.emplace(_sorted_nums_vec[i][0], i, 0);
            }
        }

        while (!min_heap.empty()) {
            Node node = min_heap.top();
            min_heap.pop();
            _nums[count++] = node.value;
            if (++node.element_index < _sorted_nums_vec[node.array_index].size()) {
                node.value = _sorted_nums_vec[node.array_index][node.element_index];
                min_heap.push(node);
            }
        }
        _sorted_nums_vec.clear();
    }

    std::pair<Ty, Ty> _merge_sort_and_get_numbers(int64_t target, bool reverse) {
        Ty first_number = 0, second_number = 0;
        size_t count = 0;
        if (reverse) {
            std::priority_queue<Node> max_heap;
            for (int i = 0; i < _sorted_nums_vec.size(); ++i) {
                if (!_sorted_nums_vec[i].empty()) {
                    max_heap.emplace(_sorted_nums_vec[i][_sorted_nums_vec[i].size() - 1], i,
                                     _sorted_nums_vec[i].size() - 1);
                }
            }

            while (!max_heap.empty()) {
                Node node = max_heap.top();
                max_heap.pop();
                if (count == target) {
                    second_number = node.value;
                } else if (count == target + 1) {
                    first_number = node.value;
                    break;
                }
                ++count;
                if (--node.element_index >= 0) {
                    node.value = _sorted_nums_vec[node.array_index][node.element_index];
                    max_heap.push(node);
                }
            }

        } else {
            std::priority_queue<Node, std::vector<Node>, std::greater<Node>> min_heap;
            for (int i = 0; i < _sorted_nums_vec.size(); ++i) {
                if (!_sorted_nums_vec[i].empty()) {
                    min_heap.emplace(_sorted_nums_vec[i][0], i, 0);
                }
            }

            while (!min_heap.empty()) {
                Node node = min_heap.top();
                min_heap.pop();
                if (count == target) {
                    first_number = node.value;
                } else if (count == target + 1) {
                    second_number = node.value;
                    break;
                }
                ++count;
                if (++node.element_index < _sorted_nums_vec[node.array_index].size()) {
                    node.value = _sorted_nums_vec[node.array_index][node.element_index];
                    min_heap.push(node);
                }
            }
        }

        return {first_number, second_number};
    }

    vectorized::PODArray<Ty> _nums;
    std::vector<vectorized::PODArray<Ty>> _sorted_nums_vec;
};

} // namespace doris
