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

    void increment(Ty key) { _nums.push_back(key); }

    void increment_batch(const vectorized::PaddedPODArray<Ty>& keys) {
        _nums.insert(keys.begin(), keys.end());
    }

    void serialize(vectorized::BufferWritable& buf) {
        if (!_nums.empty()) {
            pdqsort(_nums.begin(), _nums.end());
            size_t size = _nums.size();
            buf.write_binary(size);
            buf.write(reinterpret_cast<const char*>(_nums.data()), sizeof(Ty) * size);
        } else {
            // convert _sorted_nums_vec to _nums and do seiralize again
            _convert_sorted_num_vec_to_nums();
            serialize(buf);
        }
    }

    void unserialize(vectorized::BufferReadable& buf) {
        size_t size;
        buf.read_binary(size);
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

            if (UNLIKELY(!std::is_sorted(_nums.begin(), _nums.end()))) {
                pdqsort(_nums.begin(), _nums.end());
            }

            if (quantile == 1 || _nums.size() == 1) {
                return _nums.back();
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

        auto operator<=>(const Node& other) const { return value <=> other.value; }
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
