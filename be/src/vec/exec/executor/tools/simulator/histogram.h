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
#include <functional>
#include <iomanip>
#include <iostream>
#include <limits>
#include <list>
#include <map>
#include <numeric>
#include <sstream>
#include <type_traits>
#include <vector>

namespace doris {
namespace vectorized {

class HistogramUtils {
public:
    static std::string format_nanos(int64_t nanos);
};

template <typename K>
class Histogram {
private:
    std::vector<K> _buckets;
    bool _discrete;

public:
    Histogram(const std::vector<K>& buckets, bool discrete)
            : _buckets(buckets), _discrete(discrete) {
        std::sort(_buckets.begin(), _buckets.end());
    }

    static Histogram<K> from_discrete(const std::vector<K>& buckets) {
        return Histogram<K>(buckets, true);
    }

    static Histogram<K> from_continuous(const std::vector<K>& buckets) {
        return Histogram<K>(buckets, false);
    }

    template <typename D, typename V, typename F, typename G>
    void print_distribution(std::vector<D>& data, std::function<K(D)> key_function,
                            std::function<V(D)> value_function, std::function<F(K)> key_formatter,
                            std::function<G(std::vector<V>&)> value_formatter) const {
        if (_buckets.empty()) {
            std::cout << "No buckets" << std::endl;
            return;
        }

        if (data.empty()) {
            std::cout << "No data" << std::endl;
            return;
        }

        std::map<int, std::vector<V>> bucket_data;
        for (size_t i = 0; i < _buckets.size(); i++) {
            bucket_data[i] = std::vector<V>();
        }

        for (const auto& datum : data) {
            K key = key_function(datum);
            V value = value_function(datum);

            for (size_t i = 0; i < _buckets.size(); i++) {
                if (compare_greater_equal(key, _buckets[i]) &&
                    (i == (_buckets.size() - 1) || compare_less(key, _buckets[i + 1]))) {
                    bucket_data[i].push_back(value);
                    break;
                }
            }
        }

        if (!_discrete) {
            for (size_t i = 0; i < bucket_data.size() - 1; i++) {
                printf("[%8s, %8s) : (%5zu values) %s\n",
                       to_string(key_formatter(_buckets[i])).c_str(),
                       to_string(key_formatter(_buckets[i + 1])).c_str(), bucket_data[i].size(),
                       to_string(value_formatter(bucket_data[i])).c_str());
            }
        } else {
            for (size_t i = 0; i < bucket_data.size(); i++) {
                printf("%19s : (%5zu values) %s\n", to_string(key_formatter(_buckets[i])).c_str(),
                       bucket_data[i].size(), to_string(value_formatter(bucket_data[i])).c_str());
            }
        }
    }

private:
    template <typename T>
    bool compare_greater_equal(const T& a, const T& b) const {
        return a >= b;
    }

    template <typename T>
    bool compare_less(const T& a, const T& b) const {
        return a < b;
    }

    template <typename T>
    std::string to_string(const T& value) const {
        return std::to_string(value);
    }

    std::string to_string(const std::string& value) const { return value; }

    std::string to_string(const char* value) const { return std::string(value); }
};

} // namespace vectorized
} // namespace doris