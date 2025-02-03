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

#include "util/interval_histogram.h"

#include <algorithm>
#include <mutex>
#include <numeric>
#include <vector>

#include "gutil/integral_types.h"

namespace doris {

template <typename T>
IntervalHistogramStat<T>::IntervalHistogramStat(size_t N) : window(N) {}

template <typename T>
void IntervalHistogramStat<T>::add(T value) {
    std::unique_lock<std::shared_mutex> lock(mutex);
    if (window.full()) {
        window.pop_front();
    }
    window.push_back(value);
}

template <typename T>
T IntervalHistogramStat<T>::mean() {
    std::shared_lock<std::shared_mutex> lock(mutex);
    if (window.empty()) {
        return T();
    }
    T sum = std::accumulate(window.begin(), window.end(), T());
    return sum / window.size();
}

template <typename T>
T IntervalHistogramStat<T>::median() {
    std::shared_lock<std::shared_mutex> lock(mutex);
    if (window.empty()) {
        return T();
    }

    std::vector<T> sorted(window.begin(), window.end());
    std::sort(sorted.begin(), sorted.end());

    size_t mid = sorted.size() / 2;
    return sorted.size() % 2 == 0 ? (sorted[mid - 1] + sorted[mid]) / 2 : sorted[mid];
}

template <typename T>
T IntervalHistogramStat<T>::max() {
    std::shared_lock<std::shared_mutex> lock(mutex);
    return *std::max_element(window.begin(), window.end());
}

template <typename T>
T IntervalHistogramStat<T>::min() {
    std::shared_lock<std::shared_mutex> lock(mutex);
    return *std::min_element(window.begin(), window.end());
}

template class doris::IntervalHistogramStat<int64>;
template class doris::IntervalHistogramStat<int32>;

} // namespace doris
