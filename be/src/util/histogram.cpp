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

#include "util/histogram.h"

#include <stdio.h>
#include <limits>
#include <cinttypes>
#include <cmath>

namespace doris {

HistogramBucketMapper::HistogramBucketMapper() {
    // If you change this, you also need to change
    // size of array buckets_ in HistogramStat
    _bucket_values = {1, 2};
    _value_index_map = {{1, 0}, {2, 1}};
    double bucket_val = static_cast<double>(_bucket_values.back());
    while ((bucket_val = 1.5 * bucket_val) <=
           static_cast<double>(std::numeric_limits<uint64_t>::max())) {
        _bucket_values.push_back(static_cast<uint64_t>(bucket_val));
        // Extracts two most significant digits to make histogram buckets more
        // human-readable. E.g., 172 becomes 170.
        uint64_t pow_of_ten = 1;
        while (_bucket_values.back() / 10 > 10) {
            _bucket_values.back() /= 10;
            pow_of_ten *= 10;
        }
        _bucket_values.back() *= pow_of_ten;
        _value_index_map[_bucket_values.back()] = _bucket_values.size() - 1;
    }
    _max_bucket_value = _bucket_values.back();
    _min_bucket_value = _bucket_values.front();
}

size_t HistogramBucketMapper::index_for_value(const uint64_t& value) const {
    if (value >= _max_bucket_value) {
        return _bucket_values.size() - 1;
    } else if (value >= _min_bucket_value) {
        std::map<uint64_t, uint64_t>::const_iterator lowerBound =
                _value_index_map.lower_bound(value);
        if (lowerBound != _value_index_map.end()) {
            return static_cast<size_t>(lowerBound->second);
        } else {
            return 0;
        }
    } else {
        return 0;
    }
}

namespace {
const HistogramBucketMapper bucket_mapper;
}

HistogramStat::HistogramStat() : _num_buckets(bucket_mapper.bucket_count()) {
    DCHECK(_num_buckets == sizeof(_buckets) / sizeof(*_buckets));
    clear();
}

void HistogramStat::clear() {
    _min.store(bucket_mapper.last_value(), std::memory_order_relaxed);
    _max.store(0, std::memory_order_relaxed);
    _num.store(0, std::memory_order_relaxed);
    _sum.store(0, std::memory_order_relaxed);
    _sum_squares.store(0, std::memory_order_relaxed);
    for (unsigned int b = 0; b < _num_buckets; b++) {
        _buckets[b].store(0, std::memory_order_relaxed);
    }
};

bool HistogramStat::is_empty() const {
    return num() == 0;
}

void HistogramStat::add(const uint64_t& value) {
    // This function is designed to be lock free, as it's in the critical path
    // of any operation. Each individual value is atomic and the order of updates
    // by concurrent threads is tolerable.
    const size_t index = bucket_mapper.index_for_value(value);
    DCHECK(index < _num_buckets);
    _buckets[index].store(_buckets[index].load(std::memory_order_relaxed) + 1,
                          std::memory_order_relaxed);

    uint64_t old_min = min();
    if (value < old_min) {
        _min.store(value, std::memory_order_relaxed);
    }

    uint64_t old_max = max();
    if (value > old_max) {
        _max.store(value, std::memory_order_relaxed);
    }

    _num.store(_num.load(std::memory_order_relaxed) + 1, std::memory_order_relaxed);
    _sum.store(_sum.load(std::memory_order_relaxed) + value, std::memory_order_relaxed);
    _sum_squares.store(_sum_squares.load(std::memory_order_relaxed) + value * value,
                       std::memory_order_relaxed);
}

void HistogramStat::merge(const HistogramStat& other) {
    // This function needs to be performned with the outer lock acquired
    // However, atomic operation on every member is still need, since Add()
    // requires no lock and value update can still happen concurrently
    uint64_t old_min = min();
    uint64_t other_min = other.min();
    while (other_min < old_min && !_min.compare_exchange_weak(old_min, other_min)) {
    }

    uint64_t old_max = max();
    uint64_t other_max = other.max();
    while (other_max > old_max && !_max.compare_exchange_weak(old_max, other_max)) {
    }

    _num.fetch_add(other.num(), std::memory_order_relaxed);
    _sum.fetch_add(other.sum(), std::memory_order_relaxed);
    _sum_squares.fetch_add(other.sum_squares(), std::memory_order_relaxed);
    for (unsigned int b = 0; b < _num_buckets; b++) {
        _buckets[b].fetch_add(other.bucket_at(b), std::memory_order_relaxed);
    }
}

double HistogramStat::median() const {
    return percentile(50.0);
}

double HistogramStat::percentile(double p) const {
    double threshold = num() * (p / 100.0);
    uint64_t cumulative_sum = 0;
    for (unsigned int b = 0; b < _num_buckets; b++) {
        uint64_t bucket_value = bucket_at(b);
        cumulative_sum += bucket_value;
        if (cumulative_sum >= threshold) {
            // Scale linearly within this bucket
            uint64_t left_point = (b == 0) ? 0 : bucket_mapper.bucket_limit(b - 1);
            uint64_t right_point = bucket_mapper.bucket_limit(b);
            uint64_t left_sum = cumulative_sum - bucket_value;
            uint64_t right_sum = cumulative_sum;
            double pos = 0;
            uint64_t right_left_diff = right_sum - left_sum;
            if (right_left_diff != 0) {
                pos = (threshold - left_sum) / right_left_diff;
            }
            double r = left_point + (right_point - left_point) * pos;
            uint64_t cur_min = min();
            uint64_t cur_max = max();
            if (r < cur_min) r = static_cast<double>(cur_min);
            if (r > cur_max) r = static_cast<double>(cur_max);
            return r;
        }
    }
    return static_cast<double>(max());
}

double HistogramStat::average() const {
    uint64_t cur_num = num();
    uint64_t cur_sum = sum();
    if (cur_num == 0) return 0;
    return static_cast<double>(cur_sum) / static_cast<double>(cur_num);
}

double HistogramStat::standard_deviation() const {
    uint64_t cur_num = num();
    uint64_t cur_sum = sum();
    uint64_t cur_sum_squares = sum_squares();
    if (cur_num == 0) return 0;
    double variance = static_cast<double>(cur_sum_squares * cur_num - cur_sum * cur_sum) /
                      static_cast<double>(cur_num * cur_num);
    return std::sqrt(variance);
}
std::string HistogramStat::to_string() const {
    uint64_t cur_num = num();
    std::string r;
    char buf[1650];
    snprintf(buf, sizeof(buf), "Count: %" PRIu64 " Average: %.4f  StdDev: %.2f\n", cur_num,
             average(), standard_deviation());
    r.append(buf);
    snprintf(buf, sizeof(buf), "Min: %" PRIu64 "  Median: %.4f  Max: %" PRIu64 "\n",
             (cur_num == 0 ? 0 : min()), median(), (cur_num == 0 ? 0 : max()));
    r.append(buf);
    snprintf(buf, sizeof(buf),
             "Percentiles: "
             "P50: %.2f P75: %.2f P99: %.2f P99.9: %.2f P99.99: %.2f\n",
             percentile(50), percentile(75), percentile(99), percentile(99.9), percentile(99.99));
    r.append(buf);
    r.append("------------------------------------------------------\n");
    if (cur_num == 0) return r; // all buckets are empty
    const double mult = 100.0 / cur_num;
    uint64_t cumulative_sum = 0;
    for (unsigned int b = 0; b < _num_buckets; b++) {
        uint64_t bucket_value = bucket_at(b);
        if (bucket_value <= 0.0) continue;
        cumulative_sum += bucket_value;
        snprintf(buf, sizeof(buf), "%c %7" PRIu64 ", %7" PRIu64 " ] %8" PRIu64 " %7.3f%% %7.3f%% ",
                 (b == 0) ? '[' : '(',
                 (b == 0) ? 0 : bucket_mapper.bucket_limit(b - 1), // left
                 bucket_mapper.bucket_limit(b),                    // right
                 bucket_value,                                     // count
                 (mult * bucket_value),                            // percentage
                 (mult * cumulative_sum));                         // cumulative percentage
        r.append(buf);

        // Add hash marks based on percentage; 20 marks for 100%.
        size_t marks = static_cast<size_t>(mult * bucket_value / 5 + 0.5);
        r.append(marks, '#');
        r.push_back('\n');
    }
    return r;
}

} // namespace doris
