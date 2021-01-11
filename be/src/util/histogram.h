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

#include <string>
#include <vector>
#include <map>
#include <atomic>

#include "common/logging.h"

namespace doris {

// Histogram data structure implementation:
//
// After construction, the 'value_index_map' will be set to:
// 
// BucketValue: |   1   |   2   | 2*1.5 |2*1.5^2|2*1.5^3|  ...  |2*1.5^n|  ...  |UINT64MAX|
// Index:       |   0   |   1   |   2   |   3   |   4   |  ...  |  n-1  |  ...  |   108   |
//
// The width of bucket is growing by 1.5 times and its initial values are 1 and 2.
// The input value will be add to the bucket which lower bound is just greater than
// input value. For example, input value A > 2*1.5^n and A <= 2*1.5^(n+1), A will be added
// to the latter bucket.
class HistogramBucketMapper {
public:
    HistogramBucketMapper();
    
    // converts a value to the bucket index.
    size_t index_for_value(const uint64_t& value) const;
    // number of buckets required.

    size_t bucket_count() const {
        return _bucket_values.size();
    }

    uint64_t last_value() const {
        return _max_bucket_value;
    }

    uint64_t first_value() const {
        return _min_bucket_value;
    }

    uint64_t bucket_limit(const size_t bucket_number) const {
        DCHECK(bucket_number < bucket_count());
        return _bucket_values[bucket_number];
    }

private:
    std::vector<uint64_t> _bucket_values;
    uint64_t _max_bucket_value;
    uint64_t _min_bucket_value;
    std::map<uint64_t, uint64_t> _value_index_map;
};

struct HistogramStat {
    HistogramStat();
    ~HistogramStat() {}

    HistogramStat(const HistogramStat&) = delete;
    HistogramStat& operator=(const HistogramStat&) = delete;

    void clear();
    bool is_empty() const;
    void add(const uint64_t& value);
    void merge(const HistogramStat& other);

    inline uint64_t min() const { return _min.load(std::memory_order_relaxed); }
    inline uint64_t max() const { return _max.load(std::memory_order_relaxed); }
    inline uint64_t num() const { return _num.load(std::memory_order_relaxed); }
    inline uint64_t sum() const { return _sum.load(std::memory_order_relaxed); }
    inline uint64_t sum_squares() const {
        return _sum_squares.load(std::memory_order_relaxed);
    }
    inline uint64_t bucket_at(size_t b) const {
        return _buckets[b].load(std::memory_order_relaxed);
    }

    double median() const;
    double percentile(double p) const;
    double average() const;
    double standard_deviation() const;
    std::string to_string() const;

    // To be able to use HistogramStat as thread local variable, it
    // cannot have dynamic allocated member. That's why we're
    // using manually values from BucketMapper
    std::atomic<uint64_t> _min;
    std::atomic<uint64_t> _max;
    std::atomic<uint64_t> _num;
    std::atomic<uint64_t> _sum;
    std::atomic<uint64_t> _sum_squares;
    std::atomic<uint64_t> _buckets[109]; // 109==BucketMapper::bucket_count()
    const uint64_t _num_buckets;
};

}  // namespace doris
