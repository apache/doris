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
// This file is inpired from
// https://github.com/Alluxio/alluxio/blob/main/dora/core/client/
// fs/src/main/java/alluxio/client/file/cache/MultipleBloomShadowCacheManager.java

#pragma once

#include "io/cache/shadow_cache.h"
#include "olap/bloom_filter.hpp"

namespace doris {

class BloomFilter;

namespace io {

struct MBFShadowCacheOption {
    int num_bf;
    int64_t shadow_cache_bytes_in_memory;
};

class MultiBloomFilterShadowCache : public ShadowCache {
public:
    MultiBloomFilterShadowCache(const MBFShadowCacheOption& opt);

    ~MultiBloomFilterShadowCache() {}

    bool put(const std::string& key, int64_t size) override;

    int64_t get(const std::string& key, int64_t bytes_read) override;

    void aging() override;

    void update_working_set_size() override;

    void stop_update() override;

    int64_t get_shadow_cache_key_num() override;

    int64_t get_shadow_cache_bytes() override;

    int64_t get_shadow_cache_read() override;

    int64_t get_shadow_cache_hit() override;

    int64_t get_shadow_cache_bytes_read() override;

    int64_t get_shadow_cache_bytes_hit() override;

    double get_false_positive_ratio() override;

    std::string get_info();

private:
    void _update_bf_and_working_set(const std::string& key, int size);
    void _update_avg_page_size();

private:
    static double FALSE_POSITIVE_RATIO;
    // Totla number of bloom filters
    int _num_bf;
    // The expected max number of elements in bloom filter
    // Calculated by memory size and expected false positive ratio
    int64_t _bf_expected_insertions;
    // bloom filters
    std::vector<std::unique_ptr<doris::BloomFilter>> _bf_vec;
    // Each elment corresponds to a bloom filter in _bf_vec,
    // Record the number of insert operation of that bloom filter.
    std::vector<int64_t> _bf_insert_vec;
    // Each elment corresponds to a bloom filter in _bf_vec,
    // Record the bytes put into that bloom filter.
    std::vector<int64_t> _bf_bytes_vec;

    // The index of _bf_vec of current working bloom filter.
    int _cur_bf_idx = 0;
    // The current working bloom filter;
    std::unique_ptr<doris::BloomFilter> _cur_bf;

    // Number of keys saved in all bloom filters.
    int64_t _cache_key_num = 0;
    // The average bytes size of each cache entry,
    double _avg_cache_size = 0.0;
    // Bytes saved in all bloom filters.
    // calculated by (_cache_key_num * _avg_cache_size)
    int64_t _cache_bytes_num = 0;

    // Total num of read operations of this cache
    int64_t _num_read = 0;
    // Totla num of hit read operation of this cache.
    int64_t _num_hit = 0;
    // Total num of bytes try to read from this cache.
    int64_t _num_bytes_read = 0;
    // Total num of bytes hit read from this cache.
    int64_t _num_bytes_hit = 0;
};

} // namespace io
} // namespace doris
