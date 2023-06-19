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

#include "io/cache/multi_bloom_filter_shadow_cache.h"

#include <cmath>

namespace doris::io {

double MultiBloomFilterShadowCache::FALSE_POSITIVE_RATIO = 0.03;

MultiBloomFilterShadowCache::MultiBloomFilterShadowCache(const MBFShadowCacheOption& opt) {
    _num_bf = opt.num_bf;
    // include the 1 extra working set bloom filter
    int64_t per_bf_size = opt.shadow_cache_bytes_in_memory / (_num_bf + 1);
    // assume 3% Guava default false positive ratio
    _bf_expected_insertions = (int64_t) ((-per_bf_size * 8 * std::log(2) * std::log(2)) / std::log(FALSE_POSITIVE_RATIO));

    for (int i = 0; i < _num_bf; ++i) {
        _bf_insert_vec.push_back(0L);    
        _bf_bytes_vec.push_back(0L);    
        doris::BloomFilter* bf = new doris::BloomFilter();
        bf->init(_bf_expected_insertions, FALSE_POSITIVE_RATIO);
        _bf_vec.emplace_back(bf);    
    }

    _cur_bf = std::make_unique<doris::BloomFilter>();
    _cur_bf->init(_bf_expected_insertions, FALSE_POSITIVE_RATIO);
}


bool MultiBloomFilterShadowCache::put(const std::string& key, int64_t size) {
    _update_bf_and_working_set(key, size);
    return true;
}

void MultiBloomFilterShadowCache::_update_bf_and_working_set(const std::string& key, int size) {
    int cur_idx = _cur_bf_idx;
    doris::BloomFilter* bf = _bf_vec[cur_idx].get();
    if (!bf->test_bytes(key.c_str(), key.length())) {
        bf->add_bytes(key.c_str(), key.length());
        _bf_insert_vec[cur_idx]++;
        _bf_bytes_vec[cur_idx] += size;
        _cur_bf->add_bytes(key.c_str(), key.length());
        update_working_set_size();
    }
}

int64_t MultiBloomFilterShadowCache::get(const std::string& key, int64_t bytes_read) {
    bool seen = false;
    for (int i = 0; i < _num_bf; ++i) {
      seen |= _bf_vec[i]->test_bytes(key.c_str(), key.length());
    }
    if (seen) {
        _num_hit++;
        _num_bytes_hit += bytes_read;
    }
    _num_read++;
    _num_bytes_read += bytes_read;
    return seen ? bytes_read : 0;
}

void MultiBloomFilterShadowCache::update_working_set_size() {
    _update_avg_page_size();
    _cache_key_num = _cur_bf->approximate_count();
    _cache_bytes_num = _cache_key_num * _avg_cache_size;
}

void MultiBloomFilterShadowCache::_update_avg_page_size() {
    int64_t num_insert = 0;
    int64_t total_bytes = 0;
    for (int i = 0; i < _num_bf; ++i) {
        num_insert += _bf_insert_vec[i];
        total_bytes += _bf_bytes_vec[i];
    }
    if (num_insert == 0) {
        _avg_cache_size = 0.0;
    } else {
        _avg_cache_size = (double) total_bytes / num_insert;
    }
}

void MultiBloomFilterShadowCache::aging() {
    _update_avg_page_size();
    _cur_bf_idx = (_cur_bf_idx + 1) % _num_bf; 
    _bf_vec[_cur_bf_idx] = std::make_unique<doris::BloomFilter>();
    _bf_vec[_cur_bf_idx]->init(_bf_expected_insertions, FALSE_POSITIVE_RATIO);
    
    _bf_insert_vec[_cur_bf_idx] = 0;
    _bf_bytes_vec[_cur_bf_idx] = 0;

    _cur_bf = std::make_unique<doris::BloomFilter>();
    _cur_bf->init(_bf_expected_insertions, FALSE_POSITIVE_RATIO);

    
    for (int i = 0; i < _num_bf; ++i) {
        _cur_bf->merge(*_bf_vec[i]);
    }
}

void MultiBloomFilterShadowCache::stop_update() {

}

int64_t MultiBloomFilterShadowCache::get_shadow_cache_key_num() {
    return _cache_key_num;
}

int64_t MultiBloomFilterShadowCache::get_shadow_cache_bytes() {
    return _cache_bytes_num;
}

int64_t MultiBloomFilterShadowCache::get_shadow_cache_read() {
    return _num_read;
}

int64_t MultiBloomFilterShadowCache::get_shadow_cache_hit() {
    return _num_hit;
}

int64_t MultiBloomFilterShadowCache::get_shadow_cache_bytes_read() {
    return _num_bytes_read;
}

int64_t MultiBloomFilterShadowCache::get_shadow_cache_bytes_hit() {
    return _num_bytes_hit;
}

double MultiBloomFilterShadowCache::get_false_positive_ratio() {
    return FALSE_POSITIVE_RATIO;
}

std::string MultiBloomFilterShadowCache::get_info() {
    std::stringstream ss;
    ss << "MultiBloomFilterShadowCache: [num bf: " << _num_bf
        << ", expected insertion: " << _bf_expected_insertions
        << ", current bf idx: " << _cur_bf_idx
        << ", num of keys(est): " << _cache_key_num
        << ", num of bytes: " << _cache_bytes_num
        << ", avg of cache entry: " << _avg_cache_size
        << ", num read: " << _num_read
        << ", num hit: " << _num_hit
        << ", read hit ratio: " << (double) _num_hit / (_num_read + 1)
        << ", num bytes read: " << _num_bytes_read
        << ", num bytes hit: " << _num_bytes_hit
        << ", bytes hit ratio: " << (double) _num_bytes_hit / (_num_bytes_read + 1)
        << "]";
    return ss.str();
}

} // namespace doris::io
