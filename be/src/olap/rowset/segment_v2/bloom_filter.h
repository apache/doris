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

#include <cstdint>
#include <functional>
#include <memory>

#include "olap/utils.h"
#include "gen_cpp/segment_v2.pb.h"
#include "util/murmur_hash3.h"
#include "common/status.h"

namespace doris {
namespace segment_v2 {

struct BloomFilterOptions {
    // false positive probablity
    double fpp = 0.05;
    HashStrategyPB strategy = HASH_MURMUR3_X64_64;
};

// Base class for bloom filter
// To support null value, the size of bloom filter is optimize bytes + 1.
// The last byte is for null value flag.
class BloomFilter {
public:
    // Default seed for the hash function. It comes from date +%s.
    static const uint32_t DEFAULT_SEED = 1575457558;

    // Minimum Bloom filter size, set to the size of a tiny Bloom filter block
    static const uint32_t MINIMUM_BYTES = 32;

    // Maximum Bloom filter size, set it to half of max segment file size
    static const uint32_t MAXIMUM_BYTES = 128 * 1024 * 1024;

    // Factory function for BloomFilter
    static Status create(BloomFilterAlgorithmPB algorithm, std::unique_ptr<BloomFilter>* bf);

    // Compute the optimal bit number according to the following rule:
    //     m = -n * ln(fpp) / (ln(2) ^ 2)
    // n: expected distinct record number
    // fpp: false positive probablity
    // the result will be power of 2
    static uint32_t optimal_bit_num(uint64_t n, double fpp);

    BloomFilter() = default;

    virtual ~BloomFilter() {
        delete[] _data;
    }

    // for write
    bool init(int num_bytes, HashStrategyPB strategy) {
        if (strategy == HASH_MURMUR3_X64_64) {
            _hash_func = murmur_hash3_x64_64;
        } else {
            return false;
        }
        _num_bytes = num_bytes;
        _size = num_bytes + 1;
        // reserve last byte for null flag
        _data = new char[_size];
        _has_null = (bool*)(_data + _num_bytes);
        *_has_null = false;
        return true;
    }

    // for read
    // use deep copy to aquire the data
    bool init(char* buf, uint32_t size, HashStrategyPB strategy) {
        DCHECK(size > 1);
        if (strategy == HASH_MURMUR3_X64_64) {
            _hash_func = murmur_hash3_x64_64;
        } else {
            return false;
        }
        if (size == 0) {
            return false;
        }
        _data = new char[size];
        memcpy(_data, buf, size);
        _size = size;
        _num_bytes = _size -1;
        _has_null = (bool*)(_data + _num_bytes);
        return true;
    }

    void reset() {
        // use SIMD to set memory to 0
        char* cur = _data;
        char* end = _data + _size;
        while (cur + 8 < end) {
            *(uint64*)cur = 0;
            cur += 8;
        }
        memset(cur, 0, end - cur);
    }

    uint64_t hash(char* buf, uint32_t size) const {
        uint64_t hash_code;
        _hash_func(buf, size, DEFAULT_SEED, &hash_code);
        return hash_code;
    }

    void add_bytes(char* buf, uint32_t size) {
        if (buf == nullptr) {
            *_has_null = true;
            return;
        }
        uint64_t code = hash(buf, size);
        add_hash(code);
    }

    bool test_bytes(char* buf, uint32_t size) const {
        if (buf == nullptr) {
            return *_has_null;
        }
        uint64_t code = hash(buf, size);
        return test_hash(code);
    }

    char* data() const { return _data; }

    uint32_t num_bytes() const { return _num_bytes; }

    uint32_t size() const { return _size; }

    void set_has_null(bool has_null) {
        *_has_null = has_null;
    }

    bool has_null() const {
        return *_has_null;
    }

    virtual void add_hash(uint64_t hash) = 0;
    virtual bool test_hash(uint64_t hash) const = 0;

protected:
    // bloom filter data
    // specially add one byte for null flag
    char* _data;
    // optimal bloom filter num bytes
    // it is calculated by optimal_bit_num() / 8
    uint32_t _num_bytes;
    // equal to _num_bytes + 1
    // last byte is for has_null flag
    uint32_t _size;
    // last byte's pointer in data for null flag
    bool* _has_null;

private:
    std::function<void(const void*, const int, const uint64_t, void*)> _hash_func;
};

} // namespace segment_v2
} // namespace doris
