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

#include "common/status.h"
#include "gen_cpp/segment_v2.pb.h"
#include "gutil/strings/substitute.h"
#include "olap/utils.h"
#include "util/murmur_hash3.h"

namespace doris {
namespace segment_v2 {

struct BloomFilterOptions {
    // false positive probability
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

    BloomFilter() : _data(nullptr), _num_bytes(0), _size(0), _has_null(nullptr) {}

    virtual ~BloomFilter() { delete[] _data; }

    // for write
    Status init(uint64_t n, double fpp, HashStrategyPB strategy) {
        return this->init(optimal_bit_num(n, fpp) / 8, strategy);
    }

    Status init(uint64_t filter_size, HashStrategyPB strategy) {
        if (strategy == HASH_MURMUR3_X64_64) {
            _hash_func = murmur_hash3_x64_64;
        } else {
            return Status::InvalidArgument(strings::Substitute("invalid strategy:$0", strategy));
        }
        _num_bytes = filter_size;
        DCHECK((_num_bytes & (_num_bytes - 1)) == 0);
        _size = _num_bytes + 1;
        // reserve last byte for null flag
        _data = new char[_size];
        memset(_data, 0, _size);
        _has_null = (bool*)(_data + _num_bytes);
        *_has_null = false;
        return Status::OK();
    }

    // for read
    // use deep copy to acquire the data
    Status init(const char* buf, uint32_t size, HashStrategyPB strategy) {
        DCHECK(size > 1);
        if (strategy == HASH_MURMUR3_X64_64) {
            _hash_func = murmur_hash3_x64_64;
        } else {
            return Status::InvalidArgument(strings::Substitute("invalid strategy:$0", strategy));
        }
        if (size == 0) {
            return Status::InvalidArgument(strings::Substitute("invalid size:$0", size));
        }
        _data = new char[size];
        memcpy(_data, buf, size);
        _size = size;
        _num_bytes = _size - 1;
        DCHECK((_num_bytes & (_num_bytes - 1)) == 0);
        _has_null = (bool*)(_data + _num_bytes);
        return Status::OK();
    }

    void reset() { memset(_data, 0, _size); }

    uint64_t hash(const char* buf, uint32_t size) const {
        uint64_t hash_code;
        _hash_func(buf, size, DEFAULT_SEED, &hash_code);
        return hash_code;
    }

    void add_bytes(const char* buf, uint32_t size) {
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

    void set_has_null(bool has_null) { *_has_null = has_null; }

    bool has_null() const { return *_has_null; }

    virtual void add_hash(uint64_t hash) = 0;
    virtual bool test_hash(uint64_t hash) const = 0;

    Status merge(const BloomFilter* other) {
        DCHECK(other->size() == _size);
        for (uint32_t i = 0; i < other->size(); i++) {
            _data[i] |= other->_data[i];
        }
        return Status::OK();
    }

    // Compute the optimal bit number according to the following rule:
    //     m = -n * ln(fpp) / (ln(2) ^ 2)
    // n: expected distinct record number
    // fpp: false positive probability
    // the result will be power of 2
    static uint32_t optimal_bit_num(uint64_t n, double fpp);

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
