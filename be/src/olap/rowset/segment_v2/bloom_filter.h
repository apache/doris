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
#include <math.h>

#include "common/status.h"
#include "util/slice.h"
#include "roaring/roaring.hh"
#include "olap/olap_define.h"
#include "util/hash_util.hpp"

namespace doris {
namespace segment_v2 {

// Probe calculated from a given key. This caches the calculated
// hash values which are necessary for probing into a Bloom Filter,
// so that when many bloom filters have to be consulted for a given
// key, we only need to calculate the hashes once.
class BloomKeyProbe {
public:
    // Default constructor - this is only used to instantiate an object
    // and later init by calling init()
    BloomKeyProbe() {}

    // Construct a probe from the given key.
    //
    // NOTE: proper operation requires that the referenced memory remain
    // valid for the lifetime of this object.
    explicit BloomKeyProbe(Slice key)
        : _key(key) {
        _init();
    }

    void init(Slice key) {
        _key = key;
        _init();
    }

    const Slice& key() const { return _key; }

    // The initial hash value. See mix_hash() for usage example.
    uint32_t initial_hash() const {
        return _h_1;
    }

    // Mix the given hash function with the second calculated hash
    // value. A sequence of independent hashes can be calculated
    // by repeatedly calling mix_hash() on its previous result.
    uint32_t mix_hash(uint32_t h) const {
        return h + _h_2;
    }

private:
    void _init() {
        uint64_t hash = HashUtil::hash64(_key.data, _key.size, 104729);
        // Use the top and bottom halves of the 64-bit hash
        // as the two independent hash functions for mixing.
        _h_1 = static_cast<uint32_t>(hash);
        _h_2 = static_cast<uint32_t>(hash >> 32);
    }

private:
    Slice _key;

    // The two hashes.
    uint32_t _h_1;
    uint32_t _h_2;
};

// This class is used to build BloomFilter
// It uses Roaring Bitmap to store the underline bitmap
class BloomFilterBuilder {
public:
    // construct with expected entry num and false positive rate
    BloomFilterBuilder(uint32_t expected_num, double fp_rate) : _expected_num(expected_num),
            _fp_rate(fp_rate),
            _n_bits(_optimal_bit_num(_expected_num, _fp_rate)),
            _n_hashes(_optimal_hash_function_num(_expected_num, _n_bits)),
            _num_inserted(0) { }

    // add the given key to the bloom filter
    void add_key(const BloomKeyProbe& key);

    // get bloom filter size
    uint32_t get_bf_size() {
        return _bitmap.getSizeInBytes();
    }

    // write the bloom filter data to buf
    // the caller should make sure the size of buf is larger than getSizeInBytes()
    // return the real bytes used
    // Note: CRoaring lib has no api to get buf pointer,
    // so here has to allocate and copy memory. It is compromise for space.
    size_t write(char* buf) {
        // write a not portable format to save space
        return _bitmap.write(buf);
    }

    // clear all entries, and reset counter;
    void clear() {
        Roaring tmp;
        _bitmap.swap(tmp);
        _num_inserted = 0;
    }

    uint32_t expected_num() {
        return _expected_num;
    }

    size_t num_inserted() {
        return _num_inserted;
    }

    uint32_t hash_function_num() {
        return _n_hashes;
    }

    uint32_t bit_size() { return _n_bits; }

private:
    // Compute the optimal bit number from expected count according to the following rule:
    //     m = -n * ln(fpp) / (ln(2) ^ 2)
    uint32_t _optimal_bit_num(int64_t expected_num, double fpp) {
        LOG(INFO) << "expected_num:" << expected_num << ", fpp:" << fpp;
        return (uint32_t) (-expected_num * log(fpp) / (log(2) * log(2)));
    }

    // Compute the optimal hash function number from expected count and bitmap size
    // according to the following rule:
    //     k = round(m * ln(2) / n)
    uint32_t _optimal_hash_function_num(int64_t expected_num, uint32_t n_bytes) {
        uint32_t k = (uint32_t) round(n_bytes * log(2) / expected_num);
        LOG(INFO) << "n_bytes:" << n_bytes << ", n hashes:" << k;
        return k > 1 ? k : 1;
    }

private:
    // expected entries number
    uint32_t _expected_num;
    // false positive rate
    double _fp_rate;
    // bitmap bit size
    uint32_t _n_bits;
    // number of hash functions
    uint32_t _n_hashes;
    // number inserted
    size_t _num_inserted;
    Roaring _bitmap;
};

// BloomFilter
class BloomFilter {
public:
    BloomFilter(Slice data, uint32_t hash_function_num, uint32_t bit_size)
        : _data(data), _n_hashes(hash_function_num), _bit_size(bit_size) { }

    Status load();

    bool check_key(const BloomKeyProbe& key) const;

private:
    Slice _data;
    uint32_t _n_hashes;
    uint32_t _bit_size;
    Roaring _bitmap;
};

} // namespace segment_v2
} // namespace doris
