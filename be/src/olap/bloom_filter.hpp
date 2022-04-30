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

#ifndef DORIS_BE_SRC_OLAP_COLUMN_FILE_BLOOM_FILTER_HPP
#define DORIS_BE_SRC_OLAP_COLUMN_FILE_BLOOM_FILTER_HPP

#include <math.h>

#include <string>
#include <sstream>

#include "olap/olap_define.h"
#include "olap/utils.h"
#include "util/hash_util.hpp"

namespace doris {

static const uint64_t DEFAULT_SEED = 104729;
static const uint64_t BLOOM_FILTER_NULL_HASHCODE = 2862933555777941757ULL;

struct BloomFilterIndexHeader {
    uint64_t block_count;
    BloomFilterIndexHeader() : block_count(0) {}
} __attribute__((packed));

// Bare metal bit set implementation. For performance reasons, this implementation does not
// check for index bounds nor expand the bit set if the specified index is greater than the size.
class BitSet {
public:
    BitSet() : _data(nullptr), _data_len(0) {}

    ~BitSet() { SAFE_DELETE_ARRAY(_data); }

    // Init BitSet with given bit_num, which will align up to uint64_t
    bool init(uint32_t bit_num) {
        if (bit_num <= 0) {
            return false;
        }

        _data_len = (bit_num + sizeof(uint64_t) * 8 - 1) / (sizeof(uint64_t) * 8);
        _data = new (std::nothrow) uint64_t[_data_len];
        if (_data == nullptr) {
            return false;
        }

        memset(_data, 0, _data_len * sizeof(uint64_t));
        return true;
    }

    // Init BitSet with given buffer
    bool init(uint64_t* data, uint32_t data_len) {
        _data = data;
        _data_len = data_len;
        return true;
    }

    // Set the bit specified by param, note that uint64_t type contains 2^6 bits
    void set(uint32_t index) { _data[index >> 6] |= 1L << (index % 64); }

    // Return true if the bit specified by param is set
    bool get(uint32_t index) const { return (_data[index >> 6] & (1L << (index % 64))) != 0; }

    // Merge with another BitSet by byte, return false when the length is not equal
    bool merge(const BitSet& set) {
        if (_data_len != set.data_len()) {
            return false;
        }

        for (uint32_t i = 0; i < _data_len; ++i) {
            _data[i] |= set.data()[i];
        }

        return true;
    }

    // Convert BitSet to string to convenient debug and test
    std::string to_string() const {
        uint32_t bit_num = _data_len * sizeof(uint64_t) * 8;
        std::string str(bit_num, '0');
        for (uint32_t i = 0; i < bit_num; ++i) {
            if ((_data[i >> 6] & (1L << i)) != 0) {
                str[i] = '1';
            }
        }

        return str;
    }

    uint64_t* data() const { return _data; }

    uint32_t data_len() const { return _data_len; }

    uint32_t bit_num() const { return _data_len * sizeof(uint64_t) * 8; }

    void clear() { memset(_data, 0, _data_len * sizeof(uint64_t)); }

    void reset() {
        _data = NULL;
        _data_len = 0;
    }

private:
    uint64_t* _data;
    uint32_t _data_len;
};

class BloomFilter {
public:
    BloomFilter() : _bit_num(0), _hash_function_num(0) {}
    ~BloomFilter() {}

    // Create BloomFilter with given entry num and fpp, which is used for loading data
    bool init(int64_t expected_entries, double fpp) {
        uint32_t bit_num = _optimal_bit_num(expected_entries, fpp);
        if (!_bit_set.init(bit_num)) {
            return false;
        }

        _bit_num = _bit_set.bit_num();
        _hash_function_num = _optimal_hash_function_num(expected_entries, _bit_num);
        return true;
    }

    // Create BloomFilter with given entry num and default fpp
    bool init(int64_t expected_entries) {
        return this->init(expected_entries, BLOOM_FILTER_DEFAULT_FPP);
    }

    // Init BloomFilter with given buffer, which is used for query
    bool init(uint64_t* data, uint32_t len, uint32_t hash_function_num) {
        _bit_num = sizeof(uint64_t) * 8 * len;
        _hash_function_num = hash_function_num;
        return _bit_set.init(data, len);
    }

    // Compute hash value of given buffer and add to BloomFilter
    void add_bytes(const char* buf, uint32_t len) {
        uint64_t hash = buf == nullptr ? BLOOM_FILTER_NULL_HASHCODE
                                       : HashUtil::hash64(buf, len, DEFAULT_SEED);
        add_hash(hash);
    }

    // Generate multiple hash value according to following rule:
    //     new_hash_value = hash_high_part + (i * hash_low_part)
    void add_hash(uint64_t hash) {
        uint32_t hash1 = (uint32_t)hash;
        uint32_t hash2 = (uint32_t)(hash >> 32);

        for (uint32_t i = 0; i < _hash_function_num; ++i) {
            uint64_t combine_hash = hash1 + hash2 * i;
            uint32_t index = combine_hash % _bit_num;
            _bit_set.set(index);
        }
    }

    // Compute hash value of given buffer and verify whether exist in BloomFilter
    bool test_bytes(const char* buf, uint32_t len) const {
        uint64_t hash = buf == nullptr ? BLOOM_FILTER_NULL_HASHCODE
                                       : HashUtil::hash64(buf, len, DEFAULT_SEED);
        return test_hash(hash);
    }

    // Verify whether hash value in BloomFilter
    bool test_hash(uint64_t hash) const {
        uint32_t hash1 = (uint32_t)hash;
        uint32_t hash2 = (uint32_t)(hash >> 32);

        for (uint32_t i = 0; i < _hash_function_num; ++i) {
            uint64_t combine_hash = hash1 + hash2 * i;
            uint32_t index = combine_hash % _bit_num;
            if (!_bit_set.get(index)) {
                return false;
            }
        }

        return true;
    }

    // Merge with another BloomFilter, return false when the length
    //     and hash function number is not equal
    bool merge(const BloomFilter& that) {
        if (_bit_num == that.bit_num() && _hash_function_num == that.hash_function_num()) {
            _bit_set.merge(that.bit_set());
            return true;
        }

        return false;
    }

    void clear() { _bit_set.clear(); }

    void reset() {
        _bit_num = 0;
        _hash_function_num = 0;
        _bit_set.reset();
    }

    uint32_t bit_num() const { return _bit_num; }

    uint32_t hash_function_num() const { return _hash_function_num; }

    const BitSet& bit_set() const { return _bit_set; }

    uint64_t* bit_set_data() const { return _bit_set.data(); }

    uint32_t bit_set_data_len() const { return _bit_set.data_len(); }

    // Convert BloomFilter to string to convenient debug and test
    std::string to_string() const {
        std::stringstream bf_stream;
        bf_stream << "bit_num:" << _bit_num << " hash_function_num:" << _hash_function_num
                  << " bit_set:" << _bit_set.to_string();
        return bf_stream.str();
    }

    // Get points which set by given buffer in the BitSet
    std::string get_bytes_points_string(const char* buf, uint32_t len) const {
        uint64_t hash = buf == nullptr ? BLOOM_FILTER_NULL_HASHCODE
                                       : HashUtil::hash64(buf, len, DEFAULT_SEED);
        uint32_t hash1 = (uint32_t)hash;
        uint32_t hash2 = (uint32_t)(hash >> 32);

        std::stringstream stream;
        for (uint32_t i = 0; i < _hash_function_num; ++i) {
            if (i != 0) {
                stream << "-";
            }

            uint32_t combine_hash = hash1 + hash2 * i;
            uint32_t index = combine_hash % _bit_num;
            stream << index;
        }

        return stream.str();
    }

private:
    // Compute the optimal bit number according to the following rule:
    //     m = -n * ln(fpp) / (ln(2) ^ 2)
    uint32_t _optimal_bit_num(int64_t n, double fpp) {
        return (uint32_t)(-n * log(fpp) / (log(2) * log(2)));
    }

    // Compute the optimal hash function number according to the following rule:
    //     k = round(m * ln(2) / n)
    uint32_t _optimal_hash_function_num(int64_t n, uint32_t m) {
        uint32_t k = (uint32_t)round(m * log(2) / n);
        return k > 1 ? k : 1;
    }

    BitSet _bit_set;
    uint32_t _bit_num;
    uint32_t _hash_function_num;
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_COLUMN_FILE_BLOOM_FILTER_HPP
