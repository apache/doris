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

#include <memory>

#include "common/exception.h"
#include "olap/rowset/segment_v2/inverted_index/util/bit_set.h"

namespace doris::segment_v2::inverted_index {

class FixedBitSet : public BitSet<FixedBitSet> {
public:
    FixedBitSet(uint32_t num_bits) {
        _num_bits = num_bits;
        _bits.resize(bits2words(num_bits));
        _num_words = _bits.size();
    }

    bool get(uint32_t index) {
        assert(index < _num_bits);
        uint32_t i = index >> 6;
        uint64_t bit_mask = 1ULL << (index % 64);
        return (_bits[i] & bit_mask) != 0;
    }

    void set(uint32_t index) {
        assert(index < _num_bits);
        uint32_t word_num = index >> 6;
        uint64_t bit_mask = 1ULL << (index % 64);
        _bits[word_num] |= bit_mask;
    }

    void clear(uint32_t index) {
        assert(index < _num_bits);
        uint32_t word_num = index >> 6;
        uint64_t bit_mask = 1ULL << (index % 64);
        _bits[word_num] &= ~bit_mask;
    }

    void clear(uint32_t start_index, uint32_t end_index) {
        assert(start_index < _num_bits);
        assert(end_index <= _num_bits);
        if (end_index <= start_index) {
            return;
        }

        uint32_t start_word = start_index >> 6;
        uint32_t end_word = (end_index - 1) >> 6;

        uint64_t start_mask = ~((1ULL << (start_index % 64)) - 1);
        uint64_t end_mask = (1ULL << (end_index % 64)) - 1;

        start_mask = ~start_mask;
        end_mask = ~end_mask;

        if (start_word == end_word) {
            _bits[start_word] &= (start_mask & end_mask);
            return;
        }

        _bits[start_word] &= start_mask;
        for (uint32_t i = start_word + 1; i < end_word; ++i) {
            _bits[i] = 0;
        }
        _bits[end_word] &= end_mask;
    }

    void ensure_capacity(size_t num_bits) {
        if (num_bits >= _num_bits) {
            size_t num_words = bits2words(num_bits);
            if (num_words >= _bits.size()) {
                _bits.resize(num_words + 1, 0);
            }
        }
    }

    int32_t cardinality() {
        uint64_t tot = 0;
        for (uint64_t word : _bits) {
            tot += std::popcount(word);
            if (tot > static_cast<uint64_t>(std::numeric_limits<int32_t>::max())) {
                throw Exception(ErrorCode::INVALID_ARGUMENT, "Total bit count exceeds int range");
            }
        }
        return static_cast<int>(tot);
    }

    static uint32_t bits2words(uint32_t numBits) {
        if (numBits == 0) {
            return 0;
        }
        return ((numBits - 1) >> 6) + 1;
    }

private:
    std::vector<uint64_t> _bits;
    uint32_t _num_bits = 0;
    uint32_t _num_words = 0;
};

} // namespace doris::segment_v2::inverted_index