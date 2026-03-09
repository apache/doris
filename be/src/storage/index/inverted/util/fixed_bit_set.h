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

#include <cassert>
#include <cstdint>
#include <memory>

#include "common/exception.h"

namespace doris::segment_v2::inverted_index {
#include "common/compile_check_begin.h"

class FixedBitSet;
using FixedBitSetPtr = std::unique_ptr<FixedBitSet>;

class FixedBitSet {
public:
    FixedBitSet(int32_t num_bits) {
        assert(num_bits >= 0 && num_bits < std::numeric_limits<int32_t>::max());
        _num_bits = num_bits;
        _bits.resize(bits2words(_num_bits));
        _num_words = static_cast<int32_t>(_bits.size());
    }

    void clear() { std::fill(_bits.begin(), _bits.end(), 0ULL); }

    int32_t length() const { return _num_bits; }

    bool get(int32_t index) {
        assert(index < _num_bits);
        int32_t i = index >> 6;
        uint64_t bit_mask = 1ULL << (index % 64);
        return (_bits[i] & bit_mask) != 0;
    }

    void set(int32_t index) {
        assert(index < _num_bits);
        int32_t word_num = index >> 6;
        uint64_t bit_mask = 1ULL << (index % 64);
        _bits[word_num] |= bit_mask;
    }

    void clear(int32_t index) {
        assert(index < _num_bits);
        int32_t word_num = index >> 6;
        uint64_t bit_mask = 1ULL << (index % 64);
        _bits[word_num] &= ~bit_mask;
    }

    void clear(int32_t start_index, int32_t end_index) {
        assert(start_index >= 0 && start_index < _num_bits);
        assert(end_index >= 0 && end_index <= _num_bits);
        if (end_index <= start_index) {
            return;
        }

        int32_t start_word = start_index >> 6;
        int32_t end_word = (end_index - 1) >> 6;

        uint64_t start_mask = UINT64_MAX << (start_index % 64);
        uint64_t end_mask = UINT64_MAX >> ((-end_index) & 63);

        start_mask = ~start_mask;
        end_mask = ~end_mask;

        if (start_word == end_word) {
            _bits[start_word] &= (start_mask | end_mask);
            return;
        }

        _bits[start_word] &= start_mask;
        for (int32_t i = start_word + 1; i < end_word; ++i) {
            _bits[i] = 0;
        }
        _bits[end_word] &= end_mask;
    }

    void ensure_capacity(int32_t num_bits) {
        if (num_bits >= _num_bits) {
            int32_t num_words = bits2words(num_bits);
            if (num_words >= _bits.size()) {
                _bits.resize(num_words + 1, 0);
            }
            reset(static_cast<int32_t>(_bits.size()) << 6);
        }
    }

    int32_t cardinality() {
        uint64_t tot = 0;
        for (uint64_t word : _bits) {
            tot += std::popcount(word);
            if (tot > static_cast<uint64_t>(std::numeric_limits<int32_t>::max())) {
                throw Exception(ErrorCode::INVALID_ARGUMENT,
                                "Total bit count exceeds int32_t range");
            }
        }
        return static_cast<int32_t>(tot);
    }

    static int32_t bits2words(int32_t numBits) { return ((numBits - 1) >> 6) + 1; }

    bool intersects(const FixedBitSet& other) const {
        int32_t pos = std::min(_num_words, other._num_words);
        while (--pos >= 0) {
            if ((_bits[pos] & other._bits[pos]) != 0) {
                return true;
            }
        }
        return false;
    }

    void bit_set_or(const FixedBitSet& other) { bit_set_or(0, other._bits, other._num_words); }

    int32_t next_set_bit(int32_t index) { return next_set_bit_in_range(index, _num_bits); }

private:
    void reset(int32_t num_bits) {
        _num_words = bits2words(num_bits);
        if (_num_words > _bits.size()) {
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "The given long array is too small  to hold {} bits", num_bits);
        }
        _num_bits = num_bits;

        assert(verify_ghost_bits_clear());
    }

    bool verify_ghost_bits_clear() {
        for (size_t i = _num_words; i < _bits.size(); i++) {
            if (_bits[i] != 0) {
                return false;
            }
        }
        if ((_num_bits & 0x3f) == 0) {
            return true;
        }
        uint64_t mask = UINT64_MAX << (_num_bits % 64);
        return (_bits[_num_words - 1] & mask) == 0;
    }

    void bit_set_or(int32_t other_offset_words, const std::vector<uint64_t>& other_arr,
                    int32_t other_num_words) {
        assert(other_num_words + other_offset_words <= _num_words);
        int32_t pos = std::min(_num_words - other_offset_words, other_num_words);
        while (--pos >= 0) {
            _bits[pos + other_offset_words] |= other_arr[pos];
        }
    }

    int32_t next_set_bit_in_range(int32_t start, int32_t upperBound) {
        assert(start >= 0 && start < _num_bits);
        assert(start < upperBound);
        assert(upperBound <= _num_bits);

        int32_t i = start >> 6;
        uint64_t word = _bits[i] >> start;

        if (word != 0) {
            return start + std::countr_zero(word);
        }

        int32_t limit = (upperBound == _num_bits) ? _num_words : bits2words(upperBound);
        while (++i < limit) {
            word = _bits[i];
            if (word != 0) {
                return (i << 6) + std::countr_zero(word);
            }
        }

        return std::numeric_limits<int32_t>::max();
    }

    std::vector<uint64_t> _bits;
    int32_t _num_bits = 0;
    int32_t _num_words = 0;
};

#include "common/compile_check_end.h"
} // namespace doris::segment_v2::inverted_index