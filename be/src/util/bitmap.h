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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/util/bitmap.h
// and modified by Doris

#pragma once

#include <glog/logging.h>
#include <stdint.h>
#include <string.h>

#include <algorithm>
#include <string>
#include <vector>

#include "gutil/port.h"
#include "util/bit_util.h"

namespace doris {

// Return the number of bytes necessary to store the given number of bits.
inline size_t BitmapSize(size_t num_bits) {
    return (num_bits + 7) / 8;
}

// Set the given bit.
inline void BitmapSet(uint8_t* bitmap, size_t idx) {
    bitmap[idx >> 3] |= 1 << (idx & 7);
}

// Switch the given bit to the specified value.
inline void BitmapChange(uint8_t* bitmap, size_t idx, bool value) {
    bitmap[idx >> 3] = (bitmap[idx >> 3] & ~(1 << (idx & 7))) | ((!!value) << (idx & 7));
}

// Clear the given bit.
inline void BitmapClear(uint8_t* bitmap, size_t idx) {
    bitmap[idx >> 3] &= ~(1 << (idx & 7));
}

// Test/get the given bit.
inline bool BitmapTest(const uint8_t* bitmap, size_t idx) {
    return bitmap[idx >> 3] & (1 << (idx & 7));
}

// Merge the two bitmaps using bitwise or. Both bitmaps should have at least
// n_bits valid bits.
inline void BitmapMergeOr(uint8_t* dst, const uint8_t* src, size_t n_bits) {
    size_t n_bytes = BitmapSize(n_bits);
    for (size_t i = 0; i < n_bytes; i++) {
        *dst++ |= *src++;
    }
}

// Set bits from offset to (offset + num_bits) to the specified value
void BitmapChangeBits(uint8_t* bitmap, size_t offset, size_t num_bits, bool value);

// Find the first bit of the specified value, starting from the specified offset.
bool BitmapFindFirst(const uint8_t* bitmap, size_t offset, size_t bitmap_size, bool value,
                     size_t* idx);

// Find the first set bit in the bitmap, at the specified offset.
inline bool BitmapFindFirstSet(const uint8_t* bitmap, size_t offset, size_t bitmap_size,
                               size_t* idx) {
    return BitmapFindFirst(bitmap, offset, bitmap_size, true, idx);
}

// Find the first zero bit in the bitmap, at the specified offset.
inline bool BitmapFindFirstZero(const uint8_t* bitmap, size_t offset, size_t bitmap_size,
                                size_t* idx) {
    return BitmapFindFirst(bitmap, offset, bitmap_size, false, idx);
}

// Returns true if the bitmap contains only ones.
inline bool BitMapIsAllSet(const uint8_t* bitmap, size_t offset, size_t bitmap_size) {
    DCHECK_LT(offset, bitmap_size);
    size_t idx;
    return !BitmapFindFirstZero(bitmap, offset, bitmap_size, &idx);
}

// Returns true if the bitmap contains only zeros.
inline bool BitmapIsAllZero(const uint8_t* bitmap, size_t offset, size_t bitmap_size) {
    DCHECK_LT(offset, bitmap_size);
    size_t idx;
    return !BitmapFindFirstSet(bitmap, offset, bitmap_size, &idx);
}

// Returns true if the two bitmaps are equal.
//
// It is assumed that both bitmaps have 'bitmap_size' number of bits.
inline bool BitmapEquals(const uint8_t* bm1, const uint8_t* bm2, size_t bitmap_size) {
    size_t num_full_bytes = bitmap_size >> 3;
    if (memcmp(bm1, bm2, num_full_bytes)) {
        return false;
    }

    // Check any remaining bits in one extra operation.
    size_t num_remaining_bits = bitmap_size - (num_full_bytes << 3);
    if (num_remaining_bits == 0) {
        return true;
    }
    DCHECK_LT(num_remaining_bits, 8);
    uint8_t mask = (1 << num_remaining_bits) - 1;
    return (bm1[num_full_bytes] & mask) == (bm2[num_full_bytes] & mask);
}

// This function will print the bitmap content in a format like the following:
// eg: 0001110000100010110011001100001100110011
// output:
//      0000: 00011100 00100010 11001100 11000011
//      0016: 00110011
std::string BitmapToString(const uint8_t* bitmap, size_t num_bits);

// Iterator which yields ranges of set and unset bits.
// Example usage:
//   bool value;
//   size_t size;
//   BitmapIterator iter(bitmap, n_bits);
//   while ((size = iter.Next(&value))) {
//      printf("bitmap block len=%lu value=%d\n", size, value);
//   }
class BitmapIterator {
public:
    BitmapIterator(const uint8_t* map, size_t num_bits)
            : offset_(0), num_bits_(num_bits), map_(map) {}

    void Reset(const uint8_t* map, size_t num_bits) {
        offset_ = 0;
        num_bits_ = num_bits;
        map_ = map;
    }

    void Reset() {
        offset_ = 0;
        num_bits_ = 0;
        map_ = nullptr;
    }

    bool done() const { return (num_bits_ - offset_) == 0; }

    void SeekTo(size_t bit) {
        DCHECK_LE(bit, num_bits_);
        offset_ = bit;
    }

    size_t Next(bool* value, size_t max_run) {
        return NextWithLimit(value, std::min(num_bits_, max_run + offset_));
    }

    size_t Next(bool* value) { return NextWithLimit(value, num_bits_); }

private:
    size_t NextWithLimit(bool* value, size_t limit) {
        size_t len = limit - offset_;
        if (PREDICT_FALSE(len == 0)) return (0);

        *value = BitmapTest(map_, offset_);

        size_t index;
        if (BitmapFindFirst(map_, offset_, limit, !(*value), &index)) {
            len = index - offset_;
        } else {
            index = limit;
        }

        offset_ = index;
        return len;
    }

private:
    size_t offset_;
    size_t num_bits_;
    const uint8_t* map_ = nullptr;
};

/// Bitmap vector utility class.
/// TODO: investigate perf.
///  - Precomputed bitmap
///  - Explicit Set/Unset() apis
///  - Bigger words
///  - size bitmap to Mersenne prime.
class Bitmap {
public:
    Bitmap(int64_t num_bits) {
        DCHECK_GE(num_bits, 0);
        buffer_.resize(BitUtil::round_up_numi_64(num_bits));
        num_bits_ = num_bits;
    }

    /// Resize bitmap and set all bits to zero.
    void Reset(int64_t num_bits) {
        DCHECK_GE(num_bits, 0);
        buffer_.resize(BitUtil::round_up_numi_64(num_bits));
        num_bits_ = num_bits;
        SetAllBits(false);
    }

    /// Compute memory usage of a bitmap, not including the Bitmap object itself.
    static int64_t MemUsage(int64_t num_bits) {
        DCHECK_GE(num_bits, 0);
        return BitUtil::round_up_numi_64(num_bits) * sizeof(int64_t);
    }

    /// Compute memory usage of this bitmap, not including the Bitmap object itself.
    int64_t MemUsage() const { return MemUsage(num_bits_); }

    /// Sets the bit at 'bit_index' to v.
    void Set(int64_t bit_index, bool v) {
        int64_t word_index = bit_index >> NUM_OFFSET_BITS;
        bit_index &= BIT_INDEX_MASK;
        DCHECK_LT(word_index, buffer_.size());
        if (v) {
            buffer_[word_index] |= (1LL << bit_index);
        } else {
            buffer_[word_index] &= ~(1LL << bit_index);
        }
    }

    /// Returns true if the bit at 'bit_index' is set.
    bool Get(int64_t bit_index) const {
        int64_t word_index = bit_index >> NUM_OFFSET_BITS;
        bit_index &= BIT_INDEX_MASK;
        DCHECK_LT(word_index, buffer_.size());
        return (buffer_[word_index] & (1LL << bit_index)) != 0;
    }

    void SetAllBits(bool b) { memset(&buffer_[0], 255 * b, buffer_.size() * sizeof(uint64_t)); }

    int64_t num_bits() const { return num_bits_; }

    /// If 'print_bits' prints 0/1 per bit, otherwise it prints the int64_t value.
    std::string DebugString(bool print_bits) const;

private:
    std::vector<uint64_t> buffer_;
    int64_t num_bits_;

    /// Used for bit shifting and masking for the word and offset calculation.
    static const int64_t NUM_OFFSET_BITS = 6;
    static const int64_t BIT_INDEX_MASK = 63;
};

} // namespace doris
