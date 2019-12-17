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

#ifndef DORIS_BE_SRC_COMMON_UITL_BITMAP_H
#define DORIS_BE_SRC_COMMON_UITL_BITMAP_H

#include <cstdint>
#include "util/bit_util.h"
#include "util/coding.h"
#include "gutil/strings/fastmem.h"
#include <roaring/roaring.hh>

namespace doris {

// Return the number of bytes necessary to store the given number of bits.
inline size_t BitmapSize(size_t num_bits) {
    return (num_bits + 7) / 8;
}

// Set the given bit.
inline void BitmapSet(uint8_t *bitmap, size_t idx) {
    bitmap[idx >> 3] |= 1 << (idx & 7);
}

// Switch the given bit to the specified value.
inline void BitmapChange(uint8_t *bitmap, size_t idx, bool value) {
    bitmap[idx >> 3] = (bitmap[idx >> 3] & ~(1 << (idx & 7))) | ((!!value) << (idx & 7));
}

// Clear the given bit.
inline void BitmapClear(uint8_t *bitmap, size_t idx) {
    bitmap[idx >> 3] &= ~(1 << (idx & 7));
}

// Test/get the given bit.
inline bool BitmapTest(const uint8_t *bitmap, size_t idx) {
    return bitmap[idx >> 3] & (1 << (idx & 7));
}

// Merge the two bitmaps using bitwise or. Both bitmaps should have at least
// n_bits valid bits.
inline void BitmapMergeOr(uint8_t *dst, const uint8_t *src, size_t n_bits) {
    size_t n_bytes = BitmapSize(n_bits);
    for (size_t i = 0; i < n_bytes; i++) {
        *dst++ |= *src++;
    }
}

// Set bits from offset to (offset + num_bits) to the specified value
void BitmapChangeBits(uint8_t *bitmap, size_t offset, size_t num_bits, bool value);

// Find the first bit of the specified value, starting from the specified offset.
bool BitmapFindFirst(const uint8_t *bitmap, size_t offset, size_t bitmap_size,
                     bool value, size_t *idx);

// Find the first set bit in the bitmap, at the specified offset.
inline bool BitmapFindFirstSet(const uint8_t *bitmap, size_t offset,
                               size_t bitmap_size, size_t *idx) {
    return BitmapFindFirst(bitmap, offset, bitmap_size, true, idx);
}

// Find the first zero bit in the bitmap, at the specified offset.
inline bool BitmapFindFirstZero(const uint8_t *bitmap, size_t offset,
                                size_t bitmap_size, size_t *idx) {
    return BitmapFindFirst(bitmap, offset, bitmap_size, false, idx);
}

// Returns true if the bitmap contains only ones.
inline bool BitMapIsAllSet(const uint8_t *bitmap, size_t offset, size_t bitmap_size) {
    DCHECK_LT(offset, bitmap_size);
    size_t idx;
    return !BitmapFindFirstZero(bitmap, offset, bitmap_size, &idx);
}

// Returns true if the bitmap contains only zeros.
inline bool BitmapIsAllZero(const uint8_t *bitmap, size_t offset, size_t bitmap_size) {
    DCHECK_LT(offset, bitmap_size);
    size_t idx;
    return !BitmapFindFirstSet(bitmap, offset, bitmap_size, &idx);
}

// Returns true if the two bitmaps are equal.
//
// It is assumed that both bitmaps have 'bitmap_size' number of bits.
inline bool BitmapEquals(const uint8_t* bm1, const uint8_t* bm2, size_t bitmap_size) {
    // Use memeq() to check all of the full bytes.
    size_t num_full_bytes = bitmap_size >> 3;
    if (!strings::memeq(bm1, bm2, num_full_bytes)) {
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
std::string BitmapToString(const uint8_t *bitmap, size_t num_bits);

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
        : offset_(0), num_bits_(num_bits), map_(map)
    {}

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

    bool done() const {
        return (num_bits_ - offset_) == 0;
    }

    void SeekTo(size_t bit) {
        DCHECK_LE(bit, num_bits_);
        offset_ = bit;
    }

    size_t Next(bool* value, size_t max_run) {
        return NextWithLimit(value, std::min(num_bits_, max_run + offset_));
    }

    size_t Next(bool* value) {
        return NextWithLimit(value, num_bits_);
    }

private:

    size_t NextWithLimit(bool* value, size_t limit) {
        size_t len = limit - offset_;
        if (PREDICT_FALSE(len == 0))
            return(0);

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
    const uint8_t *map_;
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

  void SetAllBits(bool b) {
    memset(&buffer_[0], 255 * b, buffer_.size() * sizeof(uint64_t));
  }

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

// the wrapper class for RoaringBitmap
// todo(kks): improve for low cardinality set
class RoaringBitmap {
public:
    RoaringBitmap() : _type(EMPTY) {}

    explicit RoaringBitmap(uint32_t value): _int_value(value), _type(SINGLE){}

    // the src is the serialized bitmap data, the type could be EMPTY, SINGLE or BITMAP
    explicit RoaringBitmap(const char* src) {
        _type = (BitmapDataType)src[0];
        DCHECK(_type >= 0 && _type <= 2);
        switch (_type) {
            case EMPTY:
                break;
            case SINGLE:
                _int_value = decode_fixed32_le(reinterpret_cast<const uint8_t *>(src + 1));
                break;
            case BITMAP:
                _roaring = Roaring::read(src + 1);
        }
    }

    void update(const uint32_t value) {
        switch (_type) {
            case EMPTY:
                _int_value = value;
                _type = SINGLE;
                break;
            case SINGLE:
                _roaring.add(_int_value);
                _roaring.add(value);
                _type = BITMAP;
                break;
            case BITMAP:
                _roaring.add(value);
        }
    }

    // specialty improve for empty bitmap and single int
    // roaring bitmap add(int) is faster than merge(another bitmap)
    // the _type maybe change:
    // EMPTY  -> SINGLE
    // EMPTY  -> BITMAP
    // SINGLE -> BITMAP
    void merge(const RoaringBitmap& bitmap) {
        switch(bitmap._type) {
            case EMPTY:
                return;
            case SINGLE:
                update(bitmap._int_value);
                return;
            case BITMAP:
                switch (_type) {
                    case EMPTY:
                        _roaring = bitmap._roaring;
                        _type = BITMAP;
                        break;
                    case SINGLE:
                        _roaring = bitmap._roaring;
                        _roaring.add(_int_value);
                        _type = BITMAP;
                        break;
                    case BITMAP:
                        _roaring |= bitmap._roaring;
                }
                return;
        }
    }

    // the _type maybe change:
    // EMPTY  -> EMPTY
    // SINGLE -> EMPTY, SINGLE
    // BITMAP -> EMPTY, SINGLE, BITMAP
    void intersect(const RoaringBitmap& bitmap) {
        switch(bitmap._type) {
            case EMPTY:
                _type = EMPTY;
                return;
            case SINGLE:
                switch (_type) {
                    case EMPTY:
                        break;
                    case SINGLE:
                        if (_int_value != bitmap._int_value) {
                            _type = EMPTY;
                        }
                        break;
                    case BITMAP:
                        if (!_roaring.contains(bitmap._int_value)) {
                            _type = EMPTY;
                        } else {
                            _type = SINGLE;
                            _int_value = bitmap._int_value;
                        }
                }
                return;
            case BITMAP:
                switch (_type) {
                    case EMPTY:
                        break;
                    case SINGLE:
                        if (!bitmap._roaring.contains(_int_value)) {
                            _type = EMPTY;
                        }
                        break;
                    case BITMAP:
                        _roaring &= bitmap._roaring;
                }
                return;
        }
    }

    int64_t cardinality() const {
        switch (_type) {
            case EMPTY:
                return 0;
            case SINGLE:
                return 1;
            case BITMAP:
                return _roaring.cardinality();
        }
        return 0;
    }

    size_t size() {
        switch (_type) {
            case EMPTY:
                return 1;
            case SINGLE:
                return sizeof(uint32_t) + 1;
            case BITMAP:
                _roaring.runOptimize();
                return _roaring.getSizeInBytes() + 1;
        }
        return 1;
    }

    //must call size() first
    void serialize(char* dest) {
        dest[0] = _type;
        switch (_type) {
            case EMPTY:
                break;
            case SINGLE:
                encode_fixed32_le(reinterpret_cast<uint8_t *>(dest + 1), _int_value);
                break;
            case BITMAP:
                _roaring.write(dest + 1);
        }
    }

    std::string to_string() const {
        switch (_type) {
            case EMPTY:
                return {};
            case SINGLE:
                return std::to_string(_int_value);
            case BITMAP:
                return _roaring.toString();
        }
        return {};
    }

private:
    enum BitmapDataType {
        EMPTY = 0,
        SINGLE = 1, // int32
        BITMAP = 2
    };

    Roaring _roaring;
    uint32_t _int_value;
    BitmapDataType _type;
};

}

#endif

