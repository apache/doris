// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

#ifndef BDG_PALO_BE_SRC_COMMON_UITL_BITMAP_H
#define BDG_PALO_BE_SRC_COMMON_UITL_BITMAP_H

#include "util/bit_util.h"

namespace palo {

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

}

#endif

