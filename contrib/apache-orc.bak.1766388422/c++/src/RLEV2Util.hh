/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef ORC_RLEV2UTIL_HH
#define ORC_RLEV2UTIL_HH

#include "RLEv2.hh"

namespace orc {
  extern const uint8_t FBSToBitWidthMap[FixedBitSizes::SIZE];
  extern const uint8_t ClosestFixedBitsMap[65];
  extern const uint8_t ClosestAlignedFixedBitsMap[65];
  extern const uint8_t BitWidthToFBSMap[65];

  // The input n must be less than FixedBitSizes::SIZE.
  inline uint32_t decodeBitWidth(uint32_t n) {
    return FBSToBitWidthMap[n];
  }

  inline uint32_t getClosestFixedBits(uint32_t n) {
    if (n <= 64) {
      return ClosestFixedBitsMap[n];
    } else {
      return 64;
    }
  }

  inline uint32_t getClosestAlignedFixedBits(uint32_t n) {
    if (n <= 64) {
      return ClosestAlignedFixedBitsMap[n];
    } else {
      return 64;
    }
  }

  inline uint32_t encodeBitWidth(uint32_t n) {
    if (n <= 64) {
      return BitWidthToFBSMap[n];
    } else {
      return FixedBitSizes::SIXTYFOUR;
    }
  }

  inline uint32_t findClosestNumBits(int64_t value) {
    if (value < 0) {
      return getClosestFixedBits(64);
    }

    uint32_t count = 0;
    while (value != 0) {
      count++;
      value = value >> 1;
    }
    return getClosestFixedBits(count);
  }

  inline bool isSafeSubtract(int64_t left, int64_t right) {
    return ((left ^ right) >= 0) || ((left ^ (left - right)) >= 0);
  }

  inline uint32_t RleEncoderV2::getOpCode(EncodingType encoding) {
    return static_cast<uint32_t>(encoding << 6);
  }
}  // namespace orc

#endif  // ORC_RLEV2UTIL_HH
