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

#include <cstdlib>

#include "RLEV2Util.hh"

#include "wrap/gtest-wrapper.h"
#include "wrap/orc-proto-wrapper.hh"

namespace orc {
  TEST(RLEV2Util, decodeBitWidth) {
    for (uint32_t n = 0; n < FixedBitSizes::SIZE; ++n) {
      uint32_t result = orc::decodeBitWidth(n);
      if (n <= FixedBitSizes::TWENTYFOUR) {
        EXPECT_EQ(n + 1, result);
      } else if (n == FixedBitSizes::TWENTYSIX) {
        EXPECT_EQ(26, result);
      } else if (n == FixedBitSizes::TWENTYEIGHT) {
        EXPECT_EQ(28, result);
      } else if (n == FixedBitSizes::THIRTY) {
        EXPECT_EQ(30, result);
      } else if (n == FixedBitSizes::THIRTYTWO) {
        EXPECT_EQ(32, result);
      } else if (n == FixedBitSizes::FORTY) {
        EXPECT_EQ(40, result);
      } else if (n == FixedBitSizes::FORTYEIGHT) {
        EXPECT_EQ(48, result);
      } else if (n == FixedBitSizes::FIFTYSIX) {
        EXPECT_EQ(56, result);
      } else if (n == FixedBitSizes::SIXTYFOUR) {
        EXPECT_EQ(64, result);
      }
    }
  }

  TEST(RLEV2Util, getClosestFixedBits) {
    for (uint32_t n = 0; n < 66; ++n) {
      uint32_t result = orc::getClosestFixedBits(n);
      if (n == 0) {
        EXPECT_EQ(1, result);
      } else if (n >= 1 && n <= 24) {
        EXPECT_EQ(n, result);
      } else if (n <= 26) {
        EXPECT_EQ(26, result);
      } else if (n <= 28) {
        EXPECT_EQ(28, result);
      } else if (n <= 30) {
        EXPECT_EQ(30, result);
      } else if (n <= 32) {
        EXPECT_EQ(32, result);
      } else if (n <= 40) {
        EXPECT_EQ(40, result);
      } else if (n <= 48) {
        EXPECT_EQ(48, result);
      } else if (n <= 56) {
        EXPECT_EQ(56, result);
      } else {
        EXPECT_EQ(64, result);
      }
    }
  }

  TEST(RLEV2Util, getClosestAlignedFixedBits) {
    for (uint32_t n = 0; n < 66; ++n) {
      uint32_t result = orc::getClosestAlignedFixedBits(n);
      if (n == 0 || n == 1) {
        EXPECT_EQ(1, result);
      } else if (n <= 2) {
        EXPECT_EQ(2, result);
      } else if (n <= 4) {
        EXPECT_EQ(4, result);
      } else if (n <= 8) {
        EXPECT_EQ(8, result);
      } else if (n <= 16) {
        EXPECT_EQ(16, result);
      } else if (n <= 24) {
        EXPECT_EQ(24, result);
      } else if (n <= 32) {
        EXPECT_EQ(32, result);
      } else if (n <= 40) {
        EXPECT_EQ(40, result);
      } else if (n <= 48) {
        EXPECT_EQ(48, result);
      } else if (n <= 56) {
        EXPECT_EQ(56, result);
      } else {
        EXPECT_EQ(64, result);
      }
    }
  }

  TEST(RLEV2Util, encodeBitWidth) {
    for (uint32_t i = 0; i < 65; ++i) {
      uint32_t result = orc::encodeBitWidth(i);
      uint32_t n = getClosestFixedBits(i);

      if (n >= 1 && n <= 24) {
        EXPECT_EQ(n - 1, result);
      } else if (n <= 26) {
        EXPECT_EQ(FixedBitSizes::TWENTYSIX, result);
      } else if (n <= 28) {
        EXPECT_EQ(FixedBitSizes::TWENTYEIGHT, result);
      } else if (n <= 30) {
        EXPECT_EQ(FixedBitSizes::THIRTY, result);
      } else if (n <= 32) {
        EXPECT_EQ(FixedBitSizes::THIRTYTWO, result);
      } else if (n <= 40) {
        EXPECT_EQ(FixedBitSizes::FORTY, result);
      } else if (n <= 48) {
        EXPECT_EQ(FixedBitSizes::FORTYEIGHT, result);
      } else if (n <= 56) {
        EXPECT_EQ(FixedBitSizes::FIFTYSIX, result);
      } else {
        EXPECT_EQ(FixedBitSizes::SIXTYFOUR, result);
      }
    }
  }

}  // namespace orc
