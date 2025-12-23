/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "RLEV2Util.hh"

namespace orc {

  // Map FBS enum to bit width value.
  const uint8_t FBSToBitWidthMap[FixedBitSizes::SIZE] = {1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11,
                                                         12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
                                                         23, 24, 26, 28, 30, 32, 40, 48, 56, 64};

  // Map bit length i to closest fixed bit width that can contain i bits.
  const uint8_t ClosestFixedBitsMap[65] = {
      1,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
      22, 23, 24, 26, 26, 28, 28, 30, 30, 32, 32, 40, 40, 40, 40, 40, 40, 40, 40, 48, 48, 48,
      48, 48, 48, 48, 48, 56, 56, 56, 56, 56, 56, 56, 56, 64, 64, 64, 64, 64, 64, 64, 64};

  // Map bit length i to closest aligned fixed bit width that can contain i bits.
  const uint8_t ClosestAlignedFixedBitsMap[65] = {
      1,  1,  2,  4,  4,  8,  8,  8,  8,  16, 16, 16, 16, 16, 16, 16, 16, 24, 24, 24, 24, 24,
      24, 24, 24, 32, 32, 32, 32, 32, 32, 32, 32, 40, 40, 40, 40, 40, 40, 40, 40, 48, 48, 48,
      48, 48, 48, 48, 48, 56, 56, 56, 56, 56, 56, 56, 56, 64, 64, 64, 64, 64, 64, 64, 64};

  // Map bit width to FBS enum.
  const uint8_t BitWidthToFBSMap[65] = {
      FixedBitSizes::ONE,         FixedBitSizes::ONE,         FixedBitSizes::TWO,
      FixedBitSizes::THREE,       FixedBitSizes::FOUR,        FixedBitSizes::FIVE,
      FixedBitSizes::SIX,         FixedBitSizes::SEVEN,       FixedBitSizes::EIGHT,
      FixedBitSizes::NINE,        FixedBitSizes::TEN,         FixedBitSizes::ELEVEN,
      FixedBitSizes::TWELVE,      FixedBitSizes::THIRTEEN,    FixedBitSizes::FOURTEEN,
      FixedBitSizes::FIFTEEN,     FixedBitSizes::SIXTEEN,     FixedBitSizes::SEVENTEEN,
      FixedBitSizes::EIGHTEEN,    FixedBitSizes::NINETEEN,    FixedBitSizes::TWENTY,
      FixedBitSizes::TWENTYONE,   FixedBitSizes::TWENTYTWO,   FixedBitSizes::TWENTYTHREE,
      FixedBitSizes::TWENTYFOUR,  FixedBitSizes::TWENTYSIX,   FixedBitSizes::TWENTYSIX,
      FixedBitSizes::TWENTYEIGHT, FixedBitSizes::TWENTYEIGHT, FixedBitSizes::THIRTY,
      FixedBitSizes::THIRTY,      FixedBitSizes::THIRTYTWO,   FixedBitSizes::THIRTYTWO,
      FixedBitSizes::FORTY,       FixedBitSizes::FORTY,       FixedBitSizes::FORTY,
      FixedBitSizes::FORTY,       FixedBitSizes::FORTY,       FixedBitSizes::FORTY,
      FixedBitSizes::FORTY,       FixedBitSizes::FORTY,       FixedBitSizes::FORTYEIGHT,
      FixedBitSizes::FORTYEIGHT,  FixedBitSizes::FORTYEIGHT,  FixedBitSizes::FORTYEIGHT,
      FixedBitSizes::FORTYEIGHT,  FixedBitSizes::FORTYEIGHT,  FixedBitSizes::FORTYEIGHT,
      FixedBitSizes::FORTYEIGHT,  FixedBitSizes::FIFTYSIX,    FixedBitSizes::FIFTYSIX,
      FixedBitSizes::FIFTYSIX,    FixedBitSizes::FIFTYSIX,    FixedBitSizes::FIFTYSIX,
      FixedBitSizes::FIFTYSIX,    FixedBitSizes::FIFTYSIX,    FixedBitSizes::FIFTYSIX,
      FixedBitSizes::SIXTYFOUR,   FixedBitSizes::SIXTYFOUR,   FixedBitSizes::SIXTYFOUR,
      FixedBitSizes::SIXTYFOUR,   FixedBitSizes::SIXTYFOUR,   FixedBitSizes::SIXTYFOUR,
      FixedBitSizes::SIXTYFOUR,   FixedBitSizes::SIXTYFOUR};
}  // namespace orc
