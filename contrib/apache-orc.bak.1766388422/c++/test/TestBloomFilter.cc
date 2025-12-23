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

#include "BloomFilter.hh"
#include "orc/OrcFile.hh"
#include "wrap/gtest-wrapper.h"

namespace orc {

  TEST(TestOrcBloomFilter, testBitSetEqual) {
    BitSet bitSet64_1(64), bitSet64_2(64), bitSet32(128);
    EXPECT_TRUE(bitSet64_1 == bitSet64_2);
    EXPECT_FALSE(bitSet64_1 == bitSet32);

    bitSet64_1.set(6U);
    bitSet64_1.set(16U);
    bitSet64_1.set(26U);
    bitSet64_2.set(6U);
    bitSet64_2.set(16U);
    bitSet64_2.set(26U);
    EXPECT_TRUE(bitSet64_1 == bitSet64_2);
    EXPECT_EQ(bitSet64_1.get(6U), bitSet64_2.get(6U));
    EXPECT_EQ(bitSet64_1.get(16U), bitSet64_2.get(16U));
    EXPECT_EQ(bitSet64_1.get(26U), bitSet64_2.get(26U));

    bitSet64_1.set(36U);
    bitSet64_2.set(46U);
    EXPECT_FALSE(bitSet64_1 == bitSet64_2);
    EXPECT_TRUE(bitSet64_1.get(36U));
    EXPECT_TRUE(bitSet64_2.get(46U));

    bitSet64_1.clear();
    bitSet64_2.clear();
    EXPECT_TRUE(bitSet64_1 == bitSet64_2);
  }

  // ported from Java ORC
  TEST(TestOrcBloomFilter, testSetGetBitSet) {
    BitSet bitset(128);

    // set every 9th bit for a rotating pattern
    for (uint64_t l = 0; l < 8; ++l) {
      bitset.set(l * 9);
    }

    // set every non-9th bit
    for (uint64_t l = 8; l < 16; ++l) {
      for (uint64_t b = 0; b < 8; ++b) {
        if (b != l - 8) {
          bitset.set(l * 8 + b);
        }
      }
    }

    for (uint64_t b = 0; b < 64; ++b) {
      EXPECT_EQ(b % 9 == 0, bitset.get(b));
    }

    for (uint64_t b = 64; b < 128; ++b) {
      EXPECT_EQ((b % 8) != (b - 64) / 8, bitset.get(b));
    }

    // test that the longs are mapped correctly
    const uint64_t* longs = bitset.getData();
    EXPECT_EQ(128, bitset.bitSize());
    EXPECT_EQ(0x8040201008040201L, longs[0]);
    EXPECT_EQ(~0x8040201008040201L, longs[1]);
  }

  // Same test as TestOrcBloomFilter#testLongHash() in Java codes. Make sure the hash values
  // are consistent between the Java client and C++ client.
  // TODO(ORC-1025): Add exhaustive test on all numbers.
  TEST(TestOrcBloomFilter, testLongHash) {
    EXPECT_EQ(0, orc::getLongHash(0));
    EXPECT_EQ(6614246905173314819, orc::getLongHash(-1));
    EXPECT_EQ(-5218250166726157773, orc::getLongHash(-2));
    EXPECT_EQ(1396019780946710816, orc::getLongHash(-3));

    EXPECT_EQ(3691278333958578070, orc::getLongHash(-9223372036854775805));
    EXPECT_EQ(-1192099642781211952, orc::getLongHash(-9223372036854775806));
    EXPECT_EQ(-9102499068535824902, orc::getLongHash(-9223372036854775807));

    EXPECT_EQ(1499534499340523007, orc::getLongHash(790302201));
    EXPECT_EQ(-5108695154500810163, orc::getLongHash(790302202));
    EXPECT_EQ(-2450623810987162260, orc::getLongHash(790302203));
    EXPECT_EQ(-1097054448615658549, orc::getLongHash(18000000000));

    EXPECT_EQ(-4986173376161118712, orc::getLongHash(9223372036064673413));
    EXPECT_EQ(3785699328822078862, orc::getLongHash(9223372036064673414));
    EXPECT_EQ(294188322706112357, orc::getLongHash(9223372036064673415));
  }

#define CheckBitSet(bf, p1, p2, p3, p4, p5) \
  EXPECT_TRUE(bf.mBitSet->get(p1));         \
  EXPECT_TRUE(bf.mBitSet->get(p2));         \
  EXPECT_TRUE(bf.mBitSet->get(p3));         \
  EXPECT_TRUE(bf.mBitSet->get(p4));         \
  EXPECT_TRUE(bf.mBitSet->get(p5))

  // Same test as TestOrcBloomFilter#testBasicOperations() in Java codes. We also
  // verifies the bitSet positions that are set, to make sure both the Java and C++ codes
  // hash the same value into the same position.
  TEST(TestOrcBloomFilter, testBloomFilterBasicOperations) {
    BloomFilterImpl bloomFilter(128);

    // test integers
    bloomFilter.reset();
    EXPECT_FALSE(bloomFilter.testLong(1));
    EXPECT_FALSE(bloomFilter.testLong(11));
    EXPECT_FALSE(bloomFilter.testLong(111));
    EXPECT_FALSE(bloomFilter.testLong(1111));
    EXPECT_FALSE(bloomFilter.testLong(0));
    EXPECT_FALSE(bloomFilter.testLong(-1));
    EXPECT_FALSE(bloomFilter.testLong(-11));
    EXPECT_FALSE(bloomFilter.testLong(-111));
    EXPECT_FALSE(bloomFilter.testLong(-1111));

    bloomFilter.addLong(1);
    CheckBitSet(bloomFilter, 567, 288, 246, 306, 228);
    bloomFilter.addLong(11);
    CheckBitSet(bloomFilter, 228, 285, 342, 399, 456);
    bloomFilter.addLong(111);
    CheckBitSet(bloomFilter, 802, 630, 458, 545, 717);
    bloomFilter.addLong(1111);
    CheckBitSet(bloomFilter, 826, 526, 40, 480, 86);
    bloomFilter.addLong(0);
    CheckBitSet(bloomFilter, 0, 0, 0, 0, 0);
    bloomFilter.addLong(-1);
    CheckBitSet(bloomFilter, 120, 308, 335, 108, 535);
    bloomFilter.addLong(-11);
    CheckBitSet(bloomFilter, 323, 685, 215, 577, 107);
    bloomFilter.addLong(-111);
    CheckBitSet(bloomFilter, 357, 318, 279, 15, 54);
    bloomFilter.addLong(-1111);
    CheckBitSet(bloomFilter, 572, 680, 818, 434, 232);

    EXPECT_TRUE(bloomFilter.testLong(1));
    EXPECT_TRUE(bloomFilter.testLong(11));
    EXPECT_TRUE(bloomFilter.testLong(111));
    EXPECT_TRUE(bloomFilter.testLong(1111));
    EXPECT_TRUE(bloomFilter.testLong(0));
    EXPECT_TRUE(bloomFilter.testLong(-1));
    EXPECT_TRUE(bloomFilter.testLong(-11));
    EXPECT_TRUE(bloomFilter.testLong(-111));
    EXPECT_TRUE(bloomFilter.testLong(-1111));

    // test doubles
    bloomFilter.reset();
    EXPECT_FALSE(bloomFilter.testDouble(1.1));
    EXPECT_FALSE(bloomFilter.testDouble(11.11));
    EXPECT_FALSE(bloomFilter.testDouble(111.111));
    EXPECT_FALSE(bloomFilter.testDouble(1111.1111));
    EXPECT_FALSE(bloomFilter.testDouble(0.0));
    EXPECT_FALSE(bloomFilter.testDouble(-1.1));
    EXPECT_FALSE(bloomFilter.testDouble(-11.11));
    EXPECT_FALSE(bloomFilter.testDouble(-111.111));
    EXPECT_FALSE(bloomFilter.testDouble(-1111.1111));

    bloomFilter.addDouble(1.1);
    CheckBitSet(bloomFilter, 522, 692, 12, 370, 753);
    bloomFilter.addDouble(11.11);
    CheckBitSet(bloomFilter, 210, 188, 89, 720, 389);
    bloomFilter.addDouble(111.111);
    CheckBitSet(bloomFilter, 831, 252, 583, 500, 335);
    bloomFilter.addDouble(1111.1111);
    CheckBitSet(bloomFilter, 725, 175, 374, 92, 642);
    bloomFilter.addDouble(0.0);
    CheckBitSet(bloomFilter, 0, 0, 0, 0, 0);
    bloomFilter.addDouble(-1.1);
    CheckBitSet(bloomFilter, 636, 163, 565, 206, 679);
    bloomFilter.addDouble(-11.11);
    CheckBitSet(bloomFilter, 473, 192, 743, 462, 181);
    bloomFilter.addDouble(-111.111);
    CheckBitSet(bloomFilter, 167, 152, 472, 295, 24);
    bloomFilter.addDouble(-1111.1111);
    CheckBitSet(bloomFilter, 308, 346, 384, 422, 371);

    EXPECT_TRUE(bloomFilter.testDouble(1.1));
    EXPECT_TRUE(bloomFilter.testDouble(11.11));
    EXPECT_TRUE(bloomFilter.testDouble(111.111));
    EXPECT_TRUE(bloomFilter.testDouble(1111.1111));
    EXPECT_TRUE(bloomFilter.testDouble(0.0));
    EXPECT_TRUE(bloomFilter.testDouble(-1.1));
    EXPECT_TRUE(bloomFilter.testDouble(-11.11));
    EXPECT_TRUE(bloomFilter.testDouble(-111.111));
    EXPECT_TRUE(bloomFilter.testDouble(-1111.1111));

    // test strings
    bloomFilter.reset();
    const char* emptyStr = "";
    const char* enStr = "english";
    const char* cnStr = "中国字";

    EXPECT_FALSE(bloomFilter.testBytes(emptyStr, static_cast<int64_t>(strlen(emptyStr))));
    EXPECT_FALSE(bloomFilter.testBytes(enStr, static_cast<int64_t>(strlen(enStr))));
    EXPECT_FALSE(bloomFilter.testBytes(cnStr, static_cast<int64_t>(strlen(cnStr))));

    bloomFilter.addBytes(emptyStr, static_cast<int64_t>(strlen(emptyStr)));
    CheckBitSet(bloomFilter, 656, 807, 480, 151, 304);
    bloomFilter.addBytes(enStr, static_cast<int64_t>(strlen(enStr)));
    CheckBitSet(bloomFilter, 576, 221, 68, 729, 392);
    bloomFilter.addBytes(cnStr, static_cast<int64_t>(strlen(cnStr)));
    CheckBitSet(bloomFilter, 602, 636, 44, 362, 318);

    EXPECT_TRUE(bloomFilter.testBytes(emptyStr, static_cast<int64_t>(strlen(emptyStr))));
    EXPECT_TRUE(bloomFilter.testBytes(enStr, static_cast<int64_t>(strlen(enStr))));
    EXPECT_TRUE(bloomFilter.testBytes(cnStr, static_cast<int64_t>(strlen(cnStr))));
  }

  TEST(TestOrcBloomFilter, testBloomFilterSerialization) {
    BloomFilterImpl emptyFilter1(128), emptyFilter2(256);
    EXPECT_FALSE(emptyFilter1 == emptyFilter2);

    BloomFilterImpl emptyFilter3(128, 0.05), emptyFilter4(128, 0.01);
    EXPECT_FALSE(emptyFilter3 == emptyFilter4);

    BloomFilterImpl srcBloomFilter(64);
    srcBloomFilter.addLong(1);
    srcBloomFilter.addLong(11);
    srcBloomFilter.addLong(111);
    srcBloomFilter.addLong(1111);
    srcBloomFilter.addLong(0);
    srcBloomFilter.addLong(-1);
    srcBloomFilter.addLong(-11);
    srcBloomFilter.addLong(-111);
    srcBloomFilter.addLong(-1111);

    proto::BloomFilter pbBloomFilter;
    proto::ColumnEncoding encoding;
    encoding.set_bloomencoding(1);

    // serialize
    BloomFilterUTF8Utils::serialize(srcBloomFilter, pbBloomFilter);

    // deserialize
    std::unique_ptr<BloomFilter> dstBloomFilter = BloomFilterUTF8Utils::deserialize(
        proto::Stream_Kind_BLOOM_FILTER_UTF8, encoding, pbBloomFilter);

    EXPECT_TRUE(srcBloomFilter == dynamic_cast<BloomFilterImpl&>(*dstBloomFilter));
    EXPECT_TRUE(dstBloomFilter->testLong(1));
    EXPECT_TRUE(dstBloomFilter->testLong(11));
    EXPECT_TRUE(dstBloomFilter->testLong(111));
    EXPECT_TRUE(dstBloomFilter->testLong(1111));
    EXPECT_TRUE(dstBloomFilter->testLong(0));
    EXPECT_TRUE(dstBloomFilter->testLong(-1));
    EXPECT_TRUE(dstBloomFilter->testLong(-11));
    EXPECT_TRUE(dstBloomFilter->testLong(-111));
    EXPECT_TRUE(dstBloomFilter->testLong(-1111));
  }

}  // namespace orc
