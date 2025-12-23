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

#include "MemoryOutputStream.hh"
#include "RLEv1.hh"

#include "wrap/gtest-wrapper.h"
#include "wrap/orc-proto-wrapper.hh"

#ifdef __clang__
DIAGNOSTIC_IGNORE("-Wmissing-variable-declarations")
#endif

namespace orc {

  using ::testing::TestWithParam;
  using ::testing::Values;

  const int DEFAULT_MEM_STREAM_SIZE = 1024 * 1024;  // 1M

  class RleTest : public TestWithParam<bool> {
    void SetUp() override;

   protected:
    bool alignBitpacking;
    std::unique_ptr<RleEncoder> getEncoder(RleVersion version, MemoryOutputStream& memStream,
                                           bool isSigned);

    void runExampleTest(int64_t* inputData, uint64_t inputLength, unsigned char* expectedOutput,
                        uint64_t outputLength);

    void runTest(RleVersion version, uint64_t numValues, int64_t start, int64_t delta, bool random,
                 bool isSigned, uint64_t numNulls = 0);
  };

  void RleTest::SetUp() {
    alignBitpacking = GetParam();
  }

  void generateData(uint64_t numValues, int64_t start, int64_t delta, bool random, int64_t* data,
                    uint64_t numNulls = 0, char* notNull = nullptr) {
    if (numNulls != 0 && notNull != nullptr) {
      memset(notNull, 1, numValues);
      while (numNulls > 0) {
        uint64_t pos = static_cast<uint64_t>(std::rand()) % numValues;
        if (notNull[pos]) {
          notNull[pos] = static_cast<char>(0);
          --numNulls;
        }
      }
    }

    for (uint64_t i = 0; i < numValues; ++i) {
      if (notNull == nullptr || notNull[i]) {
        if (!random) {
          data[i] = start + delta * static_cast<int64_t>(i);
        } else {
          data[i] = std::rand();
        }
      }
    }
  }

  void decodeAndVerify(RleVersion version, const MemoryOutputStream& memStream, int64_t* data,
                       uint64_t numValues, const char* notNull, bool isSinged) {
    std::unique_ptr<RleDecoder> decoder = createRleDecoder(
        std::make_unique<SeekableArrayInputStream>(memStream.getData(), memStream.getLength()),
        isSinged, version, *getDefaultPool(), getDefaultReaderMetrics());

    int64_t* decodedData = new int64_t[numValues];
    decoder->next(decodedData, numValues, notNull);

    for (uint64_t i = 0; i < numValues; ++i) {
      if (!notNull || notNull[i]) {
        EXPECT_EQ(data[i], decodedData[i]);
      }
    }

    delete[] decodedData;
  }

  std::unique_ptr<RleEncoder> RleTest::getEncoder(RleVersion version, MemoryOutputStream& memStream,
                                                  bool isSigned) {
    MemoryPool* pool = getDefaultPool();

    return createRleEncoder(
        std::make_unique<BufferedOutputStream>(*pool, &memStream, 500 * 1024, 1024, nullptr),
        isSigned, version, *pool, alignBitpacking);
  }

  void RleTest::runExampleTest(int64_t* inputData, uint64_t inputLength,
                               unsigned char* expectedOutput, uint64_t outputLength) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);

    std::unique_ptr<RleEncoder> encoder = getEncoder(RleVersion_2, memStream, false);

    encoder->add(inputData, inputLength, nullptr);
    encoder->flush();
    const char* output = memStream.getData();
    for (int i = 0; i < outputLength; i++) {
      EXPECT_EQ(expectedOutput[i], static_cast<unsigned char>(output[i]));
    }
  }

  void RleTest::runTest(RleVersion version, uint64_t numValues, int64_t start, int64_t delta,
                        bool random, bool isSigned, uint64_t numNulls) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);

    std::unique_ptr<RleEncoder> encoder = getEncoder(version, memStream, isSigned);

    char* notNull = numNulls == 0 ? nullptr : new char[numValues];
    int64_t* data = new int64_t[numValues];
    generateData(numValues, start, delta, random, data, numNulls, notNull);
    encoder->add(data, numValues, notNull);
    encoder->flush();

    decodeAndVerify(version, memStream, data, numValues, notNull, isSigned);
    delete[] data;
    delete[] notNull;
  }

  TEST_P(RleTest, RleV1_delta_increasing_sequance_unsigned) {
    runTest(RleVersion_1, 1024, 0, 1, false, false);
  }

  TEST_P(RleTest, RleV1_delta_increasing_sequance_unsigned_null) {
    runTest(RleVersion_1, 1024, 0, 1, false, false, 100);
  }

  TEST_P(RleTest, RleV1_delta_decreasing_sequance_unsigned) {
    runTest(RleVersion_1, 1024, 5000, -3, false, false);
  }

  TEST_P(RleTest, RleV1_delta_decreasing_sequance_signed) {
    runTest(RleVersion_1, 1024, 100, -3, false, true);
  }

  TEST_P(RleTest, RleV1_delta_decreasing_sequance_signed_null) {
    runTest(RleVersion_1, 1024, 100, -3, false, true, 500);
  }

  TEST_P(RleTest, rRleV1_andom_sequance_signed) {
    runTest(RleVersion_1, 1024, 0, 0, true, true);
  }

  TEST_P(RleTest, RleV1_all_null) {
    runTest(RleVersion_1, 1024, 100, -3, false, true, 1024);
  }

  TEST_P(RleTest, RleV2_delta_increasing_sequance_unsigned) {
    runTest(RleVersion_2, 1024, 0, 1, false, false);
  }

  TEST_P(RleTest, RleV2_delta_increasing_sequance_unsigned_null) {
    runTest(RleVersion_2, 1024, 0, 1, false, false, 100);
  }

  TEST_P(RleTest, RleV2_delta_decreasing_sequance_unsigned) {
    runTest(RleVersion_2, 1024, 5000, -3, false, false);
  }

  TEST_P(RleTest, RleV2_delta_decreasing_sequance_signed) {
    runTest(RleVersion_2, 1024, 100, -3, false, true);
  }

  TEST_P(RleTest, RleV2_delta_decreasing_sequance_signed_null) {
    runTest(RleVersion_2, 1024, 100, -3, false, true, 500);
  }

  TEST_P(RleTest, RleV2_random_sequance_signed) {
    runTest(RleVersion_2, 1024, 0, 0, true, true);
  }

  TEST_P(RleTest, RleV2_all_null) {
    runTest(RleVersion_2, 1024, 100, -3, false, true, 1024);
  }

  TEST_P(RleTest, RleV2_delta_zero_unsigned) {
    runTest(RleVersion_2, 1024, 123, 0, false, false);
  }

  TEST_P(RleTest, RleV2_delta_zero_signed) {
    runTest(RleVersion_2, 1024, 123, 0, false, true);
  }

  TEST_P(RleTest, RleV2_short_repeat) {
    runTest(RleVersion_2, 8, 123, 0, false, false);
  }

  TEST_P(RleTest, RleV2_short_repeat_example) {
    int64_t data[5] = {10000, 10000, 10000, 10000, 10000};
    unsigned char expectedEncoded[3] = {0x0a, 0x27, 0x10};
    runExampleTest(data, 5, expectedEncoded, 3);
  }

  TEST_P(RleTest, RleV2_direct_example) {
    int64_t data[4] = {23713, 43806, 57005, 48879};
    unsigned char expectedEncoded[10] = {0x5e, 0x03, 0x5c, 0xa1, 0xab,
                                         0x1e, 0xde, 0xad, 0xbe, 0xef};
    runExampleTest(data, 4, expectedEncoded, 10);
  }

  TEST_P(RleTest, RleV2_Patched_base_example) {
    int64_t data[20] = {2030, 2000, 2020, 1000000, 2040, 2050, 2060, 2070, 2080, 2090,
                        2100, 2110, 2120, 2130,    2140, 2150, 2160, 2170, 2180, 2190};
    unsigned char expectedEncoded[28] = {0x8e, 0x13, 0x2b, 0x21, 0x07, 0xd0, 0x1e, 0x00, 0x14, 0x70,
                                         0x28, 0x32, 0x3c, 0x46, 0x50, 0x5a, 0x64, 0x6e, 0x78, 0x82,
                                         0x8c, 0x96, 0xa0, 0xaa, 0xb4, 0xbe, 0xfc, 0xe8};
    runExampleTest(data, 20, expectedEncoded, 28);
  }

  TEST_P(RleTest, RleV2_Patched_base_big_gap) {
    // The input data contains 512 values: data[0...510] are of a repeated pattern of (2, 1), and
    // the last value (i.e. data[511]) is equal to 1024. This makes the last value to be the
    // only patched value and the gap of this patch is 511.
    const int numValues = 512;
    int64_t data[512];
    for (int i = 0; i < 511; i += 2) {
      data[i] = 2;
      data[i + 1] = 1;
    }
    data[511] = 1024;

    // Invoke the encoder.
    const bool isSigned = true;
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);

    std::unique_ptr<RleEncoder> encoder = getEncoder(RleVersion_2, memStream, isSigned);
    encoder->add(data, numValues, nullptr);
    encoder->flush();

    // Decode and verify.
    decodeAndVerify(RleVersion_2, memStream, data, numValues, nullptr, isSigned);
  }

  TEST_P(RleTest, RleV2_delta_example) {
    int64_t data[10] = {2, 3, 5, 7, 11, 13, 17, 19, 23, 29};
    unsigned char expectedEncoded[8] = {0xc6, 0x09, 0x02, 0x02, 0x22, 0x42, 0x42, 0x46};
    unsigned char unalignedExpectedEncoded[7] = {0xc4, 0x09, 0x02, 0x02, 0x4A, 0x28, 0xA6};
    if (alignBitpacking) {
      runExampleTest(data, 10, expectedEncoded, 8);
    } else {
      runExampleTest(data, 10, unalignedExpectedEncoded, 7);
    }
  }

  TEST_P(RleTest, RleV2_delta_example2) {
    int64_t data[7] = {0, 10000, 10001, 10001, 10002, 10003, 10003};
    unsigned char expectedEncoded[8] = {0xc2, 0x06, 0x0, 0xa0, 0x9c, 0x01, 0x45, 0x0};
    runExampleTest(data, 7, expectedEncoded, 8);
  }

  TEST_P(RleTest, RleV2_direct_repeat_example2) {
    int64_t data[9] = {23713, 43806, 57005, 48879, 10000, 10000, 10000, 10000, 10000};
    unsigned char expectedEncoded[13] = {0x5e, 0x03, 0x5c, 0xa1, 0xab, 0x1e, 0xde,
                                         0xad, 0xbe, 0xef, 0x0a, 0x27, 0x10};
    runExampleTest(data, 9, expectedEncoded, 13);
  }

  INSTANTIATE_TEST_SUITE_P(OrcTest, RleTest, Values(true, false));
}  // namespace orc
