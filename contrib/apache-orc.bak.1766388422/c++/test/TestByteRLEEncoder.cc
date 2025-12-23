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

#include "ByteRLE.hh"
#include "MemoryOutputStream.hh"

#include "wrap/gtest-wrapper.h"
#include "wrap/orc-proto-wrapper.hh"

#include <cstdlib>

namespace orc {

  const int DEFAULT_MEM_STREAM_SIZE = 1024 * 1024;  // 1M

  void generateNotNull(uint64_t numValues, uint64_t numNulls, char* notNull) {
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
  }

  void generateData(uint64_t numValues, char* data, uint64_t numNulls = 0,
                    char* notNull = nullptr) {
    generateNotNull(numValues, numNulls, notNull);
    for (uint64_t i = 0; i < numValues; ++i) {
      data[i] = static_cast<char>(std::rand() % 256);
    }
  }

  void generateBoolData(uint64_t numValues, char* data, uint64_t numNulls = 0,
                        char* notNull = nullptr) {
    generateNotNull(numValues, numNulls, notNull);
    for (uint64_t i = 0; i < numValues; ++i) {
      data[i] = static_cast<char>(std::rand() % 2);
    }
  }

  void decodeAndVerify(const MemoryOutputStream& memStream, char* data, uint64_t numValues,
                       char* notNull) {
    auto inStream =
        std::make_unique<SeekableArrayInputStream>(memStream.getData(), memStream.getLength());

    std::unique_ptr<ByteRleDecoder> decoder =
        createByteRleDecoder(std::move(inStream), getDefaultReaderMetrics());

    char* decodedData = new char[numValues];
    decoder->next(decodedData, numValues, notNull);

    for (uint64_t i = 0; i < numValues; ++i) {
      if (!notNull || notNull[i]) {
        EXPECT_EQ(data[i], decodedData[i]);
      }
    }

    delete[] decodedData;
  }

  void decodeAndVerifyBoolean(const MemoryOutputStream& memStream, char* data, uint64_t numValues,
                              char* notNull) {
    auto inStream =
        std::make_unique<SeekableArrayInputStream>(memStream.getData(), memStream.getLength());

    std::unique_ptr<ByteRleDecoder> decoder =
        createBooleanRleDecoder(std::move(inStream), getDefaultReaderMetrics());

    char* decodedData = new char[numValues];
    decoder->next(decodedData, numValues, notNull);

    for (uint64_t i = 0; i < numValues; ++i) {
      if (!notNull || notNull[i]) {
        bool expect = data[i] != 0;
        bool actual = decodedData[i] != 0;
        EXPECT_EQ(expect, actual);
      }
    }

    delete[] decodedData;
  }

  TEST(ByteRleEncoder, random_chars) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();

    uint64_t capacity = 500 * 1024;
    uint64_t block = 1024;

    auto outStream =
        std::make_unique<BufferedOutputStream>(*pool, &memStream, capacity, block, nullptr);

    std::unique_ptr<ByteRleEncoder> encoder = createByteRleEncoder(std::move(outStream));

    char* data = new char[102400];
    generateData(102400, data);
    encoder->add(data, 102400, nullptr);
    encoder->flush();

    decodeAndVerify(memStream, data, 102400, nullptr);
    delete[] data;
  }

  TEST(ByteRleEncoder, random_chars_with_null) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();

    uint64_t capacity = 500 * 1024;
    uint64_t block = 1024;

    auto outStream =
        std::make_unique<BufferedOutputStream>(*pool, &memStream, capacity, block, nullptr);

    std::unique_ptr<ByteRleEncoder> encoder = createByteRleEncoder(std::move(outStream));

    char* notNull = new char[102400];
    char* data = new char[102400];
    generateData(102400, data, 377, notNull);
    encoder->add(data, 102400, notNull);
    encoder->flush();

    decodeAndVerify(memStream, data, 102400, notNull);
    delete[] data;
    delete[] notNull;
  }

  TEST(BooleanRleEncoder, random_bits_not_aligned) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();

    uint64_t capacity = 500 * 1024;
    uint64_t block = 1024;

    auto outStream =
        std::make_unique<BufferedOutputStream>(*pool, &memStream, capacity, block, nullptr);

    std::unique_ptr<ByteRleEncoder> encoder = createBooleanRleEncoder(std::move(outStream));

    char* data = new char[1779];
    generateBoolData(1779, data);
    encoder->add(data, 1779, nullptr);
    encoder->flush();

    decodeAndVerifyBoolean(memStream, data, 1779, nullptr);
    delete[] data;
  }

  TEST(BooleanRleEncoder, random_bits_aligned) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();

    uint64_t capacity = 500 * 1024;
    uint64_t block = 1024;

    auto outStream =
        std::make_unique<BufferedOutputStream>(*pool, &memStream, capacity, block, nullptr);

    std::unique_ptr<ByteRleEncoder> encoder = createBooleanRleEncoder(std::move(outStream));

    char* data = new char[8000];
    generateBoolData(8000, data);
    encoder->add(data, 8000, nullptr);
    encoder->flush();

    decodeAndVerifyBoolean(memStream, data, 8000, nullptr);
    delete[] data;
  }

  TEST(BooleanRleEncoder, random_bits_aligned_with_null) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();

    uint64_t capacity = 500 * 1024;
    uint64_t block = 1024;

    auto outStream =
        std::make_unique<BufferedOutputStream>(*pool, &memStream, capacity, block, nullptr);

    std::unique_ptr<ByteRleEncoder> encoder = createBooleanRleEncoder(std::move(outStream));

    char* notNull = new char[8000];
    char* data = new char[8000];
    generateBoolData(8000, data, 515, notNull);
    encoder->add(data, 8000, notNull);
    encoder->flush();

    decodeAndVerifyBoolean(memStream, data, 8000, notNull);
    delete[] data;
    delete[] notNull;
  }
}  // namespace orc
