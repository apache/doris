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

#include "orc/OrcFile.hh"

#include "MemoryInputStream.hh"
#include "MemoryOutputStream.hh"

#include "wrap/gmock.h"
#include "wrap/gtest-wrapper.h"

#include <cmath>
#include <sstream>

namespace orc {

  const int DEFAULT_MEM_STREAM_SIZE = 10 * 1024 * 1024;  // 10M
  const int DICTIONARY_SIZE_1K = 1000;
  const double DICT_THRESHOLD = 0.2;      // make sure dictionary is used
  const double FALLBACK_THRESHOLD = 0.0;  // make sure fallback happens

  static std::unique_ptr<Reader> createReader(MemoryPool* memoryPool,
                                              std::unique_ptr<InputStream> stream) {
    ReaderOptions options;
    options.setMemoryPool(*memoryPool);
    return createReader(std::move(stream), options);
  }

  static std::unique_ptr<RowReader> createRowReader(Reader* reader) {
    RowReaderOptions rowReaderOpts;
    return reader->createRowReader(rowReaderOpts);
  }

  void testStringDictionary(bool enableIndex, double threshold) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();
    std::unique_ptr<Type> type(Type::buildTypeFromString("struct<col1:string>"));

    WriterOptions options;
    options.setStripeSize(1024);
    options.setCompressionBlockSize(1024);
    options.setCompression(CompressionKind_ZLIB);
    options.setMemoryPool(pool);
    options.setDictionaryKeySizeThreshold(threshold);
    options.setRowIndexStride(enableIndex ? 10000 : 0);
    std::unique_ptr<Writer> writer = createWriter(*type, &memStream, options);

    char dataBuffer[327675];
    uint64_t offset = 0, rowCount = 65535;
    std::unique_ptr<ColumnVectorBatch> batch = writer->createRowBatch(rowCount);
    StructVectorBatch* structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    StringVectorBatch* strBatch = dynamic_cast<StringVectorBatch*>(structBatch->fields[0]);

    uint64_t dictionarySize = DICTIONARY_SIZE_1K;
    for (uint64_t i = 0; i < rowCount; ++i) {
      std::ostringstream os;
      os << (i % dictionarySize);
      memcpy(dataBuffer + offset, os.str().c_str(), os.str().size());

      strBatch->data[i] = dataBuffer + offset;
      strBatch->length[i] = static_cast<int64_t>(os.str().size());

      offset += os.str().size();
    }
    structBatch->numElements = rowCount;
    strBatch->numElements = rowCount;
    writer->add(*batch);
    writer->close();

    std::unique_ptr<InputStream> inStream(
        new MemoryInputStream(memStream.getData(), memStream.getLength()));
    std::unique_ptr<Reader> reader = createReader(pool, std::move(inStream));
    std::unique_ptr<RowReader> rowReader = createRowReader(reader.get());
    EXPECT_EQ(rowCount, reader->getNumberOfRows());

    batch = rowReader->createRowBatch(rowCount);
    EXPECT_EQ(true, rowReader->next(*batch));
    EXPECT_EQ(rowCount, batch->numElements);

    for (uint64_t i = 0; i < rowCount; ++i) {
      structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
      strBatch = dynamic_cast<StringVectorBatch*>(structBatch->fields[0]);
      std::string str(strBatch->data[i], static_cast<size_t>(strBatch->length[i]));
      EXPECT_EQ(i % dictionarySize, static_cast<uint64_t>(atoi(str.c_str())));
    }

    EXPECT_FALSE(rowReader->next(*batch));
  }

  void testVarcharDictionary(bool enableIndex, double threshold) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();
    std::unique_ptr<Type> type(Type::buildTypeFromString("struct<col1:varchar(2)>"));

    WriterOptions options;
    options.setStripeSize(1024);
    options.setCompressionBlockSize(1024);
    options.setCompression(CompressionKind_ZLIB);
    options.setMemoryPool(pool);
    options.setDictionaryKeySizeThreshold(threshold);
    options.setRowIndexStride(enableIndex ? 10000 : 0);
    std::unique_ptr<Writer> writer = createWriter(*type, &memStream, options);

    char dataBuffer[327675];
    uint64_t offset = 0, rowCount = 65535;
    std::unique_ptr<ColumnVectorBatch> batch = writer->createRowBatch(rowCount);
    StructVectorBatch* structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    StringVectorBatch* varcharBatch = dynamic_cast<StringVectorBatch*>(structBatch->fields[0]);

    uint64_t dictionarySize = DICTIONARY_SIZE_1K;
    for (uint64_t i = 0; i < rowCount; ++i) {
      std::ostringstream os;
      os << (i % dictionarySize);
      memcpy(dataBuffer + offset, os.str().c_str(), os.str().size());

      varcharBatch->data[i] = dataBuffer + offset;
      varcharBatch->length[i] = static_cast<int64_t>(os.str().size());

      offset += os.str().size();
    }
    structBatch->numElements = rowCount;
    varcharBatch->numElements = rowCount;
    writer->add(*batch);
    writer->close();

    std::unique_ptr<InputStream> inStream(
        new MemoryInputStream(memStream.getData(), memStream.getLength()));
    std::unique_ptr<Reader> reader = createReader(pool, std::move(inStream));
    std::unique_ptr<RowReader> rowReader = createRowReader(reader.get());
    EXPECT_EQ(rowCount, reader->getNumberOfRows());

    batch = rowReader->createRowBatch(rowCount);
    EXPECT_EQ(true, rowReader->next(*batch));
    EXPECT_EQ(rowCount, batch->numElements);

    for (uint64_t i = 0; i < rowCount; ++i) {
      structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
      varcharBatch = dynamic_cast<StringVectorBatch*>(structBatch->fields[0]);

      std::ostringstream os;
      os << (i % dictionarySize);
      EXPECT_FALSE(varcharBatch->length[i] > 2);
      std::string varcharRead(varcharBatch->data[i], static_cast<size_t>(varcharBatch->length[i]));
      std::string varcharExpected = os.str().substr(0, 2);
      EXPECT_EQ(varcharRead, varcharExpected);
    }

    EXPECT_FALSE(rowReader->next(*batch));
  }

  void testCharDictionary(bool enableIndex, double threshold) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();
    std::unique_ptr<Type> type(Type::buildTypeFromString("struct<col1:char(3)>"));

    WriterOptions options;
    options.setStripeSize(1024);
    options.setCompressionBlockSize(1024);
    options.setCompression(CompressionKind_ZLIB);
    options.setMemoryPool(pool);
    options.setDictionaryKeySizeThreshold(threshold);
    options.setRowIndexStride(enableIndex ? 10000 : 0);
    std::unique_ptr<Writer> writer = createWriter(*type, &memStream, options);

    char dataBuffer[327675];
    uint64_t offset = 0, rowCount = 10000;
    std::unique_ptr<ColumnVectorBatch> batch = writer->createRowBatch(rowCount);
    StructVectorBatch* structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    StringVectorBatch* charBatch = dynamic_cast<StringVectorBatch*>(structBatch->fields[0]);

    uint64_t dictionarySize = DICTIONARY_SIZE_1K;
    for (uint64_t i = 0; i < rowCount; ++i) {
      std::ostringstream os;
      os << (i % dictionarySize);
      memcpy(dataBuffer + offset, os.str().c_str(), os.str().size());
      charBatch->data[i] = dataBuffer + offset;
      charBatch->length[i] = static_cast<int64_t>(os.str().size());
      offset += os.str().size();
    }
    structBatch->numElements = rowCount;
    charBatch->numElements = rowCount;
    writer->add(*batch);
    writer->close();

    std::unique_ptr<InputStream> inStream(
        new MemoryInputStream(memStream.getData(), memStream.getLength()));
    std::unique_ptr<Reader> reader = createReader(pool, std::move(inStream));
    std::unique_ptr<RowReader> rowReader = createRowReader(reader.get());
    EXPECT_EQ(rowCount, reader->getNumberOfRows());

    batch = rowReader->createRowBatch(rowCount);
    EXPECT_EQ(true, rowReader->next(*batch));
    EXPECT_EQ(rowCount, batch->numElements);

    for (uint64_t i = 0; i < rowCount; ++i) {
      structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
      charBatch = dynamic_cast<StringVectorBatch*>(structBatch->fields[0]);

      EXPECT_EQ(3, charBatch->length[i]);
      std::string charsRead(charBatch->data[i], static_cast<size_t>(charBatch->length[i]));

      std::ostringstream os;
      os << (i % dictionarySize);
      std::string charsExpected = os.str().substr(0, 3);
      while (charsExpected.length() < 3) {
        charsExpected += ' ';
      }
      EXPECT_EQ(charsExpected, charsRead);
    }

    EXPECT_FALSE(rowReader->next(*batch));
  }

  void testStringDictionaryWithNull(double threshold, bool enableIndex) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();
    std::unique_ptr<Type> type(Type::buildTypeFromString("struct<col1:string>"));

    WriterOptions options;
    options.setStripeSize(1024);
    options.setCompressionBlockSize(1024);
    options.setCompression(CompressionKind_ZLIB);
    options.setMemoryPool(pool);
    options.setDictionaryKeySizeThreshold(threshold);
    options.setRowIndexStride(enableIndex ? 10000 : 0);
    std::unique_ptr<Writer> writer = createWriter(*type, &memStream, options);

    char dataBuffer[327675];
    uint64_t offset = 0, rowCount = 65535;
    std::unique_ptr<ColumnVectorBatch> batch = writer->createRowBatch(rowCount);
    StructVectorBatch* structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    StringVectorBatch* strBatch = dynamic_cast<StringVectorBatch*>(structBatch->fields[0]);

    uint64_t dictionarySize = DICTIONARY_SIZE_1K;
    for (uint64_t i = 0; i < rowCount; ++i) {
      if (i % 2 == 0) {
        strBatch->notNull[i] = false;
      } else {
        std::ostringstream os;
        os << (i % dictionarySize);
        memcpy(dataBuffer + offset, os.str().c_str(), os.str().size());

        strBatch->notNull[i] = true;
        strBatch->data[i] = dataBuffer + offset;
        strBatch->length[i] = static_cast<int64_t>(os.str().size());

        offset += os.str().size();
      }
    }

    structBatch->numElements = rowCount;
    strBatch->numElements = rowCount;
    strBatch->hasNulls = true;
    writer->add(*batch);
    writer->close();

    std::unique_ptr<InputStream> inStream(
        new MemoryInputStream(memStream.getData(), memStream.getLength()));
    std::unique_ptr<Reader> reader = createReader(pool, std::move(inStream));
    std::unique_ptr<RowReader> rowReader = createRowReader(reader.get());
    EXPECT_EQ(rowCount, reader->getNumberOfRows());

    batch = rowReader->createRowBatch(rowCount);
    EXPECT_EQ(true, rowReader->next(*batch));
    EXPECT_EQ(rowCount, batch->numElements);

    for (uint64_t i = 0; i < rowCount; ++i) {
      structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
      strBatch = dynamic_cast<StringVectorBatch*>(structBatch->fields[0]);

      if (i % 2 == 0) {
        EXPECT_FALSE(strBatch->notNull[i]);
      } else {
        EXPECT_TRUE(strBatch->notNull[i]);
        std::string str(strBatch->data[i], static_cast<size_t>(strBatch->length[i]));
        EXPECT_EQ(i % dictionarySize, static_cast<uint64_t>(atoi(str.c_str())));
      }
    }

    EXPECT_FALSE(rowReader->next(*batch));
  }

  void testDictionaryMultipleStripes(double threshold, bool enableIndex) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();
    std::unique_ptr<Type> type(Type::buildTypeFromString("struct<col1:string>"));

    WriterOptions options;
    options.setStripeSize(1);
    options.setCompressionBlockSize(1024);
    options.setCompression(CompressionKind_ZLIB);
    options.setMemoryPool(pool);
    options.setDictionaryKeySizeThreshold(threshold);
    options.setRowIndexStride(enableIndex ? 10000 : 0);
    std::unique_ptr<Writer> writer = createWriter(*type, &memStream, options);

    char dataBuffer[800000];
    uint64_t offset = 0, rowCount = 65535, stripeCount = 3;
    std::unique_ptr<ColumnVectorBatch> batch = writer->createRowBatch(rowCount);
    StructVectorBatch* structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    StringVectorBatch* strBatch = dynamic_cast<StringVectorBatch*>(structBatch->fields[0]);

    uint64_t dictionarySize = DICTIONARY_SIZE_1K;

    for (uint64_t stripe = 0; stripe != stripeCount; ++stripe) {
      for (uint64_t i = 0; i < rowCount; ++i) {
        std::ostringstream os;
        os << (i % dictionarySize);
        memcpy(dataBuffer + offset, os.str().c_str(), os.str().size());

        strBatch->data[i] = dataBuffer + offset;
        strBatch->length[i] = static_cast<int64_t>(os.str().size());

        offset += os.str().size();
      }

      structBatch->numElements = rowCount;
      strBatch->numElements = rowCount;

      writer->add(*batch);
    }

    writer->close();

    std::unique_ptr<InputStream> inStream(
        new MemoryInputStream(memStream.getData(), memStream.getLength()));
    std::unique_ptr<Reader> reader = createReader(pool, std::move(inStream));
    std::unique_ptr<RowReader> rowReader = createRowReader(reader.get());

    // make sure there are multiple stripes
    EXPECT_EQ(rowCount * stripeCount, reader->getNumberOfRows());
    EXPECT_EQ(stripeCount, reader->getNumberOfStripes());

    // test reading sequentially for data correctness
    batch = rowReader->createRowBatch(rowCount);
    for (uint64_t stripe = 0; stripe != stripeCount; ++stripe) {
      EXPECT_EQ(true, rowReader->next(*batch));

      for (uint64_t i = 0; i < rowCount; ++i) {
        structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
        strBatch = dynamic_cast<StringVectorBatch*>(structBatch->fields[0]);
        std::string str(strBatch->data[i], static_cast<size_t>(strBatch->length[i]));
        EXPECT_EQ(i % dictionarySize, static_cast<uint64_t>(atoi(str.c_str())));
      }
    }
    EXPECT_FALSE(rowReader->next(*batch));

    // test seeking to check positions
    batch = rowReader->createRowBatch(1);

    for (uint64_t stripe = 0; stripe != stripeCount; ++stripe) {
      for (uint64_t i = 0; i < rowCount; i += 10000 / 2) {
        rowReader->seekToRow(stripe * rowCount + i);
        EXPECT_EQ(true, rowReader->next(*batch));

        structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
        strBatch = dynamic_cast<StringVectorBatch*>(structBatch->fields[0]);
        std::string str(strBatch->data[0], static_cast<size_t>(strBatch->length[0]));
        EXPECT_EQ(i % dictionarySize, static_cast<uint64_t>(atoi(str.c_str())));
      }
    }
  }

  // test dictionary encoding with index disabled
  // the decision of using dictionary if made at the end of 1st stripe
  TEST(DictionaryEncoding, writeStringDictionaryEncodingWithoutIndex) {
    testStringDictionary(false, DICT_THRESHOLD);
    testStringDictionary(false, FALLBACK_THRESHOLD);
  }

  // test dictionary encoding with index enabled
  // the decision of using dictionary if made at the end of 1st row group
  TEST(DictionaryEncoding, writeStringDictionaryEncodingWithIndex) {
    testStringDictionary(true, DICT_THRESHOLD);
    testStringDictionary(true, FALLBACK_THRESHOLD);
  }

  TEST(DictionaryEncoding, writeVarcharDictionaryEncodingWithoutIndex) {
    testVarcharDictionary(false, DICT_THRESHOLD);
    testVarcharDictionary(false, FALLBACK_THRESHOLD);
  }

  TEST(DictionaryEncoding, writeVarcharDictionaryEncodingWithIndex) {
    testVarcharDictionary(true, DICT_THRESHOLD);
    testVarcharDictionary(true, FALLBACK_THRESHOLD);
  }

  TEST(DictionaryEncoding, writeCharDictionaryEncodingWithoutIndex) {
    testCharDictionary(false, DICT_THRESHOLD);
    testCharDictionary(false, FALLBACK_THRESHOLD);
  }

  TEST(DictionaryEncoding, writeCharDictionaryEncodingWithIndex) {
    testCharDictionary(true, DICT_THRESHOLD);
    testCharDictionary(true, FALLBACK_THRESHOLD);
  }

  TEST(DictionaryEncoding, stringDictionaryWithNullWithIndex) {
    testStringDictionaryWithNull(DICT_THRESHOLD, true);
    testStringDictionaryWithNull(FALLBACK_THRESHOLD, true);
  }

  TEST(DictionaryEncoding, stringDictionaryWithNullWithoutIndex) {
    testStringDictionaryWithNull(DICT_THRESHOLD, false);
    testStringDictionaryWithNull(FALLBACK_THRESHOLD, false);
  }

  TEST(DictionaryEncoding, multipleStripesWithIndex) {
    testDictionaryMultipleStripes(DICT_THRESHOLD, true);
    testDictionaryMultipleStripes(FALLBACK_THRESHOLD, true);
  }

  TEST(DictionaryEncoding, multipleStripesWithoutIndex) {
    testDictionaryMultipleStripes(DICT_THRESHOLD, false);
    testDictionaryMultipleStripes(FALLBACK_THRESHOLD, false);
  }
}  // namespace orc
