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

#include "Adaptor.hh"
#include "ColumnReader.hh"
#include "OrcTest.hh"
#include "orc/Exceptions.hh"

#include "wrap/gmock.h"
#include "wrap/gtest-wrapper.h"
#include "wrap/orc-proto-wrapper.hh"

#include <cmath>
#include <iostream>
#include <vector>

#ifdef __clang__
DIAGNOSTIC_IGNORE("-Winconsistent-missing-override")
DIAGNOSTIC_IGNORE("-Wmissing-variable-declarations")
#endif
#ifdef __GNUC__
DIAGNOSTIC_IGNORE("-Wparentheses")
#endif

namespace orc {
  using ::testing::TestWithParam;
  using ::testing::Values;

  class MockStripeStreams : public StripeStreams {
   public:
    ~MockStripeStreams() override;

    std::unique_ptr<SeekableInputStream> getStream(uint64_t columnId, proto::Stream_Kind kind,
                                                   bool stream) const override;

    MOCK_CONST_METHOD0(getSelectedColumns,

                       const std::vector<bool>()

    );
    MOCK_CONST_METHOD1(getEncoding, proto::ColumnEncoding(uint64_t));
    MOCK_CONST_METHOD3(getStreamProxy, SeekableInputStream*(uint64_t, proto::Stream_Kind, bool));
    MOCK_CONST_METHOD0(getErrorStream, std::ostream*());
    MOCK_CONST_METHOD0(getThrowOnHive11DecimalOverflow, bool());
    MOCK_CONST_METHOD0(getForcedScaleOnHive11Decimal, int32_t());
    MOCK_CONST_METHOD0(isDecimalAsLong, bool());

    MemoryPool& getMemoryPool() const override {
      return *getDefaultPool();
    }

    ReaderMetrics* getReaderMetrics() const override {
      return getDefaultReaderMetrics();
    }

    const Timezone& getWriterTimezone() const override {
      return getTimezoneByName("America/Los_Angeles");
    }

    const Timezone& getReaderTimezone() const override {
      return getTimezoneByName("GMT");
    }
  };

  MockStripeStreams::~MockStripeStreams() {
    // PASS
  }

  std::unique_ptr<SeekableInputStream> MockStripeStreams::getStream(uint64_t columnId,
                                                                    proto::Stream_Kind kind,
                                                                    bool shouldStream) const {
    return std::unique_ptr<SeekableInputStream>(getStreamProxy(columnId, kind, shouldStream));
  }

  bool isNotNull(tm* timeptr) {
    return timeptr != nullptr;
  }

  class TestColumnReaderEncoded : public TestWithParam<bool> {
    void SetUp() override;

   protected:
    bool encoded;
  };

  void TestColumnReaderEncoded::SetUp() {
    encoded = GetParam();
  }

  TEST(TestColumnReader, testBooleanWithNulls) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    std::vector<bool> selectedColumns(2, true);
    EXPECT_CALL(streams, getSelectedColumns()).WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_)).WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));
    // alternate 4 non-null and 4 null via [0xf0 for x in range(512 / 8)]
    const unsigned char buffer1[] = {0x3d, 0xf0};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer1, ARRAY_SIZE(buffer1))));

    // [0x0f for x in range(256 / 8)]
    const unsigned char buffer2[] = {0x1d, 0x0f};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer2, ARRAY_SIZE(buffer2))));

    // create the row type
    std::unique_ptr<Type> rowType = createStructType();
    rowType->addStructField("col0", createPrimitiveType(BOOLEAN));

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);
    LongVectorBatch* longBatch = new LongVectorBatch(1024, *getDefaultPool());
    StructVectorBatch batch(1024, *getDefaultPool());
    batch.fields.push_back(longBatch);
    reader->next(batch, 512, 0);
    ASSERT_EQ(512, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(512, longBatch->numElements);
    ASSERT_EQ(true, longBatch->hasNulls);
    unsigned int next = 0;
    for (size_t i = 0; i < batch.numElements; ++i) {
      if (i & 4) {
        EXPECT_EQ(0, longBatch->notNull[i]) << "Wrong value at " << i;
      } else {
        EXPECT_EQ(1, longBatch->notNull[i]) << "Wrong value at " << i;
        EXPECT_EQ((next++ & 4) != 0, longBatch->data[i]) << "Wrong value at " << i;
      }
    }
  }

  TEST(TestColumnReader, testBooleanSkipsWithNulls) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    std::vector<bool> selectedColumns(2, true);
    EXPECT_CALL(streams, getSelectedColumns()).WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_)).WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));
    // alternate 4 non-null and 4 null via [0xf0 for x in range(512 / 8)]
    const unsigned char buffer1[] = {0x3d, 0xf0};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer1, ARRAY_SIZE(buffer1))));
    // [0x0f for x in range(128 / 8)]
    const unsigned char buffer2[] = {0x1d, 0x0f};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer2, ARRAY_SIZE(buffer2))));

    // create the row type
    std::unique_ptr<Type> rowType = createStructType();
    rowType->addStructField("col0", createPrimitiveType(BOOLEAN));

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);
    LongVectorBatch* longBatch = new LongVectorBatch(1024, *getDefaultPool());
    StructVectorBatch batch(1024, *getDefaultPool());
    batch.fields.push_back(longBatch);
    reader->next(batch, 1, 0);
    ASSERT_EQ(1, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(1, longBatch->numElements);
    ASSERT_EQ(true, !longBatch->hasNulls);
    EXPECT_EQ(0, longBatch->data[0]);
    reader->skip(506);
    reader->next(batch, 5, 0);
    ASSERT_EQ(5, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(5, longBatch->numElements);
    ASSERT_EQ(true, longBatch->hasNulls);
    EXPECT_EQ(1, longBatch->data[0]);
    EXPECT_EQ(true, !longBatch->notNull[1]);
    EXPECT_EQ(true, !longBatch->notNull[2]);
    EXPECT_EQ(true, !longBatch->notNull[3]);
    EXPECT_EQ(true, !longBatch->notNull[4]);
  }

  TEST(TestColumnReader, testByteWithNulls) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    std::vector<bool> selectedColumns(2, true);
    EXPECT_CALL(streams, getSelectedColumns()).WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_)).WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));
    // alternate 4 non-null and 4 null via [0xf0 for x in range(512 / 8)]
    const unsigned char buffer1[] = {0x3d, 0xf0};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer1, ARRAY_SIZE(buffer1))));

    // range(256)
    char buffer[258];
    buffer[0] = '\x80';
    for (unsigned int i = 0; i < 128; ++i) {
      buffer[i + 1] = static_cast<char>(i);
    }
    buffer[129] = '\x80';
    for (unsigned int i = 128; i < 256; ++i) {
      buffer[i + 2] = static_cast<char>(i);
    }
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(testing::Return(new SeekableArrayInputStream(buffer, ARRAY_SIZE(buffer))));

    // create the row type
    std::unique_ptr<Type> rowType = createStructType();
    rowType->addStructField("col0", createPrimitiveType(BYTE));

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);
    LongVectorBatch* longBatch = new LongVectorBatch(1024, *getDefaultPool());
    StructVectorBatch batch(1024, *getDefaultPool());
    batch.fields.push_back(longBatch);
    reader->next(batch, 512, 0);
    ASSERT_EQ(512, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(512, longBatch->numElements);
    ASSERT_EQ(true, longBatch->hasNulls);
    unsigned int next = 0;
    for (size_t i = 0; i < batch.numElements; ++i) {
      if (i & 4) {
        EXPECT_EQ(0, longBatch->notNull[i]) << "Wrong value at " << i;
      } else {
        EXPECT_EQ(1, longBatch->notNull[i]) << "Wrong value at " << i;
        EXPECT_EQ(static_cast<char>(next++), static_cast<char>(longBatch->data[i]))
            << "Wrong value at " << i;
      }
    }
  }

  TEST(TestColumnReader, testByteSkipsWithNulls) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    std::vector<bool> selectedColumns(2, true);
    EXPECT_CALL(streams, getSelectedColumns()).WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_)).WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));
    // alternate 4 non-null and 4 null via [0xf0 for x in range(512 / 8)]
    const unsigned char buffer1[] = {0x3d, 0xf0};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer1, ARRAY_SIZE(buffer1))));

    // range(256)
    char buffer[258];
    buffer[0] = '\x80';
    for (unsigned int i = 0; i < 128; ++i) {
      buffer[i + 1] = static_cast<char>(i);
    }
    buffer[129] = '\x80';
    for (unsigned int i = 128; i < 256; ++i) {
      buffer[i + 2] = static_cast<char>(i);
    }
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(testing::Return(new SeekableArrayInputStream(buffer, ARRAY_SIZE(buffer))));

    // create the row type
    std::unique_ptr<Type> rowType = createStructType();
    rowType->addStructField("col0", createPrimitiveType(BYTE));

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);
    LongVectorBatch* longBatch = new LongVectorBatch(1024, *getDefaultPool());
    StructVectorBatch batch(1024, *getDefaultPool());
    batch.fields.push_back(longBatch);
    reader->next(batch, 1, 0);
    ASSERT_EQ(1, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(1, longBatch->numElements);
    ASSERT_EQ(true, !longBatch->hasNulls);
    EXPECT_EQ(0, longBatch->data[0]);
    reader->skip(506);
    reader->next(batch, 5, 0);
    ASSERT_EQ(5, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(5, longBatch->numElements);
    ASSERT_EQ(true, longBatch->hasNulls);
    EXPECT_EQ(static_cast<char>(-1), static_cast<char>(longBatch->data[0]));
    EXPECT_EQ(true, !longBatch->notNull[1]);
    EXPECT_EQ(true, !longBatch->notNull[2]);
    EXPECT_EQ(true, !longBatch->notNull[3]);
    EXPECT_EQ(true, !longBatch->notNull[4]);
  }

  TEST(TestColumnReader, testIntegerWithNulls) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    std::vector<bool> selectedColumns(2, true);

    EXPECT_CALL(streams, getSelectedColumns()).WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_)).WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));
    const unsigned char buffer1[] = {0x16, 0xf0};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer1, ARRAY_SIZE(buffer1))));
    const unsigned char buffer2[] = {0x64, 0x01, 0x00};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer2, ARRAY_SIZE(buffer2))));

    // create the row type
    std::unique_ptr<Type> rowType = createStructType();
    rowType->addStructField("myInt", createPrimitiveType(INT));

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);
    LongVectorBatch* longBatch = new LongVectorBatch(1024, *getDefaultPool());
    StructVectorBatch batch(1024, *getDefaultPool());
    batch.fields.push_back(longBatch);
    reader->next(batch, 200, 0);
    ASSERT_EQ(200, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(200, longBatch->numElements);
    ASSERT_EQ(true, longBatch->hasNulls);
    long next = 0;
    for (size_t i = 0; i < batch.numElements; ++i) {
      if (i & 4) {
        EXPECT_EQ(0, longBatch->notNull[i]);
      } else {
        EXPECT_EQ(1, longBatch->notNull[i]);
        EXPECT_EQ(next++, longBatch->data[i]);
      }
    }
  }

  TEST_P(TestColumnReaderEncoded, testDictionaryWithNulls) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    std::vector<bool> selectedColumns(2, true);
    EXPECT_CALL(streams, getSelectedColumns()).WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(0)).WillRepeatedly(testing::Return(directEncoding));
    proto::ColumnEncoding dictionaryEncoding;
    dictionaryEncoding.set_kind(proto::ColumnEncoding_Kind_DICTIONARY);
    dictionaryEncoding.set_dictionarysize(2);
    EXPECT_CALL(streams, getEncoding(1)).WillRepeatedly(testing::Return(dictionaryEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));
    const unsigned char buffer1[] = {0x19, 0xf0};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer1, ARRAY_SIZE(buffer1))));
    const unsigned char buffer2[] = {0x2f, 0x00, 0x00, 0x2f, 0x00, 0x01};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer2, ARRAY_SIZE(buffer2))));
    const unsigned char buffer3[] = {0x4f, 0x52, 0x43, 0x4f, 0x77, 0x65, 0x6e};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DICTIONARY_DATA, false))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer3, ARRAY_SIZE(buffer3))));
    const unsigned char buffer4[] = {0x02, 0x01, 0x03};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, false))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer4, ARRAY_SIZE(buffer4))));

    // create the row type
    std::unique_ptr<Type> rowType = createStructType();
    rowType->addStructField("myString", createPrimitiveType(STRING));

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    if (encoded) {
      EncodedStringVectorBatch* encodedStringBatch =
          new EncodedStringVectorBatch(1024, *getDefaultPool());
      StructVectorBatch batch(1024, *getDefaultPool());
      batch.fields.push_back(encodedStringBatch);
      reader->nextEncoded(batch, 200, 0);
      ASSERT_EQ(200, batch.numElements);
      ASSERT_EQ(true, !batch.hasNulls);
      ASSERT_EQ(200, encodedStringBatch->numElements);
      ASSERT_EQ(true, encodedStringBatch->hasNulls);
      for (size_t i = 0; i < batch.numElements; ++i) {
        if (i & 4) {
          EXPECT_EQ(0, encodedStringBatch->notNull[i]);
        } else {
          EXPECT_EQ(1, encodedStringBatch->notNull[i]);
          const char* expected = i < 98 ? "ORC" : "Owen";
          int64_t index = encodedStringBatch->index.data()[i];

          char* actualString;
          int64_t actualLength;
          encodedStringBatch->dictionary->getValueByIndex(index, actualString, actualLength);
          ASSERT_EQ(strlen(expected), actualLength) << "Wrong length at " << i;

          for (size_t letter = 0; letter < strlen(expected); ++letter) {
            EXPECT_EQ(expected[letter], actualString[letter])
                << "Wrong contents at " << i << ", " << letter;
          }
        }
      }
    } else {
      StringVectorBatch* stringBatch = new StringVectorBatch(1024, *getDefaultPool());
      StructVectorBatch batch(1024, *getDefaultPool());
      batch.fields.push_back(stringBatch);
      reader->next(batch, 200, 0);
      ASSERT_EQ(200, batch.numElements);
      ASSERT_EQ(true, !batch.hasNulls);
      ASSERT_EQ(200, stringBatch->numElements);
      ASSERT_EQ(true, stringBatch->hasNulls);
      for (size_t i = 0; i < batch.numElements; ++i) {
        if (i & 4) {
          EXPECT_EQ(0, stringBatch->notNull[i]);
        } else {
          EXPECT_EQ(1, stringBatch->notNull[i]);
          const char* expected = i < 98 ? "ORC" : "Owen";
          ASSERT_EQ(strlen(expected), stringBatch->length[i]) << "Wrong length at " << i;
          for (size_t letter = 0; letter < strlen(expected); ++letter) {
            EXPECT_EQ(expected[letter], stringBatch->data[i][letter])
                << "Wrong contents at " << i << ", " << letter;
          }
        }
      }
    }
  }

  TEST_P(TestColumnReaderEncoded, testVarcharDictionaryWithNulls) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    std::vector<bool> selectedColumns(3, true);
    selectedColumns.push_back(false);

    EXPECT_CALL(streams, getSelectedColumns()).WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(0)).WillRepeatedly(testing::Return(directEncoding));

    proto::ColumnEncoding dictionary2Encoding;
    dictionary2Encoding.set_kind(proto::ColumnEncoding_Kind_DICTIONARY);
    dictionary2Encoding.set_dictionarysize(2);
    EXPECT_CALL(streams, getEncoding(1)).WillRepeatedly(testing::Return(dictionary2Encoding));

    proto::ColumnEncoding dictionary0Encoding;
    dictionary0Encoding.set_kind(proto::ColumnEncoding_Kind_DICTIONARY);
    dictionary0Encoding.set_dictionarysize(0);
    EXPECT_CALL(streams, getEncoding(testing::Ge(2)))
        .WillRepeatedly(testing::Return(dictionary0Encoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));

    const unsigned char buffer1[] = {0x16, 0xff};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer1, ARRAY_SIZE(buffer1))));
    const unsigned char buffer2[] = {0x61, 0x00, 0x01, 0x61, 0x00, 0x00};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer2, ARRAY_SIZE(buffer2))));
    const unsigned char buffer3[] = {0x4f, 0x52, 0x43, 0x4f, 0x77, 0x65, 0x6e};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DICTIONARY_DATA, false))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer3, ARRAY_SIZE(buffer3))));
    const unsigned char buffer4[] = {0x02, 0x01, 0x03};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, false))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer4, ARRAY_SIZE(buffer4))));

    const unsigned char buffer5[] = {0x16, 0x00};
    EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer5, ARRAY_SIZE(buffer5))));

    // all three return an empty stream
    EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(testing::Return(new SeekableArrayInputStream(buffer5, 0)));
    EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_DICTIONARY_DATA, false))
        .WillRepeatedly(testing::Return(new SeekableArrayInputStream(buffer5, 0)));
    EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_LENGTH, false))
        .WillRepeatedly(testing::Return(new SeekableArrayInputStream(buffer5, 0)));

    // create the row type
    std::unique_ptr<Type> rowType = createStructType();
    rowType->addStructField("col0", createPrimitiveType(VARCHAR))
        ->addStructField("col1", createPrimitiveType(CHAR))
        ->addStructField("col2", createPrimitiveType(STRING));

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);
    if (encoded) {
      StructVectorBatch batch(1024, *getDefaultPool());
      EncodedStringVectorBatch* encodedStringBatch =
          new EncodedStringVectorBatch(1024, *getDefaultPool());
      EncodedStringVectorBatch* nullBatch = new EncodedStringVectorBatch(1024, *getDefaultPool());
      batch.fields.push_back(encodedStringBatch);
      batch.fields.push_back(nullBatch);
      reader->nextEncoded(batch, 200, 0);
      ASSERT_EQ(200, batch.numElements);
      ASSERT_EQ(true, !batch.hasNulls);
      ASSERT_EQ(200, encodedStringBatch->numElements);
      ASSERT_EQ(true, !encodedStringBatch->hasNulls);
      ASSERT_EQ(200, nullBatch->numElements);
      ASSERT_EQ(true, nullBatch->hasNulls);
      for (size_t i = 0; i < batch.numElements; ++i) {
        EXPECT_EQ(true, encodedStringBatch->notNull[i]);
        EXPECT_EQ(true, !nullBatch->notNull[i]);
        const char* expected = i < 100 ? "Owen" : "ORC";
        int64_t index = encodedStringBatch->index.data()[i];
        char* actualString;
        int64_t actualLength;
        encodedStringBatch->dictionary->getValueByIndex(index, actualString, actualLength);
        ASSERT_EQ(strlen(expected), actualLength) << "Wrong length at " << i;
        for (size_t letter = 0; letter < strlen(expected); ++letter) {
          EXPECT_EQ(expected[letter], actualString[letter])
              << "Wrong contents at " << i << ", " << letter;
        }
      }
    } else {
      StructVectorBatch batch(1024, *getDefaultPool());
      StringVectorBatch* stringBatch = new StringVectorBatch(1024, *getDefaultPool());
      StringVectorBatch* nullBatch = new StringVectorBatch(1024, *getDefaultPool());
      batch.fields.push_back(stringBatch);
      batch.fields.push_back(nullBatch);
      reader->next(batch, 200, 0);
      ASSERT_EQ(200, batch.numElements);
      ASSERT_EQ(true, !batch.hasNulls);
      ASSERT_EQ(200, stringBatch->numElements);
      ASSERT_EQ(true, !stringBatch->hasNulls);
      ASSERT_EQ(200, nullBatch->numElements);
      ASSERT_EQ(true, nullBatch->hasNulls);
      for (size_t i = 0; i < batch.numElements; ++i) {
        EXPECT_EQ(true, stringBatch->notNull[i]);
        EXPECT_EQ(true, !nullBatch->notNull[i]);
        const char* expected = i < 100 ? "Owen" : "ORC";
        ASSERT_EQ(strlen(expected), stringBatch->length[i]) << "Wrong length at " << i;
        for (size_t letter = 0; letter < strlen(expected); ++letter) {
          EXPECT_EQ(expected[letter], stringBatch->data[i][letter])
              << "Wrong contents at " << i << ", " << letter;
        }
      }
    }
  }

  TEST(TestColumnReader, testSubstructsWithNulls) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    std::vector<bool> selectedColumns(4, true);
    EXPECT_CALL(streams, getSelectedColumns()).WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_)).WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));

    const unsigned char buffer1[] = {0x16, 0x0f};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer1, ARRAY_SIZE(buffer1))));

    const unsigned char buffer2[] = {0x0a, 0x55};
    EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer2, ARRAY_SIZE(buffer2))));

    const unsigned char buffer3[] = {0x04, 0xf0};
    EXPECT_CALL(streams, getStreamProxy(3, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer3, ARRAY_SIZE(buffer3))));

    const unsigned char buffer4[] = {0x17, 0x01, 0x00};
    EXPECT_CALL(streams, getStreamProxy(3, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer4, ARRAY_SIZE(buffer4))));

    // create the row type
    std::unique_ptr<Type> innerType = createStructType();
    innerType->addStructField("col2", createPrimitiveType(LONG));

    std::unique_ptr<Type> middleType = createStructType();
    middleType->addStructField("col1", std::move(innerType));

    std::unique_ptr<Type> rowType = createStructType();
    rowType->addStructField("col0", std::move(middleType));

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    StructVectorBatch batch(1024, *getDefaultPool());
    StructVectorBatch* middle = new StructVectorBatch(1024, *getDefaultPool());
    StructVectorBatch* inner = new StructVectorBatch(1024, *getDefaultPool());
    LongVectorBatch* longs = new LongVectorBatch(1024, *getDefaultPool());
    batch.fields.push_back(middle);
    middle->fields.push_back(inner);
    inner->fields.push_back(longs);
    reader->next(batch, 200, 0);
    ASSERT_EQ(200, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(200, middle->numElements);
    ASSERT_EQ(true, middle->hasNulls);
    ASSERT_EQ(200, inner->numElements);
    ASSERT_EQ(true, inner->hasNulls);
    ASSERT_EQ(200, longs->numElements);
    ASSERT_EQ(true, longs->hasNulls);
    long middleCount = 0;
    long innerCount = 0;
    long longCount = 0;
    for (size_t i = 0; i < batch.numElements; ++i) {
      if (i & 4) {
        EXPECT_EQ(true, middle->notNull[i]) << "Wrong at " << i;
        if (middleCount++ & 1) {
          EXPECT_EQ(true, inner->notNull[i]) << "Wrong at " << i;
          if (innerCount++ & 4) {
            EXPECT_EQ(true, !longs->notNull[i]) << "Wrong at " << i;
          } else {
            EXPECT_EQ(true, longs->notNull[i]) << "Wrong at " << i;
            EXPECT_EQ(longCount++, longs->data[i]) << "Wrong at " << i;
          }
        } else {
          EXPECT_EQ(true, !inner->notNull[i]) << "Wrong at " << i;
          EXPECT_EQ(true, !longs->notNull[i]) << "Wrong at " << i;
        }
      } else {
        EXPECT_EQ(true, !middle->notNull[i]) << "Wrong at " << i;
        EXPECT_EQ(true, !inner->notNull[i]) << "Wrong at " << i;
        EXPECT_EQ(true, !longs->notNull[i]) << "Wrong at " << i;
      }
    }
  }

  TEST(TestColumnReader, testSkipWithNulls) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    std::vector<bool> selectedColumns(3, true);
    EXPECT_CALL(streams, getSelectedColumns()).WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_)).WillRepeatedly(testing::Return(directEncoding));
    proto::ColumnEncoding dictionaryEncoding;
    dictionaryEncoding.set_kind(proto::ColumnEncoding_Kind_DICTIONARY);
    dictionaryEncoding.set_dictionarysize(100);
    EXPECT_CALL(streams, getEncoding(2)).WillRepeatedly(testing::Return(dictionaryEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));
    const unsigned char buffer1[] = {0x03, 0x00, 0xff, 0x3f, 0x08, 0xff, 0xff, 0xfc, 0x03, 0x00};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer1, ARRAY_SIZE(buffer1))));
    EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer1, ARRAY_SIZE(buffer1))));

    const unsigned char buffer2[] = {0x61, 0x01, 0x00};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer2, ARRAY_SIZE(buffer2))));
    EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer2, ARRAY_SIZE(buffer2))));

    // fill the dictionary with '00' to '99'
    char digits[200];
    for (int i = 0; i < 10; ++i) {
      for (int j = 0; j < 10; ++j) {
        digits[2 * (10 * i + j)] = static_cast<char>('0' + i);
        digits[2 * (10 * i + j) + 1] = static_cast<char>('0' + j);
      }
    }
    EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_DICTIONARY_DATA, false))
        .WillRepeatedly(testing::Return(new SeekableArrayInputStream(digits, ARRAY_SIZE(digits))));
    const unsigned char buffer3[] = {0x61, 0x00, 0x02};
    EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_LENGTH, false))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer3, ARRAY_SIZE(buffer3))));

    // create the row type
    std::unique_ptr<Type> rowType = createStructType();
    rowType->addStructField("myInt", createPrimitiveType(INT));
    rowType->addStructField("myString", createPrimitiveType(STRING));

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);
    StructVectorBatch batch(100, *getDefaultPool());
    LongVectorBatch* longBatch = new LongVectorBatch(100, *getDefaultPool());
    StringVectorBatch* stringBatch = new StringVectorBatch(100, *getDefaultPool());
    batch.fields.push_back(longBatch);
    batch.fields.push_back(stringBatch);
    reader->next(batch, 20, 0);
    ASSERT_EQ(20, batch.numElements);
    ASSERT_EQ(20, longBatch->numElements);
    ASSERT_EQ(20, stringBatch->numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(true, longBatch->hasNulls);
    ASSERT_EQ(true, stringBatch->hasNulls);
    for (size_t i = 0; i < 20; ++i) {
      EXPECT_EQ(true, !longBatch->notNull[i]) << "Wrong at " << i;
      EXPECT_EQ(true, !stringBatch->notNull[i]) << "Wrong at " << i;
    }
    reader->skip(30);
    reader->next(batch, 100, 0);
    ASSERT_EQ(100, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(true, !longBatch->hasNulls);
    ASSERT_EQ(true, !stringBatch->hasNulls);
    for (size_t i = 0; i < 10; ++i) {
      for (size_t j = 0; j < 10; ++j) {
        size_t k = 10 * i + j;
        EXPECT_EQ(1, longBatch->notNull[k]) << "Wrong at " << k;
        ASSERT_EQ(2, stringBatch->length[k]) << "Wrong at " << k;
        EXPECT_EQ('0' + static_cast<char>(i), stringBatch->data[k][0]) << "Wrong at " << k;
        EXPECT_EQ('0' + static_cast<char>(j), stringBatch->data[k][1]) << "Wrong at " << k;
      }
    }
    reader->skip(50);
  }

  TEST(TestColumnReader, testBinaryDirect) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    std::vector<bool> selectedColumns(2, true);
    EXPECT_CALL(streams, getSelectedColumns()).WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_)).WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));

    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));

    char blob[200];
    for (size_t i = 0; i < 10; ++i) {
      for (size_t j = 0; j < 10; ++j) {
        blob[2 * (10 * i + j)] = static_cast<char>(i);
        blob[2 * (10 * i + j) + 1] = static_cast<char>(j);
      }
    }
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(testing::Return(new SeekableArrayInputStream(blob, ARRAY_SIZE(blob))));

    const unsigned char buffer[] = {0x61, 0x00, 0x02};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, true))
        .WillRepeatedly(testing::Return(new SeekableArrayInputStream(buffer, ARRAY_SIZE(buffer))));

    // create the row type
    std::unique_ptr<Type> rowType = createStructType();
    rowType->addStructField("col0", createPrimitiveType(BINARY));

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    StructVectorBatch batch(1024, *getDefaultPool());
    StringVectorBatch* strings = new StringVectorBatch(1024, *getDefaultPool());
    batch.fields.push_back(strings);
    for (size_t i = 0; i < 2; ++i) {
      reader->next(batch, 50, 0);
      ASSERT_EQ(50, batch.numElements);
      ASSERT_EQ(true, !batch.hasNulls);
      ASSERT_EQ(50, strings->numElements);
      ASSERT_EQ(true, !strings->hasNulls);
      for (size_t j = 0; j < batch.numElements; ++j) {
        ASSERT_EQ(2, strings->length[j]);
        EXPECT_EQ((50 * i + j) / 10, strings->data[j][0]);
        EXPECT_EQ((50 * i + j) % 10, strings->data[j][1]);
      }
    }
  }

  TEST(TestColumnReader, testBinaryDirectWithNulls) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    std::vector<bool> selectedColumns(2, true);
    EXPECT_CALL(streams, getSelectedColumns()).WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_)).WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));

    const unsigned char buffer1[] = {0x1d, 0xf0};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer1, ARRAY_SIZE(buffer1))));

    char blob[256];
    for (size_t i = 0; i < 8; ++i) {
      for (size_t j = 0; j < 16; ++j) {
        blob[2 * (16 * i + j)] = static_cast<char>('A' + i);
        blob[2 * (16 * i + j) + 1] = static_cast<char>('A' + j);
      }
    }
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(testing::Return(new SeekableArrayInputStream(blob, ARRAY_SIZE(blob))));

    const unsigned char buffer2[] = {0x7d, 0x00, 0x02};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer2, ARRAY_SIZE(buffer2))));

    // create the row type
    std::unique_ptr<Type> rowType = createStructType();
    rowType->addStructField("col0", createPrimitiveType(BINARY));

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    StructVectorBatch batch(1024, *getDefaultPool());
    StringVectorBatch* strings = new StringVectorBatch(1024, *getDefaultPool());
    batch.fields.push_back(strings);
    size_t next = 0;
    for (size_t i = 0; i < 2; ++i) {
      reader->next(batch, 128, 0);
      ASSERT_EQ(128, batch.numElements);
      ASSERT_EQ(true, !batch.hasNulls);
      ASSERT_EQ(128, strings->numElements);
      ASSERT_EQ(true, strings->hasNulls);
      for (size_t j = 0; j < batch.numElements; ++j) {
        ASSERT_EQ(((128 * i + j) & 4) == 0, strings->notNull[j]);
        if (strings->notNull[j]) {
          ASSERT_EQ(2, strings->length[j]);
          EXPECT_EQ('A' + static_cast<char>(next / 16), strings->data[j][0]);
          EXPECT_EQ('A' + static_cast<char>(next % 16), strings->data[j][1]);
          next += 1;
        }
      }
    }
  }

  TEST(TestColumnReader, testShortBlobError) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    std::vector<bool> selectedColumns(2, true);
    EXPECT_CALL(streams, getSelectedColumns()).WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_)).WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));

    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));

    char blob[100];
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(testing::Return(new SeekableArrayInputStream(blob, ARRAY_SIZE(blob))));

    const unsigned char buffer1[] = {0x61, 0x00, 0x02};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer1, ARRAY_SIZE(buffer1))));

    // create the row type
    std::unique_ptr<Type> rowType = createStructType();
    rowType->addStructField("col0", createPrimitiveType(STRING));

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    StructVectorBatch batch(1024, *getDefaultPool());
    StringVectorBatch* strings = new StringVectorBatch(1024, *getDefaultPool());
    batch.fields.push_back(strings);
    EXPECT_THROW(reader->next(batch, 100, 0), ParseError);
  }

  TEST_P(TestColumnReaderEncoded, testStringDirectShortBuffer) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    std::vector<bool> selectedColumns(2, true);
    EXPECT_CALL(streams, getSelectedColumns()).WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_)).WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));

    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));

    char blob[200];
    for (size_t i = 0; i < 10; ++i) {
      for (size_t j = 0; j < 10; ++j) {
        blob[2 * (10 * i + j)] = static_cast<char>(i);
        blob[2 * (10 * i + j) + 1] = static_cast<char>(j);
      }
    }
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(testing::Return(new SeekableArrayInputStream(blob, ARRAY_SIZE(blob), 3)));

    const unsigned char buffer1[] = {0x61, 0x00, 0x02};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer1, ARRAY_SIZE(buffer1))));

    // create the row type
    std::unique_ptr<Type> rowType = createStructType();
    rowType->addStructField("col0", createPrimitiveType(STRING));

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    StructVectorBatch batch(25, *getDefaultPool());
    StringVectorBatch* strings = new StringVectorBatch(25, *getDefaultPool());
    batch.fields.push_back(strings);
    for (size_t i = 0; i < 4; ++i) {
      if (encoded) {
        reader->nextEncoded(batch, 25, 0);
      } else {
        reader->next(batch, 25, 0);
      }
      ASSERT_EQ(25, batch.numElements);
      ASSERT_EQ(true, !batch.hasNulls);
      ASSERT_EQ(25, strings->numElements);
      ASSERT_EQ(true, !strings->hasNulls);
      for (size_t j = 0; j < batch.numElements; ++j) {
        ASSERT_EQ(2, strings->length[j]);
        EXPECT_EQ((25 * i + j) / 10, strings->data[j][0]);
        EXPECT_EQ((25 * i + j) % 10, strings->data[j][1]);
      }
    }
  }

  TEST_P(TestColumnReaderEncoded, testStringDirectShortBufferWithNulls) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    std::vector<bool> selectedColumns(2, true);
    EXPECT_CALL(streams, getSelectedColumns()).WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_)).WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));

    const unsigned char buffer1[] = {0x3d, 0xf0};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer1, ARRAY_SIZE(buffer1))));

    char blob[512];
    for (size_t i = 0; i < 16; ++i) {
      for (size_t j = 0; j < 16; ++j) {
        blob[2 * (16 * i + j)] = static_cast<char>('A' + i);
        blob[2 * (16 * i + j) + 1] = static_cast<char>('A' + j);
      }
    }
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(testing::Return(new SeekableArrayInputStream(blob, ARRAY_SIZE(blob), 30)));

    const unsigned char buffer2[] = {0x7d, 0x00, 0x02, 0x7d, 0x00, 0x02};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer2, ARRAY_SIZE(buffer2))));

    // create the row type
    std::unique_ptr<Type> rowType = createStructType();
    rowType->addStructField("col0", createPrimitiveType(STRING));

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    StructVectorBatch batch(64, *getDefaultPool());
    StringVectorBatch* strings = new StringVectorBatch(64, *getDefaultPool());
    batch.fields.push_back(strings);
    size_t next = 0;
    for (size_t i = 0; i < 8; ++i) {
      if (encoded) {
        reader->nextEncoded(batch, 64, 0);
      } else {
        reader->next(batch, 64, 0);
      }
      ASSERT_EQ(64, batch.numElements);
      ASSERT_EQ(true, !batch.hasNulls);
      ASSERT_EQ(64, strings->numElements);
      ASSERT_EQ(true, strings->hasNulls);
      for (size_t j = 0; j < batch.numElements; ++j) {
        ASSERT_EQ((j & 4) == 0, strings->notNull[j]);
        if (strings->notNull[j]) {
          ASSERT_EQ(2, strings->length[j]);
          EXPECT_EQ('A' + next / 16, strings->data[j][0]);
          EXPECT_EQ('A' + next % 16, strings->data[j][1]);
          next += 1;
        }
      }
    }
  }

  /**
   * Tests ORC-24.
   * Requires:
   *   * direct string encoding
   *   * a null value where the unused length crosses the streaming block
   *     and the actual value doesn't
   */
  TEST(TestColumnReader, testStringDirectNullAcrossWindow) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    std::vector<bool> selectedColumns(2, true);
    EXPECT_CALL(streams, getSelectedColumns()).WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_)).WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));

    const unsigned char isNull[2] = {0xff, 0x7f};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(new SeekableArrayInputStream(isNull, ARRAY_SIZE(isNull))));

    const char blob[] = "abcdefg";
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(testing::Return(new SeekableArrayInputStream(blob, ARRAY_SIZE(blob), 4)));

    // [1] * 7
    const unsigned char lenData[] = {0x04, 0x00, 0x01};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(lenData, ARRAY_SIZE(lenData))));

    // create the row type
    std::unique_ptr<Type> rowType = createStructType();
    rowType->addStructField("col0", createPrimitiveType(STRING));

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    StructVectorBatch batch(25, *getDefaultPool());
    StringVectorBatch* strings = new StringVectorBatch(25, *getDefaultPool());
    batch.fields.push_back(strings);
    // This length value won't be overwritten because the value is null,
    // but it induces the problem.
    strings->length[0] = 5;
    reader->next(batch, 8, 0);
    ASSERT_EQ(8, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(8, strings->numElements);
    ASSERT_EQ(true, strings->hasNulls);
    ASSERT_EQ(true, !strings->notNull[0]);
    for (size_t j = 1; j < batch.numElements; ++j) {
      ASSERT_EQ(true, strings->notNull[j]);
      ASSERT_EQ(1, strings->length[j]);
      ASSERT_EQ('a' + j - 1, strings->data[j][0]) << "difference at " << j;
    }
  }

  TEST(TestColumnReader, testStringDirectSkip) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    std::vector<bool> selectedColumns(2, true);
    EXPECT_CALL(streams, getSelectedColumns()).WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_)).WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));

    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));

    // sum(0 to 1199)
    const size_t BLOB_SIZE = 719400;
    char blob[BLOB_SIZE];
    size_t posn = 0;
    for (size_t item = 0; item < 1200; ++item) {
      for (size_t ch = 0; ch < item; ++ch) {
        blob[posn++] = static_cast<char>(ch);
      }
    }
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(testing::Return(new SeekableArrayInputStream(blob, BLOB_SIZE, 200)));

    // the stream of 0 to 1199
    const unsigned char buffer1[] = {0x7f, 0x01, 0x00, 0x7f, 0x01, 0x82, 0x01, 0x7f, 0x01, 0x84,
                                     0x02, 0x7f, 0x01, 0x86, 0x03, 0x7f, 0x01, 0x88, 0x04, 0x7f,
                                     0x01, 0x8a, 0x05, 0x7f, 0x01, 0x8c, 0x06, 0x7f, 0x01, 0x8e,
                                     0x07, 0x7f, 0x01, 0x90, 0x08, 0x1b, 0x01, 0x92, 0x09};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer1, ARRAY_SIZE(buffer1))));

    // create the row type
    std::unique_ptr<Type> rowType = createStructType();
    rowType->addStructField("col0", createPrimitiveType(STRING));

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    StructVectorBatch batch(2, *getDefaultPool());
    StringVectorBatch* strings = new StringVectorBatch(2, *getDefaultPool());
    batch.fields.push_back(strings);
    reader->next(batch, 2, 0);
    ASSERT_EQ(2, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(2, strings->numElements);
    ASSERT_EQ(true, !strings->hasNulls);
    for (size_t i = 0; i < batch.numElements; ++i) {
      ASSERT_EQ(i, strings->length[i]);
      for (size_t j = 0; j < i; ++j) {
        EXPECT_EQ(static_cast<char>(j), strings->data[i][j]);
      }
    }
    reader->skip(14);
    reader->next(batch, 2, 0);
    ASSERT_EQ(2, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(2, strings->numElements);
    ASSERT_EQ(true, !strings->hasNulls);
    for (size_t i = 0; i < batch.numElements; ++i) {
      ASSERT_EQ(16 + i, strings->length[i]);
      for (size_t j = 0; j < 16 + i; ++j) {
        EXPECT_EQ(static_cast<char>(j), strings->data[i][j]);
      }
    }
    reader->skip(1180);
    reader->next(batch, 2, 0);
    ASSERT_EQ(2, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(2, strings->numElements);
    ASSERT_EQ(true, !strings->hasNulls);
    for (size_t i = 0; i < batch.numElements; ++i) {
      ASSERT_EQ(1198 + i, strings->length[i]);
      for (size_t j = 0; j < 1198 + i; ++j) {
        EXPECT_EQ(static_cast<char>(j), strings->data[i][j]);
      }
    }
  }

  TEST(TestColumnReader, testStringDirectSkipWithNulls) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    std::vector<bool> selectedColumns(2, true);
    EXPECT_CALL(streams, getSelectedColumns()).WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_)).WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));

    // alternate 4 non-null and 4 null via [0xf0 for x in range(2400 / 8)]
    const unsigned char buffer1[] = {0x7f, 0xf0, 0x7f, 0xf0, 0x25, 0xf0};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer1, ARRAY_SIZE(buffer1))));

    // sum(range(1200))
    const size_t BLOB_SIZE = 719400;

    // each string is [x % 256 for x in range(r)]
    char blob[BLOB_SIZE];
    size_t posn = 0;
    for (size_t item = 0; item < 1200; ++item) {
      for (size_t ch = 0; ch < item; ++ch) {
        blob[posn++] = static_cast<char>(ch);
      }
    }
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(testing::Return(new SeekableArrayInputStream(blob, BLOB_SIZE, 200)));

    // range(1200)
    const unsigned char buffer2[] = {0x7f, 0x01, 0x00, 0x7f, 0x01, 0x82, 0x01, 0x7f, 0x01, 0x84,
                                     0x02, 0x7f, 0x01, 0x86, 0x03, 0x7f, 0x01, 0x88, 0x04, 0x7f,
                                     0x01, 0x8a, 0x05, 0x7f, 0x01, 0x8c, 0x06, 0x7f, 0x01, 0x8e,
                                     0x07, 0x7f, 0x01, 0x90, 0x08, 0x1b, 0x01, 0x92, 0x09};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer2, ARRAY_SIZE(buffer2))));

    // create the row type
    std::unique_ptr<Type> rowType = createStructType();
    rowType->addStructField("col0", createPrimitiveType(STRING));

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    StructVectorBatch batch(2, *getDefaultPool());
    StringVectorBatch* strings = new StringVectorBatch(2, *getDefaultPool());
    batch.fields.push_back(strings);
    reader->next(batch, 2, 0);
    ASSERT_EQ(2, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(2, strings->numElements);
    ASSERT_EQ(true, !strings->hasNulls);
    for (size_t i = 0; i < batch.numElements; ++i) {
      ASSERT_EQ(i, strings->length[i]);
      for (size_t j = 0; j < i; ++j) {
        EXPECT_EQ(static_cast<char>(j), strings->data[i][j]);
      }
    }
    reader->skip(30);
    reader->next(batch, 2, 0);
    ASSERT_EQ(2, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(2, strings->numElements);
    ASSERT_EQ(true, !strings->hasNulls);
    for (size_t i = 0; i < batch.numElements; ++i) {
      ASSERT_EQ(16 + i, strings->length[i]);
      for (size_t j = 0; j < 16 + i; ++j) {
        EXPECT_EQ(static_cast<char>(j), strings->data[i][j]);
      }
    }
    reader->skip(2364);
    reader->next(batch, 2, 0);
    ASSERT_EQ(2, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(2, strings->numElements);
    ASSERT_EQ(true, strings->hasNulls);
    for (size_t i = 0; i < batch.numElements; ++i) {
      EXPECT_EQ(true, !strings->notNull[i]);
    }
  }

  TEST_P(TestColumnReaderEncoded, testList) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    std::vector<bool> selectedColumns(3, true);
    EXPECT_CALL(streams, getSelectedColumns()).WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_)).WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(testing::_, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));

    // [2 for x in range(600)]
    const unsigned char buffer1[] = {0x7f, 0x00, 0x02, 0x7f, 0x00, 0x02, 0x7f, 0x00,
                                     0x02, 0x7f, 0x00, 0x02, 0x4d, 0x00, 0x02};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer1, ARRAY_SIZE(buffer1))));

    // range(1200)
    const unsigned char buffer2[] = {0x7f, 0x01, 0x00, 0x7f, 0x01, 0x84, 0x02, 0x7f, 0x01, 0x88,
                                     0x04, 0x7f, 0x01, 0x8c, 0x06, 0x7f, 0x01, 0x90, 0x08, 0x7f,
                                     0x01, 0x94, 0x0a, 0x7f, 0x01, 0x98, 0x0c, 0x7f, 0x01, 0x9c,
                                     0x0e, 0x7f, 0x01, 0xa0, 0x10, 0x1b, 0x01, 0xa4, 0x12};
    EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer2, ARRAY_SIZE(buffer2))));

    // create the row type
    std::unique_ptr<Type> rowType = createStructType();
    rowType->addStructField("col0", createListType(createPrimitiveType(LONG)));

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    StructVectorBatch batch(512, *getDefaultPool());
    ListVectorBatch* lists = new ListVectorBatch(512, *getDefaultPool());
    LongVectorBatch* longs = new LongVectorBatch(512, *getDefaultPool());
    batch.fields.push_back(lists);
    lists->elements = std::unique_ptr<ColumnVectorBatch>(longs);
    if (encoded) {
      reader->nextEncoded(batch, 512, 0);
    } else {
      reader->next(batch, 512, 0);
    }
    ASSERT_EQ(512, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(512, lists->numElements);
    ASSERT_EQ(true, !lists->hasNulls);
    ASSERT_EQ(1024, longs->numElements);
    ASSERT_EQ(true, !longs->hasNulls);
    for (size_t i = 0; i <= batch.numElements; ++i) {
      EXPECT_EQ(2 * i, lists->offsets[i]);
    }
    for (size_t i = 0; i < longs->numElements; ++i) {
      EXPECT_EQ(i, longs->data[i]);
    }
  }

  TEST(TestColumnReader, testListPropagateNulls) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    std::vector<bool> selectedColumns(4, true);
    EXPECT_CALL(streams, getSelectedColumns()).WillRepeatedly(testing::Return(selectedColumns));

    std::unique_ptr<Type> innerType = createStructType();
    innerType->addStructField("col0_0", createListType(createPrimitiveType(LONG)));
    std::unique_ptr<Type> rowType = createStructType();
    rowType->addStructField("col0", std::move(innerType));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_)).WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(testing::_, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));

    // set getStream
    const unsigned char buffer[] = {0xff, 0x00};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(new SeekableArrayInputStream(buffer, ARRAY_SIZE(buffer))));

    EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_LENGTH, true))
        .WillRepeatedly(testing::Return(new SeekableArrayInputStream(buffer, 0)));

    EXPECT_CALL(streams, getStreamProxy(3, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(testing::Return(new SeekableArrayInputStream(buffer, 0)));

    // create the row type
    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    StructVectorBatch batch(512, *getDefaultPool());
    StructVectorBatch* structs = new StructVectorBatch(512, *getDefaultPool());
    ListVectorBatch* lists = new ListVectorBatch(512, *getDefaultPool());
    LongVectorBatch* longs = new LongVectorBatch(512, *getDefaultPool());
    batch.fields.push_back(structs);
    structs->fields.push_back(lists);
    lists->elements = std::unique_ptr<ColumnVectorBatch>(longs);
    reader->next(batch, 8, 0);
    ASSERT_EQ(8, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(8, structs->numElements);
    ASSERT_EQ(true, structs->hasNulls);
    ASSERT_EQ(8, lists->numElements);
    ASSERT_EQ(true, lists->hasNulls);
    ASSERT_EQ(0, longs->numElements);
    ASSERT_EQ(true, !longs->hasNulls);
    for (size_t i = 0; i < 8; ++i) {
      EXPECT_EQ(true, !structs->notNull[i]);
    }
  }

  TEST(TestColumnReader, testListWithNulls) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    std::vector<bool> selectedColumns(3, true);
    EXPECT_CALL(streams, getSelectedColumns()).WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_)).WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));

    // [0xaa for x in range(2048/8)]
    const unsigned char buffer1[] = {0x7f, 0xaa, 0x7b, 0xaa};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer1, ARRAY_SIZE(buffer1))));

    EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));

    // [1 for x in range(260)] +
    // [4 for x in range(260)] +
    // [0 for x in range(260)] +
    // [3 for x in range(243)] +
    // [19]
    const unsigned char buffer2[] = {0x7f, 0x00, 0x01, 0x7f, 0x00, 0x01, 0x7f, 0x00, 0x04,
                                     0x7f, 0x00, 0x04, 0x7f, 0x00, 0x00, 0x7f, 0x00, 0x00,
                                     0x7f, 0x00, 0x03, 0x6e, 0x00, 0x03, 0xff, 0x13};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer2, ARRAY_SIZE(buffer2))));

    // range(2048)
    const unsigned char buffer3[] = {
        0x7f, 0x01, 0x00, 0x7f, 0x01, 0x84, 0x02, 0x7f, 0x01, 0x88, 0x04, 0x7f, 0x01,
        0x8c, 0x06, 0x7f, 0x01, 0x90, 0x08, 0x7f, 0x01, 0x94, 0x0a, 0x7f, 0x01, 0x98,
        0x0c, 0x7f, 0x01, 0x9c, 0x0e, 0x7f, 0x01, 0xa0, 0x10, 0x7f, 0x01, 0xa4, 0x12,
        0x7f, 0x01, 0xa8, 0x14, 0x7f, 0x01, 0xac, 0x16, 0x7f, 0x01, 0xb0, 0x18, 0x7f,
        0x01, 0xb4, 0x1a, 0x7f, 0x01, 0xb8, 0x1c, 0x5f, 0x01, 0xbc, 0x1e};
    EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer3, ARRAY_SIZE(buffer3))));

    // create the row type
    std::unique_ptr<Type> rowType = createStructType();
    rowType->addStructField("col0", createListType(createPrimitiveType(LONG)));

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    StructVectorBatch batch(512, *getDefaultPool());
    ListVectorBatch* lists = new ListVectorBatch(512, *getDefaultPool());
    LongVectorBatch* longs = new LongVectorBatch(512, *getDefaultPool());
    batch.fields.push_back(lists);
    lists->elements = std::unique_ptr<ColumnVectorBatch>(longs);
    reader->next(batch, 512, 0);
    ASSERT_EQ(512, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(512, lists->numElements);
    ASSERT_EQ(true, lists->hasNulls);
    ASSERT_EQ(256, longs->numElements);
    ASSERT_EQ(true, !longs->hasNulls);
    for (size_t i = 0; i < batch.numElements; ++i) {
      EXPECT_EQ(i % 2 == 0, lists->notNull[i]) << "Wrong value at " << i;
      EXPECT_EQ((i + 1) / 2, lists->offsets[i]) << "Wrong value at " << i;
    }
    EXPECT_EQ(256, lists->offsets[512]);
    for (size_t i = 0; i < longs->numElements; ++i) {
      EXPECT_EQ(i, longs->data[i]);
    }

    reader->next(batch, 512, 0);
    ASSERT_EQ(512, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(512, lists->numElements);
    ASSERT_EQ(true, lists->hasNulls);
    ASSERT_EQ(1012, longs->numElements);
    ASSERT_EQ(true, !longs->hasNulls);
    for (size_t i = 0; i < batch.numElements; ++i) {
      EXPECT_EQ(i % 2 == 0, lists->notNull[i]) << "Wrong value at " << i;
      if (i < 8) {
        EXPECT_EQ((i + 1) / 2, lists->offsets[i]) << "Wrong value at " << i;
      } else {
        EXPECT_EQ(4 * ((i + 1) / 2) - 12, lists->offsets[i]) << "Wrong value at " << i;
      }
    }
    EXPECT_EQ(1012, lists->offsets[512]);
    for (size_t i = 0; i < longs->numElements; ++i) {
      EXPECT_EQ(256 + i, longs->data[i]);
    }

    reader->next(batch, 512, 0);
    ASSERT_EQ(512, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(512, lists->numElements);
    ASSERT_EQ(true, lists->hasNulls);
    ASSERT_EQ(32, longs->numElements);
    ASSERT_EQ(true, !longs->hasNulls);
    for (size_t i = 0; i < batch.numElements; ++i) {
      EXPECT_EQ(i % 2 == 0, lists->notNull[i]) << "Wrong value at " << i;
      if (i < 16) {
        EXPECT_EQ(4 * ((i + 1) / 2), lists->offsets[i]) << "Wrong value at " << i;
      } else {
        EXPECT_EQ(32, lists->offsets[i]) << "Wrong value at " << i;
      }
    }
    EXPECT_EQ(32, lists->offsets[512]);
    for (size_t i = 0; i < longs->numElements; ++i) {
      EXPECT_EQ(1268 + i, longs->data[i]);
    }

    reader->next(batch, 512, 0);
    ASSERT_EQ(512, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(512, lists->numElements);
    ASSERT_EQ(true, lists->hasNulls);
    ASSERT_EQ(748, longs->numElements);
    ASSERT_EQ(true, !longs->hasNulls);
    for (size_t i = 0; i < batch.numElements; ++i) {
      EXPECT_EQ(i % 2 == 0, lists->notNull[i]) << "Wrong value at " << i;
      if (i < 24) {
        EXPECT_EQ(0, lists->offsets[i]) << "Wrong value at " << i;
      } else if (i < 510) {
        EXPECT_EQ(3 * ((i - 23) / 2), lists->offsets[i]) << "Wrong value at " << i;
      } else if (i < 511) {
        EXPECT_EQ(729, lists->offsets[i]) << "Wrong value at " << i;
      } else {
        EXPECT_EQ(748, lists->offsets[i]) << "Wrong value at " << i;
      }
    }
    EXPECT_EQ(748, lists->offsets[512]);
    for (size_t i = 0; i < longs->numElements; ++i) {
      EXPECT_EQ(1300 + i, longs->data[i]);
    }
  }

  TEST(TestColumnReader, testListSkipWithNulls) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    std::vector<bool> selectedColumns(3, true);
    EXPECT_CALL(streams, getSelectedColumns()).WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_)).WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));

    // [0xaa for x in range(2048/8)]
    const unsigned char buffer1[] = {0x7f, 0xaa, 0x7b, 0xaa};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer1, ARRAY_SIZE(buffer1))));

    EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));

    // [1 for x in range(260)] +
    // [4 for x in range(260)] +
    // [0 for x in range(260)] +
    // [3 for x in range(243)] +
    // [19]
    const unsigned char buffer2[] = {0x7f, 0x00, 0x01, 0x7f, 0x00, 0x01, 0x7f, 0x00, 0x04,
                                     0x7f, 0x00, 0x04, 0x7f, 0x00, 0x00, 0x7f, 0x00, 0x00,
                                     0x7f, 0x00, 0x03, 0x6e, 0x00, 0x03, 0xff, 0x13};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer2, ARRAY_SIZE(buffer2))));

    // range(2048)
    const unsigned char buffer3[] = {
        0x7f, 0x01, 0x00, 0x7f, 0x01, 0x84, 0x02, 0x7f, 0x01, 0x88, 0x04, 0x7f, 0x01,
        0x8c, 0x06, 0x7f, 0x01, 0x90, 0x08, 0x7f, 0x01, 0x94, 0x0a, 0x7f, 0x01, 0x98,
        0x0c, 0x7f, 0x01, 0x9c, 0x0e, 0x7f, 0x01, 0xa0, 0x10, 0x7f, 0x01, 0xa4, 0x12,
        0x7f, 0x01, 0xa8, 0x14, 0x7f, 0x01, 0xac, 0x16, 0x7f, 0x01, 0xb0, 0x18, 0x7f,
        0x01, 0xb4, 0x1a, 0x7f, 0x01, 0xb8, 0x1c, 0x5f, 0x01, 0xbc, 0x1e};
    EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer3, ARRAY_SIZE(buffer3))));

    // create the row type
    std::unique_ptr<Type> rowType = createStructType();
    rowType->addStructField("col0", createListType(createPrimitiveType(LONG)));

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    StructVectorBatch batch(1, *getDefaultPool());
    ListVectorBatch* lists = new ListVectorBatch(1, *getDefaultPool());
    LongVectorBatch* longs = new LongVectorBatch(1, *getDefaultPool());
    batch.fields.push_back(lists);
    lists->elements = std::unique_ptr<ColumnVectorBatch>(longs);

    reader->next(batch, 1, 0);
    ASSERT_EQ(1, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(1, lists->numElements);
    ASSERT_EQ(true, !lists->hasNulls);
    ASSERT_EQ(1, longs->numElements);
    ASSERT_EQ(true, !longs->hasNulls);
    EXPECT_EQ(0, lists->offsets[0]);
    EXPECT_EQ(1, lists->offsets[1]);
    EXPECT_EQ(0, longs->data[0]);

    reader->skip(13);
    reader->next(batch, 1, 0);
    ASSERT_EQ(1, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(1, lists->numElements);
    ASSERT_EQ(true, !lists->hasNulls);
    ASSERT_EQ(1, longs->numElements);
    ASSERT_EQ(true, !longs->hasNulls);
    EXPECT_EQ(0, lists->offsets[0]);
    EXPECT_EQ(1, lists->offsets[1]);
    EXPECT_EQ(7, longs->data[0]);

    reader->skip(2031);
    reader->next(batch, 2, 0);
    ASSERT_EQ(2, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(2, lists->numElements);
    ASSERT_EQ(true, lists->hasNulls);
    ASSERT_EQ(19, longs->numElements);
    ASSERT_EQ(true, !longs->hasNulls);
    EXPECT_EQ(0, lists->offsets[0]);
    EXPECT_EQ(19, lists->offsets[1]);
    EXPECT_EQ(19, lists->offsets[2]);
    for (size_t i = 0; i < longs->numElements; ++i) {
      EXPECT_EQ(2029 + i, longs->data[i]);
    }
  }

  TEST(TestColumnReader, testListSkipWithNullsNoData) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    std::vector<bool> selectedColumns(2, true);
    selectedColumns.push_back(false);
    EXPECT_CALL(streams, getSelectedColumns()).WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_)).WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));

    // [0xaa for x in range(2048/8)]
    const unsigned char buffer1[] = {0x7f, 0xaa, 0x7b, 0xaa};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer1, ARRAY_SIZE(buffer1))));

    EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));

    // [1 for x in range(260)] +
    // [4 for x in range(260)] +
    // [0 for x in range(260)] +
    // [3 for x in range(243)] +
    // [19]
    const unsigned char buffer2[] = {0x7f, 0x00, 0x01, 0x7f, 0x00, 0x01, 0x7f, 0x00, 0x04,
                                     0x7f, 0x00, 0x04, 0x7f, 0x00, 0x00, 0x7f, 0x00, 0x00,
                                     0x7f, 0x00, 0x03, 0x6e, 0x00, 0x03, 0xff, 0x13};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer2, ARRAY_SIZE(buffer2))));

    EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(testing::Return(nullptr));

    // create the row type
    std::unique_ptr<Type> rowType = createStructType();
    rowType->addStructField("col0", createListType(createPrimitiveType(LONG)));

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    StructVectorBatch batch(1, *getDefaultPool());
    ListVectorBatch* lists = new ListVectorBatch(1, *getDefaultPool());
    batch.fields.push_back(lists);

    reader->next(batch, 1, 0);
    ASSERT_EQ(1, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(1, lists->numElements);
    ASSERT_EQ(true, !lists->hasNulls);
    EXPECT_EQ(0, lists->offsets[0]);
    EXPECT_EQ(1, lists->offsets[1]);

    reader->skip(13);
    reader->next(batch, 1, 0);
    ASSERT_EQ(1, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(1, lists->numElements);
    ASSERT_EQ(true, !lists->hasNulls);
    EXPECT_EQ(0, lists->offsets[0]);
    EXPECT_EQ(1, lists->offsets[1]);

    reader->skip(2031);
    reader->next(batch, 2, 0);
    ASSERT_EQ(2, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(2, lists->numElements);
    ASSERT_EQ(true, lists->hasNulls);
    EXPECT_EQ(0, lists->offsets[0]);
    EXPECT_EQ(19, lists->offsets[1]);
    EXPECT_EQ(19, lists->offsets[2]);
  }

  TEST_P(TestColumnReaderEncoded, testMap) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    std::vector<bool> selectedColumns(4, true);
    EXPECT_CALL(streams, getSelectedColumns()).WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_)).WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(testing::_, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));

    // [2 for x in range(600)]
    const unsigned char buffer1[] = {0x7f, 0x00, 0x02, 0x7f, 0x00, 0x02, 0x7f, 0x00,
                                     0x02, 0x7f, 0x00, 0x02, 0x4d, 0x00, 0x02};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer1, ARRAY_SIZE(buffer1))));

    // range(1200)
    const unsigned char buffer2[] = {0x7f, 0x01, 0x00, 0x7f, 0x01, 0x84, 0x02, 0x7f, 0x01, 0x88,
                                     0x04, 0x7f, 0x01, 0x8c, 0x06, 0x7f, 0x01, 0x90, 0x08, 0x7f,
                                     0x01, 0x94, 0x0a, 0x7f, 0x01, 0x98, 0x0c, 0x7f, 0x01, 0x9c,
                                     0x0e, 0x7f, 0x01, 0xa0, 0x10, 0x1b, 0x01, 0xa4, 0x12};
    EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer2, ARRAY_SIZE(buffer2))));

    // range(8, 1208)
    const unsigned char buffer3[] = {0x7f, 0x01, 0x10, 0x7f, 0x01, 0x94, 0x02, 0x7f, 0x01, 0x98,
                                     0x04, 0x7f, 0x01, 0x9c, 0x06, 0x7f, 0x01, 0xa0, 0x08, 0x7f,
                                     0x01, 0xa4, 0x0a, 0x7f, 0x01, 0xa8, 0x0c, 0x7f, 0x01, 0xac,
                                     0x0e, 0x7f, 0x01, 0xb0, 0x10, 0x1b, 0x01, 0xb4, 0x12};
    EXPECT_CALL(streams, getStreamProxy(3, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer3, ARRAY_SIZE(buffer3))));

    // create the row type
    std::unique_ptr<Type> rowType = createStructType();
    rowType->addStructField("col0",
                            createMapType(createPrimitiveType(LONG), createPrimitiveType(LONG)));

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    StructVectorBatch batch(512, *getDefaultPool());
    MapVectorBatch* maps = new MapVectorBatch(512, *getDefaultPool());
    LongVectorBatch* keys = new LongVectorBatch(512, *getDefaultPool());
    LongVectorBatch* elements = new LongVectorBatch(512, *getDefaultPool());
    batch.fields.push_back(maps);
    maps->keys = std::unique_ptr<ColumnVectorBatch>(keys);
    maps->elements = std::unique_ptr<ColumnVectorBatch>(elements);
    if (encoded) {
      reader->nextEncoded(batch, 512, 0);
    } else {
      reader->next(batch, 512, 0);
    }
    ASSERT_EQ(512, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(512, maps->numElements);
    ASSERT_EQ(true, !maps->hasNulls);
    ASSERT_EQ(1024, keys->numElements);
    ASSERT_EQ(true, !keys->hasNulls);
    ASSERT_EQ(1024, elements->numElements);
    ASSERT_EQ(true, !elements->hasNulls);
    for (size_t i = 0; i <= batch.numElements; ++i) {
      EXPECT_EQ(2 * i, maps->offsets[i]);
    }
    for (size_t i = 0; i < keys->numElements; ++i) {
      EXPECT_EQ(i, keys->data[i]);
      EXPECT_EQ(i + 8, elements->data[i]);
    }
  }

  TEST(TestColumnReader, testMapWithNulls) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    std::vector<bool> selectedColumns(4, true);
    EXPECT_CALL(streams, getSelectedColumns()).WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_)).WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));

    // [0xaa for x in range(2048/8)]
    const unsigned char buffer1[] = {0x7f, 0xaa, 0x7b, 0xaa};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer1, ARRAY_SIZE(buffer1))));

    EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));

    // [0x55 for x in range(2048/8)]
    const unsigned char buffer2[] = {0x7f, 0x55, 0x7b, 0x55};
    EXPECT_CALL(streams, getStreamProxy(3, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer2, ARRAY_SIZE(buffer2))));

    // [1 for x in range(260)] +
    // [4 for x in range(260)] +
    // [0 for x in range(260)] +
    // [3 for x in range(243)] +
    // [19]
    const unsigned char buffer3[] = {0x7f, 0x00, 0x01, 0x7f, 0x00, 0x01, 0x7f, 0x00, 0x04,
                                     0x7f, 0x00, 0x04, 0x7f, 0x00, 0x00, 0x7f, 0x00, 0x00,
                                     0x7f, 0x00, 0x03, 0x6e, 0x00, 0x03, 0xff, 0x13};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer3, ARRAY_SIZE(buffer3))));

    // range(2048)
    const unsigned char buffer4[] = {
        0x7f, 0x01, 0x00, 0x7f, 0x01, 0x84, 0x02, 0x7f, 0x01, 0x88, 0x04, 0x7f, 0x01,
        0x8c, 0x06, 0x7f, 0x01, 0x90, 0x08, 0x7f, 0x01, 0x94, 0x0a, 0x7f, 0x01, 0x98,
        0x0c, 0x7f, 0x01, 0x9c, 0x0e, 0x7f, 0x01, 0xa0, 0x10, 0x7f, 0x01, 0xa4, 0x12,
        0x7f, 0x01, 0xa8, 0x14, 0x7f, 0x01, 0xac, 0x16, 0x7f, 0x01, 0xb0, 0x18, 0x7f,
        0x01, 0xb4, 0x1a, 0x7f, 0x01, 0xb8, 0x1c, 0x5f, 0x01, 0xbc, 0x1e};
    EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer4, ARRAY_SIZE(buffer4))));

    // range(8, 1032)
    const unsigned char buffer5[] = {0x7f, 0x01, 0x10, 0x7f, 0x01, 0x94, 0x02, 0x7f,
                                     0x01, 0x98, 0x04, 0x7f, 0x01, 0x9c, 0x06, 0x7f,
                                     0x01, 0xa0, 0x08, 0x7f, 0x01, 0xa4, 0x0a, 0x7f,
                                     0x01, 0xa8, 0x0c, 0x6f, 0x01, 0xac, 0x0e};
    EXPECT_CALL(streams, getStreamProxy(3, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer5, ARRAY_SIZE(buffer5))));

    // create the row type
    std::unique_ptr<Type> rowType = createStructType();
    rowType->addStructField("col0",
                            createMapType(createPrimitiveType(LONG), createPrimitiveType(LONG)));

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    StructVectorBatch batch(512, *getDefaultPool());
    MapVectorBatch* maps = new MapVectorBatch(512, *getDefaultPool());
    LongVectorBatch* keys = new LongVectorBatch(512, *getDefaultPool());
    LongVectorBatch* elements = new LongVectorBatch(512, *getDefaultPool());
    batch.fields.push_back(maps);
    maps->keys = std::unique_ptr<ColumnVectorBatch>(keys);
    maps->elements = std::unique_ptr<ColumnVectorBatch>(elements);
    reader->next(batch, 512, 0);
    ASSERT_EQ(512, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(512, maps->numElements);
    ASSERT_EQ(true, maps->hasNulls);
    ASSERT_EQ(256, keys->numElements);
    ASSERT_EQ(true, !keys->hasNulls);
    ASSERT_EQ(256, elements->numElements);
    ASSERT_EQ(true, elements->hasNulls);
    for (size_t i = 0; i < batch.numElements; ++i) {
      EXPECT_EQ(i % 2 == 0, maps->notNull[i]) << "Wrong value at " << i;
      EXPECT_EQ((i + 1) / 2, maps->offsets[i]) << "Wrong value at " << i;
    }
    EXPECT_EQ(256, maps->offsets[512]);
    for (size_t i = 0; i < keys->numElements; ++i) {
      EXPECT_EQ(i, keys->data[i]);
      EXPECT_EQ(i & 1, elements->notNull[i]);
      if (elements->notNull[i]) {
        EXPECT_EQ(i / 2 + 8, elements->data[i]);
      }
    }

    reader->next(batch, 512, 0);
    ASSERT_EQ(512, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(512, maps->numElements);
    ASSERT_EQ(true, maps->hasNulls);
    ASSERT_EQ(1012, keys->numElements);
    ASSERT_EQ(true, !keys->hasNulls);
    ASSERT_EQ(1012, elements->numElements);
    ASSERT_EQ(true, elements->hasNulls);
    for (size_t i = 0; i < batch.numElements; ++i) {
      EXPECT_EQ(i % 2 == 0, maps->notNull[i]) << "Wrong value at " << i;
      if (i < 8) {
        EXPECT_EQ((i + 1) / 2, maps->offsets[i]) << "Wrong value at " << i;
      } else {
        EXPECT_EQ(4 * ((i + 1) / 2) - 12, maps->offsets[i]) << "Wrong value at " << i;
      }
    }
    EXPECT_EQ(1012, maps->offsets[512]);
    for (size_t i = 0; i < keys->numElements; ++i) {
      EXPECT_EQ(256 + i, keys->data[i]);
      EXPECT_EQ(i & 1, elements->notNull[i]);
      if (elements->notNull[i]) {
        EXPECT_EQ(128 + 8 + i / 2, elements->data[i]);
      }
    }

    reader->next(batch, 512, 0);
    ASSERT_EQ(512, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(512, maps->numElements);
    ASSERT_EQ(true, maps->hasNulls);
    ASSERT_EQ(32, keys->numElements);
    ASSERT_EQ(true, !keys->hasNulls);
    ASSERT_EQ(32, elements->numElements);
    ASSERT_EQ(true, elements->hasNulls);
    for (size_t i = 0; i < batch.numElements; ++i) {
      EXPECT_EQ(i % 2 == 0, maps->notNull[i]) << "Wrong value at " << i;
      if (i < 16) {
        EXPECT_EQ(4 * ((i + 1) / 2), maps->offsets[i]) << "Wrong value at " << i;
      } else {
        EXPECT_EQ(32, maps->offsets[i]) << "Wrong value at " << i;
      }
    }
    EXPECT_EQ(32, maps->offsets[512]);
    for (size_t i = 0; i < keys->numElements; ++i) {
      EXPECT_EQ(1268 + i, keys->data[i]);
      EXPECT_EQ(i & 1, elements->notNull[i]);
      if (elements->notNull[i]) {
        EXPECT_EQ(634 + 8 + i / 2, elements->data[i]);
      }
    }

    reader->next(batch, 512, 0);
    ASSERT_EQ(512, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(512, maps->numElements);
    ASSERT_EQ(true, maps->hasNulls);
    ASSERT_EQ(748, keys->numElements);
    ASSERT_EQ(true, !keys->hasNulls);
    ASSERT_EQ(748, elements->numElements);
    ASSERT_EQ(true, elements->hasNulls);
    for (size_t i = 0; i < batch.numElements; ++i) {
      EXPECT_EQ(i % 2 == 0, maps->notNull[i]) << "Wrong value at " << i;
      if (i < 24) {
        EXPECT_EQ(0, maps->offsets[i]) << "Wrong value at " << i;
      } else if (i < 510) {
        EXPECT_EQ(3 * ((i - 23) / 2), maps->offsets[i]) << "Wrong value at " << i;
      } else if (i < 511) {
        EXPECT_EQ(729, maps->offsets[i]) << "Wrong value at " << i;
      } else {
        EXPECT_EQ(748, maps->offsets[i]) << "Wrong value at " << i;
      }
    }
    EXPECT_EQ(748, maps->offsets[512]);
    for (size_t i = 0; i < keys->numElements; ++i) {
      EXPECT_EQ(1300 + i, keys->data[i]);
      EXPECT_EQ(i & 1, elements->notNull[i]);
      if (elements->notNull[i]) {
        EXPECT_EQ(650 + 8 + i / 2, elements->data[i]);
      }
    }
  }

  TEST(TestColumnReader, testMapSkipWithNulls) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    std::vector<bool> selectedColumns(4, true);
    EXPECT_CALL(streams, getSelectedColumns()).WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_)).WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(testing::_, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));

    // [0xaa for x in range(2048/8)]
    const unsigned char buffer1[] = {0x7f, 0xaa, 0x7b, 0xaa};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer1, ARRAY_SIZE(buffer1))));

    // [1 for x in range(260)] +
    // [4 for x in range(260)] +
    // [0 for x in range(260)] +
    // [3 for x in range(243)] +
    // [19]
    const unsigned char buffer2[] = {0x7f, 0x00, 0x01, 0x7f, 0x00, 0x01, 0x7f, 0x00, 0x04,
                                     0x7f, 0x00, 0x04, 0x7f, 0x00, 0x00, 0x7f, 0x00, 0x00,
                                     0x7f, 0x00, 0x03, 0x6e, 0x00, 0x03, 0xff, 0x13};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer2, ARRAY_SIZE(buffer2))));

    // range(2048)
    const unsigned char buffer3[] = {
        0x7f, 0x01, 0x00, 0x7f, 0x01, 0x84, 0x02, 0x7f, 0x01, 0x88, 0x04, 0x7f, 0x01,
        0x8c, 0x06, 0x7f, 0x01, 0x90, 0x08, 0x7f, 0x01, 0x94, 0x0a, 0x7f, 0x01, 0x98,
        0x0c, 0x7f, 0x01, 0x9c, 0x0e, 0x7f, 0x01, 0xa0, 0x10, 0x7f, 0x01, 0xa4, 0x12,
        0x7f, 0x01, 0xa8, 0x14, 0x7f, 0x01, 0xac, 0x16, 0x7f, 0x01, 0xb0, 0x18, 0x7f,
        0x01, 0xb4, 0x1a, 0x7f, 0x01, 0xb8, 0x1c, 0x5f, 0x01, 0xbc, 0x1e};
    EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer3, ARRAY_SIZE(buffer3))));

    // range(8, 2056)
    const unsigned char buffer4[] = {
        0x7f, 0x01, 0x10, 0x7f, 0x01, 0x94, 0x02, 0x7f, 0x01, 0x98, 0x04, 0x7f, 0x01,
        0x9c, 0x06, 0x7f, 0x01, 0xa0, 0x08, 0x7f, 0x01, 0xa4, 0x0a, 0x7f, 0x01, 0xa8,
        0x0c, 0x7f, 0x01, 0xac, 0x0e, 0x7f, 0x01, 0xb0, 0x10, 0x7f, 0x01, 0xb4, 0x12,
        0x7f, 0x01, 0xb8, 0x14, 0x7f, 0x01, 0xbc, 0x16, 0x7f, 0x01, 0xc0, 0x18, 0x7f,
        0x01, 0xc4, 0x1a, 0x7f, 0x01, 0xc8, 0x1c, 0x5f, 0x01, 0xcc, 0x1e};
    EXPECT_CALL(streams, getStreamProxy(3, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer4, ARRAY_SIZE(buffer4))));

    // create the row type
    std::unique_ptr<Type> rowType = createStructType();
    rowType->addStructField("col0",
                            createMapType(createPrimitiveType(LONG), createPrimitiveType(LONG)));

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    StructVectorBatch batch(1, *getDefaultPool());
    MapVectorBatch* maps = new MapVectorBatch(1, *getDefaultPool());
    LongVectorBatch* keys = new LongVectorBatch(1, *getDefaultPool());
    LongVectorBatch* elements = new LongVectorBatch(1, *getDefaultPool());
    batch.fields.push_back(maps);
    maps->keys = std::unique_ptr<ColumnVectorBatch>(keys);
    maps->elements = std::unique_ptr<ColumnVectorBatch>(elements);

    reader->next(batch, 1, 0);
    ASSERT_EQ(1, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(1, maps->numElements);
    ASSERT_EQ(true, !maps->hasNulls);
    ASSERT_EQ(1, keys->numElements);
    ASSERT_EQ(true, !keys->hasNulls);
    ASSERT_EQ(1, elements->numElements);
    ASSERT_EQ(true, !elements->hasNulls);
    EXPECT_EQ(0, maps->offsets[0]);
    EXPECT_EQ(1, maps->offsets[1]);
    EXPECT_EQ(0, keys->data[0]);
    EXPECT_EQ(8, elements->data[0]);

    reader->skip(13);
    reader->next(batch, 1, 0);
    ASSERT_EQ(1, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(1, maps->numElements);
    ASSERT_EQ(true, !maps->hasNulls);
    ASSERT_EQ(1, keys->numElements);
    ASSERT_EQ(true, !keys->hasNulls);
    ASSERT_EQ(1, elements->numElements);
    ASSERT_EQ(true, !elements->hasNulls);
    EXPECT_EQ(0, maps->offsets[0]);
    EXPECT_EQ(1, maps->offsets[1]);
    EXPECT_EQ(7, keys->data[0]);
    EXPECT_EQ(7 + 8, elements->data[0]);

    reader->skip(2031);
    reader->next(batch, 2, 0);
    ASSERT_EQ(2, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(2, maps->numElements);
    ASSERT_EQ(true, maps->hasNulls);
    ASSERT_EQ(19, keys->numElements);
    ASSERT_EQ(true, !keys->hasNulls);
    ASSERT_EQ(19, elements->numElements);
    ASSERT_EQ(true, !elements->hasNulls);
    EXPECT_EQ(0, maps->offsets[0]);
    EXPECT_EQ(19, maps->offsets[1]);
    EXPECT_EQ(19, maps->offsets[2]);
    for (size_t i = 0; i < keys->numElements; ++i) {
      EXPECT_EQ(2029 + i, keys->data[i]);
      EXPECT_EQ(2029 + 8 + i, elements->data[i]);
    }
  }

  TEST(TestColumnReader, testMapSkipWithNullsNoData) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    std::vector<bool> selectedColumns(2, true);
    selectedColumns.push_back(false);
    selectedColumns.push_back(false);
    EXPECT_CALL(streams, getSelectedColumns()).WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_)).WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(testing::_, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));

    // [0xaa for x in range(2048/8)]
    const unsigned char buffer1[] = {0x7f, 0xaa, 0x7b, 0xaa};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer1, ARRAY_SIZE(buffer1))));

    // [1 for x in range(260)] +
    // [4 for x in range(260)] +
    // [0 for x in range(260)] +
    // [3 for x in range(243)] +
    // [19]
    const unsigned char buffer2[] = {0x7f, 0x00, 0x01, 0x7f, 0x00, 0x01, 0x7f, 0x00, 0x04,
                                     0x7f, 0x00, 0x04, 0x7f, 0x00, 0x00, 0x7f, 0x00, 0x00,
                                     0x7f, 0x00, 0x03, 0x6e, 0x00, 0x03, 0xff, 0x13};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer2, ARRAY_SIZE(buffer2))));

    // create the row type
    std::unique_ptr<Type> rowType = createStructType();
    rowType->addStructField("col0",
                            createMapType(createPrimitiveType(LONG), createPrimitiveType(LONG)));

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    StructVectorBatch batch(1, *getDefaultPool());
    MapVectorBatch* maps = new MapVectorBatch(1, *getDefaultPool());
    batch.fields.push_back(maps);

    reader->next(batch, 1, 0);
    ASSERT_EQ(1, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(1, maps->numElements);
    ASSERT_EQ(true, !maps->hasNulls);
    EXPECT_EQ(0, maps->offsets[0]);
    EXPECT_EQ(1, maps->offsets[1]);

    reader->skip(13);
    reader->next(batch, 1, 0);
    ASSERT_EQ(1, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(1, maps->numElements);
    ASSERT_EQ(true, !maps->hasNulls);
    EXPECT_EQ(0, maps->offsets[0]);
    EXPECT_EQ(1, maps->offsets[1]);

    reader->skip(2031);
    reader->next(batch, 2, 0);
    ASSERT_EQ(2, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(2, maps->numElements);
    ASSERT_EQ(true, maps->hasNulls);
    EXPECT_EQ(0, maps->offsets[0]);
    EXPECT_EQ(19, maps->offsets[1]);
    EXPECT_EQ(19, maps->offsets[2]);
  }

  TEST(TestColumnReader, testFloatWithNulls) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    std::vector<bool> selectedColumns(2, true);
    EXPECT_CALL(streams, getSelectedColumns()).WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_)).WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));

    // 13 non-nulls followed by 19 nulls
    const unsigned char buffer1[] = {0xfc, 0xff, 0xf8, 0x0, 0x0};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer1, ARRAY_SIZE(buffer1))));

    const float test_vals[] = {1.0f,
                               2.5f,
                               -100.125f,
                               10000.0f,
                               1.234567E23f,
                               -2.3456E-12f,
                               std::numeric_limits<float>::infinity(),
                               std::numeric_limits<float>::quiet_NaN(),
                               -std::numeric_limits<float>::infinity(),
                               std::numeric_limits<float>::max(),
                               -std::numeric_limits<float>::max(),
                               1.4e-45f,
                               -1.4e-45f};
    const unsigned char buffer2[] = {
        0x00, 0x00, 0x80, 0x3f, 0x00, 0x00, 0x20, 0x40, 0x00, 0x40, 0xc8, 0xc2, 0x00,
        0x40, 0x1c, 0x46, 0xcf, 0x24, 0xd1, 0x65, 0x93, 0xe,  0x25, 0xac, 0x0,  0x0,
        0x80, 0x7f, 0x0,  0x0,  0xc0, 0x7f, 0x0,  0x0,  0x80, 0xff, 0xff, 0xff, 0x7f,
        0x7f, 0xff, 0xff, 0x7f, 0xff, 0x1,  0x0,  0x0,  0x0,  0x1,  0x0,  0x0,  0x80};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer2, ARRAY_SIZE(buffer2))));

    // create the row type
    std::unique_ptr<Type> rowType = createStructType();
    rowType->addStructField("myFloat", createPrimitiveType(FLOAT));

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    DoubleVectorBatch* doubleBatch = new DoubleVectorBatch(1024, *getDefaultPool());
    StructVectorBatch batch(1024, *getDefaultPool());
    batch.fields.push_back(doubleBatch);
    reader->next(batch, 32, 0);
    ASSERT_EQ(32, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(32, doubleBatch->numElements);
    ASSERT_EQ(true, doubleBatch->hasNulls);

    for (size_t i = 0; i < batch.numElements; ++i) {
      if (i > 12) {
        EXPECT_EQ(0, doubleBatch->notNull[i]);
      } else if (i == 7) {
        EXPECT_EQ(1, doubleBatch->notNull[i]);
        EXPECT_EQ(true, std::isnan(doubleBatch->data[i]));
      } else {
        EXPECT_EQ(1, doubleBatch->notNull[i]);
        EXPECT_DOUBLE_EQ(static_cast<double>(test_vals[i]), doubleBatch->data[i]);
      }
    }
  }

  TEST(TestColumnReader, testFloatSkipWithNulls) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    std::vector<bool> selectedColumns(2, true);
    EXPECT_CALL(streams, getSelectedColumns()).WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_)).WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));

    // 2 non-nulls, 2 nulls, 2 non-nulls, 2 nulls
    const unsigned char buffer1[] = {0xff, 0xcc};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer1, ARRAY_SIZE(buffer1))));

    // 1, 2.5, -100.125, 10000
    const unsigned char buffer2[] = {0x00, 0x00, 0x80, 0x3f, 0x00, 0x00, 0x20, 0x40,
                                     0x00, 0x40, 0xc8, 0xc2, 0x00, 0x40, 0x1c, 0x46};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer2, ARRAY_SIZE(buffer2))));

    // create the row type
    std::unique_ptr<Type> rowType = createStructType();
    rowType->addStructField("myFloat", createPrimitiveType(FLOAT));

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    DoubleVectorBatch* doubleBatch = new DoubleVectorBatch(1024, *getDefaultPool());
    StructVectorBatch batch(1024, *getDefaultPool());
    batch.fields.push_back(doubleBatch);

    float test_vals[] = {1.0, 2.5, -100.125, 10000.0};
    int vals_ix = 0;

    reader->next(batch, 3, 0);
    ASSERT_EQ(3, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(3, doubleBatch->numElements);
    ASSERT_EQ(true, doubleBatch->hasNulls);

    for (size_t i = 0; i < batch.numElements; ++i) {
      if (i > 1) {
        EXPECT_EQ(0, doubleBatch->notNull[i]);
      } else {
        EXPECT_EQ(1, doubleBatch->notNull[i]);
        EXPECT_DOUBLE_EQ(static_cast<double>(test_vals[vals_ix]), doubleBatch->data[i]);
        vals_ix++;
      }
    }

    reader->skip(1);

    reader->next(batch, 4, 0);
    ASSERT_EQ(4, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(4, doubleBatch->numElements);
    ASSERT_EQ(true, doubleBatch->hasNulls);
    for (size_t i = 0; i < batch.numElements; ++i) {
      if (i > 1) {
        EXPECT_EQ(0, doubleBatch->notNull[i]);
      } else {
        EXPECT_EQ(1, doubleBatch->notNull[i]);
        EXPECT_DOUBLE_EQ(static_cast<double>(test_vals[vals_ix]), doubleBatch->data[i]);
        vals_ix++;
      }
    }
  }

  TEST(TestColumnReader, testDoubleWithNulls) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    std::vector<bool> selectedColumns(2, true);
    EXPECT_CALL(streams, getSelectedColumns()).WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_)).WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));

    // 13 non-nulls followed by 19 nulls
    const unsigned char buffer1[] = {0xfc, 0xff, 0xf8, 0x0, 0x0};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer1, ARRAY_SIZE(buffer1))));

    const double test_vals[] = {1.0,
                                2.0,
                                -2.0,
                                100.0,
                                1.23456789E32,
                                -3.42234E-18,
                                std::numeric_limits<double>::infinity(),
                                std::numeric_limits<double>::quiet_NaN(),
                                -std::numeric_limits<double>::infinity(),
                                1.7976931348623157e308,
                                -1.7976931348623157E308,
                                4.9e-324,
                                -4.9e-324};
    const unsigned char buffer2[] = {
        0x0,  0x0,  0x0,  0x0,  0x0,  0x0,  0xf0, 0x3f, 0x0,  0x0,  0x0,  0x0,  0x0,  0x0,  0x0,
        0x40, 0x0,  0x0,  0x0,  0x0,  0x0,  0x0,  0x0,  0xc0, 0x0,  0x0,  0x0,  0x0,  0x0,  0x0,
        0x59, 0x40, 0xe8, 0x38, 0x65, 0x99, 0xf9, 0x58, 0x98, 0x46, 0xa1, 0x88, 0x41, 0x98, 0xc5,
        0x90, 0x4f, 0xbc, 0x0,  0x0,  0x0,  0x0,  0x0,  0x0,  0xf0, 0x7f, 0x0,  0x0,  0x0,  0x0,
        0x0,  0x0,  0xf8, 0x7f, 0x0,  0x0,  0x0,  0x0,  0x0,  0x0,  0xf0, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xef, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xef, 0xff, 0x1,  0x0,
        0x0,  0x0,  0x0,  0x0,  0x0,  0x0,  0x1,  0x0,  0x0,  0x0,  0x0,  0x0,  0x0,  0x80};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer2, ARRAY_SIZE(buffer2))));

    // create the row type
    std::unique_ptr<Type> rowType = createStructType();
    rowType->addStructField("myDouble", createPrimitiveType(DOUBLE));

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    DoubleVectorBatch* doubleBatch = new DoubleVectorBatch(1024, *getDefaultPool());
    StructVectorBatch batch(1024, *getDefaultPool());
    batch.fields.push_back(doubleBatch);
    reader->next(batch, 32, 0);
    ASSERT_EQ(32, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(32, doubleBatch->numElements);
    ASSERT_EQ(true, doubleBatch->hasNulls);

    for (size_t i = 0; i < batch.numElements; ++i) {
      if (i > 12) {
        EXPECT_EQ(0, doubleBatch->notNull[i]) << "Wrong value at " << i;
      } else if (i == 7) {
        EXPECT_EQ(1, doubleBatch->notNull[i]) << "Wrong value at " << i;
        EXPECT_EQ(true, std::isnan(doubleBatch->data[i]));
      } else {
        EXPECT_EQ(1, doubleBatch->notNull[i]) << "Wrong value at " << i;
        EXPECT_DOUBLE_EQ(test_vals[i], doubleBatch->data[i]) << "Wrong value at " << i;
      }
    }
  }

  TEST(TestColumnReader, testDoubleSkipWithNulls) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    std::vector<bool> selectedColumns(2, true);
    EXPECT_CALL(streams, getSelectedColumns()).WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_)).WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));

    // 1 non-null, 5 nulls, 2 non-nulls
    const unsigned char buffer1[] = {0xff, 0x83};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer1, ARRAY_SIZE(buffer1))));

    // 1, 2, -2
    const unsigned char buffer2[] = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f,
                                     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40,
                                     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xc0};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer2, ARRAY_SIZE(buffer2))));

    // create the row type
    std::unique_ptr<Type> rowType = createStructType();
    rowType->addStructField("myDouble", createPrimitiveType(DOUBLE));

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    DoubleVectorBatch* doubleBatch = new DoubleVectorBatch(1024, *getDefaultPool());
    StructVectorBatch batch(1024, *getDefaultPool());
    batch.fields.push_back(doubleBatch);

    double test_vals[] = {1.0, 2.0, -2.0};
    int vals_ix = 0;

    reader->next(batch, 2, 0);
    ASSERT_EQ(2, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(2, doubleBatch->numElements);
    ASSERT_EQ(true, doubleBatch->hasNulls);

    for (size_t i = 0; i < batch.numElements; ++i) {
      if (i > 0) {
        EXPECT_EQ(0, doubleBatch->notNull[i]);
      } else {
        EXPECT_EQ(1, doubleBatch->notNull[i]);
        EXPECT_DOUBLE_EQ(test_vals[vals_ix], doubleBatch->data[i]);
        vals_ix++;
      }
    }

    reader->skip(3);

    reader->next(batch, 3, 0);
    ASSERT_EQ(3, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(3, doubleBatch->numElements);
    ASSERT_EQ(true, doubleBatch->hasNulls);
    for (size_t i = 0; i < batch.numElements; ++i) {
      if (i < 1) {
        EXPECT_EQ(0, doubleBatch->notNull[i]);
      } else {
        EXPECT_EQ(1, doubleBatch->notNull[i]);
        EXPECT_DOUBLE_EQ(test_vals[vals_ix], doubleBatch->data[i]);
        vals_ix++;
      }
    }
  }

  TEST(TestColumnReader, testTimestampSkipWithNulls) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    std::vector<bool> selectedColumns(2, true);
    EXPECT_CALL(streams, getSelectedColumns()).WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_)).WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));

    // 2 non-nulls, 2 nulls, 2 non-nulls, 2 nulls
    const unsigned char buffer1[] = {0xff, 0xcc};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer1, ARRAY_SIZE(buffer1))));

    const unsigned char buffer2[] = {0xfc, 0xbb, 0xb5, 0xbe, 0x31, 0xa1, 0xee, 0xe2, 0x10,
                                     0xf8, 0x92, 0xee, 0xf,  0x92, 0xa0, 0xd4, 0x30};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer2, ARRAY_SIZE(buffer2))));

    const unsigned char buffer3[] = {0x1, 0x8, 0x5e};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_SECONDARY, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer3, ARRAY_SIZE(buffer3))));

    // create the row type
    std::unique_ptr<Type> rowType = createStructType();
    rowType->addStructField("myTimestamp", createPrimitiveType(TIMESTAMP));

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    TimestampVectorBatch* longBatch = new TimestampVectorBatch(1024, *getDefaultPool());
    StructVectorBatch batch(1024, *getDefaultPool());
    batch.fields.push_back(longBatch);

    const char*(expected[]) = {"Fri May 10 10:40:50 2013\n", "Wed Jun 11 11:41:51 2014\n",
                               "Sun Jul 12 12:42:52 2015\n", "Sat Aug 13 13:43:53 2016\n"};
    int64_t expected_nano[] = {110000000, 120000000, 130000000, 140000000};
    int vals_ix = 0;

    reader->next(batch, 3, 0);
    ASSERT_EQ(3, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(3, longBatch->numElements);
    ASSERT_EQ(true, longBatch->hasNulls);

    for (size_t i = 0; i < batch.numElements; ++i) {
      if (i > 1) {
        EXPECT_EQ(0, longBatch->notNull[i]);
      } else {
        EXPECT_EQ(1, longBatch->notNull[i]);
        time_t time = static_cast<time_t>(longBatch->data[i]);
        tm timeStruct;
        ASSERT_PRED1(isNotNull, gmtime_r(&time, &timeStruct));
        char buffer[30];
        asctime_r(&timeStruct, buffer);
        EXPECT_STREQ(expected[vals_ix], buffer);
        EXPECT_EQ(expected_nano[vals_ix], longBatch->nanoseconds[i]);
        vals_ix++;
      }
    }

    reader->skip(1);

    reader->next(batch, 4, 0);
    ASSERT_EQ(4, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(4, longBatch->numElements);
    ASSERT_EQ(true, longBatch->hasNulls);
    for (size_t i = 0; i < batch.numElements; ++i) {
      if (i > 1) {
        EXPECT_EQ(0, longBatch->notNull[i]);
      } else {
        EXPECT_EQ(1, longBatch->notNull[i]);
        time_t time = static_cast<time_t>(longBatch->data[i]);
        tm timeStruct;
        ASSERT_PRED1(isNotNull, gmtime_r(&time, &timeStruct));
        char buffer[30];
        asctime_r(&timeStruct, buffer);
        EXPECT_STREQ(expected[vals_ix], buffer);
        EXPECT_EQ(expected_nano[vals_ix], longBatch->nanoseconds[i]);
        vals_ix++;
      }
    }
  }

  TEST(TestColumnReader, testTimestamp) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    std::vector<bool> selectedColumns(2, true);
    EXPECT_CALL(streams, getSelectedColumns()).WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_)).WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(testing::_, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));

    const unsigned char buffer1[] = {
        0xf6, 0x9f, 0xf4, 0xc6, 0xbd, 0x03, 0xff, 0xec, 0xf3, 0xbc, 0x03, 0xff, 0xb1,
        0xf8, 0x84, 0x1b, 0x9d, 0x86, 0xd7, 0xfa, 0x1a, 0x9d, 0xb8, 0xcd, 0xdc, 0x1a,
        0x9d, 0xea, 0xc3, 0xbe, 0x1a, 0x9d, 0x9c, 0xba, 0xa0, 0x1a, 0x9d, 0x88, 0xa6,
        0x82, 0x1a, 0x9d, 0xba, 0x9c, 0xe4, 0x19, 0x9d, 0xee, 0xe1, 0xcd, 0x18};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer1, ARRAY_SIZE(buffer1))));

    const unsigned char buffer2[] = {0xf6, 0x00, 0xa8, 0xd1, 0xf9, 0xd6, 0x03, 0x00,
                                     0x9e, 0x01, 0xec, 0x76, 0xf4, 0x76, 0xfc, 0x76,
                                     0x84, 0x77, 0x8c, 0x77, 0xfd, 0x0b};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_SECONDARY, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer2, ARRAY_SIZE(buffer2))));

    // create the row type
    std::unique_ptr<Type> rowType = createStructType();
    rowType->addStructField("myTimestamp", createPrimitiveType(TIMESTAMP));

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    TimestampVectorBatch* longBatch = new TimestampVectorBatch(1024, *getDefaultPool());
    StructVectorBatch batch(1024, *getDefaultPool());
    batch.fields.push_back(longBatch);

    const char*(expected[]) = {"Sun Mar 12 15:00:00 2000\n", "Mon Mar 20 12:00:00 2000\n",
                               "Mon Jan  1 00:00:00 1900\n", "Sat May  5 12:34:56 1900\n",
                               "Sun May  5 12:34:56 1901\n", "Mon May  5 12:34:56 1902\n",
                               "Tue May  5 12:34:56 1903\n", "Thu May  5 12:34:56 1904\n",
                               "Fri May  5 12:34:56 1905\n", "Thu May  5 12:34:56 1910\n"};
    const int64_t expectedNano[] = {0,         123456789, 0,         190000000, 190100000,
                                    190200000, 190300000, 190400000, 190500000, 191000000};

    reader->next(batch, 10, 0);
    ASSERT_EQ(10, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(10, longBatch->numElements);
    ASSERT_EQ(true, !longBatch->hasNulls);

    for (size_t i = 0; i < batch.numElements; ++i) {
      time_t time = static_cast<time_t>(longBatch->data[i]);
      EXPECT_EQ(expectedNano[i], longBatch->nanoseconds[i]);
#ifndef HAS_PRE_1970
      if (time < 0) continue;
#endif
      tm timeStruct;
      ASSERT_PRED1(isNotNull, gmtime_r(&time, &timeStruct));
      char buffer[30];
      asctime_r(&timeStruct, buffer);
      EXPECT_STREQ(expected[i], buffer) << "Wrong value at " << i;
    }
  }

  TEST(DecimalColumnReader, testDecimal64) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    std::vector<bool> selectedColumns(2, true);
    EXPECT_CALL(streams, getSelectedColumns()).WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_)).WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(testing::_, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));

    // [0xff] * (64/8) + [0x00] * (56/8) + [0x01]
    const unsigned char buffer1[] = {0x05, 0xff, 0x04, 0x00, 0xff, 0x01};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer1, ARRAY_SIZE(buffer1))));

    char numBuffer[65];
    for (int i = 0; i < 65; ++i) {
      if (i < 32) {
        numBuffer[i] = static_cast<char>(0x3f - 2 * i);
      } else {
        numBuffer[i] = static_cast<char>(2 * (i - 32));
      }
    }
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(numBuffer, ARRAY_SIZE(numBuffer), 3)));

    // [0x02] * 65
    const unsigned char buffer2[] = {0x3e, 0x00, 0x04};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_SECONDARY, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer2, ARRAY_SIZE(buffer2))));

    // create the row type
    std::unique_ptr<Type> rowType = createStructType();
    rowType->addStructField("col0", createDecimalType(12, 2));

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    StructVectorBatch batch(64, *getDefaultPool());
    Decimal64VectorBatch* decimals = new Decimal64VectorBatch(64, *getDefaultPool());
    batch.fields.push_back(decimals);
    reader->next(batch, 64, 0);
    EXPECT_EQ(true, !batch.hasNulls);
    EXPECT_EQ(64, batch.numElements);
    EXPECT_EQ(true, !decimals->hasNulls);
    EXPECT_EQ(64, decimals->numElements);
    EXPECT_EQ(2, decimals->scale);
    int64_t* values = decimals->values.data();
    for (int64_t i = 0; i < 64; ++i) {
      EXPECT_EQ(i - 32, values[i]);
    }
    reader->next(batch, 64, 0);
    EXPECT_EQ(true, !batch.hasNulls);
    EXPECT_EQ(64, batch.numElements);
    EXPECT_EQ(true, decimals->hasNulls);
    EXPECT_EQ(64, decimals->numElements);
    for (size_t i = 0; i < 63; ++i) {
      EXPECT_EQ(0, decimals->notNull[i]);
    }
    EXPECT_EQ(1, decimals->notNull[63]);
    EXPECT_EQ(32, decimals->values.data()[63]);
  }

  TEST(DecimalColumnReader, testDecimal64Skip) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    std::vector<bool> selectedColumns(2, true);
    EXPECT_CALL(streams, getSelectedColumns()).WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_)).WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(testing::_, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));

    // [0xff]
    unsigned char presentBuffer[] = {0xfe, 0xff, 0x80};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(
            new SeekableArrayInputStream(presentBuffer, ARRAY_SIZE(presentBuffer))));

    // [493827160549382716, 4938271605493827, 49382716054938, 493827160549,
    //  4938271605, 49382716, 493827, 4938, 49]
    const unsigned char numBuffer[] = {
        0xf8, 0xe8, 0xe2, 0xcf, 0xf4, 0xcb, 0xb6, 0xda, 0x0d, 0x86, 0xc1, 0xcc, 0xcd, 0x9e, 0xd5,
        0xc5, 0x11, 0xb4, 0xf6, 0xfc, 0xf3, 0xb9, 0xba, 0x16, 0xca, 0xe7, 0xa3, 0xa6, 0xdf, 0x1c,
        0xea, 0xad, 0xc0, 0xe5, 0x24, 0xf8, 0x94, 0x8c, 0x2f, 0x86, 0xa4, 0x3c, 0x94, 0x4d, 0x62};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(numBuffer, ARRAY_SIZE(numBuffer))));

    // [0x0a] * 9
    const unsigned char buffer1[] = {0x06, 0x00, 0x14};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_SECONDARY, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer1, ARRAY_SIZE(buffer1))));

    // create the row type
    std::unique_ptr<Type> rowType = createStructType();
    rowType->addStructField("col0", createDecimalType(12, 10));

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    StructVectorBatch batch(64, *getDefaultPool());
    Decimal64VectorBatch* decimals = new Decimal64VectorBatch(64, *getDefaultPool());
    batch.fields.push_back(decimals);
    reader->next(batch, 6, 0);
    EXPECT_EQ(true, !batch.hasNulls);
    EXPECT_EQ(6, batch.numElements);
    EXPECT_EQ(true, !decimals->hasNulls);
    EXPECT_EQ(6, decimals->numElements);
    EXPECT_EQ(10, decimals->scale);
    int64_t* values = decimals->values.data();
    EXPECT_EQ(493827160549382716, values[0]);
    EXPECT_EQ(4938271605493827, values[1]);
    EXPECT_EQ(49382716054938, values[2]);
    EXPECT_EQ(493827160549, values[3]);
    EXPECT_EQ(4938271605, values[4]);
    EXPECT_EQ(49382716, values[5]);
    reader->skip(2);
    reader->next(batch, 1, 0);
    EXPECT_EQ(true, !batch.hasNulls);
    EXPECT_EQ(1, batch.numElements);
    EXPECT_EQ(true, !decimals->hasNulls);
    EXPECT_EQ(1, decimals->numElements);
    EXPECT_EQ(49, values[0]);
  }

  TEST(DecimalColumnReader, testDecimal128) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    std::vector<bool> selectedColumns(2, true);
    EXPECT_CALL(streams, getSelectedColumns()).WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_)).WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(testing::_, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));

    // [0xff] * (64/8) + [0x00] * (56/8) + [0x01]
    const unsigned char buffer1[] = {0x05, 0xff, 0x04, 0x00, 0xff, 0x01};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer1, ARRAY_SIZE(buffer1))));

    char numBuffer[65];
    for (int i = 0; i < 65; ++i) {
      if (i < 32) {
        numBuffer[i] = static_cast<char>(0x3f - 2 * i);
      } else {
        numBuffer[i] = static_cast<char>(2 * (i - 32));
      }
    }
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(numBuffer, ARRAY_SIZE(numBuffer), 3)));

    // [0x02] * 65
    const unsigned char buffer2[] = {0x3e, 0x00, 0x04};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_SECONDARY, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer2, ARRAY_SIZE(buffer2))));

    // create the row type
    std::unique_ptr<Type> rowType = createStructType();
    rowType->addStructField("col0", createDecimalType(32, 2));

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    StructVectorBatch batch(64, *getDefaultPool());
    Decimal128VectorBatch* decimals = new Decimal128VectorBatch(64, *getDefaultPool());
    batch.fields.push_back(decimals);
    reader->next(batch, 64, 0);
    EXPECT_EQ(true, !batch.hasNulls);
    EXPECT_EQ(64, batch.numElements);
    EXPECT_EQ(true, !decimals->hasNulls);
    EXPECT_EQ(64, decimals->numElements);
    EXPECT_EQ(2, decimals->scale);
    Int128* values = decimals->values.data();
    for (int64_t i = 0; i < 64; ++i) {
      EXPECT_EQ(i - 32, values[i].toLong());
    }
    reader->next(batch, 64, 0);
    EXPECT_EQ(true, !batch.hasNulls);
    EXPECT_EQ(64, batch.numElements);
    EXPECT_EQ(true, decimals->hasNulls);
    EXPECT_EQ(64, decimals->numElements);
    for (size_t i = 0; i < 63; ++i) {
      EXPECT_EQ(0, decimals->notNull[i]);
    }
    EXPECT_EQ(1, decimals->notNull[63]);
    EXPECT_EQ(32, decimals->values.data()[63].toLong());
  }

  TEST(DecimalColumnReader, testDecimal128Skip) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    std::vector<bool> selectedColumns(2, true);
    EXPECT_CALL(streams, getSelectedColumns()).WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_)).WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(testing::_, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));

    // [0xff, 0xf8]
    unsigned char presentBuffer[] = {0xfe, 0xff, 0xf8};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(
            new SeekableArrayInputStream(presentBuffer, ARRAY_SIZE(presentBuffer))));

    // [493827160549382716, 4938271605493827, 49382716054938, 493827160549,
    //  4938271605, 49382716, 493827, 4938, 49,
    //  17320508075688772935274463415058723669,
    //  -17320508075688772935274463415058723669,
    //  99999999999999999999999999999999999999,
    //  -99999999999999999999999999999999999999]
    const unsigned char numBuffer[] = {
        0xf8, 0xe8, 0xe2, 0xcf, 0xf4, 0xcb, 0xb6, 0xda, 0x0d, 0x86, 0xc1, 0xcc, 0xcd, 0x9e, 0xd5,
        0xc5, 0x11, 0xb4, 0xf6, 0xfc, 0xf3, 0xb9, 0xba, 0x16, 0xca, 0xe7, 0xa3, 0xa6, 0xdf, 0x1c,
        0xea, 0xad, 0xc0, 0xe5, 0x24, 0xf8, 0x94, 0x8c, 0x2f, 0x86, 0xa4, 0x3c, 0x94, 0x4d, 0x62,
        0xaa, 0xcd, 0xb3, 0xf2, 0x9e, 0xf0, 0x99, 0xd6, 0xbe, 0xf8, 0xb6, 0x9e, 0xe4, 0xb7, 0xfd,
        0xce, 0x8f, 0x34, 0xa9, 0xcd, 0xb3, 0xf2, 0x9e, 0xf0, 0x99, 0xd6, 0xbe, 0xf8, 0xb6, 0x9e,
        0xe4, 0xb7, 0xfd, 0xce, 0x8f, 0x34, 0xfe, 0xff, 0xff, 0xff, 0xff, 0x8f, 0x91, 0x8a, 0x93,
        0xe8, 0xa3, 0xec, 0xd0, 0x96, 0xd4, 0xcc, 0xf6, 0xac, 0x02, 0xfd, 0xff, 0xff, 0xff, 0xff,
        0x8f, 0x91, 0x8a, 0x93, 0xe8, 0xa3, 0xec, 0xd0, 0x96, 0xd4, 0xcc, 0xf6, 0xac, 0x02,
    };
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(numBuffer, ARRAY_SIZE(numBuffer))));

    // [0x02] * 13
    unsigned char buffer2[] = {0x0a, 0x00, 0x4a};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_SECONDARY, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer2, ARRAY_SIZE(buffer2))));

    // create the row type
    std::unique_ptr<Type> rowType = createStructType();
    rowType->addStructField("col0", createDecimalType(38, 37));

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    StructVectorBatch batch(64, *getDefaultPool());
    Decimal128VectorBatch* decimals = new Decimal128VectorBatch(64, *getDefaultPool());
    batch.fields.push_back(decimals);
    reader->next(batch, 6, 0);
    EXPECT_EQ(true, !batch.hasNulls);
    EXPECT_EQ(6, batch.numElements);
    EXPECT_EQ(true, !decimals->hasNulls);
    EXPECT_EQ(6, decimals->numElements);
    EXPECT_EQ(37, decimals->scale);
    Int128* values = decimals->values.data();
    EXPECT_EQ(493827160549382716, values[0].toLong());
    EXPECT_EQ(4938271605493827, values[1].toLong());
    EXPECT_EQ(49382716054938, values[2].toLong());
    EXPECT_EQ(493827160549, values[3].toLong());
    EXPECT_EQ(4938271605, values[4].toLong());
    EXPECT_EQ(49382716, values[5].toLong());
    reader->skip(2);
    reader->next(batch, 5, 0);
    EXPECT_EQ(true, !batch.hasNulls);
    EXPECT_EQ(5, batch.numElements);
    EXPECT_EQ(true, !decimals->hasNulls);
    EXPECT_EQ(5, decimals->numElements);
    EXPECT_EQ(49, values[0].toLong());
    EXPECT_EQ("1.7320508075688772935274463415058723669",
              values[1].toDecimalString(decimals->scale));
    EXPECT_EQ("-1.7320508075688772935274463415058723669",
              values[2].toDecimalString(decimals->scale));
    EXPECT_EQ("9.9999999999999999999999999999999999999",
              values[3].toDecimalString(decimals->scale));
    EXPECT_EQ("-9.9999999999999999999999999999999999999",
              values[4].toDecimalString(decimals->scale));
  }

  TEST(DecimalColumnReader, testDecimal64V2) {
    MockStripeStreams streams;

    // set getSelectedColumns() for struct<decimal(12,2)>
    std::vector<bool> selectedColumns(2, true);
    EXPECT_CALL(streams, getSelectedColumns()).WillRepeatedly(testing::Return(selectedColumns));

    // Use the decimal encoding in ORCv2
    EXPECT_CALL(streams, isDecimalAsLong()).WillRepeatedly(testing::Return(true));

    // set encoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_)).WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    // PRESENT stream of the struct column is nullptr.
    EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));

    // PRESENT stream of the decimal column is in Boolean Run Length Encoding.
    // {0x05, 0xff} -> 8 bytes of 0xff -> 64 true values.
    // {0x04, 0x00} -> 7 bytes of 0x00 -> 56 false values.
    // {0xff, 0x01} -> 1 byte of 0x01 -> 7 false values followed with 1 true.
    const unsigned char buffer1[] = {0x05, 0xff, 0x04, 0x00, 0xff, 0x01};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer1, ARRAY_SIZE(buffer1))));

    // DATA stream of the decimal column is in RLEv2.
    // Original values: [-32, -31, -30, ..., -1, 0, 1, 2, ..., 32]. See RLEv2.basicDelta5.
    const unsigned char buffer2[] = {0xc0, 0x40, 0x3f, 0x02};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer2, ARRAY_SIZE(buffer2), 3)));

    // create the row type
    std::unique_ptr<Type> rowType = createStructType();
    rowType->addStructField("col0", createDecimalType(12, 2));

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    StructVectorBatch batch(64, *getDefaultPool());
    Decimal64VectorBatch* decimals = new Decimal64VectorBatch(64, *getDefaultPool());
    batch.fields.push_back(decimals);
    reader->next(batch, 64, 0);
    EXPECT_FALSE(batch.hasNulls);
    EXPECT_EQ(64, batch.numElements);
    EXPECT_FALSE(decimals->hasNulls);
    EXPECT_EQ(64, decimals->numElements);
    EXPECT_EQ(2, decimals->scale);
    int64_t* values = decimals->values.data();
    for (int64_t i = 0; i < 64; ++i) {
      EXPECT_EQ(i - 32, values[i]);
    }
    reader->next(batch, 64, 0);
    EXPECT_FALSE(batch.hasNulls);
    EXPECT_EQ(64, batch.numElements);
    EXPECT_TRUE(decimals->hasNulls);
    EXPECT_EQ(64, decimals->numElements);
    for (size_t i = 0; i < 63; ++i) {
      EXPECT_EQ(0, decimals->notNull[i]);
    }
    EXPECT_EQ(1, decimals->notNull[63]);
    EXPECT_EQ(32, decimals->values.data()[63]);
  }

  TEST(DecimalColumnReader, testDecimal64V2Skip) {
    MockStripeStreams streams;

    // set getSelectedColumns() for struct<decimal(12,2)>
    std::vector<bool> selectedColumns(2, true);
    EXPECT_CALL(streams, getSelectedColumns()).WillRepeatedly(testing::Return(selectedColumns));

    // Use the decimal encoding in ORCv2
    EXPECT_CALL(streams, isDecimalAsLong()).WillRepeatedly(testing::Return(true));

    // set encoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_)).WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    // PRESENT stream of the struct column is nullptr.
    EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));

    // PRESENT stream of the decimal column is in Boolean Run Length Encoding.
    // {0x05, 0xff} -> 8 bytes of 0xff -> 64 true values.
    // {0x04, 0x00} -> 7 bytes of 0x00 -> 56 false values.
    // {0xff, 0x01} -> 1 byte of 0x01 -> 7 false values followed with 1 true.
    const unsigned char buffer1[] = {0x05, 0xff, 0x04, 0x00, 0xff, 0x01};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer1, ARRAY_SIZE(buffer1))));

    // DATA stream of the decimal column is in RLEv2.
    // Original values: [-32, -31, -30, ..., -1, 0, 1, 2, ..., 32]. See RLEv2.basicDelta5.
    const unsigned char buffer2[] = {0xc0, 0x40, 0x3f, 0x02};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer2, ARRAY_SIZE(buffer2), 3)));

    // create the row type
    std::unique_ptr<Type> rowType = createStructType();
    rowType->addStructField("col0", createDecimalType(12, 2));

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);
    StructVectorBatch batch(64, *getDefaultPool());
    Decimal64VectorBatch* decimals = new Decimal64VectorBatch(64, *getDefaultPool());
    batch.fields.push_back(decimals);
    // Read 10 values
    reader->next(batch, 10, 0);
    EXPECT_FALSE(batch.hasNulls);
    EXPECT_EQ(10, batch.numElements);
    EXPECT_FALSE(decimals->hasNulls);
    EXPECT_EQ(10, decimals->numElements);
    EXPECT_EQ(2, decimals->scale);
    int64_t* values = decimals->values.data();
    for (int64_t i = 0; i < 10; ++i) {
      EXPECT_EQ(i - 32, values[i]);
    }
    // Skip 50 values and read 10 values again
    reader->skip(50);
    reader->next(batch, 10, 0);
    EXPECT_FALSE(batch.hasNulls);
    EXPECT_EQ(10, batch.numElements);
    EXPECT_TRUE(decimals->hasNulls);
    values = decimals->values.data();
    for (int64_t i = 0; i < 4; ++i) {
      EXPECT_EQ(60 + i - 32, values[i]);
    }
    for (size_t i = 4; i < 10; ++i) {
      EXPECT_EQ(0, decimals->notNull[i]);
    }
    // Skip 57 values and read the last value
    reader->skip(57);
    reader->next(batch, 1, 0);
    EXPECT_FALSE(batch.hasNulls);
    EXPECT_EQ(1, batch.numElements);
    EXPECT_FALSE(decimals->hasNulls);
    EXPECT_EQ(32, decimals->values.data()[0]);
  }

  TEST(DecimalColumnReader, testDecimalHive11) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    std::vector<bool> selectedColumns(2, true);
    EXPECT_CALL(streams, getSelectedColumns()).WillRepeatedly(testing::Return(selectedColumns));
    EXPECT_CALL(streams, getThrowOnHive11DecimalOverflow()).WillRepeatedly(testing::Return(true));
    EXPECT_CALL(streams, getForcedScaleOnHive11Decimal()).WillRepeatedly(testing::Return(6));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_)).WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(testing::_, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));

    // [0xff] * (64/8) + [0x00] * (56/8) + [0x01]
    const unsigned char buffer1[] = {0x05, 0xff, 0x04, 0x00, 0xff, 0x01};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer1, ARRAY_SIZE(buffer1))));

    char numBuffer[65];
    for (int i = 0; i < 65; ++i) {
      if (i < 32) {
        numBuffer[i] = static_cast<char>(0x3f - 2 * i);
      } else {
        numBuffer[i] = static_cast<char>(2 * (i - 32));
      }
    }
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(numBuffer, ARRAY_SIZE(numBuffer), 3)));

    const unsigned char scaleBuffer[] = {0x3e, 0x00, 0x0c};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_SECONDARY, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(scaleBuffer, ARRAY_SIZE(scaleBuffer))));

    // create the row type
    std::unique_ptr<Type> rowType = createStructType();
    rowType->addStructField("col0", createDecimalType(0, 0));

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    StructVectorBatch batch(64, *getDefaultPool());
    Decimal128VectorBatch* decimals = new Decimal128VectorBatch(64, *getDefaultPool());
    batch.fields.push_back(decimals);
    reader->next(batch, 64, 0);
    EXPECT_EQ(true, !batch.hasNulls);
    EXPECT_EQ(64, batch.numElements);
    EXPECT_EQ(true, !decimals->hasNulls);
    EXPECT_EQ(64, decimals->numElements);
    EXPECT_EQ(6, decimals->scale);
    Int128* values = decimals->values.data();
    for (int64_t i = 0; i < 64; ++i) {
      EXPECT_EQ(i - 32, values[i].toLong());
    }
    reader->next(batch, 64, 0);
    EXPECT_EQ(true, !batch.hasNulls);
    EXPECT_EQ(64, batch.numElements);
    EXPECT_EQ(true, decimals->hasNulls);
    EXPECT_EQ(64, decimals->numElements);
    for (size_t i = 0; i < 63; ++i) {
      EXPECT_EQ(0, decimals->notNull[i]);
    }
    EXPECT_EQ(1, decimals->notNull[63]);
    EXPECT_EQ(32, decimals->values.data()[63].toLong());
  }

  TEST(DecimalColumnReader, testDecimalHive11Skip) {
    MockStripeStreams streams;

    EXPECT_CALL(streams, getThrowOnHive11DecimalOverflow()).WillRepeatedly(testing::Return(false));
    EXPECT_CALL(streams, getForcedScaleOnHive11Decimal()).WillRepeatedly(testing::Return(3));

    // set getSelectedColumns()
    std::vector<bool> selectedColumns(2, true);
    EXPECT_CALL(streams, getSelectedColumns()).WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_)).WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(testing::_, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));

    // [0xff, 0xf8]
    unsigned char presentBuffer[] = {0xfe, 0xff, 0xf8};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(
            new SeekableArrayInputStream(presentBuffer, ARRAY_SIZE(presentBuffer))));

    // [493827160549382716, 4938271605493827, 49382716054938, 493827160549,
    //  4938271605, 49382716, 493827, 4938, 49,
    //  17320508075688772935274463415058723669,
    //  -17320508075688772935274463415058723669,
    //  99999999999999999999999999999999999999,
    //  -99999999999999999999999999999999999999]
    const unsigned char numBuffer[] = {
        0xf8, 0xe8, 0xe2, 0xcf, 0xf4, 0xcb, 0xb6, 0xda, 0x0d, 0x86, 0xc1, 0xcc, 0xcd, 0x9e, 0xd5,
        0xc5, 0x11, 0xb4, 0xf6, 0xfc, 0xf3, 0xb9, 0xba, 0x16, 0xca, 0xe7, 0xa3, 0xa6, 0xdf, 0x1c,
        0xea, 0xad, 0xc0, 0xe5, 0x24, 0xf8, 0x94, 0x8c, 0x2f, 0x86, 0xa4, 0x3c, 0x94, 0x4d, 0x62,
        0xaa, 0xcd, 0xb3, 0xf2, 0x9e, 0xf0, 0x99, 0xd6, 0xbe, 0xf8, 0xb6, 0x9e, 0xe4, 0xb7, 0xfd,
        0xce, 0x8f, 0x34, 0xa9, 0xcd, 0xb3, 0xf2, 0x9e, 0xf0, 0x99, 0xd6, 0xbe, 0xf8, 0xb6, 0x9e,
        0xe4, 0xb7, 0xfd, 0xce, 0x8f, 0x34, 0xfe, 0xff, 0xff, 0xff, 0xff, 0x8f, 0x91, 0x8a, 0x93,
        0xe8, 0xa3, 0xec, 0xd0, 0x96, 0xd4, 0xcc, 0xf6, 0xac, 0x02, 0xfd, 0xff, 0xff, 0xff, 0xff,
        0x8f, 0x91, 0x8a, 0x93, 0xe8, 0xa3, 0xec, 0xd0, 0x96, 0xd4, 0xcc, 0xf6, 0xac, 0x02,
    };
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(numBuffer, ARRAY_SIZE(numBuffer))));

    const unsigned char scaleBuffer[] = {0x0a, 0x00, 0x06};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_SECONDARY, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(scaleBuffer, ARRAY_SIZE(scaleBuffer))));

    // create the row type
    std::unique_ptr<Type> rowType = createStructType();
    rowType->addStructField("col0", createDecimalType(0, 0));

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    StructVectorBatch batch(64, *getDefaultPool());
    Decimal128VectorBatch* decimals = new Decimal128VectorBatch(64, *getDefaultPool());
    batch.fields.push_back(decimals);
    reader->next(batch, 6, 0);
    EXPECT_EQ(true, !batch.hasNulls);
    EXPECT_EQ(6, batch.numElements);
    EXPECT_EQ(true, !decimals->hasNulls);
    EXPECT_EQ(6, decimals->numElements);
    EXPECT_EQ(3, decimals->scale);
    Int128* values = decimals->values.data();
    EXPECT_EQ(493827160549382716, values[0].toLong());
    EXPECT_EQ(4938271605493827, values[1].toLong());
    EXPECT_EQ(49382716054938, values[2].toLong());
    EXPECT_EQ(493827160549, values[3].toLong());
    EXPECT_EQ(4938271605, values[4].toLong());
    EXPECT_EQ(49382716, values[5].toLong());
    reader->skip(2);
    reader->next(batch, 5, 0);
    EXPECT_EQ(true, !batch.hasNulls);
    EXPECT_EQ(5, batch.numElements);
    EXPECT_EQ(true, !decimals->hasNulls);
    EXPECT_EQ(5, decimals->numElements);
    EXPECT_EQ(49, values[0].toLong());
    EXPECT_EQ("17320508075688772935274463415058723.669",
              values[1].toDecimalString(decimals->scale));
    EXPECT_EQ("-17320508075688772935274463415058723.669",
              values[2].toDecimalString(decimals->scale));
    EXPECT_EQ("99999999999999999999999999999999999.999",
              values[3].toDecimalString(decimals->scale));
    EXPECT_EQ("-99999999999999999999999999999999999.999",
              values[4].toDecimalString(decimals->scale));
  }

  TEST(DecimalColumnReader, testDecimalHive11ScaleUp) {
    MockStripeStreams streams;

    EXPECT_CALL(streams, getThrowOnHive11DecimalOverflow()).WillRepeatedly(testing::Return(true));
    EXPECT_CALL(streams, getForcedScaleOnHive11Decimal()).WillRepeatedly(testing::Return(20));

    // set getSelectedColumns()
    std::vector<bool> selectedColumns(2, true);
    EXPECT_CALL(streams, getSelectedColumns()).WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_)).WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(testing::_, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));

    // [0xff, 0xff, 0xf8]
    const unsigned char presentBuffer[] = {0xfd, 0xff, 0xff, 0xf8};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(
            new SeekableArrayInputStream(presentBuffer, ARRAY_SIZE(presentBuffer))));

    // [1] * 21
    const unsigned char numBuffer[] = {0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02,
                                       0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02,
                                       0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(numBuffer, ARRAY_SIZE(numBuffer))));

    const unsigned char scaleBuffer[] = {0x12, 0xff, 0x28};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_SECONDARY, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(scaleBuffer, ARRAY_SIZE(scaleBuffer))));

    // create the row type
    std::unique_ptr<Type> rowType = createStructType();
    rowType->addStructField("col0", createDecimalType(0, 0));

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    StructVectorBatch batch(64, *getDefaultPool());
    Decimal128VectorBatch* decimals = new Decimal128VectorBatch(64, *getDefaultPool());
    batch.fields.push_back(decimals);
    reader->next(batch, 21, 0);
    EXPECT_EQ(true, !batch.hasNulls);
    EXPECT_EQ(21, batch.numElements);
    EXPECT_EQ(true, !decimals->hasNulls);
    EXPECT_EQ(21, decimals->numElements);
    EXPECT_EQ(20, decimals->scale);
    Int128* values = decimals->values.data();
    Int128 expected = 1;
    for (int i = 0; i < 21; ++i) {
      EXPECT_EQ(expected.toString(), values[i].toString());
      expected *= 10;
    }
  }

  TEST(DecimalColumnReader, testDecimalHive11ScaleDown) {
    MockStripeStreams streams;

    EXPECT_CALL(streams, getThrowOnHive11DecimalOverflow()).WillRepeatedly(testing::Return(true));
    EXPECT_CALL(streams, getForcedScaleOnHive11Decimal()).WillRepeatedly(testing::Return(0));

    // set getSelectedColumns()
    std::vector<bool> selectedColumns(2, true);
    EXPECT_CALL(streams, getSelectedColumns()).WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_)).WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(testing::_, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));

    // [0xff, 0xff, 0xf8]
    const unsigned char presentBuffer[] = {0xfd, 0xff, 0xff, 0xf8};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(
            new SeekableArrayInputStream(presentBuffer, ARRAY_SIZE(presentBuffer))));

    // [100000000000000000000] * 21
    const unsigned char numBuffer[] = {
        0x80, 0x80, 0x80, 0xb1, 0xac, 0x8b, 0xaf, 0xc7, 0xd7, 0x15, 0x80, 0x80, 0x80, 0xb1, 0xac,
        0x8b, 0xaf, 0xc7, 0xd7, 0x15, 0x80, 0x80, 0x80, 0xb1, 0xac, 0x8b, 0xaf, 0xc7, 0xd7, 0x15,
        0x80, 0x80, 0x80, 0xb1, 0xac, 0x8b, 0xaf, 0xc7, 0xd7, 0x15, 0x80, 0x80, 0x80, 0xb1, 0xac,
        0x8b, 0xaf, 0xc7, 0xd7, 0x15, 0x80, 0x80, 0x80, 0xb1, 0xac, 0x8b, 0xaf, 0xc7, 0xd7, 0x15,
        0x80, 0x80, 0x80, 0xb1, 0xac, 0x8b, 0xaf, 0xc7, 0xd7, 0x15, 0x80, 0x80, 0x80, 0xb1, 0xac,
        0x8b, 0xaf, 0xc7, 0xd7, 0x15, 0x80, 0x80, 0x80, 0xb1, 0xac, 0x8b, 0xaf, 0xc7, 0xd7, 0x15,
        0x80, 0x80, 0x80, 0xb1, 0xac, 0x8b, 0xaf, 0xc7, 0xd7, 0x15, 0x80, 0x80, 0x80, 0xb1, 0xac,
        0x8b, 0xaf, 0xc7, 0xd7, 0x15, 0x80, 0x80, 0x80, 0xb1, 0xac, 0x8b, 0xaf, 0xc7, 0xd7, 0x15,
        0x80, 0x80, 0x80, 0xb1, 0xac, 0x8b, 0xaf, 0xc7, 0xd7, 0x15, 0x80, 0x80, 0x80, 0xb1, 0xac,
        0x8b, 0xaf, 0xc7, 0xd7, 0x15, 0x80, 0x80, 0x80, 0xb1, 0xac, 0x8b, 0xaf, 0xc7, 0xd7, 0x15,
        0x80, 0x80, 0x80, 0xb1, 0xac, 0x8b, 0xaf, 0xc7, 0xd7, 0x15, 0x80, 0x80, 0x80, 0xb1, 0xac,
        0x8b, 0xaf, 0xc7, 0xd7, 0x15, 0x80, 0x80, 0x80, 0xb1, 0xac, 0x8b, 0xaf, 0xc7, 0xd7, 0x15,
        0x80, 0x80, 0x80, 0xb1, 0xac, 0x8b, 0xaf, 0xc7, 0xd7, 0x15, 0x80, 0x80, 0x80, 0xb1, 0xac,
        0x8b, 0xaf, 0xc7, 0xd7, 0x15, 0x80, 0x80, 0x80, 0xb1, 0xac, 0x8b, 0xaf, 0xc7, 0xd7, 0x15};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(numBuffer, ARRAY_SIZE(numBuffer))));

    const unsigned char scaleBuffer[] = {0x12, 0x01, 0x00};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_SECONDARY, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(scaleBuffer, ARRAY_SIZE(scaleBuffer))));

    // create the row type
    std::unique_ptr<Type> rowType = createStructType();
    rowType->addStructField("col0", createDecimalType(0, 0));

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    StructVectorBatch batch(64, *getDefaultPool());
    Decimal128VectorBatch* decimals = new Decimal128VectorBatch(64, *getDefaultPool());
    batch.fields.push_back(decimals);
    reader->next(batch, 21, 0);
    EXPECT_EQ(true, !batch.hasNulls);
    EXPECT_EQ(21, batch.numElements);
    EXPECT_EQ(true, !decimals->hasNulls);
    EXPECT_EQ(21, decimals->numElements);
    EXPECT_EQ(0, decimals->scale);
    Int128* values = decimals->values.data();
    Int128 expected = Int128(0x5, 0x6bc75e2d63100000);
    Int128 remainder;
    for (int i = 0; i < 21; ++i) {
      EXPECT_EQ(expected.toString(), values[i].toString());
      expected = expected.divide(10, remainder);
    }
  }

  TEST(DecimalColumnReader, testDecimalHive11OverflowException) {
    MockStripeStreams streams;

    EXPECT_CALL(streams, getThrowOnHive11DecimalOverflow()).WillRepeatedly(testing::Return(true));
    EXPECT_CALL(streams, getForcedScaleOnHive11Decimal()).WillRepeatedly(testing::Return(6));

    // set getSelectedColumns()
    std::vector<bool> selectedColumns(2, true);
    EXPECT_CALL(streams, getSelectedColumns()).WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_)).WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(testing::_, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));

    // [0x80]
    const unsigned char presentBuffer[] = {0xff, 0x80};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(
            new SeekableArrayInputStream(presentBuffer, ARRAY_SIZE(presentBuffer))));

    // [10000000000000000000]
    const unsigned char numBuffer[] = {0x80, 0x80, 0x80, 0x80, 0x80, 0x90, 0x91, 0x8a, 0x93, 0xe8,
                                       0xa3, 0xec, 0xd0, 0x96, 0xd4, 0xcc, 0xf6, 0xac, 0x02};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(numBuffer, ARRAY_SIZE(numBuffer))));

    const unsigned char scaleBuffer[] = {0xff, 0x0c};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_SECONDARY, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(scaleBuffer, ARRAY_SIZE(scaleBuffer))));

    // create the row type
    std::unique_ptr<Type> rowType = createStructType();
    rowType->addStructField("col0", createDecimalType(0, 0));

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    StructVectorBatch batch(64, *getDefaultPool());
    Decimal128VectorBatch* decimals = new Decimal128VectorBatch(64, *getDefaultPool());
    batch.fields.push_back(decimals);
    EXPECT_THROW(reader->next(batch, 1, 0), ParseError);
  }

  TEST(DecimalColumnReader, testDecimalHive11OverflowExceptionNull) {
    MockStripeStreams streams;

    EXPECT_CALL(streams, getThrowOnHive11DecimalOverflow()).WillRepeatedly(testing::Return(true));
    EXPECT_CALL(streams, getForcedScaleOnHive11Decimal()).WillRepeatedly(testing::Return(6));

    // set getSelectedColumns()
    std::vector<bool> selectedColumns(2, true);
    EXPECT_CALL(streams, getSelectedColumns()).WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_)).WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(testing::_, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));

    // [0x40]
    const unsigned char presentBuffer[] = {0xff, 0x40};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(
            new SeekableArrayInputStream(presentBuffer, ARRAY_SIZE(presentBuffer))));

    // [10000000000000000000]
    const unsigned char numBuffer[] = {0x80, 0x80, 0x80, 0x80, 0x80, 0x90, 0x91, 0x8a, 0x93, 0xe8,
                                       0xa3, 0xec, 0xd0, 0x96, 0xd4, 0xcc, 0xf6, 0xac, 0x02};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(numBuffer, ARRAY_SIZE(numBuffer))));

    const unsigned char scaleBuffer[] = {0xff, 0x0c};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_SECONDARY, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(scaleBuffer, ARRAY_SIZE(scaleBuffer))));

    // create the row type
    std::unique_ptr<Type> rowType = createStructType();
    rowType->addStructField("col0", createDecimalType(0, 0));

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    StructVectorBatch batch(64, *getDefaultPool());
    Decimal128VectorBatch* decimals = new Decimal128VectorBatch(64, *getDefaultPool());
    batch.fields.push_back(decimals);
    EXPECT_THROW(reader->next(batch, 2, 0), ParseError);
  }

  TEST(DecimalColumnReader, testDecimalHive11OverflowNull) {
    MockStripeStreams streams;

    std::stringstream errStream;
    EXPECT_CALL(streams, getErrorStream()).WillRepeatedly(testing::Return(&errStream));

    EXPECT_CALL(streams, getThrowOnHive11DecimalOverflow()).WillRepeatedly(testing::Return(false));
    EXPECT_CALL(streams, getForcedScaleOnHive11Decimal()).WillRepeatedly(testing::Return(6));

    // set getSelectedColumns()
    std::vector<bool> selectedColumns(2, true);
    EXPECT_CALL(streams, getSelectedColumns()).WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_)).WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(testing::_, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));

    // [0x78]
    unsigned char presentBuffer[] = {0xff, 0x78};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(presentBuffer, sizeof(presentBuffer))));

    // [1000000000000000000000000000000000000000, 1,
    //  -10000000000000000000000000000000000000, 1]
    unsigned char numBuffer[] = {0x80, 0x80, 0x80, 0x80, 0x80, 0xc0, 0xb0, 0xf5, 0xf3, 0xae, 0xfd,
                                 0xcb, 0x94, 0xd7, 0xe1, 0xf1, 0xd3, 0x8c, 0xeb, 0x01, 0x02, 0xff,
                                 0xff, 0xff, 0xff, 0xff, 0x8f, 0x91, 0x8a, 0x93, 0xe8, 0xa3, 0xec,
                                 0xd0, 0x96, 0xd4, 0xcc, 0xf6, 0xac, 0x02, 0x02};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(numBuffer, ARRAY_SIZE(numBuffer))));

    const unsigned char scaleBuffer[] = {0x01, 0x00, 0x0c};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_SECONDARY, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(scaleBuffer, ARRAY_SIZE(scaleBuffer))));

    // create the row type
    std::unique_ptr<Type> rowType = createStructType();
    rowType->addStructField("col0", createDecimalType(0, 0));

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    StructVectorBatch batch(64, *getDefaultPool());
    Decimal128VectorBatch* decimals = new Decimal128VectorBatch(64, *getDefaultPool());
    batch.fields.push_back(decimals);

    reader->next(batch, 3, 0);
    EXPECT_EQ(true, !batch.hasNulls);
    EXPECT_EQ(3, batch.numElements);
    EXPECT_EQ(true, decimals->hasNulls);
    EXPECT_EQ(3, decimals->numElements);
    EXPECT_EQ(6, decimals->scale);
    EXPECT_EQ(true, !decimals->notNull[0]);
    EXPECT_EQ(true, !decimals->notNull[1]);
    EXPECT_EQ(true, decimals->notNull[2]);
    EXPECT_EQ(1, decimals->values[2].toLong());

    reader->next(batch, 2, 0);
    EXPECT_EQ(true, !batch.hasNulls);
    EXPECT_EQ(2, batch.numElements);
    EXPECT_EQ(true, decimals->hasNulls);
    EXPECT_EQ(2, decimals->numElements);
    EXPECT_EQ(6, decimals->scale);
    EXPECT_EQ(true, !decimals->notNull[0]);
    EXPECT_EQ(true, decimals->notNull[1]);
    EXPECT_EQ(1, decimals->values[1].toLong());

    EXPECT_EQ(
        "Warning: Hive 0.11 decimal with more than 38 digits"
        " replaced by NULL.\n"
        "Warning: Hive 0.11 decimal with more than 38 digits"
        " replaced by NULL.\n",
        errStream.str());
  }

  TEST(DecimalColumnReader, testDecimalHive11BigBatches) {
    MockStripeStreams streams;

    EXPECT_CALL(streams, getThrowOnHive11DecimalOverflow()).WillRepeatedly(testing::Return(true));
    EXPECT_CALL(streams, getForcedScaleOnHive11Decimal()).WillRepeatedly(testing::Return(6));

    // set getSelectedColumns()
    std::vector<bool> selectedColumns(2, true);
    EXPECT_CALL(streams, getSelectedColumns()).WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_)).WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(testing::_, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));

    // range(64) * 32
    unsigned char numBuffer[2048];
    for (size_t i = 0; i < 2048; ++i) {
      numBuffer[i] = static_cast<unsigned char>((i % 64) * 2);
    }
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(numBuffer, ARRAY_SIZE(numBuffer))));

    // [5] * 1024 + [4] * 1024
    unsigned char scaleBuffer[48];
    for (size_t i = 0; i < 48; i += 3) {
      scaleBuffer[i] = 0x7d;
      scaleBuffer[i + 1] = 0x00;
      scaleBuffer[i + 2] = (i < 24) ? 0x0a : 0x08;
    }
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_SECONDARY, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(scaleBuffer, ARRAY_SIZE(scaleBuffer))));

    // create the row type
    std::unique_ptr<Type> rowType = createStructType();
    rowType->addStructField("col0", createDecimalType(0, 0));

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    StructVectorBatch batch(2048, *getDefaultPool());
    Decimal128VectorBatch* decimals = new Decimal128VectorBatch(2048, *getDefaultPool());
    batch.fields.push_back(decimals);

    reader->next(batch, 2048, 0);
    EXPECT_EQ(true, !batch.hasNulls);
    EXPECT_EQ(2048, batch.numElements);
    EXPECT_EQ(true, !decimals->hasNulls);
    EXPECT_EQ(2048, decimals->numElements);
    EXPECT_EQ(6, decimals->scale);
    for (size_t i = 0; i < decimals->numElements; ++i) {
      EXPECT_EQ((i % 64) * (i < 1024 ? 10 : 100), decimals->values[i].toLong())
          << "Wrong value at " << i;
    }
  }

  TEST(TestColumnReader, testUnion) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    std::vector<bool> selectedColumns(4, true);
    EXPECT_CALL(streams, getSelectedColumns()).WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_)).WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(testing::_, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));

    // [0] * 1000 + [1] * 1000 + [0] * 200 + [1] * 200
    const unsigned char buffer1[] = {0x7f, 0x00, 0x7f, 0x00, 0x7f, 0x00, 0x7f, 0x00, 0x7f, 0x00,
                                     0x7f, 0x00, 0x7f, 0x00, 0x57, 0x00, 0x7f, 0x01, 0x7f, 0x01,
                                     0x7f, 0x01, 0x7f, 0x01, 0x7f, 0x01, 0x7f, 0x01, 0x7f, 0x01,
                                     0x57, 0x01, 0x7f, 0x00, 0x43, 0x00, 0x7f, 0x01, 0x43, 0x01};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer1, ARRAY_SIZE(buffer1))));

    // range(1200)
    const unsigned char buffer2[] = {0x7f, 0x01, 0x00, 0x7f, 0x01, 0x84, 0x02, 0x7f, 0x01, 0x88,
                                     0x04, 0x7f, 0x01, 0x8c, 0x06, 0x7f, 0x01, 0x90, 0x08, 0x7f,
                                     0x01, 0x94, 0x0a, 0x7f, 0x01, 0x98, 0x0c, 0x7f, 0x01, 0x9c,
                                     0x0e, 0x7f, 0x01, 0xa0, 0x10, 0x1b, 0x01, 0xa4, 0x12};
    EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer2, ARRAY_SIZE(buffer2))));

    // range(8, 1208)
    const unsigned char buffer3[] = {0x7f, 0x01, 0x10, 0x7f, 0x01, 0x94, 0x02, 0x7f, 0x01, 0x98,
                                     0x04, 0x7f, 0x01, 0x9c, 0x06, 0x7f, 0x01, 0xa0, 0x08, 0x7f,
                                     0x01, 0xa4, 0x0a, 0x7f, 0x01, 0xa8, 0x0c, 0x7f, 0x01, 0xac,
                                     0x0e, 0x7f, 0x01, 0xb0, 0x10, 0x1b, 0x01, 0xb4, 0x12};
    EXPECT_CALL(streams, getStreamProxy(3, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer3, ARRAY_SIZE(buffer3))));

    // create the row type
    std::unique_ptr<Type> unionType = createUnionType();
    unionType->addUnionChild(createPrimitiveType(LONG));
    unionType->addUnionChild(createPrimitiveType(INT));
    std::unique_ptr<Type> rowType = createStructType();
    rowType->addStructField("col0", std::move(unionType));

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    StructVectorBatch batch(512, *getDefaultPool());
    UnionVectorBatch* unions = new UnionVectorBatch(512, *getDefaultPool());
    LongVectorBatch* child1 = new LongVectorBatch(512, *getDefaultPool());
    LongVectorBatch* child2 = new LongVectorBatch(512, *getDefaultPool());
    batch.fields.push_back(unions);
    unions->children.push_back(child1);
    unions->children.push_back(child2);
    reader->next(batch, 512, 0);
    ASSERT_EQ(512, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(512, unions->numElements);
    ASSERT_EQ(true, !unions->hasNulls);
    ASSERT_EQ(512, child1->numElements);
    ASSERT_EQ(true, !child1->hasNulls);
    ASSERT_EQ(0, child2->numElements);
    ASSERT_EQ(true, !child2->hasNulls);
    for (size_t i = 0; i < batch.numElements; ++i) {
      EXPECT_EQ(0, unions->tags[i]);
      EXPECT_EQ(i, unions->offsets[i]);
      EXPECT_EQ(i, child1->data[i]);
    }

    reader->next(batch, 511, 0);
    ASSERT_EQ(511, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(511, unions->numElements);
    ASSERT_EQ(true, !unions->hasNulls);
    ASSERT_EQ(488, child1->numElements);
    ASSERT_EQ(true, !child1->hasNulls);
    ASSERT_EQ(23, child2->numElements);
    ASSERT_EQ(true, !child2->hasNulls);
    for (size_t i = 0; i < 488; ++i) {
      EXPECT_EQ(0, unions->tags[i]);
      EXPECT_EQ(i, unions->offsets[i]);
      EXPECT_EQ(i + 512, child1->data[i]);
    }
    for (size_t i = 488; i < 511; ++i) {
      EXPECT_EQ(1, unions->tags[i]);
      EXPECT_EQ(i - 488, unions->offsets[i]);
      EXPECT_EQ(i - 480, child2->data[i - 488]);
    }

    reader->next(batch, 1, 0);
    ASSERT_EQ(1, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(1, unions->numElements);
    ASSERT_EQ(true, !unions->hasNulls);
    ASSERT_EQ(0, child1->numElements);
    ASSERT_EQ(true, !child1->hasNulls);
    ASSERT_EQ(1, child2->numElements);
    ASSERT_EQ(true, !child2->hasNulls);
    EXPECT_EQ(1, unions->tags[0]);
    EXPECT_EQ(0, unions->offsets[0]);
    EXPECT_EQ(31, child2->data[0]);

    batch.resize(1500);
    reader->next(batch, 1376, 0);
    ASSERT_EQ(1376, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(1376, unions->numElements);
    ASSERT_EQ(true, !unions->hasNulls);
    ASSERT_EQ(200, child1->numElements);
    ASSERT_EQ(true, !child1->hasNulls);
    ASSERT_EQ(1176, child2->numElements);
    ASSERT_EQ(true, !child2->hasNulls);
    for (size_t i = 0; i < 1376; ++i) {
      if (i < 976) {
        EXPECT_EQ(1, unions->tags[i]);
        EXPECT_EQ(i, unions->offsets[i]);
        EXPECT_EQ(i + 32, child2->data[i]);
      } else if (i < 1176) {
        EXPECT_EQ(0, unions->tags[i]);
        EXPECT_EQ(i - 976, unions->offsets[i]);
        EXPECT_EQ(i + 24, child1->data[i - 976]);
      } else {
        EXPECT_EQ(1, unions->tags[i]);
        EXPECT_EQ(i - 200, unions->offsets[i]);
        EXPECT_EQ(i - 168, child2->data[i - 200]);
      }
    }
    EXPECT_EQ(("Struct vector <1376 of 1500;"
               " Union vector <Long vector <200 of 512>,"
               " Long vector <1176 of 1176>; with 1376 of 1376>; >"),
              batch.toString());
  }

  TEST(TestColumnReader, testUnionWithNulls) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    std::vector<bool> selectedColumns(4, true);
    EXPECT_CALL(streams, getSelectedColumns()).WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_)).WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(testing::_, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));

    // [0] * 3 + [255] * 6 + [0] * 3
    const unsigned char buffer1[] = {0x00, 0x00, 0x03, 0xff, 0x00, 0x00};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer1, ARRAY_SIZE(buffer1))));

    // [0, 1] * 24
    const unsigned char buffer2[] = {0xd0, 0x00, 0x01, 0x00, 0x01, 0x00, 0x01, 0x00, 0x01, 0x00,
                                     0x01, 0x00, 0x01, 0x00, 0x01, 0x00, 0x01, 0x00, 0x01, 0x00,
                                     0x01, 0x00, 0x01, 0x00, 0x01, 0x00, 0x01, 0x00, 0x01, 0x00,
                                     0x01, 0x00, 0x01, 0x00, 0x01, 0x00, 0x01, 0x00, 0x01, 0x00,
                                     0x01, 0x00, 0x01, 0x00, 0x01, 0x00, 0x01, 0x00, 0x01};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer2, ARRAY_SIZE(buffer2))));

    // range(24)
    const unsigned char buffer3[] = {0x15, 0x01, 0x00};
    EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer3, ARRAY_SIZE(buffer3))));

    // range(8, 32)
    const unsigned char buffer4[] = {0x15, 0x01, 0x10};
    EXPECT_CALL(streams, getStreamProxy(3, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer4, ARRAY_SIZE(buffer4))));

    // create the row type
    std::unique_ptr<Type> unionType = createUnionType();
    unionType->addUnionChild(createPrimitiveType(LONG));
    unionType->addUnionChild(createPrimitiveType(INT));
    std::unique_ptr<Type> rowType = createStructType();
    rowType->addStructField("col0", std::move(unionType));

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    StructVectorBatch batch(512, *getDefaultPool());
    UnionVectorBatch* unions = new UnionVectorBatch(512, *getDefaultPool());
    LongVectorBatch* child1 = new LongVectorBatch(512, *getDefaultPool());
    LongVectorBatch* child2 = new LongVectorBatch(512, *getDefaultPool());
    batch.fields.push_back(unions);
    unions->children.push_back(child1);
    unions->children.push_back(child2);
    reader->next(batch, 96, 0);
    ASSERT_EQ(96, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(96, unions->numElements);
    ASSERT_EQ(true, unions->hasNulls);
    ASSERT_EQ(24, child1->numElements);
    ASSERT_EQ(true, !child1->hasNulls);
    ASSERT_EQ(24, child2->numElements);
    ASSERT_EQ(true, !child2->hasNulls);
    for (size_t i = 0; i < batch.numElements; ++i) {
      if (i < 24) {
        EXPECT_EQ(true, !unions->notNull[i]);
      } else if (i < 72) {
        EXPECT_EQ(true, unions->notNull[i]);
        EXPECT_EQ(i % 2, unions->tags[i]);
        EXPECT_EQ((i - 24) / 2, unions->offsets[i]);
        if (i % 2 == 0) {
          EXPECT_EQ((i - 24) / 2, child1->data[unions->offsets[i]]);
        } else {
          EXPECT_EQ((i - 24) / 2 + 8, child2->data[unions->offsets[i]]);
        }
      } else {
        EXPECT_EQ(true, !unions->notNull[i]);
      }
    }
  }

  TEST(TestColumnReader, testUnionSkips) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    std::vector<bool> selectedColumns(2, true);
    selectedColumns.push_back(false);
    selectedColumns.push_back(true);
    EXPECT_CALL(streams, getSelectedColumns()).WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_)).WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(testing::_, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));

    // [0] * 3 + [255] * 6 + [0] * 3
    const unsigned char buffer1[] = {0x00, 0x00, 0x03, 0xff, 0x00, 0x00};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer1, ARRAY_SIZE(buffer1))));

    // [0, 1] * 24
    const unsigned char buffer2[] = {0xd0, 0x00, 0x01, 0x00, 0x01, 0x00, 0x01, 0x00, 0x01, 0x00,
                                     0x01, 0x00, 0x01, 0x00, 0x01, 0x00, 0x01, 0x00, 0x01, 0x00,
                                     0x01, 0x00, 0x01, 0x00, 0x01, 0x00, 0x01, 0x00, 0x01, 0x00,
                                     0x01, 0x00, 0x01, 0x00, 0x01, 0x00, 0x01, 0x00, 0x01, 0x00,
                                     0x01, 0x00, 0x01, 0x00, 0x01, 0x00, 0x01, 0x00, 0x01};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer2, ARRAY_SIZE(buffer2))));

    // range(8, 32)
    const unsigned char buffer3[] = {0x15, 0x01, 0x10};
    EXPECT_CALL(streams, getStreamProxy(3, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer3, ARRAY_SIZE(buffer3))));

    // create the row type
    std::unique_ptr<Type> unionType = createUnionType();
    unionType->addUnionChild(createPrimitiveType(LONG));
    unionType->addUnionChild(createPrimitiveType(INT));
    std::unique_ptr<Type> rowType = createStructType();
    rowType->addStructField("col0", std::move(unionType));

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    StructVectorBatch batch(512, *getDefaultPool());
    UnionVectorBatch* unions = new UnionVectorBatch(512, *getDefaultPool());
    LongVectorBatch* child2 = new LongVectorBatch(512, *getDefaultPool());
    batch.fields.push_back(unions);
    unions->children.push_back(nullptr);
    unions->children.push_back(child2);

    reader->next(batch, 26, 0);
    ASSERT_EQ(26, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(26, unions->numElements);
    ASSERT_EQ(true, unions->hasNulls);
    ASSERT_EQ(1, child2->numElements);
    ASSERT_EQ(true, !child2->hasNulls);
    for (size_t i = 0; i < 24; ++i) {
      EXPECT_EQ(true, !unions->notNull[i]);
    }
    EXPECT_EQ(true, unions->notNull[24]);
    EXPECT_EQ(0, unions->tags[24]);
    EXPECT_EQ(true, unions->notNull[25]);
    EXPECT_EQ(1, unions->tags[25]);
    EXPECT_EQ(0, unions->offsets[25]);
    EXPECT_EQ(8, child2->data[unions->offsets[25]]);

    reader->skip(44);

    reader->next(batch, 26, 0);
    ASSERT_EQ(26, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(26, unions->numElements);
    ASSERT_EQ(true, unions->hasNulls);
    ASSERT_EQ(1, child2->numElements);
    ASSERT_EQ(true, !child2->hasNulls);
    EXPECT_EQ(true, unions->notNull[0]);
    EXPECT_EQ(0, unions->tags[0]);
    EXPECT_EQ(true, unions->notNull[1]);
    EXPECT_EQ(1, unions->tags[1]);
    EXPECT_EQ(0, unions->offsets[1]);
    EXPECT_EQ(31, child2->data[unions->offsets[1]]);
    for (size_t i = 2; i < 26; ++i) {
      EXPECT_EQ(true, !unions->notNull[i]);
    }
  }

  TEST(TestColumnReader, testUnionLongSkip) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    std::vector<bool> selectedColumns(3, true);
    selectedColumns.push_back(false);
    EXPECT_CALL(streams, getSelectedColumns()).WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_)).WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(testing::_, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));

    // [1] * 1300 + [0] * 1300
    const unsigned char buffer1[] = {0x7f, 0x01, 0x7f, 0x01, 0x7f, 0x01, 0x7f, 0x01, 0x7f, 0x01,
                                     0x7f, 0x01, 0x7f, 0x01, 0x7f, 0x01, 0x7f, 0x01, 0x7f, 0x01,
                                     0x7f, 0x00, 0x7f, 0x00, 0x7f, 0x00, 0x7f, 0x00, 0x7f, 0x00,
                                     0x7f, 0x00, 0x7f, 0x00, 0x7f, 0x00, 0x7f, 0x00, 0x7f, 0x00};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer1, ARRAY_SIZE(buffer1))));

    // range(0, 1300)
    const unsigned char buffer2[] = {0x7f, 0x01, 0x00, 0x7f, 0x01, 0x84, 0x02, 0x7f, 0x01, 0x88,
                                     0x04, 0x7f, 0x01, 0x8c, 0x06, 0x7f, 0x01, 0x90, 0x08, 0x7f,
                                     0x01, 0x94, 0x0a, 0x7f, 0x01, 0x98, 0x0c, 0x7f, 0x01, 0x9c,
                                     0x0e, 0x7f, 0x01, 0xa0, 0x10, 0x7f, 0x01, 0xa4, 0x12};
    EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer2, ARRAY_SIZE(buffer2))));

    // create the row type
    std::unique_ptr<Type> unionType = createUnionType();
    unionType->addUnionChild(createPrimitiveType(LONG));
    unionType->addUnionChild(createPrimitiveType(INT));
    std::unique_ptr<Type> rowType = createStructType();
    rowType->addStructField("col0", std::move(unionType));

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    StructVectorBatch batch(512, *getDefaultPool());
    UnionVectorBatch* unions = new UnionVectorBatch(512, *getDefaultPool());
    LongVectorBatch* child1 = new LongVectorBatch(512, *getDefaultPool());
    batch.fields.push_back(unions);
    unions->children.push_back(child1);
    unions->children.push_back(nullptr);

    reader->next(batch, 10, 0);
    ASSERT_EQ(10, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(10, unions->numElements);
    ASSERT_EQ(true, !unions->hasNulls);
    ASSERT_EQ(0, child1->numElements);
    ASSERT_EQ(true, !child1->hasNulls);
    for (size_t i = 0; i < batch.numElements; ++i) {
      EXPECT_EQ(1, unions->tags[i]);
      EXPECT_EQ(i, unions->offsets[i]);
    }
    reader->skip(2490);

    reader->next(batch, 100, 0);
    ASSERT_EQ(100, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(100, unions->numElements);
    ASSERT_EQ(true, !unions->hasNulls);
    ASSERT_EQ(100, child1->numElements);
    ASSERT_EQ(true, !child1->hasNulls);
    for (size_t i = 0; i < batch.numElements; ++i) {
      EXPECT_EQ(0, unions->tags[i]);
      EXPECT_EQ(i, unions->offsets[i]);
      EXPECT_EQ(i + 1200, child1->data[unions->offsets[i]]);
    }
  }

  TEST(TestColumnReader, testUnionWithManyVariants) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    std::vector<bool> selectedColumns(132, true);
    EXPECT_CALL(streams, getSelectedColumns()).WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_)).WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(testing::_, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));

    // list(range(130)) * 3
    unsigned char tagBuffer[129 * 3 + 7];
    // three literal runs of length 128 and one literal run of 6
    tagBuffer[0] = 0x80;
    tagBuffer[129] = 0x80;
    tagBuffer[258] = 0x80;
    tagBuffer[387] = 0xfa;
    for (size_t i = 0; i < 128; ++i) {
      tagBuffer[i + 1] = static_cast<unsigned char>(i);
      tagBuffer[i + 130] = static_cast<unsigned char>((i + 128) % 130);
      tagBuffer[i + 259] = static_cast<unsigned char>((i + 256) % 130);
      if (i < 6) {
        tagBuffer[i + 388] = static_cast<unsigned char>((i + 384) % 130);
      }
    }
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(tagBuffer, ARRAY_SIZE(tagBuffer))));

    // for variant in range(0, 130):
    //   [variant & 0x3f, (variant & 0x3f) + 1, (variant & 0x3f) + 2]
    unsigned char buffer[3 * 130];
    for (size_t variant = 0; variant < 130; ++variant) {
      buffer[3 * variant] = 0x00;
      buffer[3 * variant + 1] = 0x01;
      buffer[3 * variant + 2] = static_cast<unsigned char>((variant * 2) & 0x7f);
      EXPECT_CALL(streams, getStreamProxy(variant + 2, proto::Stream_Kind_DATA, true))
          .WillRepeatedly(testing::Return(new SeekableArrayInputStream(buffer + 3 * variant, 3)));
    }

    // create the row type
    std::unique_ptr<Type> unionType = createUnionType();
    for (size_t variant = 0; variant < 130; ++variant) {
      unionType->addUnionChild(createPrimitiveType(LONG));
    }
    std::unique_ptr<Type> rowType = createStructType();
    rowType->addStructField("col0", std::move(unionType));

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    StructVectorBatch batch(512, *getDefaultPool());
    UnionVectorBatch* unions = new UnionVectorBatch(512, *getDefaultPool());
    batch.fields.push_back(unions);
    for (size_t variant = 0; variant < 130; ++variant) {
      unions->children.push_back(new LongVectorBatch(512, *getDefaultPool()));
    }

    reader->next(batch, 130, 0);
    ASSERT_EQ(130, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(130, unions->numElements);
    ASSERT_EQ(true, !unions->hasNulls);
    for (size_t variant = 0; variant < 130; ++variant) {
      ASSERT_EQ(1, unions->children[variant]->numElements);
      ASSERT_EQ(true, !unions->children[variant]->hasNulls);
    }
    for (size_t i = 0; i < batch.numElements; ++i) {
      EXPECT_EQ(i, unions->tags[i]);
      EXPECT_EQ(0, unions->offsets[i]);
      EXPECT_EQ(i & 0x3f, dynamic_cast<LongVectorBatch*>(unions->children[unions->tags[i]])
                              ->data[unions->offsets[i]]);
    }
    reader->skip(30);

    reader->next(batch, 230, 0);
    ASSERT_EQ(230, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(230, unions->numElements);
    ASSERT_EQ(true, !unions->hasNulls);

    // check each variant has the right overall information
    for (size_t variant = 0; variant < 130; ++variant) {
      ASSERT_EQ(variant < 30 ? 1 : 2, unions->children[variant]->numElements);
      ASSERT_EQ(true, !unions->children[variant]->hasNulls);
    }

    // check to see if each row is right
    for (size_t i = 0; i < batch.numElements; ++i) {
      size_t variant = (i + 30) % 130;
      ASSERT_EQ(variant, unions->tags[i]);
      ASSERT_EQ(i / 130, unions->offsets[i]);
      EXPECT_EQ(
          (variant & 0x3f) + (i < 100 ? 1 : 2),
          dynamic_cast<LongVectorBatch*>(unions->children[variant])->data[unions->offsets[i]]);
    }
  }

  TEST(TestColumnReader, testStringDictinoryIndexOverflow) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    std::vector<bool> selectedColumns(2, true);
    EXPECT_CALL(streams, getSelectedColumns()).WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(0)).WillRepeatedly(testing::Return(directEncoding));
    proto::ColumnEncoding dictionaryEncoding;
    dictionaryEncoding.set_kind(proto::ColumnEncoding_Kind_DICTIONARY);
    dictionaryEncoding.set_dictionarysize(2);
    EXPECT_CALL(streams, getEncoding(1)).WillRepeatedly(testing::Return(dictionaryEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(testing::Return(nullptr));
    // [11110000 for 0..127] * 2
    const unsigned char buffer1[] = {0x7f, 0xf0, 0x7f, 0xf0};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer1, ARRAY_SIZE(buffer1))));
    // [0 for x in 1..256*2] + [1 for x in 1..256*3]
    const unsigned char buffer2[] = {0x7f, 0x00, 0x00, 0x7f, 0x00, 0x00, 0x7f, 0x00,
                                     0x01, 0x7f, 0x00, 0x01, 0x7f, 0x00, 0x01};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer2, ARRAY_SIZE(buffer2))));
    const unsigned char buffer3[] = {0x4f, 0x52, 0x43, 0x4f, 0x77, 0x65, 0x6e};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DICTIONARY_DATA, false))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer3, ARRAY_SIZE(buffer3))));
    const unsigned char buffer4[] = {0x02, 0x01, 0x03};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, false))
        .WillRepeatedly(
            testing::Return(new SeekableArrayInputStream(buffer4, ARRAY_SIZE(buffer4))));

    // create the row type
    std::unique_ptr<Type> rowType = createStructType();
    rowType->addStructField("myString", createPrimitiveType(STRING));
    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    EncodedStringVectorBatch* encodedStringBatch =
        new EncodedStringVectorBatch(1024, *getDefaultPool());

    StructVectorBatch batch(1024, *getDefaultPool());
    batch.fields.push_back(encodedStringBatch);
    reader->nextEncoded(batch, 8, 0);
    ASSERT_EQ(8, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(8, encodedStringBatch->numElements);
    ASSERT_EQ(true, encodedStringBatch->hasNulls);
    reader->nextEncoded(batch, 1100, 0);
    ASSERT_EQ(1100, batch.numElements);
    ASSERT_EQ(true, !batch.hasNulls);
    ASSERT_EQ(1100, encodedStringBatch->numElements);
    ASSERT_EQ(true, encodedStringBatch->hasNulls);
    for (size_t i = 0; i < batch.numElements; ++i) {
      if (i & 4) {
        EXPECT_EQ(0, encodedStringBatch->notNull[i]);
      } else {
        EXPECT_EQ(1, encodedStringBatch->notNull[i]);
        const char* expected = i < 512 ? "ORC" : "Owen";
        int64_t index = encodedStringBatch->index.data()[i];

        char* actualString;
        int64_t actualLength;
        encodedStringBatch->dictionary->getValueByIndex(index, actualString, actualLength);
        ASSERT_EQ(strlen(expected), actualLength) << "Wrong length at " << i;

        for (size_t letter = 0; letter < strlen(expected); ++letter) {
          EXPECT_EQ(expected[letter], actualString[letter])
              << "Wrong contents at " << i << ", " << letter;
        }
      }
    }
  }

  INSTANTIATE_TEST_SUITE_P(OrcColumnReaderTest, TestColumnReaderEncoded, Values(true, false));

}  // namespace orc
