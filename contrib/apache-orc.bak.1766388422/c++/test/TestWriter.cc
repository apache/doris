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

#include "orc/ColumnPrinter.hh"
#include "orc/OrcFile.hh"

#include "MemoryInputStream.hh"
#include "MemoryOutputStream.hh"
#include "Reader.hh"

#include "wrap/gmock.h"
#include "wrap/gtest-wrapper.h"

#include <cmath>
#include <ctime>
#include <sstream>

#ifdef __clang__
DIAGNOSTIC_IGNORE("-Wmissing-variable-declarations")
#endif

namespace orc {

  using ::testing::TestWithParam;
  using ::testing::Values;

  const int DEFAULT_MEM_STREAM_SIZE = 100 * 1024 * 1024;  // 100M

  std::unique_ptr<Writer> createWriter(uint64_t stripeSize, uint64_t compresionblockSize,
                                       CompressionKind compression, const Type& type,
                                       MemoryPool* memoryPool, OutputStream* stream,
                                       FileVersion version, uint64_t stride = 0,
                                       const std::string& timezone = "GMT",
                                       bool useTightNumericVector = false) {
    WriterOptions options;
    options.setStripeSize(stripeSize);
    options.setCompressionBlockSize(compresionblockSize);
    options.setCompression(compression);
    options.setMemoryPool(memoryPool);
    options.setRowIndexStride(stride);
    options.setFileVersion(version);
    options.setTimezoneName(timezone);
    options.setUseTightNumericVector(useTightNumericVector);
    return createWriter(type, stream, options);
  }

  std::unique_ptr<Reader> createReader(MemoryPool* memoryPool,
                                       std::unique_ptr<InputStream> stream) {
    ReaderOptions options;
    options.setMemoryPool(*memoryPool);
    return createReader(std::move(stream), options);
  }

  std::unique_ptr<RowReader> createRowReader(Reader* reader, const std::string& timezone = "GMT",
                                             bool useTightNumericVector = false) {
    RowReaderOptions rowReaderOpts;
    rowReaderOpts.setTimezoneName(timezone);
    rowReaderOpts.setUseTightNumericVector(useTightNumericVector);
    return reader->createRowReader(rowReaderOpts);
  }

  class WriterTest : public TestWithParam<FileVersion> {
    // You can implement all the usual fixture class members here.
    // To access the test parameter, call GetParam() from class
    // TestWithParam<T>.
    void SetUp() override;

   protected:
    FileVersion fileVersion;

   public:
    WriterTest() : fileVersion(FileVersion::v_0_11()) {}
  };

  void WriterTest::SetUp() {
    fileVersion = GetParam();
  }

  TEST_P(WriterTest, writeEmptyFile) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();
    std::unique_ptr<Type> type(Type::buildTypeFromString("struct<col1:int>"));

    uint64_t stripeSize = 16 * 1024;       // 16K
    uint64_t compressionBlockSize = 1024;  // 1k

    std::unique_ptr<Writer> writer =
        createWriter(stripeSize, compressionBlockSize, CompressionKind_ZLIB, *type, pool,
                     &memStream, fileVersion);
    writer->close();

    auto inStream = std::make_unique<MemoryInputStream>(memStream.getData(), memStream.getLength());
    std::unique_ptr<Reader> reader = createReader(pool, std::move(inStream));
    std::unique_ptr<RowReader> rowReader = createRowReader(reader.get());
    EXPECT_EQ(fileVersion, reader->getFormatVersion());
    EXPECT_EQ(WriterVersion_ORC_135, reader->getWriterVersion());
    EXPECT_EQ(0, reader->getNumberOfRows());

    WriterId writerId = WriterId::ORC_CPP_WRITER;
    EXPECT_EQ(writerId, reader->getWriterId());
    EXPECT_EQ(1, reader->getWriterIdValue());

    std::unique_ptr<ColumnVectorBatch> batch = rowReader->createRowBatch(1024);
    EXPECT_FALSE(rowReader->next(*batch));
  }

  TEST_P(WriterTest, writeIntFileOneStripe) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();
    std::unique_ptr<Type> type(Type::buildTypeFromString("struct<col1:int>"));

    uint64_t stripeSize = 16 * 1024;       // 16K
    uint64_t compressionBlockSize = 1024;  // 1k

    std::unique_ptr<Writer> writer =
        createWriter(stripeSize, compressionBlockSize, CompressionKind_ZLIB, *type, pool,
                     &memStream, fileVersion);
    std::unique_ptr<ColumnVectorBatch> batch = writer->createRowBatch(1024);
    StructVectorBatch* structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    LongVectorBatch* longBatch = dynamic_cast<LongVectorBatch*>(structBatch->fields[0]);

    for (uint64_t i = 0; i < 1024; ++i) {
      longBatch->data[i] = static_cast<int64_t>(i);
    }
    structBatch->numElements = 1024;
    longBatch->numElements = 1024;

    writer->add(*batch);

    for (uint64_t i = 1024; i < 2000; ++i) {
      longBatch->data[i - 1024] = static_cast<int64_t>(i);
    }
    structBatch->numElements = 2000 - 1024;
    longBatch->numElements = 2000 - 1024;

    writer->add(*batch);
    writer->addUserMetadata("name0", "value0");
    writer->addUserMetadata("name1", "value1");
    writer->close();

    auto inStream = std::make_unique<MemoryInputStream>(memStream.getData(), memStream.getLength());
    std::unique_ptr<Reader> reader = createReader(pool, std::move(inStream));
    std::unique_ptr<RowReader> rowReader = createRowReader(reader.get());
    EXPECT_EQ(2000, reader->getNumberOfRows());

    batch = rowReader->createRowBatch(2048);
    EXPECT_TRUE(rowReader->next(*batch));
    EXPECT_EQ(2000, batch->numElements);
    EXPECT_FALSE(rowReader->next(*batch));

    std::list<std::string> keys = reader->getMetadataKeys();
    EXPECT_EQ(keys.size(), 2);
    std::list<std::string>::const_iterator itr = keys.begin();
    EXPECT_EQ(*itr, "name0");
    EXPECT_EQ(reader->getMetadataValue(*itr), "value0");
    itr++;
    EXPECT_EQ(*itr, "name1");
    EXPECT_EQ(reader->getMetadataValue(*itr), "value1");

    for (uint64_t i = 0; i < 2000; ++i) {
      structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
      longBatch = dynamic_cast<LongVectorBatch*>(structBatch->fields[0]);
      EXPECT_EQ(i, longBatch->data[i]);
    }
  }

  TEST_P(WriterTest, writeIntFileMultipleStripes) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();
    std::unique_ptr<Type> type(Type::buildTypeFromString("struct<col1:int>"));

    uint64_t stripeSize = 1024;            // 1K
    uint64_t compressionBlockSize = 1024;  // 1k

    std::unique_ptr<Writer> writer =
        createWriter(stripeSize, compressionBlockSize, CompressionKind_ZLIB, *type, pool,
                     &memStream, fileVersion);
    std::unique_ptr<ColumnVectorBatch> batch = writer->createRowBatch(65535);
    StructVectorBatch* structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    LongVectorBatch* longBatch = dynamic_cast<LongVectorBatch*>(structBatch->fields[0]);

    for (uint64_t j = 0; j < 10; ++j) {
      for (uint64_t i = 0; i < 65535; ++i) {
        longBatch->data[i] = static_cast<int64_t>(i);
      }
      structBatch->numElements = 65535;
      longBatch->numElements = 65535;

      writer->add(*batch);
    }

    writer->close();

    auto inStream = std::make_unique<MemoryInputStream>(memStream.getData(), memStream.getLength());
    std::unique_ptr<Reader> reader = createReader(pool, std::move(inStream));
    std::unique_ptr<RowReader> rowReader = createRowReader(reader.get());
    EXPECT_EQ(655350, reader->getNumberOfRows());

    batch = rowReader->createRowBatch(65535);
    for (uint64_t j = 0; j < 10; ++j) {
      EXPECT_TRUE(rowReader->next(*batch));
      EXPECT_EQ(65535, batch->numElements);

      for (uint64_t i = 0; i < 65535; ++i) {
        structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
        longBatch = dynamic_cast<LongVectorBatch*>(structBatch->fields[0]);
        EXPECT_EQ(i, longBatch->data[i]);
      }
    }
    EXPECT_FALSE(rowReader->next(*batch));
  }

  TEST_P(WriterTest, writeStringAndBinaryColumn) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();
    std::unique_ptr<Type> type(Type::buildTypeFromString("struct<col1:string,col2:binary>"));

    uint64_t stripeSize = 1024;            // 1K
    uint64_t compressionBlockSize = 1024;  // 1k

    char dataBuffer[327675];
    uint64_t offset = 0;

    std::unique_ptr<Writer> writer =
        createWriter(stripeSize, compressionBlockSize, CompressionKind_ZLIB, *type, pool,
                     &memStream, fileVersion);
    std::unique_ptr<ColumnVectorBatch> batch = writer->createRowBatch(65535);
    StructVectorBatch* structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    StringVectorBatch* strBatch = dynamic_cast<StringVectorBatch*>(structBatch->fields[0]);
    StringVectorBatch* binBatch = dynamic_cast<StringVectorBatch*>(structBatch->fields[1]);

    for (uint64_t i = 0; i < 65535; ++i) {
      std::ostringstream os;
      os << i;
      strBatch->data[i] = dataBuffer + offset;
      strBatch->length[i] = static_cast<int64_t>(os.str().size());
      binBatch->data[i] = dataBuffer + offset;
      binBatch->length[i] = static_cast<int64_t>(os.str().size());
      memcpy(dataBuffer + offset, os.str().c_str(), os.str().size());
      offset += os.str().size();
    }

    structBatch->numElements = 65535;
    strBatch->numElements = 65535;
    binBatch->numElements = 65535;

    writer->add(*batch);
    writer->close();

    auto inStream = std::make_unique<MemoryInputStream>(memStream.getData(), memStream.getLength());
    std::unique_ptr<Reader> reader = createReader(pool, std::move(inStream));
    std::unique_ptr<RowReader> rowReader = createRowReader(reader.get());
    EXPECT_EQ(65535, reader->getNumberOfRows());

    batch = rowReader->createRowBatch(65535);
    EXPECT_EQ(true, rowReader->next(*batch));
    EXPECT_EQ(65535, batch->numElements);

    for (uint64_t i = 0; i < 65535; ++i) {
      structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
      strBatch = dynamic_cast<StringVectorBatch*>(structBatch->fields[0]);
      binBatch = dynamic_cast<StringVectorBatch*>(structBatch->fields[1]);
      std::string str(strBatch->data[i], static_cast<size_t>(strBatch->length[i]));
      std::string bin(binBatch->data[i], static_cast<size_t>(binBatch->length[i]));
      EXPECT_EQ(i, static_cast<uint64_t>(atoi(str.c_str())));
      EXPECT_EQ(i, static_cast<uint64_t>(atoi(bin.c_str())));
    }

    EXPECT_FALSE(rowReader->next(*batch));
  }

  TEST_P(WriterTest, writeFloatAndDoubleColumn) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();
    std::unique_ptr<Type> type(Type::buildTypeFromString("struct<col1:double,col2:float>"));

    uint64_t stripeSize = 16 * 1024;
    uint64_t compressionBlockSize = 1024;
    uint64_t rowCount = 655350;

    std::vector<double> data(rowCount);
    for (uint64_t i = 0; i < rowCount; ++i) {
      data[i] = 100000 * (std::rand() * 1.0 / RAND_MAX);
    }

    std::unique_ptr<Writer> writer =
        createWriter(stripeSize, compressionBlockSize, CompressionKind_ZLIB, *type, pool,
                     &memStream, fileVersion);
    std::unique_ptr<ColumnVectorBatch> batch = writer->createRowBatch(rowCount);
    StructVectorBatch* structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    DoubleVectorBatch* doubleBatch = dynamic_cast<DoubleVectorBatch*>(structBatch->fields[0]);
    DoubleVectorBatch* floatBatch = dynamic_cast<DoubleVectorBatch*>(structBatch->fields[1]);

    for (uint64_t i = 0; i < rowCount; ++i) {
      doubleBatch->data[i] = data[i];
      floatBatch->data[i] = data[i];
    }

    structBatch->numElements = rowCount;
    doubleBatch->numElements = rowCount;
    floatBatch->numElements = rowCount;

    writer->add(*batch);
    writer->close();

    auto inStream = std::make_unique<MemoryInputStream>(memStream.getData(), memStream.getLength());
    std::unique_ptr<Reader> reader = createReader(pool, std::move(inStream));
    std::unique_ptr<RowReader> rowReader = createRowReader(reader.get());
    EXPECT_EQ(rowCount, reader->getNumberOfRows());

    batch = rowReader->createRowBatch(rowCount);
    EXPECT_EQ(true, rowReader->next(*batch));
    EXPECT_EQ(rowCount, batch->numElements);

    structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    doubleBatch = dynamic_cast<DoubleVectorBatch*>(structBatch->fields[0]);
    floatBatch = dynamic_cast<DoubleVectorBatch*>(structBatch->fields[1]);
    for (uint64_t i = 0; i < rowCount; ++i) {
      EXPECT_TRUE(std::abs(data[i] - doubleBatch->data[i]) < 0.000001);
      EXPECT_TRUE(std::abs(static_cast<float>(data[i]) - static_cast<float>(floatBatch->data[i])) <
                  0.000001f);
    }
    EXPECT_FALSE(rowReader->next(*batch));
  }

  TEST_P(WriterTest, writeShortIntLong) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();
    std::unique_ptr<Type> type(
        Type::buildTypeFromString("struct<col1:smallint,col2:int,col3:bigint>"));

    uint64_t stripeSize = 16 * 1024;
    uint64_t compressionBlockSize = 1024;
    uint64_t rowCount = 65535;

    std::unique_ptr<Writer> writer =
        createWriter(stripeSize, compressionBlockSize, CompressionKind_ZLIB, *type, pool,
                     &memStream, fileVersion);
    std::unique_ptr<ColumnVectorBatch> batch = writer->createRowBatch(rowCount);
    StructVectorBatch* structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    LongVectorBatch* smallIntBatch = dynamic_cast<LongVectorBatch*>(structBatch->fields[0]);
    LongVectorBatch* intBatch = dynamic_cast<LongVectorBatch*>(structBatch->fields[1]);
    LongVectorBatch* bigIntBatch = dynamic_cast<LongVectorBatch*>(structBatch->fields[2]);

    for (uint64_t i = 0; i < rowCount; ++i) {
      smallIntBatch->data[i] = static_cast<int16_t>(i);
      intBatch->data[i] = static_cast<int32_t>(i);
      bigIntBatch->data[i] = static_cast<int64_t>(i);
    }
    structBatch->numElements = rowCount;
    smallIntBatch->numElements = rowCount;
    intBatch->numElements = rowCount;
    bigIntBatch->numElements = rowCount;

    writer->add(*batch);
    writer->close();

    auto inStream = std::make_unique<MemoryInputStream>(memStream.getData(), memStream.getLength());
    std::unique_ptr<Reader> reader = createReader(pool, std::move(inStream));
    std::unique_ptr<RowReader> rowReader = createRowReader(reader.get());
    EXPECT_EQ(rowCount, reader->getNumberOfRows());

    batch = rowReader->createRowBatch(rowCount);
    EXPECT_EQ(true, rowReader->next(*batch));

    structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    smallIntBatch = dynamic_cast<LongVectorBatch*>(structBatch->fields[0]);
    intBatch = dynamic_cast<LongVectorBatch*>(structBatch->fields[1]);
    bigIntBatch = dynamic_cast<LongVectorBatch*>(structBatch->fields[2]);
    for (uint64_t i = 0; i < rowCount; ++i) {
      EXPECT_EQ(static_cast<int16_t>(i), smallIntBatch->data[i]);
      EXPECT_EQ(static_cast<int32_t>(i), intBatch->data[i]);
      EXPECT_EQ(static_cast<int64_t>(i), bigIntBatch->data[i]);
    }
  }

  TEST_P(WriterTest, writeTinyint) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();
    std::unique_ptr<Type> type(Type::buildTypeFromString("struct<col1:tinyint>"));

    uint64_t stripeSize = 16 * 1024;
    uint64_t compressionBlockSize = 1024;
    uint64_t rowCount = 65535;

    std::unique_ptr<Writer> writer =
        createWriter(stripeSize, compressionBlockSize, CompressionKind_ZLIB, *type, pool,
                     &memStream, fileVersion);
    std::unique_ptr<ColumnVectorBatch> batch = writer->createRowBatch(rowCount);
    StructVectorBatch* structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    LongVectorBatch* byteBatch = dynamic_cast<LongVectorBatch*>(structBatch->fields[0]);

    for (uint64_t i = 0; i < rowCount; ++i) {
      byteBatch->data[i] = static_cast<int8_t>(i);
    }
    structBatch->numElements = rowCount;
    byteBatch->numElements = rowCount;

    writer->add(*batch);
    writer->close();

    auto inStream = std::make_unique<MemoryInputStream>(memStream.getData(), memStream.getLength());
    std::unique_ptr<Reader> reader = createReader(pool, std::move(inStream));
    std::unique_ptr<RowReader> rowReader = createRowReader(reader.get());
    EXPECT_EQ(rowCount, reader->getNumberOfRows());

    batch = rowReader->createRowBatch(rowCount);
    EXPECT_EQ(true, rowReader->next(*batch));

    structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    byteBatch = dynamic_cast<LongVectorBatch*>(structBatch->fields[0]);
    for (uint64_t i = 0; i < rowCount; ++i) {
      EXPECT_EQ(static_cast<int8_t>(i), static_cast<int8_t>(byteBatch->data[i]));
    }
  }

  TEST_P(WriterTest, writeBooleanColumn) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();
    std::unique_ptr<Type> type(Type::buildTypeFromString("struct<col1:boolean>"));

    uint64_t stripeSize = 16 * 1024;
    uint64_t compressionBlockSize = 1024;
    uint64_t rowCount = 65535;

    std::unique_ptr<Writer> writer =
        createWriter(stripeSize, compressionBlockSize, CompressionKind_ZLIB, *type, pool,
                     &memStream, fileVersion);
    std::unique_ptr<ColumnVectorBatch> batch = writer->createRowBatch(rowCount);
    StructVectorBatch* structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    LongVectorBatch* byteBatch = dynamic_cast<LongVectorBatch*>(structBatch->fields[0]);

    for (uint64_t i = 0; i < rowCount; ++i) {
      byteBatch->data[i] = (i % 3) == 0 ? 1 : 0;
    }
    structBatch->numElements = rowCount;
    byteBatch->numElements = rowCount;

    writer->add(*batch);
    writer->close();

    auto inStream = std::make_unique<MemoryInputStream>(memStream.getData(), memStream.getLength());
    std::unique_ptr<Reader> reader = createReader(pool, std::move(inStream));
    std::unique_ptr<RowReader> rowReader = createRowReader(reader.get());
    EXPECT_EQ(rowCount, reader->getNumberOfRows());

    batch = rowReader->createRowBatch(rowCount);
    EXPECT_EQ(true, rowReader->next(*batch));

    structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    byteBatch = dynamic_cast<LongVectorBatch*>(structBatch->fields[0]);
    for (uint64_t i = 0; i < rowCount; ++i) {
      EXPECT_EQ((i % 3) == 0 ? 1 : 0, byteBatch->data[i]);
    }
  }

  TEST_P(WriterTest, writeDate) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();
    std::unique_ptr<Type> type(Type::buildTypeFromString("struct<col1:date>"));

    uint64_t stripeSize = 16 * 1024;
    uint64_t compressionBlockSize = 1024;
    uint64_t rowCount = 1024;

    std::unique_ptr<Writer> writer =
        createWriter(stripeSize, compressionBlockSize, CompressionKind_ZLIB, *type, pool,
                     &memStream, fileVersion);

    std::unique_ptr<ColumnVectorBatch> batch = writer->createRowBatch(rowCount);
    StructVectorBatch* structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    LongVectorBatch* longBatch = dynamic_cast<LongVectorBatch*>(structBatch->fields[0]);

    for (uint64_t i = 0; i < rowCount; ++i) {
      longBatch->data[i] = static_cast<int32_t>(i);
    }
    structBatch->numElements = rowCount;
    longBatch->numElements = rowCount;

    writer->add(*batch);
    writer->close();

    auto inStream = std::make_unique<MemoryInputStream>(memStream.getData(), memStream.getLength());
    std::unique_ptr<Reader> reader = createReader(pool, std::move(inStream));
    std::unique_ptr<RowReader> rowReader = createRowReader(reader.get());
    EXPECT_EQ(rowCount, reader->getNumberOfRows());

    batch = rowReader->createRowBatch(rowCount);
    EXPECT_EQ(true, rowReader->next(*batch));

    structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    longBatch = dynamic_cast<LongVectorBatch*>(structBatch->fields[0]);
    for (uint64_t i = 0; i < rowCount; ++i) {
      EXPECT_EQ(static_cast<int32_t>(i), longBatch->data[i]);
    }
  }

  TEST_P(WriterTest, writeTimestamp) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();
    std::unique_ptr<Type> type(Type::buildTypeFromString("struct<col1:timestamp>"));

    uint64_t stripeSize = 16 * 1024;
    uint64_t compressionBlockSize = 1024;
    uint64_t rowCount = 102400;

    std::unique_ptr<Writer> writer =
        createWriter(stripeSize, compressionBlockSize, CompressionKind_ZLIB, *type, pool,
                     &memStream, fileVersion);
    std::unique_ptr<ColumnVectorBatch> batch = writer->createRowBatch(rowCount);
    StructVectorBatch* structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    TimestampVectorBatch* tsBatch = dynamic_cast<TimestampVectorBatch*>(structBatch->fields[0]);

    std::vector<std::time_t> times(rowCount);
    for (uint64_t i = 0; i < rowCount; ++i) {
      time_t currTime = -14210715;  // 1969-07-20 12:34:45
      times[i] = static_cast<int64_t>(currTime) + static_cast<int64_t>(i * 3660);
      tsBatch->data[i] = times[i];
      tsBatch->nanoseconds[i] = static_cast<int64_t>(i * 1000);
    }
    structBatch->numElements = rowCount;
    tsBatch->numElements = rowCount;

    writer->add(*batch);
    writer->close();

    auto inStream = std::make_unique<MemoryInputStream>(memStream.getData(), memStream.getLength());
    std::unique_ptr<Reader> reader = createReader(pool, std::move(inStream));
    std::unique_ptr<RowReader> rowReader = createRowReader(reader.get());
    EXPECT_EQ(rowCount, reader->getNumberOfRows());

    batch = rowReader->createRowBatch(rowCount);
    EXPECT_EQ(true, rowReader->next(*batch));

    structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    tsBatch = dynamic_cast<TimestampVectorBatch*>(structBatch->fields[0]);
    for (uint64_t i = 0; i < rowCount; ++i) {
      EXPECT_EQ(times[i], tsBatch->data[i]);
      EXPECT_EQ(i * 1000, tsBatch->nanoseconds[i]);
    }
  }

  TEST_P(WriterTest, writeNegativeTimestamp) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();
    std::unique_ptr<Type> type(Type::buildTypeFromString("struct<a:timestamp>"));
    auto writer = createWriter(16 * 1024 * 1024, 64 * 1024, CompressionKind_ZLIB, *type, pool,
                               &memStream, fileVersion);
    uint64_t batchCount = 5;
    auto batch = writer->createRowBatch(batchCount * 2);
    auto structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    auto tsBatch = dynamic_cast<TimestampVectorBatch*>(structBatch->fields[0]);
    structBatch->numElements = batchCount;
    tsBatch->numElements = batchCount;
    const int64_t seconds[] = {-2, -1, 0, 1, 2};

    // write 1st batch with nanosecond <= 999999
    for (uint64_t i = 0; i < batchCount; ++i) {
      tsBatch->data[i] = seconds[i];
      tsBatch->nanoseconds[i] = 999999;
    }
    writer->add(*batch);

    // write 2nd batch with nanosecond > 999999
    for (uint64_t i = 0; i < batchCount; ++i) {
      tsBatch->data[i] = seconds[i];
      tsBatch->nanoseconds[i] = 1000000;
    }
    writer->add(*batch);
    writer->close();

    auto inStream = std::make_unique<MemoryInputStream>(memStream.getData(), memStream.getLength());
    auto reader = createReader(pool, std::move(inStream));
    auto rowReader = createRowReader(reader.get());
    batch = rowReader->createRowBatch(batchCount);
    structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    tsBatch = dynamic_cast<TimestampVectorBatch*>(structBatch->fields[0]);

    // read 1st batch with nanosecond <= 999999
    EXPECT_EQ(true, rowReader->next(*batch));
    for (uint64_t i = 0; i < batchCount; ++i) {
      EXPECT_EQ(seconds[i], tsBatch->data[i]);
      EXPECT_EQ(999999, tsBatch->nanoseconds[i]);
    }

    // read 2nd batch with nanosecond > 999999
    EXPECT_EQ(true, rowReader->next(*batch));
    for (uint64_t i = 0; i < batchCount; ++i) {
      if (seconds[i] == -1) {
        // reproduce the JDK bug of java.sql.Timestamp.
        // make sure the C++ ORC writer has consistent effect.
        EXPECT_EQ(0, tsBatch->data[i]);
      } else {
        EXPECT_EQ(seconds[i], tsBatch->data[i]);
      }
      EXPECT_EQ(1000000, tsBatch->nanoseconds[i]);
    }
  }

// TODO: Disable the test below for Windows for following reasons:
// First, the timezone name provided by Windows cannot be used as
// a parameter to the getTimezoneByName function. Secondly, the
// function of setting timezone in Windows is different from Linux.
#ifndef _MSC_VER
  void testWriteTimestampWithTimezone(FileVersion fileVersion, const char* writerTimezone,
                                      const char* readerTimezone, const std::string& tsStr,
                                      int isDst = 0) {
    char* tzBk = getenv("TZ");  // backup TZ env

    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();
    std::unique_ptr<Type> type(Type::buildTypeFromString("struct<col1:timestamp>"));

    uint64_t stripeSize = 16 * 1024;
    uint64_t compressionBlockSize = 1024;
    uint64_t rowCount = 1;

    std::unique_ptr<Writer> writer =
        createWriter(stripeSize, compressionBlockSize, CompressionKind_ZLIB, *type, pool,
                     &memStream, fileVersion, 0, writerTimezone);
    auto batch = writer->createRowBatch(rowCount);
    auto& structBatch = dynamic_cast<StructVectorBatch&>(*batch);
    auto& tsBatch = dynamic_cast<TimestampVectorBatch&>(*structBatch.fields[0]);

    // write timestamp in the writer timezone
    setenv("TZ", writerTimezone, 1);
    tzset();
    struct tm tm;
    memset(&tm, 0, sizeof(struct tm));
    strptime(tsStr.c_str(), "%Y-%m-%d %H:%M:%S", &tm);
    // mktime() does depend on external hint for daylight saving time
    tm.tm_isdst = isDst;
    tsBatch.data[0] = mktime(&tm);
    tsBatch.nanoseconds[0] = 0;
    structBatch.numElements = rowCount;
    tsBatch.numElements = rowCount;
    writer->add(*batch);
    writer->close();

    // read timestamp from the reader timezone
    auto inStream = std::make_unique<MemoryInputStream>(memStream.getData(), memStream.getLength());
    std::unique_ptr<Reader> reader = createReader(pool, std::move(inStream));
    std::unique_ptr<RowReader> rowReader = createRowReader(reader.get(), readerTimezone);
    EXPECT_EQ(true, rowReader->next(*batch));

    // verify we get same wall clock in reader timezone
    setenv("TZ", readerTimezone, 1);
    tzset();
    memset(&tm, 0, sizeof(struct tm));
    time_t ttime = tsBatch.data[0];
    localtime_r(&ttime, &tm);
    char buf[20];
    strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &tm);
    EXPECT_TRUE(strncmp(buf, tsStr.c_str(), tsStr.size()) == 0);

    // restore TZ env
    if (tzBk) {
      setenv("TZ", tzBk, 1);
      tzset();
    } else {
      unsetenv("TZ");
      tzset();
    }
  }

  TEST_P(WriterTest, writeTimestampWithTimezone) {
    const int IS_DST = 1, NOT_DST = 0;
    testWriteTimestampWithTimezone(fileVersion, "GMT", "GMT", "2001-11-12 18:31:01");
    // behavior for Apache Orc (writer & reader timezone can change)
    testWriteTimestampWithTimezone(fileVersion, "America/Los_Angeles", "America/Los_Angeles",
                                   "2001-11-12 18:31:01");
    testWriteTimestampWithTimezone(fileVersion, "Asia/Shanghai", "Asia/Shanghai",
                                   "2001-11-12 18:31:01");
    testWriteTimestampWithTimezone(fileVersion, "America/Los_Angeles", "Asia/Shanghai",
                                   "2001-11-12 18:31:01");
    testWriteTimestampWithTimezone(fileVersion, "Asia/Shanghai", "America/Los_Angeles",
                                   "2001-11-12 18:31:01");
    testWriteTimestampWithTimezone(fileVersion, "GMT", "Asia/Shanghai", "2001-11-12 18:31:01");
    testWriteTimestampWithTimezone(fileVersion, "Asia/Shanghai", "GMT", "2001-11-12 18:31:01");
    testWriteTimestampWithTimezone(fileVersion, "Asia/Shanghai", "America/Los_Angeles",
                                   "2018-01-01 23:59:59");
    // daylight saving started at 2012-03-11 02:00:00 in Los Angeles
    testWriteTimestampWithTimezone(fileVersion, "America/Los_Angeles", "Asia/Shanghai",
                                   "2012-03-11 01:59:59", NOT_DST);
    testWriteTimestampWithTimezone(fileVersion, "America/Los_Angeles", "Asia/Shanghai",
                                   "2012-03-11 03:00:00", IS_DST);
    testWriteTimestampWithTimezone(fileVersion, "America/Los_Angeles", "Asia/Shanghai",
                                   "2012-03-11 03:00:01", IS_DST);
    // daylight saving ended at 2012-11-04 02:00:00 in Los Angeles
    testWriteTimestampWithTimezone(fileVersion, "America/Los_Angeles", "Asia/Shanghai",
                                   "2012-11-04 01:59:59", IS_DST);
    testWriteTimestampWithTimezone(fileVersion, "America/Los_Angeles", "Asia/Shanghai",
                                   "2012-11-04 02:00:00", NOT_DST);
    testWriteTimestampWithTimezone(fileVersion, "America/Los_Angeles", "Asia/Shanghai",
                                   "2012-11-04 02:00:01", NOT_DST);
    // other daylight saving time
    testWriteTimestampWithTimezone(fileVersion, "America/Los_Angeles", "Asia/Shanghai",
                                   "2014-06-06 12:34:56", IS_DST);
    testWriteTimestampWithTimezone(fileVersion, "America/Los_Angeles", "America/Los_Angeles",
                                   "2014-06-06 12:34:56", IS_DST);
  }
#endif

  TEST_P(WriterTest, writeTimestampInstant) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();
    std::unique_ptr<Type> type(
        Type::buildTypeFromString("struct<col1:timestamp with local time zone>"));

    uint64_t stripeSize = 16 * 1024;
    uint64_t compressionBlockSize = 1024;
    uint64_t rowCount = 102400;

    std::unique_ptr<Writer> writer =
        createWriter(stripeSize, compressionBlockSize, CompressionKind_ZLIB, *type, pool,
                     &memStream, fileVersion);
    std::unique_ptr<ColumnVectorBatch> batch = writer->createRowBatch(rowCount);
    StructVectorBatch* structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    TimestampVectorBatch* tsBatch = dynamic_cast<TimestampVectorBatch*>(structBatch->fields[0]);

    std::vector<std::time_t> times(rowCount);
    for (uint64_t i = 0; i < rowCount; ++i) {
      time_t currTime = -14210715;  // 1969-07-20 12:34:45
      times[i] = static_cast<int64_t>(currTime) + static_cast<int64_t>(i * 3660);
      tsBatch->data[i] = times[i];
      tsBatch->nanoseconds[i] = static_cast<int64_t>(i * 1000);
    }
    structBatch->numElements = rowCount;
    tsBatch->numElements = rowCount;

    writer->add(*batch);
    writer->close();

    auto inStream = std::make_unique<MemoryInputStream>(memStream.getData(), memStream.getLength());
    std::unique_ptr<Reader> reader = createReader(pool, std::move(inStream));
    std::unique_ptr<RowReader> rowReader = createRowReader(reader.get());
    EXPECT_EQ(rowCount, reader->getNumberOfRows());

    batch = rowReader->createRowBatch(rowCount);
    EXPECT_EQ(true, rowReader->next(*batch));

    structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    tsBatch = dynamic_cast<TimestampVectorBatch*>(structBatch->fields[0]);
    for (uint64_t i = 0; i < rowCount; ++i) {
      EXPECT_EQ(times[i], tsBatch->data[i]);
      EXPECT_EQ(i * 1000, tsBatch->nanoseconds[i]);
    }
  }

  TEST_P(WriterTest, writeCharAndVarcharColumn) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();
    std::unique_ptr<Type> type(Type::buildTypeFromString("struct<col1:char(3),col2:varchar(4)>"));

    uint64_t stripeSize = 1024;
    uint64_t compressionBlockSize = 1024;
    uint64_t rowCount = 65535;

    char dataBuffer[327675];
    uint64_t offset = 0;

    std::unique_ptr<Writer> writer =
        createWriter(stripeSize, compressionBlockSize, CompressionKind_ZLIB, *type, pool,
                     &memStream, fileVersion);

    std::unique_ptr<ColumnVectorBatch> batch = writer->createRowBatch(rowCount);
    StructVectorBatch* structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    StringVectorBatch* charBatch = dynamic_cast<StringVectorBatch*>(structBatch->fields[0]);
    StringVectorBatch* varcharBatch = dynamic_cast<StringVectorBatch*>(structBatch->fields[1]);

    for (uint64_t i = 0; i < rowCount; ++i) {
      std::ostringstream os;
      os << i;
      charBatch->data[i] = dataBuffer + offset;
      charBatch->length[i] = static_cast<int64_t>(os.str().size());

      varcharBatch->data[i] = charBatch->data[i];
      varcharBatch->length[i] = charBatch->length[i];

      memcpy(dataBuffer + offset, os.str().c_str(), os.str().size());
      offset += os.str().size();
    }

    structBatch->numElements = rowCount;
    charBatch->numElements = rowCount;
    varcharBatch->numElements = rowCount;

    writer->add(*batch);
    writer->close();

    auto inStream = std::make_unique<MemoryInputStream>(memStream.getData(), memStream.getLength());
    std::unique_ptr<Reader> reader = createReader(pool, std::move(inStream));
    std::unique_ptr<RowReader> rowReader = createRowReader(reader.get());
    EXPECT_EQ(rowCount, reader->getNumberOfRows());

    batch = rowReader->createRowBatch(rowCount);
    EXPECT_EQ(true, rowReader->next(*batch));
    EXPECT_EQ(rowCount, batch->numElements);

    for (uint64_t i = 0; i < rowCount; ++i) {
      structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
      charBatch = dynamic_cast<StringVectorBatch*>(structBatch->fields[0]);
      varcharBatch = dynamic_cast<StringVectorBatch*>(structBatch->fields[1]);

      EXPECT_EQ(3, charBatch->length[i]);
      EXPECT_FALSE(varcharBatch->length[i] > 4);

      // test char data
      std::string charsRead(charBatch->data[i], static_cast<size_t>(charBatch->length[i]));

      std::ostringstream os;
      os << i;
      std::string charsExpected = os.str().substr(0, 3);
      while (charsExpected.length() < 3) {
        charsExpected += ' ';
      }
      EXPECT_EQ(charsExpected, charsRead);

      // test varchar data
      std::string varcharRead(varcharBatch->data[i], static_cast<size_t>(varcharBatch->length[i]));
      std::string varcharExpected = os.str().substr(0, 4);
      EXPECT_EQ(varcharRead, varcharExpected);
    }

    EXPECT_FALSE(rowReader->next(*batch));
  }

  TEST_P(WriterTest, writeDecimal64Column) {
    const uint64_t maxPrecision = 18;
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();
    std::unique_ptr<Type> type(Type::buildTypeFromString("struct<col1:decimal(18,5)>"));

    uint64_t stripeSize = 16 * 1024;       // 16K
    uint64_t compressionBlockSize = 1024;  // 1k
    uint64_t rowCount = 1024;

    std::unique_ptr<Writer> writer =
        createWriter(stripeSize, compressionBlockSize, CompressionKind_ZLIB, *type, pool,
                     &memStream, fileVersion);
    std::unique_ptr<ColumnVectorBatch> batch = writer->createRowBatch(rowCount);
    StructVectorBatch* structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    Decimal64VectorBatch* decBatch = dynamic_cast<Decimal64VectorBatch*>(structBatch->fields[0]);

    // write positive decimals
    for (uint64_t i = 0; i < rowCount; ++i) {
      decBatch->values[i] = static_cast<int64_t>(i + 10000);
    }
    structBatch->numElements = decBatch->numElements = rowCount;
    writer->add(*batch);

    // write negative decimals
    for (uint64_t i = 0; i < rowCount; ++i) {
      decBatch->values[i] = static_cast<int64_t>(i - 10000);
    }
    structBatch->numElements = decBatch->numElements = rowCount;
    writer->add(*batch);

    // write all precision decimals
    int64_t dec;
    for (uint64_t i = dec = 0; i < maxPrecision; ++i) {
      dec = dec * 10 + 9;
      decBatch->values[i] = dec;
      decBatch->values[i + maxPrecision] = -dec;
    }
    structBatch->numElements = decBatch->numElements = 2 * maxPrecision;
    writer->add(*batch);

    writer->close();

    auto inStream = std::make_unique<MemoryInputStream>(memStream.getData(), memStream.getLength());
    std::unique_ptr<Reader> reader = createReader(pool, std::move(inStream));
    std::unique_ptr<RowReader> rowReader = createRowReader(reader.get());
    EXPECT_EQ((rowCount + maxPrecision) * 2, reader->getNumberOfRows());

    // test reading positive decimals
    batch = rowReader->createRowBatch(rowCount);
    EXPECT_EQ(true, rowReader->next(*batch));
    structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    decBatch = dynamic_cast<Decimal64VectorBatch*>(structBatch->fields[0]);
    for (uint64_t i = 0; i < rowCount; ++i) {
      EXPECT_EQ(static_cast<int64_t>(i + 10000), decBatch->values[i]);
    }

    // test reading negative decimals
    EXPECT_EQ(true, rowReader->next(*batch));
    structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    decBatch = dynamic_cast<Decimal64VectorBatch*>(structBatch->fields[0]);
    for (uint64_t i = 0; i < rowCount; ++i) {
      EXPECT_EQ(static_cast<int64_t>(i - 10000), decBatch->values[i]);
    }

    // test reading all precision decimals
    EXPECT_EQ(true, rowReader->next(*batch));
    structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    decBatch = dynamic_cast<Decimal64VectorBatch*>(structBatch->fields[0]);
    for (uint64_t i = dec = 0; i < maxPrecision; ++i) {
      dec = dec * 10 + 9;
      EXPECT_EQ(dec, decBatch->values[i]);
      EXPECT_EQ(-dec, decBatch->values[i + maxPrecision]);
    }
  }

  TEST_P(WriterTest, writeDecimal128Column) {
    const uint64_t maxPrecision = 38;
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();
    std::unique_ptr<Type> type(Type::buildTypeFromString("struct<col1:decimal(38,10)>"));

    uint64_t stripeSize = 16 * 1024;
    uint64_t compressionBlockSize = 1024;
    uint64_t rowCount = 1024;

    std::unique_ptr<Writer> writer =
        createWriter(stripeSize, compressionBlockSize, CompressionKind_ZLIB, *type, pool,
                     &memStream, fileVersion);
    std::unique_ptr<ColumnVectorBatch> batch = writer->createRowBatch(rowCount);
    StructVectorBatch* structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    Decimal128VectorBatch* decBatch = dynamic_cast<Decimal128VectorBatch*>(structBatch->fields[0]);

    // write positive decimals
    std::string base = "1" + std::string(1, '0');
    for (uint64_t i = 0; i < rowCount; ++i) {
      std::ostringstream os;
      os << i;
      decBatch->values[i] = Int128(base + os.str());
    }
    structBatch->numElements = decBatch->numElements = rowCount;
    writer->add(*batch);

    // write negative decimals
    std::string nbase = "-" + base;
    for (uint64_t i = 0; i < rowCount; ++i) {
      std::ostringstream os;
      os << i;
      decBatch->values[i] = Int128(nbase + os.str());
    }
    structBatch->numElements = rowCount;
    decBatch->numElements = rowCount;
    writer->add(*batch);

    // write all precision decimals
    for (uint64_t i = 0; i < maxPrecision; ++i) {
      std::string expected = std::string(i + 1, '9');
      decBatch->values[i] = Int128(expected);
      decBatch->values[i + maxPrecision] = Int128("-" + expected);
    }
    structBatch->numElements = decBatch->numElements = 2 * maxPrecision;
    writer->add(*batch);

    writer->close();

    auto inStream = std::make_unique<MemoryInputStream>(memStream.getData(), memStream.getLength());
    std::unique_ptr<Reader> reader = createReader(pool, std::move(inStream));
    std::unique_ptr<RowReader> rowReader = createRowReader(reader.get());
    EXPECT_EQ((rowCount + maxPrecision) * 2, reader->getNumberOfRows());

    // test reading positive decimals
    batch = rowReader->createRowBatch(rowCount);
    EXPECT_EQ(true, rowReader->next(*batch));
    structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    decBatch = dynamic_cast<Decimal128VectorBatch*>(structBatch->fields[0]);
    for (uint64_t i = 0; i < rowCount; ++i) {
      std::ostringstream os;
      os << i;
      EXPECT_EQ(base + os.str(), decBatch->values[i].toString());
    }

    // test reading negative decimals
    EXPECT_EQ(true, rowReader->next(*batch));
    structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    decBatch = dynamic_cast<Decimal128VectorBatch*>(structBatch->fields[0]);
    for (uint64_t i = 0; i < rowCount; ++i) {
      std::ostringstream os;
      os << i;
      EXPECT_EQ(nbase + os.str(), decBatch->values[i].toString());
    }

    // test reading all precision decimals
    EXPECT_EQ(true, rowReader->next(*batch));
    structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    decBatch = dynamic_cast<Decimal128VectorBatch*>(structBatch->fields[0]);
    for (uint64_t i = 0; i < maxPrecision; ++i) {
      std::string expected = std::string(i + 1, '9');
      EXPECT_EQ(expected, decBatch->values[i].toString());
      EXPECT_EQ("-" + expected, decBatch->values[i + maxPrecision].toString());
    }
  }

  TEST_P(WriterTest, writeListColumn) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();

    std::unique_ptr<Type> type(Type::buildTypeFromString("struct<col1:array<int>>"));

    uint64_t stripeSize = 1024 * 1024;
    uint64_t compressionBlockSize = 64 * 1024;
    uint64_t rowCount = 1024;
    uint64_t maxListLength = 10;
    uint64_t offset = 0;

    std::unique_ptr<Writer> writer =
        createWriter(stripeSize, compressionBlockSize, CompressionKind_ZLIB, *type, pool,
                     &memStream, fileVersion);
    std::unique_ptr<ColumnVectorBatch> batch = writer->createRowBatch(rowCount * maxListLength);

    StructVectorBatch* structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    ListVectorBatch* listBatch = dynamic_cast<ListVectorBatch*>(structBatch->fields[0]);
    LongVectorBatch* intBatch = dynamic_cast<LongVectorBatch*>(listBatch->elements.get());
    int64_t* data = intBatch->data.data();
    int64_t* offsets = listBatch->offsets.data();

    for (uint64_t i = 0; i < rowCount; ++i) {
      offsets[i] = static_cast<int64_t>(offset);
      for (uint64_t length = i % maxListLength + 1; length != 0; --length) {
        data[offset++] = static_cast<int64_t>(i);
      }
    }
    offsets[rowCount] = static_cast<int64_t>(offset);

    structBatch->numElements = rowCount;
    listBatch->numElements = rowCount;

    writer->add(*batch);
    writer->close();

    auto inStream = std::make_unique<MemoryInputStream>(memStream.getData(), memStream.getLength());
    std::unique_ptr<Reader> reader = createReader(pool, std::move(inStream));
    std::unique_ptr<RowReader> rowReader = createRowReader(reader.get());
    EXPECT_EQ(rowCount, reader->getNumberOfRows());

    batch = rowReader->createRowBatch(rowCount * maxListLength);
    EXPECT_EQ(true, rowReader->next(*batch));

    structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    listBatch = dynamic_cast<ListVectorBatch*>(structBatch->fields[0]);
    intBatch = dynamic_cast<LongVectorBatch*>(listBatch->elements.get());
    data = intBatch->data.data();
    offsets = listBatch->offsets.data();

    EXPECT_EQ(rowCount, listBatch->numElements);
    EXPECT_EQ(offset, intBatch->numElements);

    for (uint64_t i = 0; i < rowCount; ++i) {
      uint64_t length = i % maxListLength + 1;
      for (int64_t j = 0; j != length; ++j) {
        EXPECT_EQ(static_cast<int64_t>(i), data[offsets[i] + j]);
      }
    }
  }

  TEST_P(WriterTest, writeMapColumn) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();
    std::unique_ptr<Type> type(Type::buildTypeFromString("struct<col1:map<string,int>>"));

    uint64_t stripeSize = 16 * 1024;
    uint64_t compressionBlockSize = 1024;
    uint64_t rowCount = 1024, maxListLength = 10, offset = 0;

    std::unique_ptr<Writer> writer =
        createWriter(stripeSize, compressionBlockSize, CompressionKind_ZLIB, *type, pool,
                     &memStream, fileVersion);
    std::unique_ptr<ColumnVectorBatch> batch = writer->createRowBatch(rowCount * maxListLength);
    StructVectorBatch* structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    MapVectorBatch* mapBatch = dynamic_cast<MapVectorBatch*>(structBatch->fields[0]);
    StringVectorBatch* keyBatch = dynamic_cast<StringVectorBatch*>(mapBatch->keys.get());
    LongVectorBatch* elemBatch = dynamic_cast<LongVectorBatch*>(mapBatch->elements.get());

    char dataBuffer[327675];  // 300k
    uint64_t strOffset = 0;

    int64_t* offsets = mapBatch->offsets.data();
    char** keyData = keyBatch->data.data();
    int64_t* keyLength = keyBatch->length.data();
    int64_t* elemData = elemBatch->data.data();

    for (uint64_t i = 0; i < rowCount; ++i) {
      offsets[i] = static_cast<int64_t>(offset);
      for (uint64_t j = 0; j != i % maxListLength + 1; ++j) {
        std::ostringstream os;
        os << (i + j);
        memcpy(dataBuffer + strOffset, os.str().c_str(), os.str().size());
        keyData[offset] = dataBuffer + strOffset;

        keyLength[offset] = static_cast<int64_t>(os.str().size());
        elemData[offset++] = static_cast<int64_t>(i);

        strOffset += os.str().size();
      }
    }
    offsets[rowCount] = static_cast<int64_t>(offset);

    structBatch->numElements = rowCount;
    mapBatch->numElements = rowCount;

    writer->add(*batch);
    writer->close();

    auto inStream = std::make_unique<MemoryInputStream>(memStream.getData(), memStream.getLength());
    std::unique_ptr<Reader> reader = createReader(pool, std::move(inStream));
    std::unique_ptr<RowReader> rowReader = createRowReader(reader.get());
    EXPECT_EQ(rowCount, reader->getNumberOfRows());

    batch = rowReader->createRowBatch(rowCount * maxListLength);
    EXPECT_EQ(true, rowReader->next(*batch));

    structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    mapBatch = dynamic_cast<MapVectorBatch*>(structBatch->fields[0]);
    keyBatch = dynamic_cast<StringVectorBatch*>(mapBatch->keys.get());
    elemBatch = dynamic_cast<LongVectorBatch*>(mapBatch->elements.get());
    offsets = mapBatch->offsets.data();
    keyData = keyBatch->data.data();
    keyLength = keyBatch->length.data();
    elemData = elemBatch->data.data();

    EXPECT_EQ(rowCount, mapBatch->numElements);
    EXPECT_EQ(offset, keyBatch->numElements);
    EXPECT_EQ(offset, elemBatch->numElements);

    for (uint64_t i = 0; i != rowCount; ++i) {
      for (int64_t j = 0; j != i % maxListLength + 1; ++j) {
        std::ostringstream os;
        os << i + static_cast<uint64_t>(j);
        uint64_t lenRead = static_cast<uint64_t>(keyLength[offsets[i] + j]);
        EXPECT_EQ(os.str(), std::string(keyData[offsets[i] + j], lenRead));
        EXPECT_EQ(static_cast<int64_t>(i), elemData[offsets[i] + j]);
      }
    }
  }

  TEST_P(WriterTest, writeUnionColumn) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();
    std::unique_ptr<Type> type(
        Type::buildTypeFromString("struct<col1:uniontype<int,double,boolean>>"));

    uint64_t stripeSize = 16 * 1024;
    uint64_t compressionBlockSize = 1024;
    uint64_t rowCount = 3333;

    std::unique_ptr<Writer> writer =
        createWriter(stripeSize, compressionBlockSize, CompressionKind_ZLIB, *type, pool,
                     &memStream, fileVersion);
    std::unique_ptr<ColumnVectorBatch> batch = writer->createRowBatch(rowCount);
    StructVectorBatch* structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    UnionVectorBatch* unionBatch = dynamic_cast<UnionVectorBatch*>(structBatch->fields[0]);
    unsigned char* tags = unionBatch->tags.data();
    uint64_t* offsets = unionBatch->offsets.data();

    LongVectorBatch* intBatch = dynamic_cast<LongVectorBatch*>(unionBatch->children[0]);
    DoubleVectorBatch* doubleBatch = dynamic_cast<DoubleVectorBatch*>(unionBatch->children[1]);
    LongVectorBatch* boolBatch = dynamic_cast<LongVectorBatch*>(unionBatch->children[2]);
    int64_t* intData = intBatch->data.data();
    double* doubleData = doubleBatch->data.data();
    int64_t* boolData = boolBatch->data.data();

    uint64_t intOffset = 0, doubleOffset = 0, boolOffset = 0, tag = 0;
    for (uint64_t i = 0; i < rowCount; ++i) {
      tags[i] = static_cast<unsigned char>(tag);
      switch (tag) {
        case 0:
          offsets[i] = intOffset;
          intData[intOffset++] = static_cast<int64_t>(i);
          break;
        case 1:
          offsets[i] = doubleOffset;
          doubleData[doubleOffset++] = static_cast<double>(i) + 0.5;
          break;
        case 2:
          offsets[i] = boolOffset;
          boolData[boolOffset++] = (i % 2 == 0) ? 1 : 0;
          break;
      }
      tag = (tag + 1) % 3;
    }

    structBatch->numElements = rowCount;
    unionBatch->numElements = rowCount;

    writer->add(*batch);
    writer->close();

    auto inStream = std::make_unique<MemoryInputStream>(memStream.getData(), memStream.getLength());
    std::unique_ptr<Reader> reader = createReader(pool, std::move(inStream));
    std::unique_ptr<RowReader> rowReader = createRowReader(reader.get());
    EXPECT_EQ(rowCount, reader->getNumberOfRows());

    batch = rowReader->createRowBatch(rowCount);
    EXPECT_EQ(true, rowReader->next(*batch));

    structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    unionBatch = dynamic_cast<UnionVectorBatch*>(structBatch->fields[0]);
    tags = unionBatch->tags.data();
    offsets = unionBatch->offsets.data();

    intBatch = dynamic_cast<LongVectorBatch*>(unionBatch->children[0]);
    doubleBatch = dynamic_cast<DoubleVectorBatch*>(unionBatch->children[1]);
    boolBatch = dynamic_cast<LongVectorBatch*>(unionBatch->children[2]);
    intData = intBatch->data.data();
    doubleData = doubleBatch->data.data();
    boolData = boolBatch->data.data();

    EXPECT_EQ(rowCount, unionBatch->numElements);
    EXPECT_EQ(rowCount / 3, intBatch->numElements);
    EXPECT_EQ(rowCount / 3, doubleBatch->numElements);
    EXPECT_EQ(rowCount / 3, boolBatch->numElements);

    uint64_t offset;
    for (uint64_t i = 0; i < rowCount; ++i) {
      tag = tags[i];
      offset = offsets[i];

      switch (tag) {
        case 0:
          EXPECT_EQ(i, intData[offset]);
          break;
        case 1:
          EXPECT_TRUE(std::abs(static_cast<double>(i) + 0.5 - doubleData[offset]) < 0.000001);
          break;
        case 2:
          EXPECT_EQ(i % 2 == 0 ? 1 : 0, boolData[offset]);
          break;
      }
    }
  }

  TEST_P(WriterTest, writeUTF8CharAndVarcharColumn) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();
    std::unique_ptr<Type> type(Type::buildTypeFromString("struct<col1:char(2),col2:varchar(2)>"));

    uint64_t stripeSize = 1024;
    uint64_t compressionBlockSize = 1024;
    uint64_t rowCount = 3;
    std::unique_ptr<Writer> writer =
        createWriter(stripeSize, compressionBlockSize, CompressionKind_ZLIB, *type, pool,
                     &memStream, fileVersion);
    std::unique_ptr<ColumnVectorBatch> batch = writer->createRowBatch(rowCount);
    StructVectorBatch* structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    StringVectorBatch* charBatch = dynamic_cast<StringVectorBatch*>(structBatch->fields[0]);
    StringVectorBatch* varcharBatch = dynamic_cast<StringVectorBatch*>(structBatch->fields[1]);
    std::vector<std::vector<char>> strs;

    // input character is 'à' (0xC3, 0xA0)
    // in total 3 rows, each has 1, 2, and 3 'à' respectively
    std::vector<char> vec;
    for (uint64_t i = 0; i != rowCount; ++i) {
      vec.push_back('\xC3');
      vec.push_back('\xA0');
      strs.push_back(vec);
      charBatch->data[i] = varcharBatch->data[i] = strs.back().data();
      charBatch->length[i] = varcharBatch->length[i] = static_cast<int64_t>(strs.back().size());
    }

    structBatch->numElements = rowCount;
    charBatch->numElements = rowCount;
    varcharBatch->numElements = rowCount;

    writer->add(*batch);
    writer->close();

    // read and verify data
    auto inStream = std::make_unique<MemoryInputStream>(memStream.getData(), memStream.getLength());
    std::unique_ptr<Reader> reader = createReader(pool, std::move(inStream));
    std::unique_ptr<RowReader> rowReader = createRowReader(reader.get());
    EXPECT_EQ(rowCount, reader->getNumberOfRows());

    batch = rowReader->createRowBatch(rowCount);
    structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    charBatch = dynamic_cast<StringVectorBatch*>(structBatch->fields[0]);
    varcharBatch = dynamic_cast<StringVectorBatch*>(structBatch->fields[1]);

    EXPECT_EQ(true, rowReader->next(*batch));
    EXPECT_EQ(rowCount, batch->numElements);

    char expectedPadded[3] = {'\xC3', '\xA0', ' '};
    char expectedOneChar[2] = {'\xC3', '\xA0'};
    char expectedTwoChars[4] = {'\xC3', '\xA0', '\xC3', '\xA0'};

    EXPECT_EQ(3, charBatch->length[0]);
    EXPECT_EQ(4, charBatch->length[1]);
    EXPECT_EQ(4, charBatch->length[2]);
    EXPECT_TRUE(memcmp(charBatch->data[0], expectedPadded, 3) == 0);
    EXPECT_TRUE(memcmp(charBatch->data[1], expectedTwoChars, 4) == 0);
    EXPECT_TRUE(memcmp(charBatch->data[2], expectedTwoChars, 4) == 0);

    EXPECT_EQ(2, varcharBatch->length[0]);
    EXPECT_EQ(4, varcharBatch->length[1]);
    EXPECT_EQ(4, varcharBatch->length[2]);
    EXPECT_TRUE(memcmp(varcharBatch->data[0], expectedOneChar, 2) == 0);
    EXPECT_TRUE(memcmp(varcharBatch->data[1], expectedTwoChars, 4) == 0);
    EXPECT_TRUE(memcmp(varcharBatch->data[2], expectedTwoChars, 4) == 0);

    EXPECT_FALSE(rowReader->next(*batch));
  }

  TEST_P(WriterTest, testWriteListColumnWithNull) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();
    std::unique_ptr<Type> type(Type::buildTypeFromString("struct<col1:array<tinyint>>"));

    uint64_t stripeSize = 1024;
    uint64_t compressionBlockSize = 1024;

    std::unique_ptr<Writer> writer =
        createWriter(stripeSize, compressionBlockSize, CompressionKind_ZLIB, *type, pool,
                     &memStream, fileVersion);

    std::unique_ptr<ColumnVectorBatch> batch = writer->createRowBatch(4);
    StructVectorBatch* structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    ListVectorBatch* listBatch = dynamic_cast<ListVectorBatch*>(structBatch->fields[0]);
    LongVectorBatch* intBatch = dynamic_cast<LongVectorBatch*>(listBatch->elements.get());

    // test data looks like below -
    // {[1, 2]}
    // null
    // {[3, 4]}
    // {[5, 6]}
    int64_t* offsets = listBatch->offsets.data();
    offsets[0] = 0;
    offsets[1] = 2;
    offsets[2] = 2;
    offsets[3] = 4;
    offsets[4] = 6;

    intBatch->resize(6);
    for (uint64_t i = 0; i < 6; ++i) {
      intBatch->notNull[i] = 1;
    }

    int64_t* data = intBatch->data.data();
    for (int8_t i = 1; i < 7; ++i) {
      data[i - 1] = i;
    }

    structBatch->numElements = 4;

    listBatch->notNull[1] = 0;
    listBatch->hasNulls = true;
    listBatch->numElements = 4;

    writer->add(*batch);
    writer->close();

    auto inStream = std::make_unique<MemoryInputStream>(memStream.getData(), memStream.getLength());
    std::unique_ptr<Reader> reader = createReader(pool, std::move(inStream));
    std::unique_ptr<RowReader> rowReader = createRowReader(reader.get());
    EXPECT_EQ(4, reader->getNumberOfRows());

    batch = rowReader->createRowBatch(4 * 2);
    EXPECT_EQ(true, rowReader->next(*batch));

    structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    listBatch = dynamic_cast<ListVectorBatch*>(structBatch->fields[0]);
    intBatch = dynamic_cast<LongVectorBatch*>(listBatch->elements.get());

    data = intBatch->data.data();
    offsets = listBatch->offsets.data();

    EXPECT_EQ(4, structBatch->numElements);
    EXPECT_EQ(4, listBatch->numElements);
    EXPECT_EQ(1, listBatch->notNull[0]);
    EXPECT_EQ(0, listBatch->notNull[1]);
    EXPECT_EQ(1, listBatch->notNull[2]);
    EXPECT_EQ(1, listBatch->notNull[3]);

    EXPECT_EQ(0, offsets[0]);
    EXPECT_EQ(2, offsets[1]);
    EXPECT_EQ(2, offsets[2]);
    EXPECT_EQ(4, offsets[3]);
    EXPECT_EQ(6, offsets[4]);

    for (int8_t i = 1; i < 7; ++i) {
      EXPECT_EQ(i, data[i - 1]);
    }
  }

  TEST_P(WriterTest, testWriteNestedStructWithNull) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();
    std::unique_ptr<Type> type(Type::buildTypeFromString("struct<col0:struct<col1:bigint>>"));

    uint64_t stripeSize = 1024;
    uint64_t compressionBlockSize = 1024;

    std::unique_ptr<Writer> writer =
        createWriter(stripeSize, compressionBlockSize, CompressionKind_ZLIB, *type, pool,
                     &memStream, fileVersion);

    // test data looks like below -
    // {0}
    // null
    // {1}
    // {2}
    // {3}
    std::unique_ptr<ColumnVectorBatch> batch = writer->createRowBatch(5);
    StructVectorBatch* structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    StructVectorBatch* structBatch2 = dynamic_cast<StructVectorBatch*>(structBatch->fields[0]);
    LongVectorBatch* intBatch = dynamic_cast<LongVectorBatch*>(structBatch2->fields[0]);

    structBatch->numElements = 5;
    structBatch2->numElements = 5;
    structBatch2->hasNulls = true;
    structBatch2->notNull[1] = 0;

    intBatch->resize(5);
    for (int64_t i = 0; i < 5; ++i) {
      intBatch->data.data()[i] = i;
    }
    intBatch->notNull[1] = 0;
    intBatch->hasNulls = true;

    writer->add(*batch);
    writer->close();

    auto inStream = std::make_unique<MemoryInputStream>(memStream.getData(), memStream.getLength());
    std::unique_ptr<Reader> reader = createReader(pool, std::move(inStream));
    std::unique_ptr<RowReader> rowReader = createRowReader(reader.get());
    EXPECT_EQ(5, reader->getNumberOfRows());

    batch = rowReader->createRowBatch(5);
    EXPECT_EQ(true, rowReader->next(*batch));

    structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    structBatch2 = dynamic_cast<StructVectorBatch*>(structBatch->fields[0]);
    intBatch = dynamic_cast<LongVectorBatch*>(structBatch2->fields[0]);

    for (uint64_t i = 0; i < 5; ++i) {
      EXPECT_EQ(1, structBatch->notNull[i]);
    }

    for (uint64_t i = 0; i < 5; ++i) {
      if (i == 1) {
        EXPECT_EQ(0, structBatch2->notNull[i]);
      } else {
        EXPECT_EQ(1, structBatch2->notNull[i]);
      }
    }

    for (uint64_t i = 0; i < 5; ++i) {
      if (i == 1) {
        EXPECT_EQ(0, intBatch->notNull[i]);
      } else {
        EXPECT_EQ(1, intBatch->notNull[i]);
      }
    }

    int64_t* data = intBatch->data.data();
    for (int8_t i = 0; i < 5; ++i) {
      if (i != 1) {
        EXPECT_EQ(i, data[i]);
      }
    }
  }

  TEST_P(WriterTest, testWriteNestedStructWithNullIndex) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();
    std::unique_ptr<Type> type(Type::buildTypeFromString("struct<col0:struct<col1:bigint>>"));

    uint64_t stripeSize = 1024;
    uint64_t compressionBlockSize = 1024;

    // 10000 rows with every 1000 row as an RG
    // Each RG has 100 null rows except that the 5th RG is all null
    std::unique_ptr<Writer> writer =
        createWriter(stripeSize, compressionBlockSize, CompressionKind_ZLIB, *type, pool,
                     &memStream, fileVersion, 1000);

    std::unique_ptr<ColumnVectorBatch> batch = writer->createRowBatch(10000);
    StructVectorBatch* structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    StructVectorBatch* structBatch2 = dynamic_cast<StructVectorBatch*>(structBatch->fields[0]);
    LongVectorBatch* intBatch = dynamic_cast<LongVectorBatch*>(structBatch2->fields[0]);

    structBatch->numElements = 10000;
    structBatch2->numElements = 10000;
    structBatch2->hasNulls = true;

    intBatch->resize(10000);
    intBatch->hasNulls = true;
    for (uint64_t i = 0; i < 10; ++i) {
      for (uint64_t j = i * 1000 + 100 * i; j < i * 1000 + 100 * i + 100; ++j) {
        structBatch2->notNull[j] = 0;
        intBatch->notNull[j] = 0;
      }
    }

    for (uint64_t i = 5000; i < 6000; ++i) {
      structBatch2->notNull[i] = 0;
      intBatch->notNull[i] = 0;
    }

    for (int64_t i = 0; i < 10000; ++i) {
      intBatch->data.data()[i] = i;
    }

    writer->add(*batch);
    writer->close();

    auto inStream = std::make_unique<MemoryInputStream>(memStream.getData(), memStream.getLength());
    std::unique_ptr<Reader> reader = createReader(pool, std::move(inStream));
    std::unique_ptr<RowReader> rowReader = createRowReader(reader.get());
    EXPECT_EQ(10000, reader->getNumberOfRows());

    batch = rowReader->createRowBatch(1000);
    structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    structBatch2 = dynamic_cast<StructVectorBatch*>(structBatch->fields[0]);
    intBatch = dynamic_cast<LongVectorBatch*>(structBatch2->fields[0]);

    // Read rows 0 - 1000
    EXPECT_EQ(true, rowReader->next(*batch));
    for (uint64_t i = 0; i < 1000; ++i) {
      EXPECT_EQ(1, structBatch->notNull[i]);
    }

    for (uint64_t i = 0; i < 1000; ++i) {
      if (i < 100) {
        EXPECT_EQ(0, structBatch2->notNull[i]);
      } else {
        EXPECT_EQ(1, structBatch2->notNull[i]);
      }
    }

    for (uint64_t i = 0; i < 1000; ++i) {
      if (i < 100) {
        EXPECT_EQ(0, intBatch->notNull[i]);
      } else {
        EXPECT_EQ(1, intBatch->notNull[i]);
        EXPECT_EQ(i, intBatch->data.data()[i]);
      }
    }

    // Read rows 1800 - 2800, in which 2200 - 2300 are nulls
    rowReader->seekToRow(1800);
    EXPECT_EQ(true, rowReader->next(*batch));
    for (uint64_t i = 0; i < 1000; ++i) {
      EXPECT_EQ(1, structBatch->notNull[i]);
    }

    for (uint64_t i = 0; i < 1000; ++i) {
      if (i >= 400 && i < 500) {
        EXPECT_EQ(0, structBatch2->notNull[i]);
      } else {
        EXPECT_EQ(1, structBatch2->notNull[i]);
      }
    }

    for (uint64_t i = 0; i < 1000; ++i) {
      if (i >= 400 && i < 500) {
        EXPECT_EQ(0, intBatch->notNull[i]);
      } else {
        EXPECT_EQ(1, intBatch->notNull[i]);
        EXPECT_EQ(i + 1800, intBatch->data.data()[i]);
      }
    }

    // Read rows 5000 - 6000, all nulls
    rowReader->seekToRow(5000);
    EXPECT_EQ(true, rowReader->next(*batch));
    for (uint64_t i = 0; i < 1000; ++i) {
      EXPECT_EQ(1, structBatch->notNull[i]);
    }

    for (uint64_t i = 0; i < 1000; ++i) {
      EXPECT_EQ(0, structBatch2->notNull[i]);
    }

    for (uint64_t i = 0; i < 1000; ++i) {
      EXPECT_EQ(0, intBatch->notNull[i]);
    }

    // Read rows 7200 - 8200, in which 7700 - 7800 are null
    rowReader->seekToRow(7200);
    EXPECT_EQ(true, rowReader->next(*batch));
    for (uint64_t i = 0; i < 1000; ++i) {
      EXPECT_EQ(1, structBatch->notNull[i]);
    }

    for (uint64_t i = 0; i < 1000; ++i) {
      if (i >= 500 && i < 600) {
        EXPECT_EQ(0, structBatch2->notNull[i]);
      } else {
        EXPECT_EQ(1, structBatch2->notNull[i]);
      }
    }

    for (uint64_t i = 0; i < 1000; ++i) {
      if (i >= 500 && i < 600) {
        EXPECT_EQ(0, intBatch->notNull[i]);
      } else {
        EXPECT_EQ(1, intBatch->notNull[i]);
        EXPECT_EQ(i + 7200, intBatch->data.data()[i]);
      }
    }
  }

  TEST_P(WriterTest, testBloomFilter) {
    WriterOptions options;
    options.setStripeSize(1024)
        .setCompressionBlockSize(64)
        .setCompression(CompressionKind_ZSTD)
        .setMemoryPool(getDefaultPool())
        .setRowIndexStride(10000)
        .setFileVersion(fileVersion)
        .setColumnsUseBloomFilter({1, 2, 3});

    // write 65535 rows of data
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();
    std::unique_ptr<Type> type(Type::buildTypeFromString("struct<c1:bigint,c2:string,c3:binary>"));

    char dataBuffer[327675];  // 300k
    uint64_t offset = 0;
    uint64_t rowCount = 65535;

    std::unique_ptr<Writer> writer = createWriter(*type, &memStream, options);
    std::unique_ptr<ColumnVectorBatch> batch = writer->createRowBatch(rowCount);
    StructVectorBatch& structBatch = dynamic_cast<StructVectorBatch&>(*batch);
    LongVectorBatch& longBatch = dynamic_cast<LongVectorBatch&>(*structBatch.fields[0]);
    StringVectorBatch& strBatch = dynamic_cast<StringVectorBatch&>(*structBatch.fields[1]);
    StringVectorBatch& binBatch = dynamic_cast<StringVectorBatch&>(*structBatch.fields[2]);

    for (uint64_t i = 0; i < rowCount; ++i) {
      // each row group has a unique value
      uint64_t data = (i / options.getRowIndexStride());

      // c1
      longBatch.data[i] = static_cast<int64_t>(data);

      // c2
      std::ostringstream os;
      os << data;
      strBatch.data[i] = dataBuffer + offset;
      strBatch.length[i] = static_cast<int64_t>(os.str().size());
      memcpy(dataBuffer + offset, os.str().c_str(), os.str().size());

      // c3
      binBatch.data[i] = dataBuffer + offset;
      binBatch.length[i] = static_cast<int64_t>(os.str().size());
      memcpy(dataBuffer + offset, os.str().c_str(), os.str().size());
      offset += os.str().size();
    }

    structBatch.numElements = rowCount;
    longBatch.numElements = rowCount;
    strBatch.numElements = rowCount;
    binBatch.numElements = rowCount;
    writer->add(*batch);
    writer->close();

    // verify bloomfilters
    auto inStream = std::make_unique<MemoryInputStream>(memStream.getData(), memStream.getLength());
    std::unique_ptr<Reader> reader = createReader(pool, std::move(inStream));
    EXPECT_EQ(rowCount, reader->getNumberOfRows());

    EXPECT_EQ(3, reader->getBloomFilters(0, {}).size());
    EXPECT_EQ(1, reader->getBloomFilters(0, {1}).size());
    EXPECT_EQ(1, reader->getBloomFilters(0, {2}).size());
    EXPECT_EQ(1, reader->getBloomFilters(0, {3}).size());

    std::map<uint32_t, BloomFilterIndex> bfs = reader->getBloomFilters(0, {1, 2, 3});
    EXPECT_EQ(3, bfs.size());
    EXPECT_EQ(7, bfs[1].entries.size());
    EXPECT_EQ(7, bfs[2].entries.size());
    EXPECT_EQ(7, bfs[3].entries.size());

    // test bloomfilters
    for (uint64_t rg = 0; rg <= rowCount / options.getRowIndexStride(); ++rg) {
      for (uint64_t value = 0; value <= 100; ++value) {
        std::string str = to_string(static_cast<int64_t>(value));
        if (value == rg) {
          EXPECT_TRUE(bfs[1].entries[rg]->testLong(static_cast<int64_t>(value)));
          EXPECT_TRUE(bfs[2].entries[rg]->testBytes(str.c_str(), static_cast<int64_t>(str.size())));
          EXPECT_TRUE(bfs[3].entries[rg]->testBytes(str.c_str(), static_cast<int64_t>(str.size())));
        } else {
          EXPECT_FALSE(bfs[1].entries[rg]->testLong(static_cast<int64_t>(value)));
          EXPECT_FALSE(
              bfs[2].entries[rg]->testBytes(str.c_str(), static_cast<int64_t>(str.size())));
          EXPECT_FALSE(
              bfs[3].entries[rg]->testBytes(str.c_str(), static_cast<int64_t>(str.size())));
        }
      }
    }
  }

  TEST(WriterTest, testSuppressPresentStream) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();
    size_t rowCount = 2000;
    {
      auto type = std::unique_ptr<Type>(Type::buildTypeFromString("struct<col1:int,col2:int>"));
      WriterOptions options;
      options.setStripeSize(1024 * 1024)
          .setCompressionBlockSize(1024)
          .setCompression(CompressionKind_NONE)
          .setMemoryPool(pool)
          .setRowIndexStride(1000);

      auto writer = createWriter(*type, &memStream, options);
      auto batch = writer->createRowBatch(rowCount);
      auto& structBatch = dynamic_cast<StructVectorBatch&>(*batch);
      auto& longBatch1 = dynamic_cast<LongVectorBatch&>(*structBatch.fields[0]);
      auto& longBatch2 = dynamic_cast<LongVectorBatch&>(*structBatch.fields[1]);
      structBatch.numElements = rowCount;
      longBatch1.numElements = rowCount;
      longBatch2.numElements = rowCount;
      longBatch1.hasNulls = true;
      for (size_t i = 0; i < rowCount; ++i) {
        if (i % 2 == 0) {
          longBatch1.notNull[i] = 0;
        } else {
          longBatch1.notNull[i] = 1;
          longBatch1.data[i] = static_cast<int64_t>(i * 100);
        }
        longBatch2.data[i] = static_cast<int64_t>(i * 300);
      }
      writer->add(*batch);
      writer->close();
    }
    // read file & check the present stream
    {
      auto inStream =
          std::make_unique<MemoryInputStream>(memStream.getData(), memStream.getLength());
      ReaderOptions readerOptions;
      readerOptions.setMemoryPool(*pool);
      std::unique_ptr<Reader> reader = createReader(std::move(inStream), readerOptions);
      EXPECT_EQ(rowCount, reader->getNumberOfRows());
      std::unique_ptr<RowReader> rowReader = createRowReader(reader.get());
      auto batch = rowReader->createRowBatch(1000);
      EXPECT_TRUE(rowReader->next(*batch));
      EXPECT_EQ(1000, batch->numElements);
      auto& structBatch = dynamic_cast<StructVectorBatch&>(*batch);
      auto& longBatch1 = dynamic_cast<LongVectorBatch&>(*structBatch.fields[0]);
      auto& longBatch2 = dynamic_cast<LongVectorBatch&>(*structBatch.fields[1]);
      for (size_t i = 0; i < 1000; ++i) {
        if (i % 2 == 0) {
          EXPECT_FALSE(longBatch1.notNull[i]);
        } else {
          EXPECT_TRUE(longBatch1.notNull[i]);
          EXPECT_EQ(longBatch1.data[i], static_cast<int64_t>(i * 100));
        }
        EXPECT_EQ(longBatch2.data[i], static_cast<int64_t>(i * 300));
      }
      // Read rows 1500 - 2000
      rowReader->seekToRow(1500);
      EXPECT_TRUE(rowReader->next(*batch));
      EXPECT_EQ(500, batch->numElements);
      for (size_t i = 0; i < 500; ++i) {
        if (i % 2 == 0) {
          EXPECT_FALSE(longBatch1.notNull[i]);
        } else {
          EXPECT_TRUE(longBatch1.notNull[i]);
          EXPECT_EQ(longBatch1.data[i], static_cast<int64_t>((i + 1500) * 100));
        }
        EXPECT_EQ(longBatch2.data[i], static_cast<int64_t>((i + 1500) * 300));
      }
      // fetch StripeFooter from pb stream
      std::unique_ptr<StripeInformation> stripeInfo = reader->getStripe(0);
      ReaderImpl* readerImpl = dynamic_cast<ReaderImpl*>(reader.get());
      auto pbStream = std::make_unique<SeekableFileInputStream>(
          readerImpl->getStream(),
          stripeInfo->getOffset() + stripeInfo->getIndexLength() + stripeInfo->getDataLength(),
          stripeInfo->getFooterLength(), *pool);
      proto::StripeFooter stripeFooter;
      if (!stripeFooter.ParseFromZeroCopyStream(pbStream.get())) {
        throw ParseError("Parse stripe footer from pb stream failed");
      }
      for (int i = 0; i < stripeFooter.streams_size(); ++i) {
        const proto::Stream& stream = stripeFooter.streams(i);
        if (stream.has_kind() && stream.kind() == proto::Stream_Kind_PRESENT) {
          EXPECT_EQ(stream.column(), 1UL);
        }
      }
    }
  }

  // Before the fix of ORC-1288, this case will trigger the bug about
  // invalid memory freeing with zlib compression when writing a orc file
  // that contains multiple stripes, and each stripe contains multiple columns
  // with no null values.
  void testSuppressPresentStream(orc::CompressionKind kind) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();
    uint64_t rowCount = 5000000;
    auto type = std::unique_ptr<Type>(Type::buildTypeFromString("struct<c0:int>"));
    WriterOptions options;
    options.setStripeSize(1024).setCompressionBlockSize(1024).setCompression(kind).setMemoryPool(
        pool);

    auto writer = createWriter(*type, &memStream, options);
    auto batch = writer->createRowBatch(rowCount);
    auto& structBatch = dynamic_cast<StructVectorBatch&>(*batch);
    auto& longBatch = dynamic_cast<LongVectorBatch&>(*structBatch.fields[0]);
    uint64_t rows = 0;
    uint64_t batchSize = 10000;
    for (uint64_t i = 0; i < rowCount; ++i) {
      longBatch.data[i] = static_cast<int64_t>(i);
      ++rows;
      if (rows == batchSize) {
        structBatch.numElements = rows;
        longBatch.numElements = rows;
        writer->add(*batch);
        rows = 0;
      }
    }
    if (rows != 0) {
      structBatch.numElements = rows;
      longBatch.numElements = rows;
      writer->add(*batch);
      rows = 0;
    }
    writer->close();
  }

  TEST(WriterTest, suppressPresentStreamWithCompressionKinds) {
    testSuppressPresentStream(CompressionKind_ZLIB);
    testSuppressPresentStream(CompressionKind_ZSTD);
    testSuppressPresentStream(CompressionKind_LZ4);
    testSuppressPresentStream(CompressionKind_SNAPPY);
  }

  void testSetOutputBufferCapacity(uint64_t capacity) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();
    size_t rowCount = 1000;
    {
      auto type = std::unique_ptr<Type>(Type::buildTypeFromString("struct<col1:int,col2:int>"));
      WriterOptions options;
      options.setStripeSize(1024 * 1024)
          .setCompressionBlockSize(64 * 1024)
          .setCompression(CompressionKind_NONE)
          .setMemoryPool(pool)
          .setRowIndexStride(1000)
          .setOutputBufferCapacity(capacity);

      auto writer = createWriter(*type, &memStream, options);
      auto batch = writer->createRowBatch(rowCount);
      auto& structBatch = dynamic_cast<StructVectorBatch&>(*batch);
      auto& longBatch1 = dynamic_cast<LongVectorBatch&>(*structBatch.fields[0]);
      auto& longBatch2 = dynamic_cast<LongVectorBatch&>(*structBatch.fields[1]);
      structBatch.numElements = rowCount;
      longBatch1.numElements = rowCount;
      longBatch2.numElements = rowCount;
      for (size_t i = 0; i < rowCount; ++i) {
        longBatch1.data[i] = static_cast<int64_t>(i * 100);
        longBatch2.data[i] = static_cast<int64_t>(i * 300);
      }
      writer->add(*batch);
      writer->close();
    }
    // read orc file & check the data
    {
      std::unique_ptr<InputStream> inStream(
          new MemoryInputStream(memStream.getData(), memStream.getLength()));
      ReaderOptions readerOptions;
      readerOptions.setMemoryPool(*pool);
      std::unique_ptr<Reader> reader = createReader(std::move(inStream), readerOptions);
      std::unique_ptr<RowReader> rowReader = createRowReader(reader.get());
      auto batch = rowReader->createRowBatch(rowCount);
      EXPECT_TRUE(rowReader->next(*batch));
      EXPECT_EQ(rowCount, batch->numElements);
      auto& structBatch = dynamic_cast<StructVectorBatch&>(*batch);
      auto& longBatch1 = dynamic_cast<LongVectorBatch&>(*structBatch.fields[0]);
      auto& longBatch2 = dynamic_cast<LongVectorBatch&>(*structBatch.fields[1]);
      for (size_t i = 0; i < rowCount; ++i) {
        EXPECT_EQ(longBatch1.data[i], static_cast<int64_t>(i * 100));
        EXPECT_EQ(longBatch2.data[i], static_cast<int64_t>(i * 300));
      }
    }
  }

  TEST(WriterTest, setOutputBufferCapacity) {
    // compression block size > output buffer capacity
    testSetOutputBufferCapacity(1024);
    // compression block size = output buffer capacity
    testSetOutputBufferCapacity(64 * 1024);
    // compression block size < output buffer capacity
    testSetOutputBufferCapacity(1024 * 1024);
  }

  TEST_P(WriterTest, testWriteFixedWidthNumericVectorBatch) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();
    std::unique_ptr<Type> type(
        Type::buildTypeFromString("struct<col1:double,col2:float,col3:int,col4:smallint,col5:"
                                  "tinyint,col6:bigint,col7:boolean>"));

    uint64_t stripeSize = 16 * 1024;
    uint64_t compressionBlockSize = 1024;
    uint64_t rowCount = 65530;

    std::vector<double> data(rowCount);
    for (uint64_t i = 0; i < rowCount; ++i) {
      data[i] = 100000 * (std::rand() * 1.0 / RAND_MAX);
    }

    std::unique_ptr<Writer> writer =
        createWriter(stripeSize, compressionBlockSize, CompressionKind_ZLIB, *type, pool,
                     &memStream, fileVersion, 0, "GMT", true);
    // start from here/
    std::unique_ptr<ColumnVectorBatch> batch = writer->createRowBatch(rowCount / 2);
    StructVectorBatch* structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    DoubleVectorBatch* doubleBatch = dynamic_cast<DoubleVectorBatch*>(structBatch->fields[0]);
    FloatVectorBatch* floatBatch = dynamic_cast<FloatVectorBatch*>(structBatch->fields[1]);
    IntVectorBatch* intBatch = dynamic_cast<IntVectorBatch*>(structBatch->fields[2]);
    ShortVectorBatch* shortBatch = dynamic_cast<ShortVectorBatch*>(structBatch->fields[3]);
    ByteVectorBatch* byteBatch = dynamic_cast<ByteVectorBatch*>(structBatch->fields[4]);
    LongVectorBatch* longBatch = dynamic_cast<LongVectorBatch*>(structBatch->fields[5]);
    ByteVectorBatch* boolBatch = dynamic_cast<ByteVectorBatch*>(structBatch->fields[6]);
    structBatch->resize(rowCount);
    doubleBatch->resize(rowCount);
    floatBatch->resize(rowCount);
    intBatch->resize(rowCount);
    shortBatch->resize(rowCount);
    byteBatch->resize(rowCount);
    longBatch->resize(rowCount);
    boolBatch->resize(rowCount);

    for (uint64_t i = 0; i < rowCount; ++i) {
      structBatch->notNull[i] = 1;
      doubleBatch->notNull[i] = 1;
      floatBatch->notNull[i] = 1;
      intBatch->notNull[i] = 1;
      shortBatch->notNull[i] = 1;
      byteBatch->notNull[i] = 1;
      longBatch->notNull[i] = 1;
      boolBatch->notNull[i] = 1;

      doubleBatch->data[i] = data[i];
      floatBatch->data[i] = static_cast<float>(data[i]);
      intBatch->data[i] = static_cast<int32_t>(i);
      shortBatch->data[i] = static_cast<int16_t>(i);
      byteBatch->data[i] = static_cast<int8_t>(i);
      longBatch->data[i] = static_cast<int64_t>(i);
      boolBatch->data[i] = static_cast<bool>((i % 17) % 2);
    }

    structBatch->numElements = rowCount;
    doubleBatch->numElements = rowCount;
    floatBatch->numElements = rowCount;
    intBatch->numElements = rowCount;
    shortBatch->numElements = rowCount;
    byteBatch->numElements = rowCount;
    longBatch->numElements = rowCount;
    boolBatch->numElements = rowCount;

    writer->add(*batch);
    writer->close();

    auto inStream = std::make_unique<MemoryInputStream>(memStream.getData(), memStream.getLength());
    std::unique_ptr<Reader> reader = createReader(pool, std::move(inStream));
    std::unique_ptr<RowReader> rowReader = createRowReader(reader.get(), "GMT", true);

    EXPECT_EQ(rowCount, reader->getNumberOfRows());

    batch = rowReader->createRowBatch(rowCount);
    EXPECT_EQ(true, rowReader->next(*batch));
    EXPECT_EQ(rowCount, batch->numElements);

    structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    doubleBatch = dynamic_cast<DoubleVectorBatch*>(structBatch->fields[0]);
    floatBatch = dynamic_cast<FloatVectorBatch*>(structBatch->fields[1]);
    intBatch = dynamic_cast<IntVectorBatch*>(structBatch->fields[2]);
    shortBatch = dynamic_cast<ShortVectorBatch*>(structBatch->fields[3]);
    byteBatch = dynamic_cast<ByteVectorBatch*>(structBatch->fields[4]);
    longBatch = dynamic_cast<LongVectorBatch*>(structBatch->fields[5]);
    boolBatch = dynamic_cast<ByteVectorBatch*>(structBatch->fields[6]);
    for (uint64_t i = 0; i < rowCount; ++i) {
      EXPECT_TRUE(std::abs(data[i] - doubleBatch->data[i]) < 0.000001);
      EXPECT_TRUE(std::abs(static_cast<float>(data[i]) - static_cast<float>(floatBatch->data[i])) <
                  0.000001f);
      EXPECT_EQ(intBatch->data[i], static_cast<int32_t>(i));
      EXPECT_EQ(shortBatch->data[i], static_cast<int16_t>(i));
      EXPECT_EQ(byteBatch->data[i], static_cast<int8_t>(i));
      EXPECT_EQ(longBatch->data[i], static_cast<int64_t>(i));
      EXPECT_EQ(boolBatch->data[i], static_cast<bool>((i % 17) % 2));
    }
    EXPECT_FALSE(rowReader->next(*batch));
  }

  // first stripe has no null value and second stripe has null value.
  // make sure stripes do not have dirty data in the present streams.
  TEST_P(WriterTest, testSuppressPresentStreamInPreStripe) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();

    // [1-998000): notNull, value is equal to index
    // [998000-999000): null
    // [999000-1000000]: notNoll, value is equal to index
    size_t rowCount = 1000000;
    size_t nullBeginCount = 998000;
    size_t nullEndCount = 999000;
    size_t batchSize = 5;
    {
      auto type = std::unique_ptr<Type>(Type::buildTypeFromString("struct<col1:int>"));
      WriterOptions options;
      options.setStripeSize(16 * 1024)
          .setCompressionBlockSize(1024)
          .setCompression(CompressionKind_NONE)
          .setMemoryPool(pool)
          .setRowIndexStride(1000);

      auto writer = createWriter(*type, &memStream, options);

      uint64_t batchCount = rowCount / batchSize;
      size_t rowsWrite = 0;
      for (uint64_t batchIdx = 0; batchIdx < batchCount; batchIdx++) {
        auto batch = writer->createRowBatch(batchSize);
        auto& structBatch = dynamic_cast<StructVectorBatch&>(*batch);
        auto& longBatch = dynamic_cast<LongVectorBatch&>(*structBatch.fields[0]);
        structBatch.numElements = batchSize;
        longBatch.numElements = batchSize;
        longBatch.hasNulls = false;
        for (uint64_t row = 0; row < batchSize; ++row) {
          size_t rowIndex = rowsWrite + row + 1;
          if (rowIndex < nullBeginCount || rowIndex >= nullEndCount) {
            longBatch.data[row] = static_cast<int64_t>(rowIndex);
          } else {
            longBatch.notNull[row] = 0;
            longBatch.hasNulls = true;
          }
        }

        writer->add(*batch);
        rowsWrite += batch->numElements;
      }
      writer->close();
    }
    // read file & check the column value correct
    {
      std::unique_ptr<MemoryInputStream> inStream(new MemoryInputStream(
        memStream.getData(), memStream.getLength()));
      ReaderOptions readerOptions;
      readerOptions.setMemoryPool(*pool);
      std::unique_ptr<Reader> reader = createReader(std::move(inStream), readerOptions);
      EXPECT_EQ(reader->getNumberOfStripes(), 2);
      EXPECT_EQ(rowCount, reader->getNumberOfRows());
      std::unique_ptr<RowReader> rowReader = createRowReader(reader.get());
      size_t rowsRead = 0;
      while (rowsRead < rowCount) {
        auto batch = rowReader->createRowBatch(1000);
        EXPECT_TRUE(rowReader->next(*batch));
        auto& structBatch = dynamic_cast<StructVectorBatch&>(*batch);
        auto& longBatch = dynamic_cast<LongVectorBatch&>(*structBatch.fields[0]);
        for (size_t i = 0; i < batch->numElements; ++i) {
          size_t rowIndex = rowsRead + i + 1;
          if (rowIndex < nullBeginCount || rowIndex >= nullEndCount) {
            EXPECT_TRUE(longBatch.notNull[i]);
            EXPECT_EQ(longBatch.data[i], static_cast<int64_t>(rowIndex));
          } else {
            EXPECT_FALSE(longBatch.notNull[i]);
          }
        }
        rowsRead += batch->numElements;
      }
    }
  }

  INSTANTIATE_TEST_SUITE_P(OrcTest, WriterTest,
                           Values(FileVersion::v_0_11(), FileVersion::v_0_12(),
                                  FileVersion::UNSTABLE_PRE_2_0()));
}  // namespace orc
