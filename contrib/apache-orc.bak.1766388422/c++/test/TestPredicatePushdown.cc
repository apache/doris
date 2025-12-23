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

#include "MemoryInputStream.hh"
#include "MemoryOutputStream.hh"
#include "orc/OrcFile.hh"
#include "orc/sargs/SearchArgument.hh"
#include "wrap/gtest-wrapper.h"

namespace orc {

  static const int DEFAULT_MEM_STREAM_SIZE = 10 * 1024 * 1024;  // 10M

  void createMemTestFile(MemoryOutputStream& memStream, uint64_t rowIndexStride) {
    MemoryPool* pool = getDefaultPool();
    auto type =
        std::unique_ptr<Type>(Type::buildTypeFromString("struct<int1:bigint,string1:string>"));
    WriterOptions options;
    options.setStripeSize(1024 * 1024)
        .setCompressionBlockSize(1024)
        .setCompression(CompressionKind_NONE)
        .setMemoryPool(pool)
        .setRowIndexStride(rowIndexStride);

    auto writer = createWriter(*type, &memStream, options);
    auto batch = writer->createRowBatch(3500);
    auto& structBatch = dynamic_cast<StructVectorBatch&>(*batch);
    auto& longBatch = dynamic_cast<LongVectorBatch&>(*structBatch.fields[0]);
    auto& strBatch = dynamic_cast<StringVectorBatch&>(*structBatch.fields[1]);

    // row group stride is 1000, here 3500 rows of data constitute 4 row groups.
    // the min/max pair of each row group is as below:
    // int1: 0/299700, 300000/599700, 600000/899700, 900000/1049700
    // string1: "0"/"9990", "10000"/"19990", "20000"/"29990", "30000"/"34990"
    char buffer[3500 * 5];
    uint64_t offset = 0;
    for (uint64_t i = 0; i < 3500; ++i) {
      longBatch.data[i] = static_cast<int64_t>(i * 300);

      std::ostringstream ss;
      ss << 10 * i;
      std::string str = ss.str();
      memcpy(buffer + offset, str.c_str(), str.size());
      strBatch.data[i] = buffer + offset;
      strBatch.length[i] = static_cast<int64_t>(str.size());
      offset += str.size();
    }
    structBatch.numElements = 3500;
    longBatch.numElements = 3500;
    strBatch.numElements = 3500;
    writer->add(*batch);
    writer->close();
  }

  void TestRangePredicates(Reader* reader) {
    // Build search argument (x >= 300000 AND x < 600000) for column 'int1'.
    // Test twice for using column name and column id respectively.
    for (int k = 0; k < 2; ++k) {
      std::unique_ptr<SearchArgument> sarg;
      if (k == 0) {
        sarg =
            SearchArgumentFactory::newBuilder()
                ->startAnd()
                .startNot()
                .lessThan("int1", PredicateDataType::LONG, Literal(static_cast<int64_t>(300000L)))
                .end()
                .lessThan("int1", PredicateDataType::LONG, Literal(static_cast<int64_t>(600000L)))
                .end()
                .build();
      } else {
        sarg = SearchArgumentFactory::newBuilder()
                   ->startAnd()
                   .startNot()
                   .lessThan(/*columnId=*/1, PredicateDataType::LONG,
                             Literal(static_cast<int64_t>(300000L)))
                   .end()
                   .lessThan(/*columnId=*/1, PredicateDataType::LONG,
                             Literal(static_cast<int64_t>(600000L)))
                   .end()
                   .build();
      }

      RowReaderOptions rowReaderOpts;
      rowReaderOpts.searchArgument(std::move(sarg));
      auto rowReader = reader->createRowReader(rowReaderOpts);

      auto readBatch = rowReader->createRowBatch(2000);
      auto& batch0 = dynamic_cast<StructVectorBatch&>(*readBatch);
      auto& batch1 = dynamic_cast<LongVectorBatch&>(*batch0.fields[0]);
      auto& batch2 = dynamic_cast<StringVectorBatch&>(*batch0.fields[1]);

      EXPECT_EQ(true, rowReader->next(*readBatch));
      EXPECT_EQ(1000, readBatch->numElements);
      EXPECT_EQ(1000, rowReader->getRowNumber());
      for (uint64_t i = 1000; i < 2000; ++i) {
        EXPECT_EQ(300 * i, batch1.data[i - 1000]);
        EXPECT_EQ(std::to_string(10 * i),
                  std::string(batch2.data[i - 1000], static_cast<size_t>(batch2.length[i - 1000])));
      }
      EXPECT_EQ(false, rowReader->next(*readBatch));
      EXPECT_EQ(3500, rowReader->getRowNumber());
    }
  }

  void TestNoRowsSelected(Reader* reader) {
    // Look through the file with no rows selected: x < 0
    // Test twice for using column name and column id respectively.
    for (int i = 0; i < 2; ++i) {
      std::unique_ptr<SearchArgument> sarg;
      if (i == 0) {
        sarg = SearchArgumentFactory::newBuilder()
                   ->startAnd()
                   .lessThan("int1", PredicateDataType::LONG, Literal(static_cast<int64_t>(0)))
                   .end()
                   .build();
      } else {
        sarg =
            SearchArgumentFactory::newBuilder()
                ->startAnd()
                .lessThan(/*columnId=*/1, PredicateDataType::LONG, Literal(static_cast<int64_t>(0)))
                .end()
                .build();
      }

      RowReaderOptions rowReaderOpts;
      rowReaderOpts.searchArgument(std::move(sarg));
      auto rowReader = reader->createRowReader(rowReaderOpts);

      auto readBatch = rowReader->createRowBatch(2000);
      EXPECT_EQ(false, rowReader->next(*readBatch));
      EXPECT_EQ(3500, rowReader->getRowNumber());
    }
  }

  void TestOrPredicates(Reader* reader) {
    // Select first 1000 and last 500 rows: x < 30000 OR x >= 1020000
    // Test twice for using column name and column id respectively.
    for (int k = 0; k < 2; ++k) {
      std::unique_ptr<SearchArgument> sarg;
      if (k == 0) {
        sarg =
            SearchArgumentFactory::newBuilder()
                ->startOr()
                .lessThan("int1", PredicateDataType::LONG, Literal(static_cast<int64_t>(300 * 100)))
                .startNot()
                .lessThan("int1", PredicateDataType::LONG,
                          Literal(static_cast<int64_t>(300 * 3400)))
                .end()
                .end()
                .build();
      } else {
        sarg = SearchArgumentFactory::newBuilder()
                   ->startOr()
                   .lessThan(/*columnId=*/1, PredicateDataType::LONG,
                             Literal(static_cast<int64_t>(300 * 100)))
                   .startNot()
                   .lessThan(/*columnId=*/1, PredicateDataType::LONG,
                             Literal(static_cast<int64_t>(300 * 3400)))
                   .end()
                   .end()
                   .build();
      }

      RowReaderOptions rowReaderOpts;
      rowReaderOpts.searchArgument(std::move(sarg));
      auto rowReader = reader->createRowReader(rowReaderOpts);

      auto readBatch = rowReader->createRowBatch(2000);
      auto& batch0 = dynamic_cast<StructVectorBatch&>(*readBatch);
      auto& batch1 = dynamic_cast<LongVectorBatch&>(*batch0.fields[0]);
      auto& batch2 = dynamic_cast<StringVectorBatch&>(*batch0.fields[1]);

      EXPECT_EQ(true, rowReader->next(*readBatch));
      EXPECT_EQ(1000, readBatch->numElements);
      EXPECT_EQ(0, rowReader->getRowNumber());
      for (uint64_t i = 0; i < 1000; ++i) {
        EXPECT_EQ(300 * i, batch1.data[i]);
        EXPECT_EQ(std::to_string(10 * i),
                  std::string(batch2.data[i], static_cast<size_t>(batch2.length[i])));
      }

      EXPECT_EQ(true, rowReader->next(*readBatch));
      EXPECT_EQ(500, readBatch->numElements);
      EXPECT_EQ(3000, rowReader->getRowNumber());
      for (uint64_t i = 3000; i < 3500; ++i) {
        EXPECT_EQ(300 * i, batch1.data[i - 3000]);
        EXPECT_EQ(std::to_string(10 * i),
                  std::string(batch2.data[i - 3000], static_cast<size_t>(batch2.length[i - 3000])));
      }

      EXPECT_EQ(false, rowReader->next(*readBatch));
      EXPECT_EQ(3500, rowReader->getRowNumber());

      // test seek to 3rd row group but is adjusted to 4th row group
      rowReader->seekToRow(2500);
      EXPECT_EQ(true, rowReader->next(*readBatch));
      EXPECT_EQ(3000, rowReader->getRowNumber());
      EXPECT_EQ(500, readBatch->numElements);
      for (uint64_t i = 3000; i < 3500; ++i) {
        EXPECT_EQ(300 * i, batch1.data[i - 3000]);
        EXPECT_EQ(std::to_string(10 * i),
                  std::string(batch2.data[i - 3000], static_cast<size_t>(batch2.length[i - 3000])));
      }
      EXPECT_EQ(false, rowReader->next(*readBatch));
      EXPECT_EQ(3500, rowReader->getRowNumber());
    }
  }

  void TestSeekWithPredicates(Reader* reader, uint64_t seekRowNumber) {
    // Build search argument (x < 300000) for column 'int1'. Only the first row group
    // will be selected. It has 1000 rows: (0, "0"), (300, "10"), (600, "20"), ...,
    // (299700, "9990").
    std::unique_ptr<SearchArgument> sarg =
        SearchArgumentFactory::newBuilder()
            ->lessThan("int1", PredicateDataType::LONG, Literal(static_cast<int64_t>(300000)))
            .build();
    RowReaderOptions rowReaderOpts;
    rowReaderOpts.searchArgument(std::move(sarg));
    auto rowReader = reader->createRowReader(rowReaderOpts);
    auto readBatch = rowReader->createRowBatch(2000);
    auto& batch0 = dynamic_cast<StructVectorBatch&>(*readBatch);
    auto& batch1 = dynamic_cast<LongVectorBatch&>(*batch0.fields[0]);
    auto& batch2 = dynamic_cast<StringVectorBatch&>(*batch0.fields[1]);

    rowReader->seekToRow(seekRowNumber);
    if (seekRowNumber >= 1000) {
      // Seek advance the first row group will go to the end of file
      EXPECT_FALSE(rowReader->next(*readBatch));
      EXPECT_EQ(0, readBatch->numElements);
      EXPECT_EQ(3500, rowReader->getRowNumber());
      return;
    }
    EXPECT_TRUE(rowReader->next(*readBatch));
    EXPECT_EQ(1000 - seekRowNumber, readBatch->numElements);
    EXPECT_EQ(seekRowNumber, rowReader->getRowNumber());
    for (uint64_t i = 0; i < readBatch->numElements; ++i) {
      EXPECT_EQ(300 * (i + seekRowNumber), batch1.data[i]);
      EXPECT_EQ(std::to_string(10 * (i + seekRowNumber)),
                std::string(batch2.data[i], static_cast<size_t>(batch2.length[i])));
    }
    EXPECT_FALSE(rowReader->next(*readBatch));
    EXPECT_EQ(3500, rowReader->getRowNumber());
  }

  void TestMultipleSeeksWithPredicates(Reader* reader) {
    // Build search argument (x >= 300000 AND x < 600000) for column 'int1'. Only the 2nd
    // row group will be selected.
    std::unique_ptr<SearchArgument> sarg =
        SearchArgumentFactory::newBuilder()
            ->startAnd()
            .startNot()
            .lessThan("int1", PredicateDataType::LONG, Literal(static_cast<int64_t>(300000L)))
            .end()
            .lessThan("int1", PredicateDataType::LONG, Literal(static_cast<int64_t>(600000L)))
            .end()
            .build();
    RowReaderOptions rowReaderOpts;
    rowReaderOpts.searchArgument(std::move(sarg));
    auto rowReader = reader->createRowReader(rowReaderOpts);

    // Read only one row after each seek
    auto readBatch = rowReader->createRowBatch(1);
    auto& batch0 = dynamic_cast<StructVectorBatch&>(*readBatch);
    auto& batch1 = dynamic_cast<LongVectorBatch&>(*batch0.fields[0]);
    auto& batch2 = dynamic_cast<StringVectorBatch&>(*batch0.fields[1]);

    // Seek within the 1st row group will go to the start of the 2nd row group
    rowReader->seekToRow(10);
    EXPECT_TRUE(rowReader->next(*readBatch));
    EXPECT_EQ(1000, rowReader->getRowNumber()) << "Should start at the 2nd row group";
    EXPECT_EQ(1, readBatch->numElements);
    EXPECT_EQ(300000, batch1.data[0]);
    EXPECT_EQ("10000", std::string(batch2.data[0], static_cast<size_t>(batch2.length[0])));

    // Seek within the 2nd row group (1000 rows) which is selected by the search argument
    uint64_t seekRowNum[] = {1001, 1010, 1100, 1500, 1999};
    for (uint64_t pos : seekRowNum) {
      rowReader->seekToRow(pos);
      EXPECT_TRUE(rowReader->next(*readBatch));
      EXPECT_EQ(pos, rowReader->getRowNumber());
      EXPECT_EQ(1, readBatch->numElements);
      EXPECT_EQ(300 * pos, batch1.data[0]);
      EXPECT_EQ(std::to_string(10 * pos),
                std::string(batch2.data[0], static_cast<size_t>(batch2.length[0])));
    }

    // Seek advance the 2nd row group will go to the end of file
    rowReader->seekToRow(2000);
    EXPECT_FALSE(rowReader->next(*readBatch));
    EXPECT_EQ(3500, rowReader->getRowNumber());
    EXPECT_EQ(0, readBatch->numElements);
  }

  TEST(TestPredicatePushdown, testPredicatePushdown) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();
    createMemTestFile(memStream, 1000);
    auto inStream = std::make_unique<MemoryInputStream>(memStream.getData(), memStream.getLength());
    ReaderOptions readerOptions;
    readerOptions.setMemoryPool(*pool);
    std::unique_ptr<Reader> reader = createReader(std::move(inStream), readerOptions);
    EXPECT_EQ(3500, reader->getNumberOfRows());

    TestRangePredicates(reader.get());
    TestNoRowsSelected(reader.get());
    TestOrPredicates(reader.get());

    uint64_t seekRowNumbers[] = {0, 10, 100, 500, 999, 1000, 1001, 4000};
    for (uint64_t seekRowNumber : seekRowNumbers) {
      TestSeekWithPredicates(reader.get(), seekRowNumber);
    }

    TestMultipleSeeksWithPredicates(reader.get());
  }

  void TestMultipleSeeksWithoutRowIndexes(Reader* reader, bool createSarg) {
    RowReaderOptions rowReaderOpts;
    if (createSarg) {
      // Build search argument x < 300000 for column 'int1'. All rows will be selected
      // since there are no row indexes in the file.
      std::unique_ptr<SearchArgument> sarg =
          SearchArgumentFactory::newBuilder()
              ->lessThan("int1", PredicateDataType::LONG, Literal(static_cast<int64_t>(300000L)))
              .build();
      rowReaderOpts.searchArgument(std::move(sarg));
    }
    auto rowReader = reader->createRowReader(rowReaderOpts);

    // Read only one row after each seek
    auto readBatch = rowReader->createRowBatch(1);
    auto& batch0 = dynamic_cast<StructVectorBatch&>(*readBatch);
    auto& batch1 = dynamic_cast<LongVectorBatch&>(*batch0.fields[0]);
    auto& batch2 = dynamic_cast<StringVectorBatch&>(*batch0.fields[1]);

    // Seeks within the file
    uint64_t seekRowNum[] = {0, 1, 100, 999, 1001, 1010, 1100, 1500, 1999, 3000, 3499};
    for (uint64_t pos : seekRowNum) {
      rowReader->seekToRow(pos);
      EXPECT_TRUE(rowReader->next(*readBatch));
      EXPECT_EQ(pos, rowReader->getRowNumber());
      EXPECT_EQ(1, readBatch->numElements);
      EXPECT_EQ(300 * pos, batch1.data[0]);
      EXPECT_EQ(std::to_string(10 * pos),
                std::string(batch2.data[0], static_cast<size_t>(batch2.length[0])));
    }

    // Seek advance the end of file
    rowReader->seekToRow(4000);
    EXPECT_FALSE(rowReader->next(*readBatch));
    EXPECT_EQ(3500, rowReader->getRowNumber());
    EXPECT_EQ(0, readBatch->numElements);
  }

  TEST(TestPredicatePushdown, testPredicatePushdownWithoutRowIndexes) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();
    // Create the file with rowIndexStride=0, so there are no row groups or row indexes.
    createMemTestFile(memStream, 0);
    auto inStream = std::make_unique<MemoryInputStream>(memStream.getData(), memStream.getLength());
    ReaderOptions readerOptions;
    readerOptions.setMemoryPool(*pool);
    std::unique_ptr<Reader> reader = createReader(std::move(inStream), readerOptions);
    EXPECT_EQ(3500, reader->getNumberOfRows());

    TestMultipleSeeksWithoutRowIndexes(reader.get(), true);
    TestMultipleSeeksWithoutRowIndexes(reader.get(), false);
  }

  // Test Sarg skips the whole file based on file stats.
  // Seeking to 'seekRowNumber' (if it's non-negative) before reads.
  void TestNoRowsSelectedWithFileStats(Reader* reader, int seekRowNumber) {
    std::unique_ptr<SearchArgument> sarg =
        SearchArgumentFactory::newBuilder()
            ->startAnd()
            .lessThan("col1", PredicateDataType::LONG, Literal(static_cast<int64_t>(0)))
            .end()
            .build();

    RowReaderOptions rowReaderOpts;
    rowReaderOpts.searchArgument(std::move(sarg));
    auto rowReader = reader->createRowReader(rowReaderOpts);

    auto readBatch = rowReader->createRowBatch(2000);
    if (seekRowNumber >= 0) {
      rowReader->seekToRow(static_cast<uint64_t>(seekRowNumber));
    }
    EXPECT_EQ(false, rowReader->next(*readBatch));
    EXPECT_EQ(7000, rowReader->getRowNumber());
  }

  void TestLastStripeSelectedWithStripeStats(Reader* reader, int seekRowNumber) {
    // Sargs: col1 between 3500 and 7000. First stripe (3500 rows) will be skipped.
    std::unique_ptr<SearchArgument> sarg =
        SearchArgumentFactory::newBuilder()
            ->between("col1", PredicateDataType::LONG, Literal(static_cast<int64_t>(3500)),
                      Literal(static_cast<int64_t>(7000)))
            .build();

    RowReaderOptions rowReaderOpts;
    rowReaderOpts.searchArgument(std::move(sarg));
    auto rowReader = reader->createRowReader(rowReaderOpts);

    if (seekRowNumber >= 0) {
      rowReader->seekToRow(static_cast<uint64_t>(seekRowNumber));
    }
    // Seek within the first stripe which is skipped due to PPD. Any seeks within it
    // will go to the end of the first stripe.
    if (seekRowNumber < 3500) {
      auto readBatch = rowReader->createRowBatch(2000);
      // 1st batch of 2000 rows
      EXPECT_EQ(true, rowReader->next(*readBatch));
      // test previous row number
      EXPECT_EQ(3500, rowReader->getRowNumber());
      EXPECT_EQ(2000, readBatch->numElements);
      auto& batch0 = dynamic_cast<StructVectorBatch&>(*readBatch);
      auto& batch1 = dynamic_cast<LongVectorBatch&>(*batch0.fields[0]);
      for (uint64_t i = 0; i < 2000; ++i) {
        EXPECT_EQ(i + 3500, batch1.data[i]);
      }

      // 2nd batch of the remaining 1500 rows
      EXPECT_EQ(true, rowReader->next(*readBatch));
      // test previous row number
      EXPECT_EQ(5500, rowReader->getRowNumber());
      EXPECT_EQ(1500, readBatch->numElements);
      for (uint64_t i = 0; i < 1500; ++i) {
        EXPECT_EQ(i + 5500, batch1.data[i]);
      }
      // no more batches
      EXPECT_EQ(false, rowReader->next(*readBatch));
      return;
    }

    // Seek to the end of file
    if (seekRowNumber >= 7000) {
      auto readBatch = rowReader->createRowBatch(2000);
      EXPECT_EQ(false, rowReader->next(*readBatch));
      EXPECT_EQ(7000, rowReader->getRowNumber());
      return;
    }

    {
      // Seek within the second stripe. Use 3500 as the batch size so we can read all rows
      // at once.
      auto readBatch = rowReader->createRowBatch(3500);
      EXPECT_EQ(true, rowReader->next(*readBatch));
      EXPECT_EQ(seekRowNumber, rowReader->getRowNumber());
      EXPECT_EQ(7000 - seekRowNumber, readBatch->numElements);
      auto& batch0 = dynamic_cast<StructVectorBatch&>(*readBatch);
      auto& batch1 = dynamic_cast<LongVectorBatch&>(*batch0.fields[0]);
      for (uint64_t i = 0; i < readBatch->numElements; ++i) {
        EXPECT_EQ(i + static_cast<unsigned long>(seekRowNumber), batch1.data[i]);
      }
      // no more batches
      EXPECT_EQ(false, rowReader->next(*readBatch));
    }
  }

  void TestFirstStripeSelectedWithStripeStats(Reader* reader, int seekRowNumber) {
    // Sargs: col1 < 3500. Last stripe (3500 rows) will be skipped.
    std::unique_ptr<SearchArgument> sarg =
        SearchArgumentFactory::newBuilder()
            ->lessThan("col1", PredicateDataType::LONG, Literal(static_cast<int64_t>(3500)))
            .build();
    RowReaderOptions rowReaderOpts;
    rowReaderOpts.searchArgument(std::move(sarg));
    auto rowReader = reader->createRowReader(rowReaderOpts);

    auto readBatch = rowReader->createRowBatch(3500);
    auto& batch0 = dynamic_cast<StructVectorBatch&>(*readBatch);
    auto& batch1 = dynamic_cast<LongVectorBatch&>(*batch0.fields[0]);

    uint64_t firstRowNumber = 0;
    if (seekRowNumber >= 0) {
      rowReader->seekToRow(static_cast<uint64_t>(seekRowNumber));
      firstRowNumber = static_cast<uint64_t>(seekRowNumber);
    }
    if (seekRowNumber < 3500) {
      EXPECT_EQ(true, rowReader->next(*readBatch));
      EXPECT_EQ(firstRowNumber, rowReader->getRowNumber());
      EXPECT_EQ(3500 - firstRowNumber, readBatch->numElements);
      for (uint64_t i = 0; i < readBatch->numElements; ++i) {
        EXPECT_EQ(i + firstRowNumber, batch1.data[i]);
      }
    }
    // no more batches
    EXPECT_EQ(false, rowReader->next(*readBatch));
    EXPECT_EQ(7000, rowReader->getRowNumber());
  }

  TEST(TestPredicatePushdown, testStripeAndFileStats) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();
    auto type = std::unique_ptr<Type>(Type::buildTypeFromString("struct<col1:bigint>"));
    WriterOptions options;
    options.setStripeSize(1)
        .setCompressionBlockSize(1024)
        .setCompression(CompressionKind_NONE)
        .setMemoryPool(pool)
        .setRowIndexStride(1000);

    auto writer = createWriter(*type, &memStream, options);
    auto batch = writer->createRowBatch(3500);
    auto& structBatch = dynamic_cast<StructVectorBatch&>(*batch);
    auto& longBatch = dynamic_cast<LongVectorBatch&>(*structBatch.fields[0]);

    // stripe 1 : 0 <= col1 < 3500
    // stripe 2 : 3500<= col1 < 7000
    uint64_t stripeCount = 2;
    for (uint64_t currentStripe = 0; currentStripe < stripeCount; ++currentStripe) {
      for (uint64_t i = 0; i < 3500; ++i) {
        longBatch.data[i] = static_cast<int64_t>(i + currentStripe * 3500);
      }
      structBatch.numElements = 3500;
      longBatch.numElements = 3500;
      writer->add(*batch);
    }
    writer->close();
    auto inStream = std::make_unique<MemoryInputStream>(memStream.getData(), memStream.getLength());
    ReaderOptions readerOptions;
    readerOptions.setMemoryPool(*pool);
    std::unique_ptr<Reader> reader = createReader(std::move(inStream), readerOptions);
    EXPECT_EQ(7000, reader->getNumberOfRows());
    EXPECT_EQ(stripeCount, reader->getNumberOfStripes());

    // Seek to different positions before each test. -1 means no seek.
    int seekRowNumber[] = {-1, 0, 1000, 4000, 8000};
    for (int pos : seekRowNumber) {
      TestNoRowsSelectedWithFileStats(reader.get(), pos);
      TestLastStripeSelectedWithStripeStats(reader.get(), pos);
      TestFirstStripeSelectedWithStripeStats(reader.get(), pos);
    }
  }
}  // namespace orc
