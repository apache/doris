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
#include "orc/Reader.hh"

#include "Adaptor.hh"

#include "MemoryInputStream.hh"
#include "MemoryOutputStream.hh"

#include "wrap/gmock.h"
#include "wrap/gtest-wrapper.h"

namespace orc {

  static const int DEFAULT_MEM_STREAM_SIZE = 1024 * 1024;  // 1M

  TEST(TestTimestampStatistics, testOldFile) {
    std::stringstream ss;
    if (const char* example_dir = std::getenv("ORC_EXAMPLE_DIR")) {
      ss << example_dir;
    } else {
      ss << "../../../examples";
    }
    ss << "/TestOrcFile.testTimestamp.orc";
    orc::ReaderOptions readerOpts;
    std::unique_ptr<orc::Reader> reader =
        createReader(readLocalFile(ss.str().c_str(), readerOpts.getReaderMetrics()), readerOpts);

    std::unique_ptr<orc::ColumnStatistics> footerStats = reader->getColumnStatistics(0);
    const orc::TimestampColumnStatistics* footerColStats =
        reinterpret_cast<const orc::TimestampColumnStatistics*>(footerStats.get());

    std::unique_ptr<orc::StripeStatistics> stripeStats = reader->getStripeStatistics(0);
    const orc::TimestampColumnStatistics* stripeColStats =
        reinterpret_cast<const orc::TimestampColumnStatistics*>(
            stripeStats->getColumnStatistics(0));

    EXPECT_FALSE(footerColStats->hasMinimum());
    EXPECT_FALSE(footerColStats->hasMaximum());
    EXPECT_TRUE(footerColStats->hasLowerBound());
    EXPECT_TRUE(footerColStats->hasUpperBound());
    EXPECT_EQ(
        "Data type: Timestamp\nValues: 12\nHas null: no\nMinimum is not defined\nLowerBound: "
        "1994-12-31 07:00:00.688\nMaximum is not defined\nUpperBound: 2037-01-02 09:00:00.1\n",
        footerColStats->toString());

    EXPECT_TRUE(stripeColStats->hasMinimum());
    EXPECT_TRUE(stripeColStats->hasMaximum());
    EXPECT_EQ(
        "Data type: Timestamp\nValues: 12\nHas null: no\nMinimum: 1995-01-01 "
        "00:00:00.688\nLowerBound: 1995-01-01 00:00:00.688\nMaximum: 2037-01-01 "
        "00:00:00.0\nUpperBound: 2037-01-01 00:00:00.1\n",
        stripeColStats->toString());
  }

  TEST(TestTimestampStatistics, testTimezoneUTC) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();
    std::unique_ptr<Type> type(Type::buildTypeFromString("struct<col:timestamp>"));
    WriterOptions wOptions;
    wOptions.setMemoryPool(pool);
    std::unique_ptr<Writer> writer = createWriter(*type, &memStream, wOptions);
    std::unique_ptr<ColumnVectorBatch> batch = writer->createRowBatch(1024);
    StructVectorBatch* root = dynamic_cast<StructVectorBatch*>(batch.get());
    TimestampVectorBatch* col = dynamic_cast<orc::TimestampVectorBatch*>(root->fields[0]);

    int64_t expectedMinMillis = 1650133963321;  // 2022-04-16T18:32:43.321+00:00
    int64_t expectedMaxMillis = 1650133964321;  // 2022-04-16T18:32:44.321+00:00

    col->data[0] = expectedMinMillis / 1000;
    col->nanoseconds[0] = expectedMinMillis % 1000 * 1000000;
    col->data[1] = expectedMaxMillis / 1000;
    col->nanoseconds[1] = expectedMaxMillis % 1000 * 1000000;
    col->numElements = 2;
    root->numElements = 2;

    writer->add(*batch);
    writer->close();

    auto inStream = std::make_unique<MemoryInputStream>(memStream.getData(), memStream.getLength());
    ReaderOptions rOptions;
    rOptions.setMemoryPool(*pool);
    std::unique_ptr<Reader> reader = createReader(std::move(inStream), rOptions);

    std::unique_ptr<StripeStatistics> stripeStats = reader->getStripeStatistics(0);
    const TimestampColumnStatistics* stripeColStats =
        reinterpret_cast<const TimestampColumnStatistics*>(stripeStats->getColumnStatistics(1));

    EXPECT_TRUE(stripeColStats->hasLowerBound());
    EXPECT_TRUE(stripeColStats->hasUpperBound());
    EXPECT_TRUE(stripeColStats->hasMinimum());
    EXPECT_TRUE(stripeColStats->hasMaximum());
    EXPECT_EQ(stripeColStats->getMinimum(), expectedMinMillis);
    EXPECT_EQ(stripeColStats->getMaximum(), expectedMaxMillis);
    EXPECT_EQ(stripeColStats->getLowerBound(), expectedMinMillis);
    EXPECT_EQ(stripeColStats->getUpperBound(), expectedMaxMillis + 1);
  }

  TEST(TestTimestampStatistics, testTimezoneNonUTC) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();
    std::unique_ptr<Type> type(Type::buildTypeFromString("struct<col:timestamp>"));
    WriterOptions wOptions;
    wOptions.setMemoryPool(pool);
    wOptions.setTimezoneName("America/Los_Angeles");
    std::unique_ptr<Writer> writer = createWriter(*type, &memStream, wOptions);
    std::unique_ptr<ColumnVectorBatch> batch = writer->createRowBatch(1024);
    StructVectorBatch* root = dynamic_cast<StructVectorBatch*>(batch.get());
    TimestampVectorBatch* col = dynamic_cast<orc::TimestampVectorBatch*>(root->fields[0]);

    int64_t minMillis = 1650133963321;  // 2022-04-16T18:32:43.321+00:00
    int64_t maxMillis = 1650133964321;  // 2022-04-16T18:32:44.321+00:00

    col->data[0] = minMillis / 1000;
    col->nanoseconds[0] = minMillis % 1000 * 1000000;
    col->data[1] = maxMillis / 1000;
    col->nanoseconds[1] = maxMillis % 1000 * 1000000;
    col->numElements = 2;
    root->numElements = 2;

    writer->add(*batch);
    writer->close();

    auto inStream = std::make_unique<MemoryInputStream>(memStream.getData(), memStream.getLength());
    ReaderOptions rOptions;
    rOptions.setMemoryPool(*pool);
    std::unique_ptr<Reader> reader = createReader(std::move(inStream), rOptions);

    std::unique_ptr<StripeStatistics> stripeStats = reader->getStripeStatistics(0);
    const TimestampColumnStatistics* stripeColStats =
        reinterpret_cast<const TimestampColumnStatistics*>(stripeStats->getColumnStatistics(1));

    int64_t expectedMaxMillis = 1650108764321;  // 2022-04-16T11:32:44.321+00:00
    int64_t expectedMinMillis = 1650108763321;  // 2022-04-16T11:32:43.321+00:00

    EXPECT_TRUE(stripeColStats->hasLowerBound());
    EXPECT_TRUE(stripeColStats->hasUpperBound());
    EXPECT_TRUE(stripeColStats->hasMinimum());
    EXPECT_TRUE(stripeColStats->hasMaximum());
    EXPECT_EQ(stripeColStats->getMinimum(), expectedMinMillis);
    EXPECT_EQ(stripeColStats->getMaximum(), expectedMaxMillis);
    EXPECT_EQ(stripeColStats->getLowerBound(), expectedMinMillis);
    EXPECT_EQ(stripeColStats->getUpperBound(), expectedMaxMillis + 1);
  }

}  // namespace orc
