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

#include "sargs/SargsApplier.hh"
#include "wrap/gtest-wrapper.h"

namespace orc {

  TEST(TestSargsApplier, findColumnTest) {
    auto type = std::unique_ptr<Type>(
        Type::buildTypeFromString("struct<a:int,c:string,e:struct<f:bigint,g:double>>"));
    EXPECT_EQ(1, SargsApplier::findColumn(*type, "a"));
    EXPECT_EQ(2, SargsApplier::findColumn(*type, "c"));
    EXPECT_EQ(3, SargsApplier::findColumn(*type, "e"));
    EXPECT_EQ(4, SargsApplier::findColumn(*type, "f"));
    EXPECT_EQ(5, SargsApplier::findColumn(*type, "g"));
    EXPECT_EQ(std::numeric_limits<uint64_t>::max(), SargsApplier::findColumn(*type, "b"));
  }

  TEST(TestSargsApplier, findArrayColumnTest) {
    auto type = std::unique_ptr<Type>(
        Type::buildTypeFromString("struct<a:int,c:string,e:array<struct<f:bigint,g:double>>>"));
    EXPECT_EQ(1, SargsApplier::findColumn(*type, "a"));
    EXPECT_EQ(2, SargsApplier::findColumn(*type, "c"));
    EXPECT_EQ(3, SargsApplier::findColumn(*type, "e"));
    EXPECT_EQ(5, SargsApplier::findColumn(*type, "f"));
    EXPECT_EQ(6, SargsApplier::findColumn(*type, "g"));
    EXPECT_EQ(std::numeric_limits<uint64_t>::max(), SargsApplier::findColumn(*type, "b"));
  }

  TEST(TestSargsApplier, findMapColumnTest) {
    auto type = std::unique_ptr<Type>(
        Type::buildTypeFromString("struct<a:int,c:string,e:map<int,struct<f:bigint,g:double>>>"));
    EXPECT_EQ(1, SargsApplier::findColumn(*type, "a"));
    EXPECT_EQ(2, SargsApplier::findColumn(*type, "c"));
    EXPECT_EQ(3, SargsApplier::findColumn(*type, "e"));
    EXPECT_EQ(6, SargsApplier::findColumn(*type, "f"));
    EXPECT_EQ(7, SargsApplier::findColumn(*type, "g"));
    EXPECT_EQ(std::numeric_limits<uint64_t>::max(), SargsApplier::findColumn(*type, "b"));
  }

  static proto::ColumnStatistics createIntStats(int64_t min, int64_t max, bool hasNull = false) {
    proto::ColumnStatistics statistics;
    statistics.set_hasnull(hasNull);
    auto intStats = statistics.mutable_intstatistics();
    intStats->set_minimum(min);
    intStats->set_maximum(max);
    return statistics;
  }

  TEST(TestSargsApplier, testPickRowGroups) {
    auto type = std::unique_ptr<Type>(Type::buildTypeFromString("struct<x:int,y:int>"));
    auto sarg = SearchArgumentFactory::newBuilder()
                    ->startAnd()
                    .equals("x", PredicateDataType::LONG, Literal(static_cast<int64_t>(100)))
                    .equals("y", PredicateDataType::LONG, Literal(static_cast<int64_t>(10)))
                    .end()
                    .build();

    // prepare row group column statistics
    std::unordered_map<uint64_t, proto::RowIndex> rowIndexes;
    // col 1
    proto::RowIndex rowIndex1;
    *rowIndex1.mutable_entry()->Add()->mutable_statistics() = createIntStats(0L, 10L);
    *rowIndex1.mutable_entry()->Add()->mutable_statistics() = createIntStats(100L, 200L);
    *rowIndex1.mutable_entry()->Add()->mutable_statistics() = createIntStats(300L, 500L);
    *rowIndex1.mutable_entry()->Add()->mutable_statistics() = createIntStats(100L, 100L);
    rowIndexes[1] = rowIndex1;

    // col 2
    proto::RowIndex rowIndex2;
    *rowIndex2.mutable_entry()->Add()->mutable_statistics() = createIntStats(0L, 9L);
    *rowIndex2.mutable_entry()->Add()->mutable_statistics() = createIntStats(11L, 20L);
    *rowIndex2.mutable_entry()->Add()->mutable_statistics() = createIntStats(10L, 10L);
    *rowIndex2.mutable_entry()->Add()->mutable_statistics() = createIntStats(0L, 100LL);
    rowIndexes[2] = rowIndex2;

    // evaluate row group index
    ReaderMetrics metrics;
    SchemaEvolution se(nullptr, type.get());
    SargsApplier applier(*type, sarg.get(), 1000, WriterVersion_ORC_135, &metrics, &se);
    EXPECT_TRUE(applier.pickRowGroups(4000, rowIndexes, {}));
    const auto& nextSkippedRows = applier.getNextSkippedRows();
    EXPECT_EQ(4, nextSkippedRows.size());
    EXPECT_EQ(0, nextSkippedRows[0]);
    EXPECT_EQ(0, nextSkippedRows[1]);
    EXPECT_EQ(0, nextSkippedRows[2]);
    EXPECT_EQ(4000, nextSkippedRows[3]);
    EXPECT_EQ(metrics.SelectedRowGroupCount.load(), 1);
    EXPECT_EQ(metrics.EvaluatedRowGroupCount.load(), 4);
  }

  TEST(TestSargsApplier, testStripeAndFileStats) {
    auto type = std::unique_ptr<Type>(Type::buildTypeFromString("struct<x:int,y:int>"));
    auto sarg = SearchArgumentFactory::newBuilder()
                    ->startAnd()
                    .equals("x", PredicateDataType::LONG, Literal(static_cast<int64_t>(20)))
                    .equals("y", PredicateDataType::LONG, Literal(static_cast<int64_t>(40)))
                    .end()
                    .build();
    // Test stripe stats 0 <= x <= 10 and 0 <= y <= 50
    {
      orc::proto::StripeStatistics stripeStats;
      proto::ColumnStatistics structStatistics;
      structStatistics.set_hasnull(false);
      *stripeStats.add_colstats() = structStatistics;
      *stripeStats.add_colstats() = createIntStats(0L, 10L);
      *stripeStats.add_colstats() = createIntStats(0L, 50L);
      ReaderMetrics metrics;
      SargsApplier applier(*type, sarg.get(), 1000, WriterVersion_ORC_135, &metrics);
      EXPECT_FALSE(applier.evaluateStripeStatistics(stripeStats, 1));
      EXPECT_EQ(metrics.SelectedRowGroupCount.load(), 0);
      EXPECT_EQ(metrics.EvaluatedRowGroupCount.load(), 1);
    }
    // Test stripe stats 0 <= x <= 50 and 0 <= y <= 50
    {
      orc::proto::StripeStatistics stripeStats;
      proto::ColumnStatistics structStatistics;
      structStatistics.set_hasnull(false);
      *stripeStats.add_colstats() = structStatistics;
      *stripeStats.add_colstats() = createIntStats(0L, 50L);
      *stripeStats.add_colstats() = createIntStats(0L, 50L);
      ReaderMetrics metrics;
      SargsApplier applier(*type, sarg.get(), 1000, WriterVersion_ORC_135, &metrics);
      EXPECT_TRUE(applier.evaluateStripeStatistics(stripeStats, 1));
      EXPECT_EQ(metrics.SelectedRowGroupCount.load(), 0);
      EXPECT_EQ(metrics.EvaluatedRowGroupCount.load(), 0);
    }
    // Test file stats 0 <= x <= 10 and 0 <= y <= 50
    {
      orc::proto::Footer footer;
      proto::ColumnStatistics structStatistics;
      structStatistics.set_hasnull(false);
      *footer.add_statistics() = structStatistics;
      *footer.add_statistics() = createIntStats(0L, 10L);
      *footer.add_statistics() = createIntStats(0L, 50L);
      ReaderMetrics metrics;
      SargsApplier applier(*type, sarg.get(), 1000, WriterVersion_ORC_135, &metrics);
      EXPECT_FALSE(applier.evaluateFileStatistics(footer, 1));
      EXPECT_EQ(metrics.SelectedRowGroupCount.load(), 0);
      EXPECT_EQ(metrics.EvaluatedRowGroupCount.load(), 1);
    }
    // Test file stats 0 <= x <= 50 and 0 <= y <= 30
    {
      orc::proto::Footer footer;
      proto::ColumnStatistics structStatistics;
      structStatistics.set_hasnull(false);
      *footer.add_statistics() = structStatistics;
      *footer.add_statistics() = createIntStats(0L, 50L);
      *footer.add_statistics() = createIntStats(0L, 30L);
      ReaderMetrics metrics;
      SargsApplier applier(*type, sarg.get(), 1000, WriterVersion_ORC_135, &metrics);
      EXPECT_FALSE(applier.evaluateFileStatistics(footer, 1));
      EXPECT_EQ(metrics.SelectedRowGroupCount.load(), 0);
      EXPECT_EQ(metrics.EvaluatedRowGroupCount.load(), 1);
    }
    // Test file stats 0 <= x <= 50 and 0 <= y <= 50
    {
      orc::proto::Footer footer;
      proto::ColumnStatistics structStatistics;
      structStatistics.set_hasnull(false);
      *footer.add_statistics() = structStatistics;
      *footer.add_statistics() = createIntStats(0L, 50L);
      *footer.add_statistics() = createIntStats(0L, 50L);
      ReaderMetrics metrics;
      SargsApplier applier(*type, sarg.get(), 1000, WriterVersion_ORC_135, &metrics);
      EXPECT_TRUE(applier.evaluateFileStatistics(footer, 1));
      EXPECT_EQ(metrics.SelectedRowGroupCount.load(), 0);
      EXPECT_EQ(metrics.EvaluatedRowGroupCount.load(), 0);
    }
  }
}  // namespace orc
