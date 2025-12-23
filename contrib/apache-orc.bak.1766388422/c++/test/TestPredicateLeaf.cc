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
#include "Statistics.hh"
#include "orc/BloomFilter.hh"
#include "orc/sargs/Literal.hh"
#include "sargs/PredicateLeaf.cc"
#include "wrap/gtest-wrapper.h"

namespace orc {

  TEST(TestPredicateLeaf, testCompareToRangeInt) {
    EXPECT_EQ(Location::BEFORE, compareToRange(19L, 20L, 40L));
    EXPECT_EQ(Location::AFTER, compareToRange(41L, 20L, 40L));
    EXPECT_EQ(Location::MIN, compareToRange(20L, 20L, 40L));
    EXPECT_EQ(Location::MIDDLE, compareToRange(21L, 20L, 40L));
    EXPECT_EQ(Location::MAX, compareToRange(40L, 20L, 40L));
    EXPECT_EQ(Location::BEFORE, compareToRange(0L, 1L, 1L));
    EXPECT_EQ(Location::MIN, compareToRange(1L, 1L, 1L));
    EXPECT_EQ(Location::AFTER, compareToRange(2L, 1L, 1L));
  }

  TEST(TestPredicateLeaf, testCompareToRangeString) {
    EXPECT_EQ(Location::BEFORE,
              compareToRange(std::string("a"), std::string("b"), std::string("c")));
    EXPECT_EQ(Location::AFTER,
              compareToRange(std::string("d"), std::string("b"), std::string("c")));
    EXPECT_EQ(Location::MIN, compareToRange(std::string("b"), std::string("b"), std::string("c")));
    EXPECT_EQ(Location::MIDDLE,
              compareToRange(std::string("bb"), std::string("b"), std::string("c")));
    EXPECT_EQ(Location::MAX, compareToRange(std::string("c"), std::string("b"), std::string("c")));
    EXPECT_EQ(Location::BEFORE,
              compareToRange(std::string("a"), std::string("b"), std::string("b")));
    EXPECT_EQ(Location::MIN, compareToRange(std::string("b"), std::string("b"), std::string("b")));
    EXPECT_EQ(Location::AFTER,
              compareToRange(std::string("c"), std::string("b"), std::string("b")));
  }

  TEST(TestPredicateLeaf, testCompareToCharNeedConvert) {
    EXPECT_EQ(Location::BEFORE,
              compareToRange(std::string("apple"), std::string("hello"), std::string("world")));
    EXPECT_EQ(Location::AFTER,
              compareToRange(std::string("zombie"), std::string("hello"), std::string("world")));
    EXPECT_EQ(Location::MIN,
              compareToRange(std::string("hello"), std::string("hello"), std::string("world")));
    EXPECT_EQ(Location::MIDDLE,
              compareToRange(std::string("pilot"), std::string("hello"), std::string("world")));
    EXPECT_EQ(Location::MAX,
              compareToRange(std::string("world"), std::string("hello"), std::string("world")));
    EXPECT_EQ(Location::BEFORE,
              compareToRange(std::string("apple"), std::string("hello"), std::string("hello")));
    EXPECT_EQ(Location::MIN,
              compareToRange(std::string("hello"), std::string("hello"), std::string("hello")));
    EXPECT_EQ(Location::AFTER,
              compareToRange(std::string("zombie"), std::string("hello"), std::string("hello")));
  }

  static proto::ColumnStatistics createBooleanStats(uint64_t n, uint64_t trueCount,
                                                    bool hasNull = false) {
    proto::ColumnStatistics colStats;
    colStats.set_hasnull(hasNull);
    colStats.set_numberofvalues(n);

    proto::BucketStatistics* boolStats = colStats.mutable_bucketstatistics();
    boolStats->add_count(trueCount);
    return colStats;
  }

  static proto::ColumnStatistics createIntStats(int64_t min, int64_t max, bool hasNull = false) {
    proto::ColumnStatistics colStats;
    colStats.set_hasnull(hasNull);
    colStats.set_numberofvalues(10);

    proto::IntegerStatistics* intStats = colStats.mutable_intstatistics();
    intStats->set_minimum(min);
    intStats->set_maximum(max);
    return colStats;
  }

  static proto::ColumnStatistics createDoubleStats(double min, double max, bool hasNull = false) {
    proto::ColumnStatistics colStats;
    colStats.set_hasnull(hasNull);
    colStats.set_numberofvalues(10);

    proto::DoubleStatistics* doubleStats = colStats.mutable_doublestatistics();
    const auto& curr_sum = min + max;
    doubleStats->set_minimum(min);
    doubleStats->set_maximum(max);
    doubleStats->set_sum(curr_sum);
    return colStats;
  }

  static proto::ColumnStatistics createDecimalStats(Decimal min, Decimal max,
                                                    bool hasNull = false) {
    proto::ColumnStatistics colStats;
    colStats.set_hasnull(hasNull);
    colStats.set_numberofvalues(10);

    proto::DecimalStatistics* decimalStats = colStats.mutable_decimalstatistics();
    decimalStats->set_minimum(min.toString(true));
    decimalStats->set_maximum(max.toString(true));
    return colStats;
  }

  static proto::ColumnStatistics createDateStats(int32_t min, int32_t max, bool hasNull = false) {
    proto::ColumnStatistics colStats;
    colStats.set_hasnull(hasNull);
    colStats.set_numberofvalues(10);

    proto::DateStatistics* dateStats = colStats.mutable_datestatistics();
    dateStats->set_minimum(min);
    dateStats->set_maximum(max);
    return colStats;
  }

  static proto::ColumnStatistics createTimestampStats(int64_t min, int64_t max,
                                                      bool hasNull = false) {
    proto::ColumnStatistics colStats;
    colStats.set_hasnull(hasNull);
    colStats.set_numberofvalues(10);

    proto::TimestampStatistics* tsStats = colStats.mutable_timestampstatistics();
    tsStats->set_minimumutc(min);
    tsStats->set_maximumutc(max);
    return colStats;
  }

  static proto::ColumnStatistics createTimestampStats(int64_t minSecond, int32_t minNano,
                                                      int64_t maxSecond, int32_t maxNano,
                                                      bool hasNull = false) {
    proto::ColumnStatistics colStats;
    colStats.set_hasnull(hasNull);
    colStats.set_numberofvalues(10);

    proto::TimestampStatistics* tsStats = colStats.mutable_timestampstatistics();
    tsStats->set_minimumutc(minSecond * 1000 + minNano / 1000000);
    tsStats->set_maximumutc(maxSecond * 1000 + maxNano / 1000000);
    tsStats->set_minimumnanos((minNano % 1000000) + 1);
    tsStats->set_maximumnanos((maxNano % 1000000) + 1);
    return colStats;
  }

  static proto::ColumnStatistics createStringStats(std::string min, std::string max,
                                                   bool hasNull = false) {
    proto::ColumnStatistics colStats;
    colStats.set_hasnull(hasNull);
    colStats.set_numberofvalues(10);

    proto::StringStatistics* strStats = colStats.mutable_stringstatistics();
    strStats->set_minimum(min);
    strStats->set_maximum(max);
    return colStats;
  }

  static TruthValue evaluate(const PredicateLeaf& pred, const proto::ColumnStatistics& pbStats,
                             const BloomFilter* bf = nullptr) {
    return pred.evaluate(WriterVersion_ORC_135, pbStats, bf);
  }

  TEST(TestPredicateLeaf, testPredEvalWithColStats) {
    PredicateLeaf pred0(PredicateLeaf::Operator::NULL_SAFE_EQUALS, PredicateDataType::BOOLEAN, "x",
                        Literal(true));
    EXPECT_EQ(TruthValue::YES, evaluate(pred0, createBooleanStats(10, 10)));
    EXPECT_EQ(TruthValue::NO, evaluate(pred0, createBooleanStats(10, 0)));

    PredicateLeaf pred1(PredicateLeaf::Operator::NULL_SAFE_EQUALS, PredicateDataType::BOOLEAN, "x",
                        Literal(false));
    EXPECT_EQ(TruthValue::NO, evaluate(pred1, createBooleanStats(10, 10)));
    EXPECT_EQ(TruthValue::YES, evaluate(pred1, createBooleanStats(10, 0)));

    PredicateLeaf pred2(PredicateLeaf::Operator::NULL_SAFE_EQUALS, PredicateDataType::LONG, "x",
                        Literal(static_cast<int64_t>(15)));
    EXPECT_EQ(TruthValue::YES_NO, evaluate(pred2, createIntStats(10, 100)));
    EXPECT_EQ(TruthValue::NO, evaluate(pred2, createIntStats(50, 100)));

    // delibrately pass column statistics of float type
    PredicateLeaf pred3(PredicateLeaf::Operator::NULL_SAFE_EQUALS, PredicateDataType::FLOAT, "x",
                        Literal(1.0));
    EXPECT_EQ(TruthValue::YES_NO_NULL, evaluate(pred3, createIntStats(10, 100)));

    PredicateLeaf pred4(PredicateLeaf::Operator::NULL_SAFE_EQUALS, PredicateDataType::FLOAT, "x",
                        Literal(15.0));
    EXPECT_EQ(TruthValue::YES_NO, evaluate(pred4, createDoubleStats(10.0, 100.0)));
    EXPECT_EQ(TruthValue::NO, evaluate(pred4, createDoubleStats(50.0, 100.0)));

    PredicateLeaf pred5(PredicateLeaf::Operator::NULL_SAFE_EQUALS, PredicateDataType::STRING, "x",
                        Literal("100", 3));
    EXPECT_EQ(TruthValue::YES_NO, evaluate(pred5, createStringStats("10", "1000")));

    PredicateLeaf pred6(PredicateLeaf::Operator::NULL_SAFE_EQUALS, PredicateDataType::DATE, "x",
                        Literal(PredicateDataType::DATE, 15));
    EXPECT_EQ(TruthValue::YES_NO, evaluate(pred6, createDateStats(10, 100)));

    PredicateLeaf pred7(PredicateLeaf::Operator::NULL_SAFE_EQUALS, PredicateDataType::DATE, "x",
                        Literal(PredicateDataType::DATE, 150));
    EXPECT_EQ(TruthValue::NO, evaluate(pred7, createDateStats(10, 100)));

    PredicateLeaf pred8(PredicateLeaf::Operator::NULL_SAFE_EQUALS, PredicateDataType::TIMESTAMP,
                        "x", Literal(500L, 0));
    EXPECT_EQ(TruthValue::NO, evaluate(pred8, createTimestampStats(450LL, 490L)));

    PredicateLeaf pred9(PredicateLeaf::Operator::NULL_SAFE_EQUALS, PredicateDataType::DECIMAL, "x",
                        Literal(1500, 4, 2));
    EXPECT_EQ(TruthValue::YES_NO,
              evaluate(pred9, createDecimalStats(Decimal("10.0"), Decimal("100.0"))));

    PredicateLeaf pred10(PredicateLeaf::Operator::NULL_SAFE_EQUALS, PredicateDataType::LONG, "x",
                         Literal(PredicateDataType::LONG));
    EXPECT_EQ(TruthValue::NO, evaluate(pred2, createIntStats(50, 100)));
  }

  TEST(TestPredicateLeaf, testEquals) {
    PredicateLeaf pred(PredicateLeaf::Operator::EQUALS, PredicateDataType::LONG, "x",
                       Literal(static_cast<int64_t>(15)));
    EXPECT_EQ(TruthValue::NO_NULL, evaluate(pred, createIntStats(20L, 30L, true)));
    EXPECT_EQ(TruthValue::YES_NO_NULL, evaluate(pred, createIntStats(15L, 30L, true)));
    EXPECT_EQ(TruthValue::YES_NO_NULL, evaluate(pred, createIntStats(10L, 30L, true)));
    EXPECT_EQ(TruthValue::YES_NO_NULL, evaluate(pred, createIntStats(10L, 15L, true)));
    EXPECT_EQ(TruthValue::NO_NULL, evaluate(pred, createIntStats(0L, 10L, true)));
    EXPECT_EQ(TruthValue::YES_NULL, evaluate(pred, createIntStats(15L, 15L, true)));
    EXPECT_EQ(TruthValue::NO, evaluate(pred, createIntStats(20L, 30L)));
    EXPECT_EQ(TruthValue::YES_NO, evaluate(pred, createIntStats(15L, 30L)));
    EXPECT_EQ(TruthValue::YES_NO, evaluate(pred, createIntStats(10L, 30L)));
    EXPECT_EQ(TruthValue::YES_NO, evaluate(pred, createIntStats(10L, 15L)));
    EXPECT_EQ(TruthValue::NO, evaluate(pred, createIntStats(0L, 10L)));
    EXPECT_EQ(TruthValue::YES, evaluate(pred, createIntStats(15L, 15L)));
  }

  TEST(TestPredicateLeaf, testNullSafeEquals) {
    PredicateLeaf pred(PredicateLeaf::Operator::NULL_SAFE_EQUALS, PredicateDataType::LONG, "x",
                       Literal(static_cast<int64_t>(15)));
    EXPECT_EQ(TruthValue::NO, evaluate(pred, createIntStats(20L, 30L, true)));
    EXPECT_EQ(TruthValue::YES_NO, evaluate(pred, createIntStats(15L, 30L, true)));
    EXPECT_EQ(TruthValue::YES_NO, evaluate(pred, createIntStats(10L, 30L, true)));
    EXPECT_EQ(TruthValue::YES_NO, evaluate(pred, createIntStats(10L, 15L, true)));
    EXPECT_EQ(TruthValue::NO, evaluate(pred, createIntStats(0L, 10L, true)));
    EXPECT_EQ(TruthValue::YES_NO, evaluate(pred, createIntStats(15L, 15L, true)));
  }

  TEST(TestPredicateLeaf, testLessThan) {
    PredicateLeaf pred(PredicateLeaf::Operator::LESS_THAN, PredicateDataType::LONG, "x",
                       Literal(static_cast<int64_t>(15)));
    EXPECT_EQ(TruthValue::NO_NULL, evaluate(pred, createIntStats(20L, 30L, true)));
    EXPECT_EQ(TruthValue::NO_NULL, evaluate(pred, createIntStats(15L, 30L, true)));
    EXPECT_EQ(TruthValue::YES_NO_NULL, evaluate(pred, createIntStats(10L, 30L, true)));
    EXPECT_EQ(TruthValue::YES_NO_NULL, evaluate(pred, createIntStats(10L, 15L, true)));
    EXPECT_EQ(TruthValue::YES_NULL, evaluate(pred, createIntStats(0L, 10L, true)));
  }

  TEST(TestPredicateLeaf, testLessThanEquals) {
    PredicateLeaf pred(PredicateLeaf::Operator::LESS_THAN_EQUALS, PredicateDataType::LONG, "x",
                       Literal(static_cast<int64_t>(15)));
    EXPECT_EQ(TruthValue::NO_NULL, evaluate(pred, createIntStats(20L, 30L, true)));
    EXPECT_EQ(TruthValue::YES_NO_NULL, evaluate(pred, createIntStats(15L, 30L, true)));
    EXPECT_EQ(TruthValue::YES_NO_NULL, evaluate(pred, createIntStats(10L, 30L, true)));
    EXPECT_EQ(TruthValue::YES_NULL, evaluate(pred, createIntStats(10L, 15L, true)));
    EXPECT_EQ(TruthValue::YES_NULL, evaluate(pred, createIntStats(0L, 10L, true)));
    // Edge cases where minValue == maxValue
    EXPECT_EQ(TruthValue::YES_NULL, evaluate(pred, createIntStats(10L, 10L, true)));
    EXPECT_EQ(TruthValue::YES_NULL, evaluate(pred, createIntStats(15L, 15L, true)));
    EXPECT_EQ(TruthValue::NO_NULL, evaluate(pred, createIntStats(20L, 20L, true)));
    // Edge case where stats contain NaN or Inf numbers
    PredicateLeaf pred4(PredicateLeaf::Operator::LESS_THAN, PredicateDataType::FLOAT, "x",
                        Literal(10.0));
    const auto& dInf = static_cast<double>(INFINITY);
    EXPECT_EQ(TruthValue::YES_NO, evaluate(pred4, createDoubleStats(dInf, dInf)));
    EXPECT_EQ(TruthValue::YES_NO_NULL, evaluate(pred4, createDoubleStats(dInf, dInf, true)));
  }

  TEST(TestPredicateLeaf, testIn) {
    PredicateLeaf pred(PredicateLeaf::Operator::IN, PredicateDataType::LONG, "x",
                       {Literal(static_cast<int64_t>(10)), Literal(static_cast<int64_t>(20))});
    EXPECT_EQ(TruthValue::YES_NULL, evaluate(pred, createIntStats(20L, 20L, true)));
    EXPECT_EQ(TruthValue::NO_NULL, evaluate(pred, createIntStats(30L, 30L, true)));
    EXPECT_EQ(TruthValue::YES_NO_NULL, evaluate(pred, createIntStats(10L, 30L, true)));
    EXPECT_EQ(TruthValue::NO_NULL, evaluate(pred, createIntStats(12L, 18L, true)));

    std::vector<Literal> inList{static_cast<int64_t>(10), static_cast<int64_t>(15),
                                static_cast<int64_t>(20)};
    PredicateLeaf pred2(PredicateLeaf::Operator::IN, PredicateDataType::LONG, "y", inList);
    EXPECT_EQ(TruthValue::YES_NULL, evaluate(pred2, createIntStats(20L, 20L, true)));
    EXPECT_EQ(TruthValue::NO_NULL, evaluate(pred2, createIntStats(12L, 14L, true)));
    EXPECT_EQ(TruthValue::NO_NULL, evaluate(pred2, createIntStats(16L, 19L, true)));
    EXPECT_EQ(TruthValue::YES_NO_NULL, evaluate(pred2, createIntStats(12L, 18L, true)));
  }

  TEST(TestPredicateLeaf, testBetween) {
    PredicateLeaf pred(PredicateLeaf::Operator::BETWEEN, PredicateDataType::LONG, "x",
                       {Literal(static_cast<int64_t>(10)), Literal(static_cast<int64_t>(20))});
    EXPECT_EQ(TruthValue::NO_NULL, evaluate(pred, createIntStats(0L, 5L, true)));
    EXPECT_EQ(TruthValue::NO_NULL, evaluate(pred, createIntStats(30L, 40L, true)));
    EXPECT_EQ(TruthValue::YES_NO_NULL, evaluate(pred, createIntStats(5L, 15L, true)));
    EXPECT_EQ(TruthValue::YES_NO_NULL, evaluate(pred, createIntStats(15L, 25L, true)));
    EXPECT_EQ(TruthValue::YES_NO_NULL, evaluate(pred, createIntStats(5L, 25L, true)));
    EXPECT_EQ(TruthValue::YES_NULL, evaluate(pred, createIntStats(10L, 20L, true)));
    EXPECT_EQ(TruthValue::YES_NULL, evaluate(pred, createIntStats(12L, 18L, true)));

    // check with empty predicate list
    PredicateLeaf pred1(PredicateLeaf::Operator::BETWEEN, PredicateDataType::LONG, "x", {});
    EXPECT_EQ(TruthValue::YES_NO, evaluate(pred1, createIntStats(0L, 5L, true)));
    EXPECT_EQ(TruthValue::YES_NO, evaluate(pred1, createIntStats(30L, 40L, true)));
    EXPECT_EQ(TruthValue::YES_NO, evaluate(pred1, createIntStats(5L, 15L, true)));
    EXPECT_EQ(TruthValue::YES_NO, evaluate(pred1, createIntStats(10L, 20L, true)));
  }

  TEST(TestPredicateLeaf, testIsNull) {
    PredicateLeaf pred(PredicateLeaf::Operator::IS_NULL, PredicateDataType::LONG, "x", {});
    EXPECT_EQ(TruthValue::YES_NO, evaluate(pred, createIntStats(20L, 30L, true)));
  }

  TEST(TestPredicateLeaf, testEqualsWithNullInStats) {
    PredicateLeaf pred(PredicateLeaf::Operator::EQUALS, PredicateDataType::STRING, "x",
                       Literal("c", 1));
    EXPECT_EQ(TruthValue::NO_NULL, evaluate(pred, createStringStats("d", "e", true)));
    EXPECT_EQ(TruthValue::NO_NULL, evaluate(pred, createStringStats("a", "b", true)));
    EXPECT_EQ(TruthValue::YES_NO_NULL, evaluate(pred, createStringStats("b", "c", true)));
    EXPECT_EQ(TruthValue::YES_NO_NULL, evaluate(pred, createStringStats("c", "d", true)));
    EXPECT_EQ(TruthValue::YES_NO_NULL, evaluate(pred, createStringStats("b", "d", true)));
    EXPECT_EQ(TruthValue::YES_NULL, evaluate(pred, createStringStats("c", "c", true)));
  }

  TEST(TestPredicateLeaf, testNullSafeEqualsWithNullInStats) {
    PredicateLeaf pred(PredicateLeaf::Operator::NULL_SAFE_EQUALS, PredicateDataType::STRING, "x",
                       Literal("c", 1));
    EXPECT_EQ(TruthValue::NO, evaluate(pred, createStringStats("d", "e", true)));
    EXPECT_EQ(TruthValue::NO, evaluate(pred, createStringStats("a", "b", true)));
    EXPECT_EQ(TruthValue::YES_NO, evaluate(pred, createStringStats("b", "c", true)));
    EXPECT_EQ(TruthValue::YES_NO, evaluate(pred, createStringStats("c", "d", true)));
    EXPECT_EQ(TruthValue::YES_NO, evaluate(pred, createStringStats("b", "d", true)));
    EXPECT_EQ(TruthValue::YES_NO, evaluate(pred, createStringStats("c", "c", true)));
  }

  TEST(TestPredicateLeaf, testLessThanWithNullInStats) {
    PredicateLeaf pred(PredicateLeaf::Operator::LESS_THAN, PredicateDataType::STRING, "x",
                       Literal("c", 1));
    EXPECT_EQ(TruthValue::NO_NULL, evaluate(pred, createStringStats("d", "e", true)));
    EXPECT_EQ(TruthValue::YES_NULL, evaluate(pred, createStringStats("a", "b", true)));
    EXPECT_EQ(TruthValue::YES_NO_NULL, evaluate(pred, createStringStats("b", "c", true)));
    EXPECT_EQ(TruthValue::NO_NULL, evaluate(pred, createStringStats("c", "d", true)));
    EXPECT_EQ(TruthValue::YES_NO_NULL, evaluate(pred, createStringStats("b", "d", true)));
    EXPECT_EQ(TruthValue::NO_NULL, evaluate(pred, createStringStats("c", "c", true)));
  }

  TEST(TestPredicateLeaf, testLessThanEqualsWithNullInStats) {
    PredicateLeaf pred(PredicateLeaf::Operator::LESS_THAN_EQUALS, PredicateDataType::STRING, "x",
                       Literal("c", 1));
    EXPECT_EQ(TruthValue::NO_NULL, evaluate(pred, createStringStats("d", "e", true)));
    EXPECT_EQ(TruthValue::YES_NULL, evaluate(pred, createStringStats("a", "b", true)));
    EXPECT_EQ(TruthValue::YES_NULL, evaluate(pred, createStringStats("b", "c", true)));
    EXPECT_EQ(TruthValue::YES_NO_NULL, evaluate(pred, createStringStats("c", "d", true)));
    EXPECT_EQ(TruthValue::YES_NO_NULL, evaluate(pred, createStringStats("b", "d", true)));
    // Edge cases where minValue == maxValue
    EXPECT_EQ(TruthValue::YES_NULL, evaluate(pred, createStringStats("a", "a", true)));
    EXPECT_EQ(TruthValue::YES_NULL, evaluate(pred, createStringStats("c", "c", true)));
    EXPECT_EQ(TruthValue::NO_NULL, evaluate(pred, createStringStats("d", "d", true)));
  }

  TEST(TestPredicateLeaf, testInWithNullInStats) {
    PredicateLeaf pred(PredicateLeaf::Operator::IN, PredicateDataType::STRING, "x",
                       {Literal("c", 1), Literal("f", 1)});
    EXPECT_EQ(TruthValue::NO_NULL, evaluate(pred, createStringStats("d", "e", true)));
    EXPECT_EQ(TruthValue::NO_NULL, evaluate(pred, createStringStats("a", "b", true)));
    EXPECT_EQ(TruthValue::YES_NO_NULL, evaluate(pred, createStringStats("e", "f", true)));
    EXPECT_EQ(TruthValue::YES_NO_NULL, evaluate(pred, createStringStats("c", "d", true)));
    EXPECT_EQ(TruthValue::YES_NO_NULL, evaluate(pred, createStringStats("b", "d", true)));
    EXPECT_EQ(TruthValue::YES_NULL, evaluate(pred, createStringStats("c", "c", true)));
  }

  TEST(TestPredicateLeaf, testBetweenWithNullInStats) {
    PredicateLeaf pred(PredicateLeaf::Operator::BETWEEN, PredicateDataType::STRING, "x",
                       {Literal("c", 1), Literal("f", 1)});
    EXPECT_EQ(TruthValue::YES_NULL, evaluate(pred, createStringStats("d", "e", true)));
    EXPECT_EQ(TruthValue::YES_NULL, evaluate(pred, createStringStats("e", "f", true)));
    EXPECT_EQ(TruthValue::NO_NULL, evaluate(pred, createStringStats("h", "g", true)));
    EXPECT_EQ(TruthValue::YES_NO_NULL, evaluate(pred, createStringStats("f", "g", true)));
    EXPECT_EQ(TruthValue::YES_NO_NULL, evaluate(pred, createStringStats("e", "g", true)));

    EXPECT_EQ(TruthValue::YES_NULL, evaluate(pred, createStringStats("c", "e", true)));
    EXPECT_EQ(TruthValue::YES_NULL, evaluate(pred, createStringStats("c", "f", true)));
    EXPECT_EQ(TruthValue::YES_NO_NULL, evaluate(pred, createStringStats("c", "g", true)));

    EXPECT_EQ(TruthValue::NO_NULL, evaluate(pred, createStringStats("a", "b", true)));
    EXPECT_EQ(TruthValue::YES_NO_NULL, evaluate(pred, createStringStats("a", "c", true)));
    EXPECT_EQ(TruthValue::YES_NO_NULL, evaluate(pred, createStringStats("b", "d", true)));
    EXPECT_EQ(TruthValue::YES_NULL, evaluate(pred, createStringStats("c", "c", true)));
  }

  TEST(TestPredicateLeaf, testIsNullWithNullInStats) {
    PredicateLeaf pred(PredicateLeaf::Operator::IS_NULL, PredicateDataType::STRING, "x", {});
    EXPECT_EQ(TruthValue::YES_NO, evaluate(pred, createStringStats("c", "d", true)));
    EXPECT_EQ(TruthValue::NO, evaluate(pred, createStringStats("c", "d", false)));
  }

  TEST(TestPredicateLeaf, testIntNullSafeEqualsBloomFilter) {
    PredicateLeaf pred(PredicateLeaf::Operator::NULL_SAFE_EQUALS, PredicateDataType::LONG, "x",
                       Literal(static_cast<int64_t>(15)));
    BloomFilterImpl bf(10000);
    for (int64_t i = 20; i < 1000; i++) {
      bf.addLong(i);
    }
    EXPECT_EQ(TruthValue::NO, evaluate(pred, createIntStats(10, 100), &bf));
    bf.addLong(15);
    EXPECT_EQ(TruthValue::YES_NO, evaluate(pred, createIntStats(10, 100), &bf));
  }

  TEST(TestPredicateLeaf, testIntEqualsBloomFilter) {
    PredicateLeaf pred(PredicateLeaf::Operator::EQUALS, PredicateDataType::LONG, "x",
                       Literal(static_cast<int64_t>(15)));
    BloomFilterImpl bf(10000);
    for (int64_t i = 20; i < 1000; i++) {
      bf.addLong(i);
    }
    EXPECT_EQ(TruthValue::NO_NULL, evaluate(pred, createIntStats(10, 100, true), &bf));
    bf.addLong(15);
    EXPECT_EQ(TruthValue::YES_NO_NULL, evaluate(pred, createIntStats(10, 100, true), &bf));
  }

  TEST(TestPredicateLeaf, testIntInBloomFilter) {
    PredicateLeaf pred(PredicateLeaf::Operator::IN, PredicateDataType::LONG, "x",
                       {Literal(static_cast<int64_t>(15)), Literal(static_cast<int64_t>(19))});
    BloomFilterImpl bf(10000);
    for (int64_t i = 20; i < 1000; i++) {
      bf.addLong(i);
    }
    bf.addLong(19);
    EXPECT_EQ(TruthValue::YES_NO_NULL, evaluate(pred, createIntStats(10, 100, true), &bf));
    bf.addLong(15);
    EXPECT_EQ(TruthValue::YES_NO_NULL, evaluate(pred, createIntStats(10, 100, true), &bf));
  }

  TEST(TestPredicateLeaf, testDoubleNullSafeEqualsBloomFilter) {
    PredicateLeaf pred(PredicateLeaf::Operator::NULL_SAFE_EQUALS, PredicateDataType::FLOAT, "x",
                       Literal(15.0));
    BloomFilterImpl bf(10000);
    for (int64_t i = 20; i < 1000; i++) {
      bf.addDouble(static_cast<double>(i));
    }
    EXPECT_EQ(TruthValue::NO, evaluate(pred, createIntStats(10.0, 100.0, true), &bf));
    bf.addDouble(15.0);
    EXPECT_EQ(TruthValue::YES_NO, evaluate(pred, createIntStats(10.0, 100.0, true), &bf));
  }

  TEST(TestPredicateLeaf, testDoubleEqualsBloomFilter) {
    PredicateLeaf pred(PredicateLeaf::Operator::EQUALS, PredicateDataType::FLOAT, "x",
                       Literal(15.0));
    BloomFilterImpl bf(10000);
    for (int64_t i = 20; i < 1000; i++) {
      bf.addDouble(static_cast<double>(i));
    }
    EXPECT_EQ(TruthValue::NO_NULL, evaluate(pred, createIntStats(10.0, 100.0, true), &bf));
    bf.addDouble(15.0);
    EXPECT_EQ(TruthValue::YES_NO_NULL, evaluate(pred, createIntStats(10.0, 100.0, true), &bf));
  }

  TEST(TestPredicateLeaf, testDoubleInBloomFilter) {
    PredicateLeaf pred(PredicateLeaf::Operator::IN, PredicateDataType::FLOAT, "x",
                       {Literal(15.0), Literal(19.0)});
    BloomFilterImpl bf(10000);
    for (int64_t i = 20; i < 1000; i++) {
      bf.addDouble(static_cast<double>(i));
    }
    bf.addDouble(19.0);
    EXPECT_EQ(TruthValue::YES_NO_NULL, evaluate(pred, createIntStats(10.0, 100.0, true), &bf));
    bf.addDouble(15.0);
    EXPECT_EQ(TruthValue::YES_NO_NULL, evaluate(pred, createIntStats(10.0, 100.0, true), &bf));
  }

  TEST(TestPredicateLeaf, testStringEqualsBloomFilter) {
    PredicateLeaf pred(PredicateLeaf::Operator::EQUALS, PredicateDataType::STRING, "x",
                       Literal("str_15", 6));
    BloomFilterImpl bf(10000);
    for (int64_t i = 20; i < 1000; i++) {
      std::string str = "str_" + std::to_string(i);
      bf.addBytes(str.c_str(), static_cast<int64_t>(str.size()));
    }
    EXPECT_EQ(TruthValue::NO_NULL,
              evaluate(pred, createStringStats("str_10", "str_200", true), &bf));
    bf.addBytes("str_15", 6);
    EXPECT_EQ(TruthValue::YES_NO_NULL,
              evaluate(pred, createStringStats("str_10", "str_200", true), &bf));
  }

  TEST(TestPredicateLeaf, testStringInBloomFilter) {
    PredicateLeaf pred(PredicateLeaf::Operator::IN, PredicateDataType::STRING, "x",
                       {Literal("str_15", 6), Literal("str_19", 6)});
    BloomFilterImpl bf(10000);
    for (int64_t i = 20; i < 1000; i++) {
      std::string str = "str_" + std::to_string(i);
      bf.addBytes(str.c_str(), static_cast<int64_t>(str.size()));
    }
    EXPECT_EQ(TruthValue::NO_NULL,
              evaluate(pred, createStringStats("str_10", "str_200", true), &bf));
    bf.addBytes("str_19", 6);
    EXPECT_EQ(TruthValue::YES_NO_NULL,
              evaluate(pred, createStringStats("str_10", "str_200", true), &bf));
    bf.addBytes("str_15", 6);
    EXPECT_EQ(TruthValue::YES_NO_NULL,
              evaluate(pred, createStringStats("str_10", "str_200", true), &bf));
  }

  TEST(TestPredicateLeaf, testDateNullSafeEqualsBloomFilter) {
    PredicateLeaf pred(PredicateLeaf::Operator::NULL_SAFE_EQUALS, PredicateDataType::DATE, "x",
                       Literal(PredicateDataType::DATE, 15));
    BloomFilterImpl bf(10000);
    for (int64_t i = 20; i < 1000; i++) {
      bf.addLong(i);
    }
    EXPECT_EQ(TruthValue::NO, evaluate(pred, createDateStats(10, 100), &bf));
    bf.addLong(15);
    EXPECT_EQ(TruthValue::YES_NO, evaluate(pred, createDateStats(10.0, 100.0), &bf));
  }

  TEST(TestPredicateLeaf, testDateEqualsBloomFilter) {
    PredicateLeaf pred(PredicateLeaf::Operator::EQUALS, PredicateDataType::DATE, "x",
                       Literal(PredicateDataType::DATE, 15));
    BloomFilterImpl bf(10000);
    for (int64_t i = 20; i < 1000; i++) {
      bf.addLong(i);
    }
    EXPECT_EQ(TruthValue::NO_NULL, evaluate(pred, createDateStats(10, 100, true), &bf));
    bf.addLong(15);
    EXPECT_EQ(TruthValue::YES_NO_NULL, evaluate(pred, createDateStats(10.0, 100.0, true), &bf));
  }

  TEST(TestPredicateLeaf, testDateInBloomFilter) {
    PredicateLeaf pred(
        PredicateLeaf::Operator::IN, PredicateDataType::DATE, "x",
        {Literal(PredicateDataType::DATE, 15), Literal(PredicateDataType::DATE, 19)});
    BloomFilterImpl bf(10000);
    for (int64_t i = 20; i < 1000; i++) {
      bf.addLong(i);
    }
    EXPECT_EQ(TruthValue::NO_NULL, evaluate(pred, createDateStats(10, 100, true), &bf));
    bf.addLong(19);
    EXPECT_EQ(TruthValue::YES_NO_NULL, evaluate(pred, createDateStats(10.0, 100.0, true), &bf));
    bf.addLong(15);
    EXPECT_EQ(TruthValue::YES_NO_NULL, evaluate(pred, createDateStats(10.0, 100.0, true), &bf));
  }

  TEST(TestPredicateLeaf, testDecimalEqualsBloomFilter) {
    PredicateLeaf pred(PredicateLeaf::Operator::EQUALS, PredicateDataType::DECIMAL, "x",
                       Literal(15, 2, 0));
    BloomFilterImpl bf(10000);
    for (int64_t i = 20; i < 1000; i++) {
      std::string str = Decimal(i, 0).toString(true);
      bf.addBytes(str.c_str(), static_cast<int64_t>(str.size()));
    }
    EXPECT_EQ(TruthValue::NO_NULL,
              evaluate(pred, createDecimalStats(Decimal("10"), Decimal("100"), true), &bf));

    std::string str = Decimal(15, 0).toString();
    bf.addBytes(str.c_str(), static_cast<int64_t>(str.size()));
    EXPECT_EQ(TruthValue::YES_NO_NULL,
              evaluate(pred, createDecimalStats(Decimal("10"), Decimal("100"), true), &bf));
  }

  TEST(TestPredicateLeaf, testDecimalInBloomFilter) {
    PredicateLeaf pred(PredicateLeaf::Operator::IN, PredicateDataType::DECIMAL, "x",
                       {Literal(15, 2, 0), Literal(19, 2, 0)});
    BloomFilterImpl bf(10000);
    for (int64_t i = 20; i < 1000; i++) {
      std::string str = Decimal(i, 0).toString(true);
      bf.addBytes(str.c_str(), static_cast<int64_t>(str.size()));
    }
    EXPECT_EQ(TruthValue::NO_NULL,
              evaluate(pred, createDecimalStats(Decimal("10"), Decimal("100"), true), &bf));

    std::string str = Decimal(15, 0).toString();
    bf.addBytes(str.c_str(), static_cast<int64_t>(str.size()));
    EXPECT_EQ(TruthValue::YES_NO_NULL,
              evaluate(pred, createDecimalStats(Decimal("10"), Decimal("100"), true), &bf));

    str = Decimal(19, 0).toString();
    bf.addBytes(str.c_str(), static_cast<int64_t>(str.size()));
    EXPECT_EQ(TruthValue::YES_NO_NULL,
              evaluate(pred, createDecimalStats(Decimal("10"), Decimal("100"), true), &bf));
  }

  TEST(TestPredicateLeaf, testNullsInBloomFilter) {
    PredicateLeaf pred(PredicateLeaf::Operator::IN, PredicateDataType::DECIMAL, "x",
                       {Literal(15, 2, 0), Literal(19, 2, 0), Literal(PredicateDataType::DECIMAL)});
    BloomFilterImpl bf(10000);
    for (int64_t i = 20; i < 1000; i++) {
      std::string str = Decimal(i, 0).toString(true);
      bf.addBytes(str.c_str(), static_cast<int64_t>(str.size()));
    }

    // hasNull is false, so bloom filter should return NO
    EXPECT_EQ(TruthValue::NO,
              evaluate(pred, createDecimalStats(Decimal("10"), Decimal("200"), false), &bf));

    // hasNull is true, so bloom filter should return YES_NO_NULL
    EXPECT_EQ(TruthValue::YES_NO_NULL,
              evaluate(pred, createDecimalStats(Decimal("10"), Decimal("200"), true), &bf));

    std::string str = Decimal(19, 0).toString();
    bf.addBytes(str.c_str(), static_cast<int64_t>(str.size()));
    EXPECT_EQ(TruthValue::YES_NO_NULL,
              evaluate(pred, createDecimalStats(Decimal("10"), Decimal("200"), true), &bf));

    str = Decimal(15, 0).toString();
    bf.addBytes(str.c_str(), static_cast<int64_t>(str.size()));
    EXPECT_EQ(TruthValue::YES_NO_NULL,
              evaluate(pred, createDecimalStats(Decimal("10"), Decimal("200"), true), &bf));
  }

  TEST(TestPredicateLeaf, testTimestampWithNanos) {
    // 1970-01-01 00:00:00
    PredicateLeaf pred1(PredicateLeaf::Operator::EQUALS, PredicateDataType::TIMESTAMP, "x",
                        Literal(static_cast<int64_t>(0), 500000));
    EXPECT_EQ(TruthValue::YES, evaluate(pred1, createTimestampStats(0, 500000, 0, 500000)));

    PredicateLeaf pred2(PredicateLeaf::Operator::LESS_THAN_EQUALS, PredicateDataType::TIMESTAMP,
                        "x", Literal(static_cast<int64_t>(0), 500000));
    EXPECT_EQ(TruthValue::YES, evaluate(pred2, createTimestampStats(0, 499999, 0, 499999)));
    EXPECT_EQ(TruthValue::YES, evaluate(pred2, createTimestampStats(0, 500000, 0, 500000)));
    EXPECT_EQ(TruthValue::NO, evaluate(pred2, createTimestampStats(0, 500001, 0, 500001)));

    PredicateLeaf pred3(PredicateLeaf::Operator::LESS_THAN, PredicateDataType::TIMESTAMP, "x",
                        Literal(static_cast<int64_t>(0), 500000));
    EXPECT_EQ(TruthValue::NO, evaluate(pred3, createTimestampStats(0, 500000, 0, 500000)));

    // 2037-01-01 00:00:00
    PredicateLeaf pred4(PredicateLeaf::Operator::EQUALS, PredicateDataType::TIMESTAMP, "x",
                        Literal(2114380800, 1109000));
    EXPECT_EQ(TruthValue::YES_NO,
              evaluate(pred4, createTimestampStats(2114380800, 1109000, 2114380800, 6789100)));

    PredicateLeaf pred5(PredicateLeaf::Operator::EQUALS, PredicateDataType::TIMESTAMP, "x",
                        Literal(2114380800, 1000000));
    EXPECT_EQ(TruthValue::NO,
              evaluate(pred5, createTimestampStats(2114380800, 1109000, 2114380800, 6789100)));

    PredicateLeaf pred6(PredicateLeaf::Operator::LESS_THAN, PredicateDataType::TIMESTAMP, "x",
                        Literal(2114380800, 6789000));
    EXPECT_EQ(TruthValue::YES_NO,
              evaluate(pred6, createTimestampStats(2114380800, 1109000, 2114380800, 6789100)));

    PredicateLeaf pred7(PredicateLeaf::Operator::LESS_THAN, PredicateDataType::TIMESTAMP, "x",
                        Literal(2114380800, 2000000));
    EXPECT_EQ(TruthValue::YES_NO,
              evaluate(pred7, createTimestampStats(2114380800, 1109000, 2114380800, 6789100)));

    PredicateLeaf pred8(PredicateLeaf::Operator::LESS_THAN, PredicateDataType::TIMESTAMP, "x",
                        Literal(2114380800, 1000000));
    EXPECT_EQ(TruthValue::NO,
              evaluate(pred8, createTimestampStats(2114380800, 1109000, 2114380800, 6789100)));
  }

}  // namespace orc
