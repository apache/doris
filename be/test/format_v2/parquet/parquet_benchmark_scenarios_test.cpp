// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "../../../benchmark/parquet/parquet_benchmark_scenarios.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <set>
#include <tuple>

namespace doris::parquet_benchmark {
namespace {

TEST(ParquetBenchmarkScenariosTest, DecoderMatrixCoversNativeEncodingAndTypeFamilies) {
    const auto scenarios = decoder_scenarios();
    const std::set<std::pair<Encoding, ValueType>> actual = [&] {
        std::set<std::pair<Encoding, ValueType>> values;
        for (const auto& scenario : scenarios) {
            values.emplace(scenario.encoding, scenario.value_type);
        }
        return values;
    }();

    const std::set<std::pair<Encoding, ValueType>> expected {
            {Encoding::PLAIN, ValueType::INT32},
            {Encoding::PLAIN, ValueType::INT64},
            {Encoding::PLAIN, ValueType::FLOAT},
            {Encoding::PLAIN, ValueType::DOUBLE},
            {Encoding::PLAIN, ValueType::BYTE_ARRAY},
            {Encoding::PLAIN, ValueType::FIXED_LEN_BYTE_ARRAY},
            {Encoding::DICTIONARY, ValueType::INT32},
            {Encoding::DICTIONARY, ValueType::INT64},
            {Encoding::DICTIONARY, ValueType::FLOAT},
            {Encoding::DICTIONARY, ValueType::DOUBLE},
            {Encoding::DICTIONARY, ValueType::BYTE_ARRAY},
            {Encoding::DICTIONARY, ValueType::FIXED_LEN_BYTE_ARRAY},
            {Encoding::BYTE_STREAM_SPLIT, ValueType::FLOAT},
            {Encoding::BYTE_STREAM_SPLIT, ValueType::DOUBLE},
            {Encoding::BYTE_STREAM_SPLIT, ValueType::FIXED_LEN_BYTE_ARRAY},
            {Encoding::DELTA_BINARY_PACKED, ValueType::INT32},
            {Encoding::DELTA_BINARY_PACKED, ValueType::INT64},
            {Encoding::DELTA_LENGTH_BYTE_ARRAY, ValueType::BYTE_ARRAY},
            {Encoding::DELTA_BYTE_ARRAY, ValueType::BYTE_ARRAY},
    };
    EXPECT_EQ(actual, expected);
}

TEST(ParquetBenchmarkScenariosTest, ReaderMatrixCoversNullableSparseAndProjectionAxes) {
    const auto scenarios = reader_scenarios();
    for (const int null_percent : {0, 1, 10, 50, 90}) {
        for (const auto pattern : {Pattern::CLUSTERED, Pattern::ALTERNATING}) {
            for (const int selectivity : {0, 1, 10, 50, 90, 100}) {
                for (const auto projection :
                     {Projection::PREDICATE_ONLY, Projection::PREDICATE_PROJECTED}) {
                    EXPECT_TRUE(std::ranges::any_of(scenarios, [&](const ReaderScenario& scenario) {
                        return scenario.operation == ReaderOperation::PREDICATE_SCAN &&
                               scenario.encoding == Encoding::PLAIN &&
                               scenario.null_percent == null_percent &&
                               scenario.null_pattern == pattern &&
                               scenario.selectivity_percent == selectivity &&
                               scenario.projection == projection && scenario.schema_width == 32;
                    })) << "missing nullable sparse axis combination";
                }
            }
        }
    }
}

TEST(ParquetBenchmarkScenariosTest, ReaderMatrixCoversOperationsEncodingsAndSchemaWidths) {
    const auto scenarios = reader_scenarios();
    for (const auto operation :
         {ReaderOperation::OPEN_TO_FIRST_BLOCK, ReaderOperation::FULL_SCAN,
          ReaderOperation::PREDICATE_SCAN, ReaderOperation::COMPLEX_RESIDUAL_SCAN,
          ReaderOperation::LIMIT_1, ReaderOperation::LIMIT_1000}) {
        EXPECT_TRUE(std::ranges::any_of(scenarios, [&](const ReaderScenario& scenario) {
            return scenario.operation == operation;
        }));
    }
    for (const auto encoding : {Encoding::PLAIN, Encoding::DICTIONARY, Encoding::BYTE_STREAM_SPLIT,
                                Encoding::DELTA_BINARY_PACKED}) {
        for (const auto operation : {ReaderOperation::FULL_SCAN, ReaderOperation::PREDICATE_SCAN}) {
            EXPECT_TRUE(std::ranges::any_of(scenarios, [&](const ReaderScenario& scenario) {
                return scenario.encoding == encoding && scenario.operation == operation;
            }));
        }
    }
    for (const int width : {4, 32, 128, 512}) {
        for (const int predicate_position : {0, width - 1}) {
            EXPECT_TRUE(std::ranges::any_of(scenarios, [&](const ReaderScenario& scenario) {
                return scenario.schema_width == width &&
                       scenario.predicate_position == predicate_position;
            }));
        }
    }
}

TEST(ParquetBenchmarkScenariosTest, ReaderMatrixCoversComplexResidualLazyMaterialization) {
    const auto scenarios = reader_scenarios();
    EXPECT_TRUE(std::ranges::any_of(scenarios, [](const ReaderScenario& scenario) {
        return scenario.operation == ReaderOperation::COMPLEX_RESIDUAL_SCAN &&
               scenario.encoding == Encoding::PLAIN && scenario.selectivity_percent == 10 &&
               scenario.schema_width == 32;
    }));
}

TEST(ParquetBenchmarkScenariosTest, ReaderMatrixCoversFixedWidthRawFilterAxes) {
    const auto scenarios = reader_scenarios();
    for (const auto encoding : {Encoding::BYTE_STREAM_SPLIT, Encoding::DELTA_BINARY_PACKED}) {
        for (const int selectivity : {1, 10, 50, 90}) {
            for (const auto projection :
                 {Projection::PREDICATE_ONLY, Projection::PREDICATE_PROJECTED}) {
                EXPECT_TRUE(std::ranges::any_of(scenarios, [&](const ReaderScenario& scenario) {
                    return scenario.operation == ReaderOperation::PREDICATE_SCAN &&
                           scenario.encoding == encoding && scenario.null_percent == 10 &&
                           scenario.null_pattern == Pattern::ALTERNATING &&
                           scenario.selectivity_percent == selectivity &&
                           scenario.projection == projection && scenario.schema_width == 32 &&
                           scenario.predicate_position == 0;
                })) << "missing fixed-width raw filter axis combination";
            }
        }
    }
}

TEST(ParquetBenchmarkScenariosTest, SelectionPlanDistinguishesClusteredAndSparseRuns) {
    const auto clustered = make_selection_plan(1000, 10, Pattern::CLUSTERED);
    EXPECT_EQ(clustered.total_rows, 1000);
    EXPECT_EQ(clustered.selected_rows, 100);
    ASSERT_EQ(clustered.ranges.size(), 1);
    EXPECT_EQ(clustered.ranges.front().count, 100);

    const auto alternating = make_selection_plan(1000, 10, Pattern::ALTERNATING);
    EXPECT_EQ(alternating.total_rows, 1000);
    EXPECT_EQ(alternating.selected_rows, 100);
    EXPECT_EQ(alternating.ranges.size(), 100);

    EXPECT_EQ(make_selection_plan(1000, 0, Pattern::ALTERNATING).ranges.size(), 0);
    EXPECT_EQ(make_selection_plan(1000, 100, Pattern::ALTERNATING).ranges.size(), 1);
}

} // namespace
} // namespace doris::parquet_benchmark
