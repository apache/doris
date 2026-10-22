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

#include <gtest/gtest.h>

#include "core/column/column_nullable.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"
#include "testutil/column_helper.h"

namespace doris {
namespace {

void check_indexed_sum(const DataTypePtr& type, const ColumnPtr& input,
                       AggregateFunctionAttr attr = {}, const std::string& name = "sum") {
    auto function =
            AggregateFunctionSimpleFactory::instance().get(name, {type}, nullptr, true, -1, attr);
    ASSERT_NE(function, nullptr);
    Arena arena;
    const auto offset = function->align_of_data();
    std::vector<AggregateDataPtr> indexed;
    std::vector<AggregateDataPtr> scalar;
    for (size_t i = 0; i < 3; ++i) {
        indexed.push_back(arena.aligned_alloc(offset + function->size_of_data(), offset));
        scalar.push_back(arena.aligned_alloc(offset + function->size_of_data(), offset));
        function->create(indexed.back() + offset);
        function->create(scalar.back() + offset);
    }
    const IColumn* columns[] = {input.get()};
    std::vector<uint32_t> rows(8193);
    std::vector<AggregateDataPtr> places(rows.size());
    for (size_t i = 0; i < rows.size(); ++i) {
        const auto group = i % 3;
        // Group 2 repeats row 1, including an all-NULL group when that input row is NULL.
        rows[i] = group == 2 ? 1 : (i * 7 + 3) % input->size();
        places[i] = indexed[group];
        function->add(scalar[group] + offset, columns, rows[i], arena);
    }
    // Exercise both a full match buffer and a partial final buffer.
    function->add_batch_indexed(4096, places.data(), offset, columns, rows.data(), arena);
    function->add_batch_indexed(4096, places.data() + 4096, offset, columns, rows.data() + 4096,
                                arena);
    function->add_batch_indexed(1, places.data() + 8192, offset, columns, rows.data() + 8192,
                                arena);
    function->add_batch_indexed(0, nullptr, offset, columns, nullptr, arena);
    function->add_batch_indexed(0, nullptr, offset, nullptr, nullptr, arena);
    auto actual = function->get_return_type()->create_column();
    auto expected = function->get_return_type()->create_column();
    for (size_t i = 0; i < indexed.size(); ++i) {
        function->insert_result_into(indexed[i] + offset, *actual);
        function->insert_result_into(scalar[i] + offset, *expected);
        function->destroy(indexed[i] + offset);
        function->destroy(scalar[i] + offset);
    }
    for (size_t i = 0; i < indexed.size(); ++i) {
        EXPECT_EQ(actual->compare_at(i, i, *expected, 1), 0) << i;
    }
    if (type->is_nullable()) {
        EXPECT_EQ(assert_cast<const ColumnNullable&>(*actual).is_null_at(2),
                  assert_cast<const ColumnNullable&>(*input).is_null_at(1));
    }
}

TEST(AggregateFunctionIndexedTest, SumRepeatedRowsAndPlaces) {
    check_indexed_sum(std::make_shared<DataTypeInt32>(),
                      ColumnHelper::create_column<DataTypeInt32>({2, 3, -5, 7, 11}));
}

TEST(AggregateFunctionIndexedTest, GenericNullableAggregatesPreserveIndexedRows) {
    for (const auto& name : {"avg", "min", "max"}) {
        for (const bool null_v2 : {false, true}) {
            AggregateFunctionAttr attr;
            attr.enable_aggregate_function_null_v2 = null_v2;
            check_indexed_sum(
                    std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>()),
                    ColumnHelper::create_nullable_column<DataTypeFloat64>({2, 12345, -5, 7, 11},
                                                                          {0, 1, 0, 0, 1}),
                    attr, name);
        }
    }
}

TEST(AggregateFunctionIndexedTest, CountStarAcceptsNoArgumentColumns) {
    auto function = AggregateFunctionSimpleFactory::instance().get("count", {}, nullptr, false, -1);
    ASSERT_NE(function, nullptr);
    Arena arena;
    auto* first = arena.aligned_alloc(function->size_of_data(), function->align_of_data());
    auto* second = arena.aligned_alloc(function->size_of_data(), function->align_of_data());
    function->create(first);
    function->create(second);
    AggregateDataPtr places[] = {first, second, first, first};
    const uint32_t rows[] = {9, 5, 9, 100};
    function->add_batch_indexed(4, places, 0, nullptr, rows, arena);
    auto result = function->get_return_type()->create_column();
    function->insert_result_into(first, *result);
    function->insert_result_into(second, *result);
    EXPECT_EQ(result->get_int(0), 3);
    EXPECT_EQ(result->get_int(1), 1);
    function->destroy(first);
    function->destroy(second);
}

TEST(AggregateFunctionIndexedTest, MultiArgumentNullsUseSelectedSourceRows) {
    auto type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>());
    auto function = AggregateFunctionSimpleFactory::instance().get("covar_pop", {type, type},
                                                                   nullptr, true, -1);
    ASSERT_NE(function, nullptr);
    auto left = ColumnHelper::create_nullable_column<DataTypeFloat64>({1, 999, 3, 4, 5},
                                                                      {0, 1, 0, 0, 0});
    auto right = ColumnHelper::create_nullable_column<DataTypeFloat64>({2, 4, 999, 8, 10},
                                                                       {0, 0, 1, 0, 0});
    const IColumn* columns[] = {left.get(), right.get()};
    Arena arena;
    auto* indexed = arena.aligned_alloc(function->size_of_data(), function->align_of_data());
    auto* scalar = arena.aligned_alloc(function->size_of_data(), function->align_of_data());
    function->create(indexed);
    function->create(scalar);
    const uint32_t rows[] = {4, 0, 4, 1, 2, 3};
    AggregateDataPtr places[] = {indexed, indexed, indexed, indexed, indexed, indexed};
    function->add_batch_indexed(6, places, 0, columns, rows, arena);
    for (const auto row : rows) {
        function->add(scalar, columns, row, arena);
    }
    auto result = function->get_return_type()->create_column();
    function->insert_result_into(indexed, *result);
    function->insert_result_into(scalar, *result);
    EXPECT_EQ(result->compare_at(0, 1, *result, 1), 0);
    function->destroy(indexed);
    function->destroy(scalar);
}

TEST(AggregateFunctionIndexedTest, NullableSumPreservesAllNullGroups) {
    check_indexed_sum(std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()),
                      ColumnHelper::create_nullable_column<DataTypeInt32>({2, 12345, -5, 7, 11},
                                                                          {0, 1, 0, 0, 1}));
}

TEST(AggregateFunctionIndexedTest, FloatingSumPreservesUpdateOrder) {
    check_indexed_sum(std::make_shared<DataTypeFloat64>(),
                      ColumnHelper::create_column<DataTypeFloat64>({1e16, 1, -1e16, 0.25, -0.5}));
}

TEST(AggregateFunctionIndexedTest, NullableSumV2PreservesAllNullGroups) {
    AggregateFunctionAttr attr;
    attr.enable_aggregate_function_null_v2 = true;
    check_indexed_sum(std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()),
                      ColumnHelper::create_nullable_column<DataTypeInt32>({2, 12345, -5, 7, 11},
                                                                          {0, 1, 0, 0, 1}),
                      attr);
}

TEST(AggregateFunctionIndexedTest, NullableFloatingSumPreservesUpdateOrder) {
    for (const bool null_v2 : {false, true}) {
        AggregateFunctionAttr attr;
        attr.enable_aggregate_function_null_v2 = null_v2;
        check_indexed_sum(std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>()),
                          ColumnHelper::create_nullable_column<DataTypeFloat64>(
                                  {1e16, 12345, -1e16, 0.25, -0.5}, {0, 1, 0, 0, 0}),
                          attr);
    }
}

TEST(AggregateFunctionIndexedTest, NullableSumDenseAndSparseNullBatches) {
    for (const bool null_v2 : {false, true}) {
        AggregateFunctionAttr attr;
        attr.enable_aggregate_function_null_v2 = null_v2;
        for (const std::vector<uint8_t>& nulls :
             {std::vector<uint8_t> {0, 0, 0, 0, 0}, std::vector<uint8_t> {1, 1, 1, 1, 0},
              std::vector<uint8_t> {1, 1, 1, 1, 1}}) {
            check_indexed_sum(std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()),
                              ColumnHelper::create_nullable_column<DataTypeInt32>(
                                      {2, 12345, -5, 7, 11}, nulls),
                              attr);
        }
    }
}

} // namespace
} // namespace doris
