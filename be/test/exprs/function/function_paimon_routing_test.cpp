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

#include <memory>

#include "core/block/block.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "exprs/function/simple_function_factory.h"

namespace doris {
namespace {

constexpr size_t ROWS = 8;

void assert_constant_key_routes(const ColumnPtr& key, const DataTypePtr& key_type) {
    auto int32_type = std::make_shared<DataTypeInt32>();
    ColumnsWithTypeAndName arguments {
            {ColumnConst::create(ColumnInt32::create(1, 4), ROWS), int32_type, "buckets"},
            {key, key_type, "key"}};
    auto function = SimpleFunctionFactory::instance().get_function("__paimon_fixed_bucket_v1",
                                                                   arguments, int32_type);

    Block block(arguments);
    block.insert({nullptr, int32_type, "result"});
    ASSERT_TRUE(function->execute(nullptr, block, {0, 1}, 2, ROWS).ok());

    const auto* result = assert_cast<const ColumnInt32*>(block.get_by_position(2).column.get());
    ASSERT_EQ(ROWS, result->size());
    for (size_t row = 1; row < ROWS; ++row) {
        EXPECT_EQ(result->get_data()[0], result->get_data()[row]);
    }
}

TEST(FunctionPaimonRoutingTest, MultiRowConstantKey) {
    assert_constant_key_routes(ColumnConst::create(ColumnInt32::create(1, 7), ROWS),
                               std::make_shared<DataTypeInt32>());
}

TEST(FunctionPaimonRoutingTest, MultiRowConstantNullKey) {
    auto int32_type = std::make_shared<DataTypeInt32>();
    auto nullable_type = std::make_shared<DataTypeNullable>(int32_type);
    auto nullable_key =
            ColumnNullable::create(ColumnInt32::create(1, 0), ColumnUInt8::create(1, 1));
    assert_constant_key_routes(ColumnConst::create(std::move(nullable_key), ROWS), nullable_type);
}

} // namespace
} // namespace doris
