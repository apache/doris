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

#include "core/column/column_variant.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_variant.h"
#include "core/data_type/data_type_variant_v2.h"
#include "exprs/function/functions_comparison.h"
#include "exprs/function/parse/variant_string_parse.h"

namespace doris {

TEST(VariantV2ExecutionTest, ExecutionTypeSelectsPhysicalColumn) {
    DataTypeVariant legacy;
    DataTypeVariantV2 compute_v2;

    EXPECT_NE(check_and_get_column<ColumnVariant>(*legacy.create_column()), nullptr);
    EXPECT_NE(check_and_get_column<ColumnVariantV2>(*compute_v2.create_column()), nullptr);
}

TEST(VariantV2ExecutionTest, CanonicalComparison) {
    auto parse = [](std::initializer_list<std::string_view> rows) {
        JsonStringToVariantEncoder encoder({.max_json_key_length = 255,
                                            .throw_on_invalid_json = true,
                                            .check_duplicate_json_path = false});
        for (auto json : rows) {
            encoder.add_json({json.data(), json.size()});
        }
        auto column = ColumnVariantV2::create();
        column->insert_encoded_batch(encoder.finish_batch());
        return column;
    };
    auto left = parse({"1", "1", "null", R"({"a":1,"b":[2,null]})", "[1,2]", "true"});
    auto right = parse({"1.0", R"("1")", "null", R"({"b":[2.0,null],"a":1.0})", "[2,1]", "1"});
    auto type = std::make_shared<DataTypeVariantV2>();
    Block block {{left->get_ptr(), type, "left"},
                 {right->get_ptr(), type, "right"},
                 {nullptr, std::make_shared<DataTypeUInt8>(), "result"}};
    FunctionComparison<EqualsOp, NameEquals> equals;
    ASSERT_TRUE(equals.execute_impl(nullptr, block, {0, 1}, 2, left->size()).ok());
    EXPECT_EQ(assert_cast<const ColumnUInt8&>(*block.get_by_position(2).column).get_data(),
              (ColumnUInt8::Container {1, 0, 1, 1, 0, 0}));
    FunctionComparison<NotEqualsOp, NameNotEquals> not_equals;
    ASSERT_TRUE(not_equals.execute_impl(nullptr, block, {0, 1}, 2, left->size()).ok());
    EXPECT_EQ(assert_cast<const ColumnUInt8&>(*block.get_by_position(2).column).get_data(),
              (ColumnUInt8::Container {0, 1, 0, 0, 1, 1}));
    FunctionComparison<LessOp, NameLess> less;
    ASSERT_TRUE(less.execute_impl(nullptr, block, {0, 1}, 2, left->size()).ok());
    EXPECT_EQ(assert_cast<const ColumnUInt8&>(*block.get_by_position(2).column).get_data(),
              (ColumnUInt8::Container {0, 1, 0, 0, 1, 1}));

    auto one = parse({"1.0"});
    block.get_by_position(1).column = ColumnConst::create(std::move(one), left->size());
    ASSERT_TRUE(equals.execute_impl(nullptr, block, {0, 1}, 2, left->size()).ok());
    EXPECT_EQ(assert_cast<const ColumnUInt8&>(*block.get_by_position(2).column).get_data(),
              (ColumnUInt8::Container {1, 1, 0, 0, 0, 0}));
    ASSERT_TRUE(equals.execute_impl(nullptr, block, {1, 0}, 2, left->size()).ok());
    EXPECT_EQ(assert_cast<const ColumnUInt8&>(*block.get_by_position(2).column).get_data(),
              (ColumnUInt8::Container {1, 1, 0, 0, 0, 0}));
}

TEST(VariantV2ExecutionTest, TypedEquality) {
    auto numbers = ColumnInt32::create();
    numbers->get_data() = {1, 2, 3};
    auto nulls = ColumnUInt8::create();
    nulls->get_data() = {0, 0, 1};
    auto typed = ColumnVariantV2::create_typed(
            ColumnNullable::create(std::move(numbers), std::move(nulls)),
            std::make_shared<DataTypeInt32>());
    auto encoded = ColumnVariantV2::create();
    encoded->insert_range_from(*typed, 0, typed->size());
    encoded->ensure_encoded();
    for (const auto& left : {typed->get_ptr(), encoded->get_ptr()}) {
        for (const auto& right : {typed->get_ptr(), encoded->get_ptr()}) {
            for (size_t row = 0; row < left->size(); ++row) {
                EXPECT_EQ(left->compare_at(row, row, *right, 1), 0);
            }
        }
    }
}

} // namespace doris
