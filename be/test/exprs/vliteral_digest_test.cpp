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

#include <utility>
#include <vector>

#include "agent/be_exec_version_manager.h"
#include "core/data_type/data_type_agg_state.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "exprs/vliteral.h"
#include "testutil/mock/mock_fn_call.h"

namespace doris {

TEST(VLiteralDigestTest, ComplexNullDisablesParentDigest) {
    for (auto type : {TYPE_BITMAP, TYPE_HLL, TYPE_QUANTILE_STATE, TYPE_VARBINARY}) {
        SCOPED_TRACE(type_to_string(type));
        auto data_type = DataTypeFactory::instance().create_data_type(type, true);
        auto literal = VLiteral::create_shared(data_type, Field());
        EXPECT_EQ(literal->get_digest(42), 0);

        // A later scalar child must not revive a digest disabled by the complex literal.
        auto parent = MockFnCall::create("if");
        parent->add_child(literal);
        parent->add_child(VLiteral::create_shared(std::make_shared<DataTypeInt32>(),
                                                  Field::create_field<TYPE_INT>(1)));
        EXPECT_EQ(parent->get_digest(42), 0);
    }
}

TEST(VLiteralDigestTest, NonNullComplexValuesDisableDigest) {
    auto bitmap = VLiteral::create_shared(
            DataTypeFactory::instance().create_data_type(TYPE_BITMAP, false),
            Field::create_field<TYPE_BITMAP>(BitmapValue(1)));
    auto hll =
            VLiteral::create_shared(DataTypeFactory::instance().create_data_type(TYPE_HLL, false),
                                    Field::create_field<TYPE_HLL>(HyperLogLog()));
    auto quantile = VLiteral::create_shared(
            DataTypeFactory::instance().create_data_type(TYPE_QUANTILE_STATE, false),
            Field::create_field<TYPE_QUANTILE_STATE>(QuantileState()));
    EXPECT_EQ(bitmap->get_digest(42), 0);
    EXPECT_EQ(hll->get_digest(42), 0);
    EXPECT_EQ(quantile->get_digest(42), 0);

    auto binary = VLiteral::create_shared(
            DataTypeFactory::instance().create_data_type(TYPE_VARBINARY, false),
            Field::create_field<TYPE_VARBINARY>(StringView("a", 1)));
    EXPECT_EQ(binary->get_digest(42), 0);
}

TEST(VLiteralDigestTest, NestedUnsupportedColumnsDisableDigest) {
    auto bitmap_type = DataTypeFactory::instance().create_data_type(TYPE_BITMAP, true);
    auto int_type = std::make_shared<DataTypeInt32>();
    auto array_type = std::make_shared<DataTypeArray>(bitmap_type);
    auto array = Field::create_field<TYPE_ARRAY>(
            Array {Field::create_field<TYPE_BITMAP>(BitmapValue(1)), Field()});
    auto nested_array_type = std::make_shared<DataTypeArray>(array_type);
    auto nested_array = Field::create_field<TYPE_ARRAY>(Array {array});
    auto map_type = std::make_shared<DataTypeMap>(int_type, array_type);
    auto map = Field::create_field<TYPE_MAP>(
            Map {Field::create_field<TYPE_ARRAY>(Array {Field::create_field<TYPE_INT>(1)}),
                 Field::create_field<TYPE_ARRAY>(Array {array})});
    auto struct_type = std::make_shared<DataTypeStruct>(DataTypes {int_type, map_type});
    auto structure =
            Field::create_field<TYPE_STRUCT>(Struct {Field::create_field<TYPE_INT>(1), map});

    for (const auto& [type, value] :
         std::vector<std::pair<DataTypePtr, Field>> {{array_type, array},
                                                     {nested_array_type, nested_array},
                                                     {map_type, map},
                                                     {struct_type, structure},
                                                     {make_nullable(array_type), Field()}}) {
        SCOPED_TRACE(type->get_name());
        auto literal = VLiteral::create_shared(type, value);
        EXPECT_EQ(literal->get_digest(42), 0);
    }
}

TEST(VLiteralDigestTest, AggStateDigestUsesSerializedColumn) {
    const auto version = BeExecVersionManager::get_newest_version();
    auto sum_type = std::make_shared<DataTypeAggState>(
            DataTypes {std::make_shared<DataTypeInt32>()}, false, "sum", version);
    auto sum_null = VLiteral::create_shared(make_nullable(sum_type), Field());
    EXPECT_EQ(sum_null->get_digest(42), 0);

    // HLL aggregate states serialize as ColumnString, unlike direct HLL literals.
    auto hll_type = DataTypeFactory::instance().create_data_type(TYPE_HLL, false);
    auto hll_state_type =
            std::make_shared<DataTypeAggState>(DataTypes {hll_type}, false, "hll_union", version);
    auto hll_state_null = VLiteral::create_shared(make_nullable(hll_state_type), Field());
    EXPECT_NE(hll_state_null->get_digest(42), 0);
    EXPECT_EQ(hll_state_null->get_digest(42), hll_state_null->get_digest(42));
}

TEST(VLiteralDigestTest, SupportedContainersKeepDistinctDigests) {
    auto int_type = std::make_shared<DataTypeInt32>();
    auto array_type = std::make_shared<DataTypeArray>(int_type);
    auto one = Field::create_field<TYPE_INT>(1);
    auto two = Field::create_field<TYPE_INT>(2);
    auto first = VLiteral::create_shared(array_type, Field::create_field<TYPE_ARRAY>(Array {one}));
    auto second = VLiteral::create_shared(array_type, Field::create_field<TYPE_ARRAY>(Array {two}));
    EXPECT_NE(first->get_digest(42), 0);
    EXPECT_NE(first->get_digest(42), second->get_digest(42));
    EXPECT_EQ(first->get_digest(42), first->get_digest(42));

    auto map_type = std::make_shared<DataTypeMap>(int_type, int_type);
    auto map = Field::create_field<TYPE_MAP>(Map {Field::create_field<TYPE_ARRAY>(Array {one}),
                                                  Field::create_field<TYPE_ARRAY>(Array {two})});
    auto map_literal = VLiteral::create_shared(map_type, map);
    EXPECT_NE(map_literal->get_digest(42), 0);
    auto struct_type = std::make_shared<DataTypeStruct>(DataTypes {int_type, map_type});
    auto struct_literal = VLiteral::create_shared(
            struct_type, Field::create_field<TYPE_STRUCT>(Struct {one, map}));
    EXPECT_NE(struct_literal->get_digest(42), 0);
}

TEST(VLiteralDigestTest, ScalarValuesKeepDistinctStableDigests) {
    auto int_type = std::make_shared<DataTypeInt32>();
    auto one = VLiteral::create_shared(int_type, Field::create_field<TYPE_INT>(1));
    auto two = VLiteral::create_shared(int_type, Field::create_field<TYPE_INT>(2));
    EXPECT_NE(one->get_digest(42), 0);
    EXPECT_EQ(one->get_digest(42), one->get_digest(42));
    EXPECT_NE(one->get_digest(42), two->get_digest(42));
    EXPECT_NE(one->get_digest(42), one->get_digest(43));

    auto string_type = std::make_shared<DataTypeString>();
    auto a = VLiteral::create_shared(string_type, Field::create_field<TYPE_STRING>(String("a")));
    auto b = VLiteral::create_shared(string_type, Field::create_field<TYPE_STRING>(String("b")));
    EXPECT_NE(a->get_digest(42), 0);
    EXPECT_NE(a->get_digest(42), b->get_digest(42));

    auto scalar_null = VLiteral::create_shared(
            DataTypeFactory::instance().create_data_type(TYPE_INT, true), Field());
    EXPECT_NE(scalar_null->get_digest(42), 0);
}

} // namespace doris
