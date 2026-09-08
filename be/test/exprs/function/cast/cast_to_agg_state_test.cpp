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

#include "agent/be_exec_version_manager.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_agg_state.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_variant.h"
#include "exprs/function/cast/cast_base.h"

namespace doris {
namespace {

DataTypePtr create_state_type() {
    return std::make_shared<DataTypeAggState>(DataTypes {std::make_shared<DataTypeString>()}, true,
                                              "group_concat",
                                              BeExecVersionManager::get_newest_version());
}

} // namespace

TEST(CastToAggStateTest, RejectOrdinaryInput) {
    auto state_type = create_state_type();
    DataTypes input_types {std::make_shared<DataTypeString>(), std::make_shared<DataTypeVariant>(),
                           std::make_shared<DataTypeInt32>()};
    for (const auto& input_type : input_types) {
        for (bool nullable : {false, true}) {
            DataTypePtr from_type = nullable ? make_nullable(input_type) : input_type;
            DataTypePtr to_type = nullable ? make_nullable(state_type) : state_type;
            auto input = from_type->create_column();
            input->insert_default();
            Block block {{std::move(input), from_type, "input"}, {nullptr, to_type, "result"}};
            auto wrapper = CastWrapper::prepare_unpack_dictionaries(nullptr, from_type, to_type);
            auto status = wrapper(nullptr, block, {0}, 1, 1, nullptr);
            EXPECT_FALSE(status.ok());
            EXPECT_NE(status.to_string().find("Cast to AggState only supports AggState input"),
                      std::string::npos);
        }
    }
}

TEST(CastToAggStateTest, PreserveIdenticalState) {
    auto state_type = create_state_type();
    auto function = assert_cast<const DataTypeAggState*>(state_type.get())->get_nested_function();
    Arena arena;
    auto* place = reinterpret_cast<AggregateDataPtr>(
            arena.aligned_alloc(function->size_of_data(), function->align_of_data()));
    function->create(place);
    auto input = state_type->create_column();
    function->serialize_without_key_to_column(place, *input);
    function->destroy(place);

    Block block {{input->get_ptr(), state_type, "input"}, {nullptr, state_type, "result"}};
    auto wrapper = CastWrapper::prepare_unpack_dictionaries(nullptr, state_type, state_type);
    ASSERT_TRUE(wrapper(nullptr, block, {0}, 1, 1, nullptr).ok());
    EXPECT_EQ(block.get_by_position(1).column, block.get_by_position(0).column);
}

TEST(CastToAggStateTest, PreserveNullLiteral) {
    auto null_type = std::make_shared<DataTypeUInt8>();
    null_type->set_null_literal(true);
    auto from_type = make_nullable(null_type);
    auto to_type = make_nullable(create_state_type());
    Block block {{nullptr, to_type, "result"}};
    auto wrapper = CastWrapper::prepare_unpack_dictionaries(nullptr, from_type, to_type);
    ASSERT_TRUE(wrapper(nullptr, block, {}, 0, 1, nullptr).ok());
    EXPECT_TRUE(block.get_by_position(0).column->is_null_at(0));
}

} // namespace doris
