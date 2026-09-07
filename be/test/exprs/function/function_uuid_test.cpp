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

#include <set>

#include "exprs/function/function_test_util.h"
#include "exprs/function/uuid.cpp"

namespace doris {

using namespace ut_type;

TEST(function_uuid_test, function_is_uuid_test) {
    std::string func_name = "is_uuid";
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};
    DataSet data_set = {
            {{STRING("6ccd780c-baba-1026-9564-5b8c656024db")}, BOOLEAN(1)},
            {{STRING("6ccd780c-baba-1026-9564-5b8c656024dbaaaa")}, BOOLEAN(0)},
            {{STRING("6ccd780c-baba-1026-9564-5b8c656024gg")}, BOOLEAN(0)},
            {{STRING("6ccd780-cbaba-1026-9564-5b8c656024db")}, BOOLEAN(0)},
            {{STRING("6ccd780-cbaba-1026-95645-b8c656024db")}, BOOLEAN(0)},
            {{STRING("6ccd780-cbaba-1026-95645-b8c65602")}, BOOLEAN(0)},
            {{STRING("{6ccd780c-baba-1026-9564-5b8c656024db}")}, BOOLEAN(1)},
            {{STRING("{6ccd780c-baba-1026-95645b8c656024db}")}, BOOLEAN(0)},
            {{STRING("{6ccd780c-baba-1026-95645-b8c656024db}")}, BOOLEAN(0)},
            {{STRING("6ccd780c-baba-1026-95645-b8c656024db}")}, BOOLEAN(0)},
            {{STRING("6ccd780cbaba102695645b8c656024db")}, BOOLEAN(1)},
            {{STRING("6ccd780cbaba102695645b8c656024dz")}, BOOLEAN(0)},
            {{STRING("6ccd780cbaba102")}, BOOLEAN(0)},
            {{STRING("{6ccd780cbaba102}")}, BOOLEAN(0)},
            {{Null()}, Null()},
    };

    check_function_all_arg_comb<DataTypeBool, true>(func_name, input_types, data_set);
}

TEST(function_uuid_test, generate_uuid_v4) {
    Block block;
    block.insert({ColumnUUID::create(), std::make_shared<DataTypeUUID>(), "result"});

    FunctionGenerateUUID<false> function;
    ASSERT_TRUE(function.execute_impl(nullptr, block, {}, 0, 128).ok());

    const auto& data = assert_cast<const ColumnUUID&>(*block.get_by_position(0).column).get_data();
    std::set<UUIDValueType> unique_values;
    for (const auto value : data) {
        EXPECT_EQ(UUIDValue::version(value), 4);
        EXPECT_EQ(static_cast<uint8_t>((value >> 62) & 0x03), 2);
        unique_values.insert(value);
    }
    EXPECT_EQ(unique_values.size(), data.size());
}

TEST(function_uuid_test, generate_uuid_v7) {
    Block block;
    block.insert({ColumnUUID::create(), std::make_shared<DataTypeUUID>(), "result"});

    FunctionGenerateUUID<true> function;
    ASSERT_TRUE(function.execute_impl(nullptr, block, {}, 0, 128).ok());

    const auto& data = assert_cast<const ColumnUUID&>(*block.get_by_position(0).column).get_data();
    for (size_t i = 0; i < data.size(); ++i) {
        EXPECT_EQ(UUIDValue::version(data[i]), 7);
        EXPECT_EQ(static_cast<uint8_t>((data[i] >> 62) & 0x03), 2);
        if (i > 0) {
            EXPECT_LT(data[i - 1], data[i]);
        }
    }
}

TEST(function_uuid_test, uuid_version) {
    UUIDValueType v4;
    UUIDValueType v7;
    ASSERT_TRUE(UUIDValue::from_string(v4, "550e8400-e29b-41d4-a716-446655440000"));
    ASSERT_TRUE(UUIDValue::from_string(v7, "018f4c2a-4b5c-7def-8123-456789abcdef"));

    auto input = ColumnUUID::create();
    input->insert_value(v4);
    input->insert_value(v7);
    Block block;
    block.insert({std::move(input), std::make_shared<DataTypeUUID>(), "input"});
    block.insert({ColumnInt8::create(), std::make_shared<DataTypeInt8>(), "result"});

    FunctionUUIDVersion function;
    ASSERT_TRUE(function.execute_impl(nullptr, block, {0}, 1, 2).ok());
    const auto& result =
            assert_cast<const ColumnInt8&>(*block.get_by_position(1).column).get_data();
    ASSERT_EQ(result.size(), 2);
    EXPECT_EQ(result[0], 4);
    EXPECT_EQ(result[1], 7);
}

} // namespace doris
