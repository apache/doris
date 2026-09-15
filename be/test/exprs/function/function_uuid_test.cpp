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

TEST(function_uuid_test, parse_uuid_fallback_nullable_and_const) {
    const auto uuid_type = std::make_shared<DataTypeUUID>();
    const auto string_type = make_nullable(std::make_shared<DataTypeString>());
    UUIDValueType normal =
            UUIDValue::from_big_endian(reinterpret_cast<const uint8_t*>("0123456789abcdef"));
    for (bool constant : {false, true}) {
        auto strings = ColumnString::create();
        strings->insert_data("bad", 3);
        strings->insert_data("00112233445566778899AABBCCDDEEFF", 32);
        strings->insert_default();
        auto source_nulls = ColumnUInt8::create(3, 0);
        source_nulls->get_data()[2] = 1;
        ColumnPtr input = ColumnNullable::create(std::move(strings), std::move(source_nulls));
        if (constant) {
            input = ColumnConst::create(input->clone_resized(1), 3);
        }
        auto fallback = ColumnUUID::create(3, normal);
        auto fallback_nulls = ColumnUInt8::create(3, 0);
        fallback_nulls->get_data()[1] = 1;
        Block block;
        block.insert({input, string_type, "input"});
        block.insert({ColumnNullable::create(std::move(fallback), std::move(fallback_nulls)),
                      make_nullable(uuid_type), "fallback"});
        block.insert({nullptr, make_nullable(uuid_type), "result"});
        FunctionToUUID<UUIDParseMode::Default> function;
        ASSERT_TRUE(function.execute_impl(nullptr, block, {0, 1}, 2, 3).ok());
        const auto& result = assert_cast<const ColumnNullable&>(*block.get_by_position(2).column);
        const auto& values = assert_cast<const ColumnUUID&>(result.get_nested_column());
        EXPECT_EQ(values.get_element(0), normal);
        EXPECT_EQ(values.get_element(2), normal);
        EXPECT_EQ(result.is_null_at(1), constant);
        if (!constant) {
            EXPECT_EQ(UUIDValue::to_string(values.get_element(1)),
                      "00112233-4455-6677-8899-aabbccddeeff");
        }
        FunctionToUUID<UUIDParseMode::Zero> zero;
        ASSERT_TRUE(zero.execute_impl(nullptr, block, {0}, 2, 3).ok());
        const auto& zeros = assert_cast<const ColumnNullable&>(*block.get_by_position(2).column);
        EXPECT_FALSE(zeros.is_null_at(0));
        EXPECT_EQ(zeros.is_null_at(2), !constant);
        FunctionToUUID<UUIDParseMode::Null> null_function;
        ASSERT_TRUE(null_function.execute_impl(nullptr, block, {0}, 2, 3).ok());
        const auto& null_result =
                assert_cast<const ColumnNullable&>(*block.get_by_position(2).column);
        EXPECT_TRUE(null_result.is_null_at(0));
        EXPECT_TRUE(null_result.is_null_at(2));
    }
}

TEST(function_uuid_test, timestamp_counter_and_overflow) {
    UUIDTimestampCounter counter;
    auto first = counter.next(1234, 1710000000123);
    auto second = counter.next(1, 1710000000123);
    EXPECT_LT(first, second);
    EXPECT_EQ(static_cast<uint64_t>(first >> 80), 1710000000123);
    EXPECT_EQ(UUIDValue::version(first), 7);
    EXPECT_EQ(static_cast<uint64_t>((first >> 62) & 3), 2);
    EXPECT_EQ(static_cast<uint64_t>(counter.next(99, 1) >> 80), 1);
    counter.counter = (uint64_t {1} << 42) - 1;
    auto overflow = counter.next(99, 1);
    EXPECT_EQ(static_cast<uint64_t>(overflow >> 80), 2);
    auto after_overflow = counter.next(1, 1);
    EXPECT_EQ(static_cast<uint64_t>(after_overflow >> 80), 2);
    EXPECT_GT(after_overflow, overflow);
    EXPECT_EQ(static_cast<uint64_t>(counter.next(99, 10) >> 80), 10);
    EXPECT_EQ(static_cast<uint64_t>(counter.next(99, 1) >> 80), 1);
}

TEST(function_uuid_test, datetime_roundtrip_const_rows_and_range) {
    RuntimeState state;
    state.set_timezone("UTC");
    auto context = FunctionContext::create_context(&state, {}, {});
    DateV2Value<DateTimeV2ValueType> value;
    value.unchecked_set_time(2026, 9, 10, 12, 34, 56, 789000);
    auto dates = ColumnDateTimeV2::create(1, value);
    Block block;
    block.insert({ColumnConst::create(std::move(dates), 64),
                  std::make_shared<DataTypeDateTimeV2>(3), "date"});
    block.insert({nullptr, make_nullable(std::make_shared<DataTypeUUID>()), "uuid"});
    FunctionDateTimeToUUIDv7 generate;
    ASSERT_TRUE(generate.execute_impl(context.get(), block, {0}, 1, 64).ok());
    const auto& generated = assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
    const auto& uuids = assert_cast<const ColumnUUID&>(generated.get_nested_column());
    std::set<UUIDValueType> unique(uuids.get_data().begin(), uuids.get_data().end());
    EXPECT_EQ(unique.size(), 64);
    block.insert({uuids.get_ptr(), std::make_shared<DataTypeUUID>(), "uuid_values"});
    block.insert({nullptr, make_nullable(std::make_shared<DataTypeDateTimeV2>(3)), "decoded"});
    FunctionUUIDv7ToDateTime decode;
    ASSERT_TRUE(decode.execute_impl(context.get(), block, {2}, 3, 64).ok());
    const auto& decoded = assert_cast<const ColumnNullable&>(*block.get_by_position(3).column);
    const auto& decoded_dates = assert_cast<const ColumnDateTimeV2&>(decoded.get_nested_column());
    for (size_t i = 0; i < 64; ++i) {
        EXPECT_FALSE(decoded.is_null_at(i));
        EXPECT_EQ(decoded_dates.get_element(i), value);
    }
    auto boundary = ColumnUUID::create();
    boundary->insert_value((UUIDValueType {0xffffffffffff} << 80) | (UUIDValueType {7} << 76));
    block.get_by_position(2).column = std::move(boundary);
    ASSERT_TRUE(decode.execute_impl(context.get(), block, {2}, 3, 1).ok());
    EXPECT_TRUE(block.get_by_position(3).column->is_null_at(0));
}

} // namespace doris
