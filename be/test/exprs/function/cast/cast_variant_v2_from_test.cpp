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

#include <array>

#include "common/exception.h"
#include "core/assert_cast.h"
#include "core/column/column_array.h"
#include "core/column/column_const.h"
#include "core/column/column_decimal.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_variant_v2.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_jsonb.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_time.h"
#include "core/data_type/data_type_variant.h"
#include "core/field.h"
#include "core/value/vdatetime_value.h"
#include "exprs/function/cast/cast_variant_v2.h"
#include "exprs/function_context.h"
#include "gtest/gtest.h"
#include "runtime/runtime_state.h"
#include "util/jsonb_utils.h"
#include "util/variant/variant_block_builder.h"

namespace doris::CastWrapper {
namespace {

struct CastResult {
    Status status;
    ColumnPtr column;
    ColumnPtr initial_result;
};

CastResult execute_from_variant(const ColumnPtr& source, const DataTypePtr& target_type,
                                const NullMap::value_type* null_map = nullptr,
                                std::string_view timezone = "UTC") {
    auto variant_type = std::make_shared<DataTypeVariant>();
    ColumnPtr initial_result = target_type->create_column();
    Block block {{source, variant_type, "source"}, {initial_result, target_type, "result"}};
    RuntimeState state;
    state.set_timezone(std::string(timezone));
    auto context = FunctionContext::create_context(&state, {}, {});
    Status status = create_cast_from_variant_v2_wrapper(target_type)(context.get(), block, {0}, 1,
                                                                     source->size(), null_map);
    return CastResult {.status = std::move(status),
                       .column = block.get_by_position(1).column,
                       .initial_result = std::move(initial_result)};
}

ColumnVariantV2::MutablePtr finish(VariantBlockBuilder* builder) {
    VariantEncodedBlock block = builder->finish_block();
    auto result = ColumnVariantV2::create();
    result->insert_encoded_block(block.view());
    return result;
}

ColumnVariantV2::MutablePtr mixed_scalar_values() {
    VariantBlockBuilder builder(VariantBlockBuilder::ReserveHint {.rows = 4});
    {
        auto row = builder.begin_row();
        row.add_string(StringRef("42"));
        row.finish();
    }
    {
        auto row = builder.begin_row();
        row.add_int(7);
        row.finish();
    }
    {
        auto row = builder.begin_row();
        auto object = row.start_object();
        object.add_key(StringRef("a"));
        row.add_int(1);
        object.finish();
        row.finish();
    }
    {
        auto row = builder.begin_row();
        row.add_null();
        row.finish();
    }
    return finish(&builder);
}

ColumnVariantV2::MutablePtr typed_ints() {
    auto values = ColumnInt32::create();
    values->get_data().push_back(42);
    values->get_data().push_back(0);
    values->get_data().push_back(-3);
    auto nulls = ColumnUInt8::create();
    nulls->get_data().push_back(0);
    nulls->get_data().push_back(1);
    nulls->get_data().push_back(0);
    return ColumnVariantV2::create_typed_from_cast(
            ColumnNullable::create(std::move(values), std::move(nulls)),
            std::make_shared<DataTypeInt32>());
}

const ColumnNullable& nullable_result(const ColumnPtr& column) {
    return assert_cast<const ColumnNullable&>(*column);
}

std::string jsonb_text(const IColumn& column, size_t row) {
    const StringRef document = column.get_data_at(row);
    return JsonbToJson::jsonb_to_json_string(document.data, document.size);
}

void expect_datetime(const ColumnDateTimeV2& values, size_t row, int year, int month, int day,
                     int hour, int minute, int second, int microsecond) {
    DateV2Value<DateTimeV2ValueType> value {values.get_data()[row]};
    EXPECT_EQ(value.year(), year) << row;
    EXPECT_EQ(value.month(), month) << row;
    EXPECT_EQ(value.day(), day) << row;
    EXPECT_EQ(value.hour(), hour) << row;
    EXPECT_EQ(value.minute(), minute) << row;
    EXPECT_EQ(value.second(), second) << row;
    EXPECT_EQ(value.microsecond(), microsecond) << row;
}

} // namespace

TEST(CastVariantV2FromTest, DormantFactoryExists) {
    EXPECT_TRUE(static_cast<bool>(
            create_cast_from_variant_v2_wrapper(std::make_shared<DataTypeInt32>())));
}

TEST(CastVariantV2FromTest, UnsupportedTargetReturnsAnErrorInsteadOfNullRows) {
    CastResult cast = execute_from_variant(typed_ints(), std::make_shared<DataTypeTimeV2>());
    EXPECT_TRUE(cast.status.is<ErrorCode::INVALID_ARGUMENT>()) << cast.status;
    EXPECT_EQ(cast.column.get(), cast.initial_result.get());
}

TEST(CastVariantV2FromTest, EncodedScalarGroupsRestoreInputOrderAndNullFailures) {
    ColumnPtr source = mixed_scalar_values();
    CastResult cast = execute_from_variant(source, std::make_shared<DataTypeInt32>());
    ASSERT_TRUE(cast.status.ok()) << cast.status;
    const auto& nullable = nullable_result(cast.column);
    const auto& values = assert_cast<const ColumnInt32&>(nullable.get_nested_column());
    ASSERT_EQ(values.size(), 4);
    EXPECT_EQ(nullable.get_null_map_data()[0], 0);
    EXPECT_EQ(values.get_data()[0], 42);
    EXPECT_EQ(nullable.get_null_map_data()[1], 0);
    EXPECT_EQ(values.get_data()[1], 7);
    EXPECT_EQ(nullable.get_null_map_data()[2], 1);
    EXPECT_EQ(nullable.get_null_map_data()[3], 1);
}

TEST(CastVariantV2FromTest, TypedScalarDelegatesToConcreteNonStrictCast) {
    ColumnPtr source = typed_ints();
    CastResult cast = execute_from_variant(source, std::make_shared<DataTypeInt64>());
    ASSERT_TRUE(cast.status.ok()) << cast.status;
    const auto& nullable = nullable_result(cast.column);
    const auto& values = assert_cast<const ColumnInt64&>(nullable.get_nested_column());
    EXPECT_EQ(values.get_data()[0], 42);
    EXPECT_EQ(nullable.get_null_map_data()[0], 0);
    EXPECT_EQ(nullable.get_null_map_data()[1], 1);
    EXPECT_EQ(values.get_data()[2], -3);
    EXPECT_EQ(nullable.get_null_map_data()[2], 0);
    EXPECT_TRUE(assert_cast<const ColumnVariantV2&>(*source).is_typed());
}

TEST(CastVariantV2FromTest, StringUsesConcreteScalarsAndExplicitDocumentRules) {
    VariantBlockBuilder builder(VariantBlockBuilder::ReserveHint {.rows = 5});
    {
        auto row = builder.begin_row();
        auto object = row.start_object();
        object.add_key(StringRef("a"));
        row.add_int(1);
        object.finish();
        row.finish();
    }
    {
        auto row = builder.begin_row();
        row.add_string(StringRef("raw"));
        row.finish();
    }
    {
        auto row = builder.begin_row();
        row.add_null();
        row.finish();
    }
    {
        auto row = builder.begin_row();
        row.add_date(0);
        row.finish();
    }
    {
        auto row = builder.begin_row();
        row.add_int(-9);
        row.finish();
    }
    ColumnPtr source = finish(&builder);
    CastResult cast = execute_from_variant(source, std::make_shared<DataTypeString>());
    ASSERT_TRUE(cast.status.ok()) << cast.status;
    const auto& nullable = nullable_result(cast.column);
    const auto& strings = assert_cast<const ColumnString&>(nullable.get_nested_column());
    ASSERT_EQ(strings.size(), 5);
    EXPECT_EQ(strings.get_data_at(0), StringRef(R"({"a":1})"));
    EXPECT_EQ(strings.get_data_at(1), StringRef("raw"));
    EXPECT_EQ(strings.get_data_at(2), StringRef("null"));
    EXPECT_EQ(strings.get_data_at(3), StringRef("1970-01-01"));
    EXPECT_EQ(strings.get_data_at(4), StringRef("-9"));
    EXPECT_FALSE(nullable.has_null());
}

TEST(CastVariantV2FromTest, TypedInnerNullStringifiesAsLiteralNull) {
    CastResult cast = execute_from_variant(typed_ints(), std::make_shared<DataTypeString>());
    ASSERT_TRUE(cast.status.ok()) << cast.status;
    const auto& nullable = nullable_result(cast.column);
    const auto& strings = assert_cast<const ColumnString&>(nullable.get_nested_column());
    EXPECT_EQ(strings.get_data_at(0), StringRef("42"));
    EXPECT_EQ(strings.get_data_at(1), StringRef("null"));
    EXPECT_EQ(strings.get_data_at(2), StringRef("-3"));
    EXPECT_FALSE(nullable.has_null());
}

TEST(CastVariantV2FromTest, EOnlyScalarStringRulesStayJsonQuoted) {
    const std::array<char, 2> binary {0, 1};
    std::array<uint8_t, 16> uuid {};
    for (size_t index = 0; index < uuid.size(); ++index) {
        uuid[index] = static_cast<uint8_t>(index);
    }
    VariantBlockBuilder builder(VariantBlockBuilder::ReserveHint {.rows = 3});
    {
        auto row = builder.begin_row();
        row.add_binary(StringRef(binary.data(), binary.size()));
        row.finish();
    }
    {
        auto row = builder.begin_row();
        row.add_time_ntz_micros(1);
        row.finish();
    }
    {
        auto row = builder.begin_row();
        row.add_uuid(uuid);
        row.finish();
    }
    CastResult cast = execute_from_variant(finish(&builder), std::make_shared<DataTypeString>());
    ASSERT_TRUE(cast.status.ok()) << cast.status;
    const auto& nullable = nullable_result(cast.column);
    const auto& strings = assert_cast<const ColumnString&>(nullable.get_nested_column());
    EXPECT_EQ(strings.get_data_at(0), StringRef(R"("AAE=")"));
    EXPECT_EQ(strings.get_data_at(1), StringRef(R"("00:00:00.000001")"));
    EXPECT_EQ(strings.get_data_at(2), StringRef(R"("00010203-0405-0607-0809-0a0b0c0d0e0f")"));
    EXPECT_FALSE(nullable.has_null());
}

TEST(CastVariantV2FromTest, JsonbPreservesObjectAndInnerVariantNull) {
    VariantBlockBuilder builder(VariantBlockBuilder::ReserveHint {.rows = 2});
    {
        auto row = builder.begin_row();
        auto object = row.start_object();
        object.add_key(StringRef("a"));
        row.add_int(1);
        object.finish();
        row.finish();
    }
    {
        auto row = builder.begin_row();
        row.add_null();
        row.finish();
    }
    CastResult cast = execute_from_variant(finish(&builder), std::make_shared<DataTypeJsonb>());
    ASSERT_TRUE(cast.status.ok()) << cast.status;
    const auto& nullable = nullable_result(cast.column);
    EXPECT_FALSE(nullable.has_null());
    EXPECT_EQ(jsonb_text(nullable.get_nested_column(), 0), R"({"a":1})");
    EXPECT_EQ(jsonb_text(nullable.get_nested_column(), 1), "null");
}

TEST(CastVariantV2FromTest, NanosFloorToMicrosAndUtcUsesSessionTimezone) {
    constexpr std::array<int64_t, 6> NANOS {-1, -999, -1000, 0, 999, 1000};
    VariantBlockBuilder builder(VariantBlockBuilder::ReserveHint {.rows = NANOS.size() + 1});
    for (int64_t nanos : NANOS) {
        auto row = builder.begin_row();
        row.add_timestamp_nanos(nanos, false);
        row.finish();
    }
    {
        auto row = builder.begin_row();
        row.add_timestamp_nanos(0, true);
        row.finish();
    }
    CastResult cast = execute_from_variant(
            finish(&builder), std::make_shared<DataTypeDateTimeV2>(6), nullptr, "Asia/Shanghai");
    ASSERT_TRUE(cast.status.ok()) << cast.status;
    const auto& nullable = nullable_result(cast.column);
    const auto& values = assert_cast<const ColumnDateTimeV2&>(nullable.get_nested_column());
    for (size_t row : {size_t {0}, size_t {1}, size_t {2}}) {
        expect_datetime(values, row, 1969, 12, 31, 23, 59, 59, 999999);
    }
    for (size_t row : {size_t {3}, size_t {4}}) {
        expect_datetime(values, row, 1970, 1, 1, 0, 0, 0, 0);
    }
    expect_datetime(values, 5, 1970, 1, 1, 0, 0, 0, 1);
    expect_datetime(values, 6, 1970, 1, 1, 8, 0, 0, 0);
}

TEST(CastVariantV2FromTest, ArrayLeafNullSemanticsAreTargetSpecific) {
    VariantBlockBuilder builder(VariantBlockBuilder::ReserveHint {.rows = 1});
    auto row = builder.begin_row();
    auto array = row.start_array();
    row.add_null();
    array.finish();
    row.finish();
    ColumnPtr source = finish(&builder);

    auto string_array = std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    CastResult strings = execute_from_variant(source, string_array);
    ASSERT_TRUE(strings.status.ok()) << strings.status;
    const auto& string_outer = nullable_result(strings.column);
    const auto& string_values = assert_cast<const ColumnArray&>(string_outer.get_nested_column());
    const auto& string_elements = assert_cast<const ColumnNullable&>(string_values.get_data());
    EXPECT_EQ(string_elements.get_null_map_data()[0], 0);
    EXPECT_EQ(assert_cast<const ColumnString&>(string_elements.get_nested_column()).get_data_at(0),
              StringRef("null"));

    auto jsonb_array = std::make_shared<DataTypeArray>(std::make_shared<DataTypeJsonb>());
    CastResult jsonb = execute_from_variant(source, jsonb_array);
    ASSERT_TRUE(jsonb.status.ok()) << jsonb.status;
    const auto& jsonb_outer = nullable_result(jsonb.column);
    const auto& jsonb_values = assert_cast<const ColumnArray&>(jsonb_outer.get_nested_column());
    const auto& jsonb_elements = assert_cast<const ColumnNullable&>(jsonb_values.get_data());
    EXPECT_EQ(jsonb_elements.get_null_map_data()[0], 0);
    EXPECT_EQ(jsonb_text(jsonb_elements.get_nested_column(), 0), "null");

    auto variant_array = std::make_shared<DataTypeArray>(std::make_shared<DataTypeVariant>());
    CastResult variants = execute_from_variant(source, variant_array);
    ASSERT_TRUE(variants.status.ok()) << variants.status;
    const auto& variant_outer = nullable_result(variants.column);
    const auto& variant_values = assert_cast<const ColumnArray&>(variant_outer.get_nested_column());
    const auto& variant_elements = assert_cast<const ColumnNullable&>(variant_values.get_data());
    EXPECT_EQ(variant_elements.get_null_map_data()[0], 0);
    EXPECT_TRUE(assert_cast<const ColumnVariantV2&>(variant_elements.get_nested_column())
                        .get_value_ref(0)
                        .is_null());

    auto int_array = std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>());
    CastResult integers = execute_from_variant(source, int_array);
    ASSERT_TRUE(integers.status.ok()) << integers.status;
    const auto& int_outer = nullable_result(integers.column);
    const auto& int_values = assert_cast<const ColumnArray&>(int_outer.get_nested_column());
    EXPECT_EQ(assert_cast<const ColumnNullable&>(int_values.get_data()).get_null_map_data()[0], 1);
}

TEST(CastVariantV2FromTest, NestedArrayRoundTripPreservesNullAndEmptyArray) {
    auto inner_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>());
    auto outer_type = std::make_shared<DataTypeArray>(inner_type);
    MutableColumnPtr source = outer_type->create_column();
    Array first_inner {Field::create_field<TYPE_INT>(1), Field::create_field<TYPE_NULL>(Null())};
    Array empty_inner;
    Array outer {Field::create_field<TYPE_ARRAY>(std::move(first_inner)),
                 Field::create_field<TYPE_ARRAY>(std::move(empty_inner))};
    source->insert(Field::create_field<TYPE_ARRAY>(std::move(outer)));

    auto variant_type = std::make_shared<DataTypeVariant>();
    Block to_block {{source->get_ptr(), outer_type, "source"},
                    {variant_type->create_column(), variant_type, "result"}};
    RuntimeState state;
    auto context = FunctionContext::create_context(&state, {}, {});
    ASSERT_TRUE(create_cast_to_variant_v2_wrapper(outer_type)(context.get(), to_block, {0}, 1, 1,
                                                              nullptr)
                        .ok());
    ColumnPtr encoded = to_block.get_by_position(1).column;
    VariantValueRef root = assert_cast<const ColumnVariantV2&>(*encoded).get_value_ref(0);
    ASSERT_EQ(root.basic_type(), VariantBasicType::ARRAY);
    ASSERT_EQ(root.num_elements(), 2);
    EXPECT_EQ(root.array_at(0).num_elements(), 2);
    EXPECT_TRUE(root.array_at(0).array_at(1).is_null());
    EXPECT_EQ(root.array_at(1).num_elements(), 0);

    CastResult round_trip = execute_from_variant(encoded, outer_type);
    ASSERT_TRUE(round_trip.status.ok()) << round_trip.status;
    const auto& top_nullable = nullable_result(round_trip.column);
    const auto& top = assert_cast<const ColumnArray&>(top_nullable.get_nested_column());
    ASSERT_EQ(top.size(), 1);
    ASSERT_EQ(top.size_at(0), 2);
    const auto& inner_nullable = assert_cast<const ColumnNullable&>(top.get_data());
    EXPECT_EQ(inner_nullable.get_null_map_data()[0], 0);
    EXPECT_EQ(inner_nullable.get_null_map_data()[1], 0);
    const auto& inner = assert_cast<const ColumnArray&>(inner_nullable.get_nested_column());
    EXPECT_EQ(inner.size_at(0), 2);
    EXPECT_EQ(inner.size_at(1), 0);
    const auto& values = assert_cast<const ColumnNullable&>(inner.get_data());
    EXPECT_EQ(values.get_null_map_data()[0], 0);
    EXPECT_EQ(values.get_null_map_data()[1], 1);
    EXPECT_EQ(assert_cast<const ColumnInt32&>(values.get_nested_column()).get_data()[0], 1);
}

TEST(CastVariantV2FromTest, DecimalScale38CastsAndScale39IsRejectedAtEncodingBoundary) {
    VariantBlockBuilder builder(VariantBlockBuilder::ReserveHint {.rows = 1});
    auto row = builder.begin_row();
    row.add_decimal(1, 38);
    row.finish();
    CastResult cast =
            execute_from_variant(finish(&builder), std::make_shared<DataTypeDecimal128>(38, 38));
    ASSERT_TRUE(cast.status.ok()) << cast.status;
    const auto& nullable = nullable_result(cast.column);
    EXPECT_EQ(nullable.get_null_map_data()[0], 0);
    EXPECT_EQ(assert_cast<const ColumnDecimal128V3&>(nullable.get_nested_column())
                      .get_data()[0]
                      .value,
              1);

    VariantBlockBuilder invalid_builder;
    auto invalid_row = invalid_builder.begin_row();
    EXPECT_THROW(invalid_row.add_decimal(1, 39), Exception);
}

TEST(CastVariantV2FromTest, OuterNullMapMasksValueAndConstContractIsExplicit) {
    ColumnPtr source = mixed_scalar_values();
    constexpr std::array<NullMap::value_type, 4> NULLS {1, 0, 0, 0};
    CastResult cast =
            execute_from_variant(source, std::make_shared<DataTypeString>(), NULLS.data());
    ASSERT_TRUE(cast.status.ok()) << cast.status;
    EXPECT_EQ(nullable_result(cast.column).get_null_map_data()[0], 1);

    ColumnPtr one = source->clone_resized(1);
    ColumnPtr constant = ColumnConst::create(IColumn::mutate(one), 3);
    CastResult const_result = execute_from_variant(constant, std::make_shared<DataTypeInt32>());
    EXPECT_TRUE(const_result.status.is<ErrorCode::INVALID_ARGUMENT>());
    EXPECT_EQ(const_result.column.get(), const_result.initial_result.get());
}

} // namespace doris::CastWrapper
