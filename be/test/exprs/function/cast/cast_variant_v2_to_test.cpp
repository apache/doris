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

#include "core/assert_cast.h"
#include "core/column/column_array.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_variant_v2.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_jsonb.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nothing.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_time.h"
#include "core/data_type/data_type_variant.h"
#include "core/field.h"
#include "exprs/function/cast/cast_variant_v2.h"
#include "exprs/function_context.h"
#include "gtest/gtest.h"
#include "runtime/runtime_state.h"
#include "util/jsonb_writer.h"
#include "util/variant/variant_block_builder.h"

namespace doris::CastWrapper {
namespace {

struct CastResult {
    Status status;
    ColumnPtr column;
    ColumnPtr initial_result;
};

CastResult execute_to_variant(const ColumnPtr& source, const DataTypePtr& source_type,
                              const NullMap::value_type* null_map = nullptr) {
    auto result_type = std::make_shared<DataTypeVariant>();
    ColumnPtr initial_result = result_type->create_column();
    Block block {{source, source_type, "source"}, {initial_result, result_type, "result"}};
    RuntimeState state;
    auto context = FunctionContext::create_context(&state, {}, {});
    Status status = create_cast_to_variant_v2_wrapper(source_type)(context.get(), block, {0}, 1,
                                                                   source->size(), null_map);
    return CastResult {.status = std::move(status),
                       .column = block.get_by_position(1).column,
                       .initial_result = std::move(initial_result)};
}

ColumnVariantV2::MutablePtr one_encoded_int(int64_t value) {
    VariantBlockBuilder builder(VariantBlockBuilder::ReserveHint {.rows = 1});
    auto row = builder.begin_row();
    row.add_int(value);
    row.finish();
    VariantEncodedBlock block = builder.finish_block();
    auto result = ColumnVariantV2::create();
    result->insert_encoded_block(block.view());
    return result;
}

} // namespace

TEST(CastVariantV2ToTest, DormantFactoryExists) {
    EXPECT_TRUE(static_cast<bool>(
            create_cast_to_variant_v2_wrapper(std::make_shared<DataTypeInt32>())));
}

TEST(CastVariantV2ToTest, UnsupportedSourceReturnsAnError) {
    auto type = std::make_shared<DataTypeTimeV2>();
    MutableColumnPtr source = type->create_column();
    source->insert_default();
    CastResult cast = execute_to_variant(source->get_ptr(), type);
    EXPECT_TRUE(cast.status.is<ErrorCode::INVALID_ARGUMENT>()) << cast.status;
    EXPECT_EQ(cast.column.get(), cast.initial_result.get());
}

TEST(CastVariantV2ToTest, ScalarCreatesTypedStateAndCopiesOuterNullMap) {
    auto source = ColumnInt32::create();
    source->insert_value(7);
    source->insert_value(99);
    const IColumn* source_identity = source.get();
    constexpr std::array<NullMap::value_type, 2> NULLS {0, 1};

    CastResult cast =
            execute_to_variant(source->get_ptr(), std::make_shared<DataTypeInt32>(), NULLS.data());
    ASSERT_TRUE(cast.status.ok()) << cast.status;
    const auto& variant = assert_cast<const ColumnVariantV2&>(*cast.column);
    ASSERT_TRUE(variant.is_typed());
    EXPECT_EQ(variant.typed_type()->get_primitive_type(), TYPE_INT);
    const auto& nullable = assert_cast<const ColumnNullable&>(variant.typed_column());
    EXPECT_EQ(nullable.get_null_map_data()[0], 0);
    EXPECT_EQ(nullable.get_null_map_data()[1], 1);
    EXPECT_NE(&nullable.get_nested_column(), source_identity);
    EXPECT_EQ(source->get_data()[0], 7);
    EXPECT_EQ(source->get_data()[1], 99);
}

TEST(CastVariantV2ToTest, StringIsAStringScalarAndIsNotParsed) {
    auto source = ColumnString::create();
    source->insert_data(R"({"a":1})", 7);
    CastResult cast = execute_to_variant(source->get_ptr(), std::make_shared<DataTypeString>());
    ASSERT_TRUE(cast.status.ok()) << cast.status;
    auto encoded = IColumn::mutate(cast.column);
    auto& variant = assert_cast<ColumnVariantV2&>(*encoded);
    variant.ensure_encoded();
    ASSERT_EQ(variant.get_value_ref(0).basic_type(), VariantBasicType::SHORT_STRING);
    EXPECT_EQ(variant.get_value_ref(0).get_string(), StringRef(R"({"a":1})"));
}

TEST(CastVariantV2ToTest, JsonbObjectUsesDocumentTranscode) {
    JsonbWriter writer;
    ASSERT_TRUE(writer.writeStartObject());
    ASSERT_TRUE(writer.writeKey("a", 1));
    ASSERT_TRUE(writer.writeInt(1));
    ASSERT_TRUE(writer.writeEndObject());
    auto source = ColumnString::create();
    source->insert_data(writer.getOutput()->getBuffer(), writer.getOutput()->getSize());

    CastResult cast = execute_to_variant(source->get_ptr(), std::make_shared<DataTypeJsonb>());
    ASSERT_TRUE(cast.status.ok()) << cast.status;
    const auto& variant = assert_cast<const ColumnVariantV2&>(*cast.column);
    VariantValueRef value = variant.get_value_ref(0);
    ASSERT_EQ(value.basic_type(), VariantBasicType::OBJECT);
    VariantValueRef field;
    ASSERT_TRUE(value.object_find(StringRef("a"), &field));
    EXPECT_EQ(field.get_int(), 1);
}

TEST(CastVariantV2ToTest, ArrayRecursesAndPreservesElementNull) {
    auto nested = ColumnInt32::create();
    nested->insert_value(1);
    nested->insert_value(0);
    nested->insert_value(3);
    auto element_nulls = ColumnUInt8::create();
    element_nulls->get_data().push_back(0);
    element_nulls->get_data().push_back(1);
    element_nulls->get_data().push_back(0);
    auto elements = ColumnNullable::create(std::move(nested), std::move(element_nulls));
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->get_data().push_back(3);
    auto source = ColumnArray::create(std::move(elements), std::move(offsets));
    auto type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>());

    CastResult cast = execute_to_variant(source->get_ptr(), type);
    ASSERT_TRUE(cast.status.ok()) << cast.status;
    VariantValueRef array = assert_cast<const ColumnVariantV2&>(*cast.column).get_value_ref(0);
    ASSERT_EQ(array.basic_type(), VariantBasicType::ARRAY);
    ASSERT_EQ(array.num_elements(), 3);
    EXPECT_EQ(array.array_at(0).get_int(), 1);
    EXPECT_TRUE(array.array_at(1).is_null());
    EXPECT_EQ(array.array_at(2).get_int(), 3);
}

TEST(CastVariantV2ToTest, EmptyArrayOfNothingIsLegal) {
    auto type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeNothing>());
    MutableColumnPtr source = type->create_column();
    source->insert_many_defaults(2);
    CastResult cast = execute_to_variant(source->get_ptr(), type);
    ASSERT_TRUE(cast.status.ok()) << cast.status;
    const auto& variant = assert_cast<const ColumnVariantV2&>(*cast.column);
    ASSERT_EQ(variant.size(), 2);
    EXPECT_EQ(variant.get_value_ref(0).num_elements(), 0);
    EXPECT_EQ(variant.get_value_ref(1).num_elements(), 0);
}

TEST(CastVariantV2ToTest, NullArrayRowDoesNotParseHiddenJsonbElements) {
    auto jsonb = ColumnString::create();
    jsonb->insert_data("not-jsonb", 9);
    auto element_nulls = ColumnUInt8::create(1, 0);
    auto elements = ColumnNullable::create(std::move(jsonb), std::move(element_nulls));
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->get_data().push_back(1);
    auto source = ColumnArray::create(std::move(elements), std::move(offsets));
    auto type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeJsonb>());
    constexpr std::array<NullMap::value_type, 1> NULLS {1};

    CastResult cast = execute_to_variant(source->get_ptr(), type, NULLS.data());
    ASSERT_TRUE(cast.status.ok()) << cast.status;
    EXPECT_TRUE(assert_cast<const ColumnVariantV2&>(*cast.column).get_value_ref(0).is_null());
}

TEST(CastVariantV2ToTest, NullArrayRowDoesNotEncodeHiddenTypedVariantValue) {
    auto invalid_date = ColumnDateV2::create();
    DateV2Value<DateV2ValueType> invalid_date_value;
    ASSERT_FALSE(invalid_date_value.is_valid_date());
    invalid_date->insert_value(invalid_date_value);
    auto internal_nulls = ColumnUInt8::create(1, 0);
    auto typed_variant = ColumnVariantV2::create_typed_from_cast(
            ColumnNullable::create(std::move(invalid_date), std::move(internal_nulls)),
            std::make_shared<DataTypeDateV2>());
    const ColumnVariantV2* const typed_identity = typed_variant.get();
    auto element_nulls = ColumnUInt8::create(1, 0);
    auto elements = ColumnNullable::create(std::move(typed_variant), std::move(element_nulls));
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->get_data().push_back(1);
    auto source = ColumnArray::create(std::move(elements), std::move(offsets));
    auto type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeVariant>());
    constexpr std::array<NullMap::value_type, 1> NULLS {1};

    CastResult unmasked = execute_to_variant(source->get_ptr(), type);
    EXPECT_TRUE(unmasked.status.is<ErrorCode::INVALID_ARGUMENT>());
    EXPECT_EQ(unmasked.column.get(), unmasked.initial_result.get());
    EXPECT_TRUE(typed_identity->is_typed());

    CastResult cast = execute_to_variant(source->get_ptr(), type, NULLS.data());
    ASSERT_TRUE(cast.status.ok()) << cast.status;
    EXPECT_TRUE(assert_cast<const ColumnVariantV2&>(*cast.column).get_value_ref(0).is_null());
    EXPECT_TRUE(typed_identity->is_typed());
}

TEST(CastVariantV2ToTest, NothingSourceProducesInternalNullRows) {
    auto type = std::make_shared<DataTypeNothing>();
    MutableColumnPtr source = type->create_column();
    source->insert_many_defaults(2);
    constexpr std::array<NullMap::value_type, 2> NULLS {1, 1};
    CastResult cast = execute_to_variant(source->get_ptr(), type, NULLS.data());
    ASSERT_TRUE(cast.status.ok()) << cast.status;
    const auto& variant = assert_cast<const ColumnVariantV2&>(*cast.column);
    EXPECT_TRUE(variant.get_value_ref(0).is_null());
    EXPECT_TRUE(variant.get_value_ref(1).is_null());
}

TEST(CastVariantV2ToTest, VariantIdentityKeepsColumnPointer) {
    ColumnPtr source = one_encoded_int(5);
    CastResult cast = execute_to_variant(source, std::make_shared<DataTypeVariant>());
    ASSERT_TRUE(cast.status.ok()) << cast.status;
    EXPECT_EQ(cast.column.get(), source.get());
}

TEST(CastVariantV2ToTest, UnsupportedMapAndConstInputLeaveResultUntouched) {
    auto map_type = std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(),
                                                  std::make_shared<DataTypeInt32>());
    MutableColumnPtr map = map_type->create_column();
    map->insert_default();
    CastResult unsupported = execute_to_variant(map->get_ptr(), map_type);
    EXPECT_TRUE(unsupported.status.is<ErrorCode::INVALID_ARGUMENT>());
    EXPECT_EQ(unsupported.column.get(), unsupported.initial_result.get());

    auto value = ColumnInt32::create();
    value->insert_value(1);
    ColumnPtr constant = ColumnConst::create(std::move(value), 3);
    CastResult const_result = execute_to_variant(constant, std::make_shared<DataTypeInt32>());
    EXPECT_TRUE(const_result.status.is<ErrorCode::INVALID_ARGUMENT>());
    EXPECT_EQ(const_result.column.get(), const_result.initial_result.get());
}

} // namespace doris::CastWrapper
