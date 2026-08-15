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

#include <initializer_list>
#include <memory>
#include <vector>

#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_variant_v2.h"
#include "storage/segment/variant/v2/variant_column_reader.h"

namespace doris::segment_v2::variant_v2 {
namespace {

ColumnUInt8::MutablePtr uint8_column(std::initializer_list<uint8_t> values) {
    auto column = ColumnUInt8::create();
    for (uint8_t value : values) {
        column->insert_value(value);
    }
    return column;
}

ColumnNullable::MutablePtr typed_int_batch(std::initializer_list<int32_t> values,
                                           std::initializer_list<uint8_t> outer_nulls) {
    DORIS_CHECK_EQ(values.size(), outer_nulls.size());
    auto ints = ColumnInt32::create();
    auto inner_nulls = ColumnUInt8::create();
    for (int32_t value : values) {
        ints->insert_value(value);
        inner_nulls->insert_value(0);
    }

    auto variant = ColumnVariantV2::create_typed(
            ColumnNullable::create(std::move(ints), std::move(inner_nulls)),
            std::make_shared<DataTypeInt32>());
    return ColumnNullable::create(std::move(variant), uint8_column(outer_nulls));
}

MutableColumnPtr nullable_variant_destination() {
    return make_nullable(std::make_shared<DataTypeVariantV2>(3, false))->create_column();
}

} // namespace

TEST(VariantV2ColumnReaderAdapterTest, FirstTypedBatchTransfersOwnershipAndLaterBatchAppends) {
    MutableColumnPtr destination = nullable_variant_destination();
    auto first = typed_int_batch({10, 20}, {0, 0});
    const auto* first_wrapper = first.get();
    const auto* first_values = &assert_cast<const ColumnVariantV2&>(first->get_nested_column());
    const auto* first_typed_column = &first_values->typed_column();

    auto status = append_assembled_variant(destination, std::move(first));
    ASSERT_TRUE(status.ok()) << status;

    EXPECT_EQ(destination.get(), first_wrapper);
    const auto& nullable = assert_cast<const ColumnNullable&>(*destination);
    const auto& variant = assert_cast<const ColumnVariantV2&>(nullable.get_nested_column());
    EXPECT_EQ(&variant, first_values);
    EXPECT_EQ(&variant.typed_column(), first_typed_column);
    ASSERT_TRUE(variant.is_typed());
    ASSERT_EQ(nullable.size(), 2);

    auto second = typed_int_batch({30}, {1});
    status = append_assembled_variant(destination, std::move(second));
    ASSERT_TRUE(status.ok()) << status;

    const auto& appended_nullable = assert_cast<const ColumnNullable&>(*destination);
    const auto& appended_variant =
            assert_cast<const ColumnVariantV2&>(appended_nullable.get_nested_column());
    EXPECT_EQ(&appended_variant, first_values);
    EXPECT_TRUE(appended_variant.is_typed());
    ASSERT_EQ(appended_nullable.size(), 3);
    EXPECT_FALSE(appended_nullable.is_null_at(0));
    EXPECT_FALSE(appended_nullable.is_null_at(1));
    EXPECT_TRUE(appended_nullable.is_null_at(2));

    const auto& typed_nullable =
            assert_cast<const ColumnNullable&>(appended_variant.typed_column());
    const auto& ints = assert_cast<const ColumnInt32&>(typed_nullable.get_nested_column());
    ASSERT_EQ(ints.size(), 3);
    EXPECT_EQ(ints.get_element(0), 10);
    EXPECT_EQ(ints.get_element(1), 20);
    EXPECT_EQ(ints.get_element(2), 30);
}

TEST(VariantV2ColumnReaderAdapterTest,
     NonNullableFirstBatchTransfersOwnershipAndLaterBatchAppends) {
    MutableColumnPtr destination = ColumnVariantV2::create();
    auto assembled = typed_int_batch({7, 8}, {0, 0});
    const auto* assembled_values =
            &assert_cast<const ColumnVariantV2&>(assembled->get_nested_column());
    const auto* assembled_typed_column = &assembled_values->typed_column();

    auto status = append_assembled_variant(destination, std::move(assembled));
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(destination.get(), assembled_values);
    const auto& first_variant = assert_cast<const ColumnVariantV2&>(*destination);
    EXPECT_TRUE(first_variant.is_typed());
    EXPECT_EQ(&first_variant.typed_column(), assembled_typed_column);

    auto second = typed_int_batch({9}, {0});
    status = append_assembled_variant(destination, std::move(second));
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(destination.get(), assembled_values);
    const auto& appended_variant = assert_cast<const ColumnVariantV2&>(*destination);
    ASSERT_TRUE(appended_variant.is_typed());
    const auto& typed_nullable =
            assert_cast<const ColumnNullable&>(appended_variant.typed_column());
    const auto& ints = assert_cast<const ColumnInt32&>(typed_nullable.get_nested_column());
    ASSERT_EQ(ints.size(), 3);
    EXPECT_EQ(ints.get_element(0), 7);
    EXPECT_EQ(ints.get_element(1), 8);
    EXPECT_EQ(ints.get_element(2), 9);
}

TEST(VariantV2ColumnReaderAdapterTest, RejectsNullAssembledOutput) {
    MutableColumnPtr destination = nullable_variant_destination();

    ColumnNullable::MutablePtr assembled;
    auto status = append_assembled_variant(destination, std::move(assembled));
    EXPECT_TRUE(status.is<ErrorCode::INVALID_ARGUMENT>()) << status;
}

TEST(VariantV2ColumnReaderAdapterTest, RejectsWrongDestinationAndNullableMismatch) {
    MutableColumnPtr string_destination = ColumnString::create();
    auto status = append_assembled_variant(string_destination, typed_int_batch({1}, {0}));
    EXPECT_TRUE(status.is<ErrorCode::INVALID_ARGUMENT>()) << status;

    MutableColumnPtr nullable_string_destination =
            make_nullable(std::make_shared<DataTypeString>())->create_column();
    status = append_assembled_variant(nullable_string_destination, typed_int_batch({1}, {0}));
    EXPECT_TRUE(status.is<ErrorCode::INVALID_ARGUMENT>()) << status;
}

TEST(VariantV2ColumnReaderAdapterTest, RejectsSqlNullForNonNullableDestination) {
    MutableColumnPtr destination = ColumnVariantV2::create();
    const auto* destination_before = destination.get();
    auto status = append_assembled_variant(destination, typed_int_batch({1, 2}, {0, 1}));
    EXPECT_TRUE(status.is<ErrorCode::CORRUPTION>()) << status;
    EXPECT_EQ(destination.get(), destination_before);
    EXPECT_TRUE(destination->empty());
}

TEST(VariantV2ColumnReaderAdapterTest, DetectsNullableAndNonNullableDestinations) {
    auto nullable_variant = nullable_variant_destination();
    auto variant = ColumnVariantV2::create();
    auto nullable_string = make_nullable(std::make_shared<DataTypeString>())->create_column();
    auto string = ColumnString::create();

    EXPECT_NE(try_get_variant_v2_destination(*nullable_variant), nullptr);
    EXPECT_NE(try_get_variant_v2_destination(*variant), nullptr);
    EXPECT_EQ(try_get_variant_v2_destination(*nullable_string), nullptr);
    EXPECT_EQ(try_get_variant_v2_destination(*string), nullptr);
}

} // namespace doris::segment_v2::variant_v2
