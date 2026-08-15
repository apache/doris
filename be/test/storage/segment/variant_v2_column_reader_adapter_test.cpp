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
#include "core/value/variant/variant_parquet_encoding.h"
#include "exprs/function/parse/variant_string_parse.h"
#include "storage/segment/variant/v2/variant_assembler.h"
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

struct JsonWriter {
    void write(const char* data, size_t size) { value.append(data, size); }

    std::string value;
};

std::string json_at(const ColumnVariantV2& column, size_t row) {
    auto encoded = column.materialize_encoded_range(row, 1);
    JsonWriter writer;
    to_json(encoded->get_value_ref(0), writer);
    return writer.value;
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

// NOLINTNEXTLINE(readability-function-cognitive-complexity) -- GTest macros inflate the ownership matrix.
TEST(VariantV2ColumnReaderAdapterTest,
     WholeRootShreddedBatchesOwnScratchAndAppendWithoutMaterializing) {
    VariantAssemblerOptions options;
    options.materialized_paths = {
            {.path = PathInData("a"), .type = std::make_shared<DataTypeInt32>()},
    };
    auto assembler_result = VariantAssembler::create(std::move(options));
    ASSERT_TRUE(assembler_result.has_value()) << assembler_result.error();
    auto assembler = std::move(assembler_result).value();

    const auto assemble = [&](std::initializer_list<Int32> input,
                              ColumnNullable::MutablePtr* output) -> Status {
        auto materialized = ColumnInt32::create();
        for (Int32 value : input) {
            materialized->insert_value(value);
        }
        const IColumn* materialized_ptr = materialized.get();
        VariantAssemblerBatch batch;
        batch.num_rows = input.size();
        batch.materialized_columns = {&materialized_ptr, 1};
        RETURN_IF_ERROR(assembler->assemble(batch, output));
        if (!assert_cast<const ColumnVariantV2&>((*output)->get_nested_column()).is_shredded()) {
            return Status::InternalError("Whole-root Variant assembler did not publish S-state");
        }
        materialized->clear();
        return Status::OK();
    };

    MutableColumnPtr destination = nullable_variant_destination();
    ColumnNullable::MutablePtr first;
    ASSERT_TRUE(assemble({10, 20}, &first).ok());
    const auto* first_wrapper = first.get();
    ASSERT_TRUE(append_assembled_variant(destination, std::move(first)).ok());
    EXPECT_EQ(destination.get(), first_wrapper);

    ColumnNullable::MutablePtr second;
    ASSERT_TRUE(assemble({30}, &second).ok());
    ASSERT_TRUE(append_assembled_variant(destination, std::move(second)).ok());
    EXPECT_EQ(destination.get(), first_wrapper);

    const auto& nullable = assert_cast<const ColumnNullable&>(*destination);
    const auto& variant = assert_cast<const ColumnVariantV2&>(nullable.get_nested_column());
    ASSERT_TRUE(variant.is_shredded());
    ASSERT_EQ(variant.shredded_field_count(), 1);
    EXPECT_EQ(variant.shredded_field_path(0).get_path(), "a");
    EXPECT_EQ(variant.shredded_field_presence(0).get_data(), (PaddedPODArray<uint8_t> {1, 1, 1}));
    const auto& child = variant.shredded_field_values(0);
    ASSERT_TRUE(child.is_typed());
    const auto& typed_nullable = assert_cast<const ColumnNullable&>(child.typed_column());
    const auto& ints = assert_cast<const ColumnInt32&>(typed_nullable.get_nested_column());
    ASSERT_EQ(ints.size(), 3);
    EXPECT_EQ(ints.get_element(0), 10);
    EXPECT_EQ(ints.get_element(1), 20);
    EXPECT_EQ(ints.get_element(2), 30);
}

TEST(VariantV2ColumnReaderAdapterTest,
     FirstAllMissingShreddedBatchKeepsLayoutForLaterPresentValue) {
    VariantAssemblerOptions options;
    options.materialized_paths = {
            {.path = PathInData("a"), .type = make_nullable(std::make_shared<DataTypeInt32>())},
    };
    auto assembler_result = VariantAssembler::create(std::move(options));
    ASSERT_TRUE(assembler_result.has_value()) << assembler_result.error();
    auto assembler = std::move(assembler_result).value();

    const auto assemble = [&](std::initializer_list<Int32> input,
                              std::initializer_list<uint8_t> nulls) {
        DORIS_CHECK_EQ(input.size(), nulls.size());
        auto values = ColumnInt32::create();
        for (Int32 value : input) {
            values->insert_value(value);
        }
        auto materialized = ColumnNullable::create(std::move(values), uint8_column(nulls));
        const IColumn* materialized_ptr = materialized.get();
        VariantAssemblerBatch batch;
        batch.num_rows = input.size();
        batch.materialized_columns = {&materialized_ptr, 1};
        ColumnNullable::MutablePtr output;
        const Status status = assembler->assemble(batch, &output);
        EXPECT_TRUE(status.ok()) << status;
        return output;
    };

    MutableColumnPtr destination = nullable_variant_destination();
    auto first = assemble({0, 0}, {1, 1});
    const auto* first_wrapper = first.get();
    const auto& first_values = assert_cast<const ColumnVariantV2&>(first->get_nested_column());
    ASSERT_TRUE(first_values.is_shredded());
    ASSERT_EQ(first_values.shredded_field_count(), 1);
    EXPECT_EQ(first_values.shredded_field_path(0).get_path(), "a");
    EXPECT_EQ(first_values.shredded_field_presence(0).get_data(), (PaddedPODArray<UInt8> {0, 0}));
    ASSERT_TRUE(append_assembled_variant(destination, std::move(first)).ok());
    EXPECT_EQ(destination.get(), first_wrapper);

    auto second = assemble({30}, {0});
    const auto& second_values = assert_cast<const ColumnVariantV2&>(second->get_nested_column());
    ASSERT_TRUE(second_values.is_shredded());
    ASSERT_EQ(second_values.shredded_field_count(), 1);
    EXPECT_EQ(second_values.shredded_field_path(0).get_path(), "a");
    EXPECT_EQ(second_values.shredded_field_presence(0).get_data(), (PaddedPODArray<UInt8> {1}));
    EXPECT_EQ(ColumnVariantV2::TestAccess::encoded_range_materializations(second_values), 0);
    ASSERT_TRUE(append_assembled_variant(destination, std::move(second)).ok());

    const auto& nullable = assert_cast<const ColumnNullable&>(*destination);
    const auto& values = assert_cast<const ColumnVariantV2&>(nullable.get_nested_column());
    ASSERT_TRUE(values.is_shredded());
    ASSERT_EQ(values.shredded_field_count(), 1);
    EXPECT_EQ(values.shredded_field_path(0).get_path(), "a");
    EXPECT_EQ(values.shredded_field_presence(0).get_data(), (PaddedPODArray<UInt8> {0, 0, 1}));
    EXPECT_EQ(ColumnVariantV2::TestAccess::encoded_range_materializations(values), 0);
    EXPECT_EQ(json_at(values, 0), "{}");
    EXPECT_EQ(json_at(values, 1), "{}");
    EXPECT_EQ(json_at(values, 2), R"({"a":30})");
}

TEST(VariantV2ColumnReaderAdapterTest, AlternatingOwnedSourceLayoutsKeepFirstBlockLayout) {
    const auto create_assembler = [](std::string path) {
        VariantAssemblerOptions options;
        options.materialized_paths = {
                {.path = PathInData(std::move(path)), .type = std::make_shared<DataTypeInt32>()},
        };
        auto result = VariantAssembler::create(std::move(options));
        EXPECT_TRUE(result.has_value()) << result.error();
        return std::move(result).value();
    };
    auto assembler_a = create_assembler("a");
    auto assembler_b = create_assembler("b");

    const auto assemble_owned = [](VariantAssembler& assembler, Int32 value,
                                   const IColumn** transferred_payload) {
        auto native = ColumnInt32::create();
        native->insert_value(value);
        *transferred_payload = native.get();
        DorisVector<MutableColumnPtr> owners;
        owners.push_back(std::move(native));
        VariantAssemblerBatch batch;
        batch.num_rows = 1;
        batch.owned_materialized_columns = owners;
        ColumnNullable::MutablePtr output;
        const Status status = assembler.assemble(batch, &output);
        EXPECT_TRUE(status.ok()) << status;
        EXPECT_EQ(owners[0].get(), nullptr);
        return output;
    };

    MutableColumnPtr destination = nullable_variant_destination();
    const IColumn* first_payload = nullptr;
    auto first = assemble_owned(*assembler_a, 10, &first_payload);
    const auto& first_child = assert_cast<const ColumnVariantV2&>(first->get_nested_column())
                                      .shredded_field_values(0);
    EXPECT_EQ(&assert_cast<const ColumnNullable&>(first_child.typed_column()).get_nested_column(),
              first_payload);
    ASSERT_TRUE(append_assembled_variant(destination, std::move(first)).ok());

    const IColumn* second_payload = nullptr;
    auto second = assemble_owned(*assembler_b, 20, &second_payload);
    ASSERT_TRUE(append_assembled_variant(destination, std::move(second)).ok());

    const IColumn* third_payload = nullptr;
    auto third = assemble_owned(*assembler_a, 30, &third_payload);
    ASSERT_TRUE(append_assembled_variant(destination, std::move(third)).ok());

    const IColumn* fourth_payload = nullptr;
    auto fourth = assemble_owned(*assembler_b, 40, &fourth_payload);
    ASSERT_TRUE(append_assembled_variant(destination, std::move(fourth)).ok());

    const auto& nullable = assert_cast<const ColumnNullable&>(*destination);
    const auto& values = assert_cast<const ColumnVariantV2&>(nullable.get_nested_column());
    ASSERT_TRUE(values.is_shredded());
    ASSERT_EQ(values.shredded_field_count(), 1);
    EXPECT_EQ(values.shredded_field_path(0).get_path(), "a");
    EXPECT_EQ(values.shredded_field_presence(0).get_data(), (PaddedPODArray<UInt8> {1, 0, 1, 0}));
    EXPECT_TRUE(values.shredded_field_values(0).is_typed());
    EXPECT_EQ(ColumnVariantV2::TestAccess::shredded_conflict_slow_rows(values), 2);
    EXPECT_EQ(json_at(values, 0), R"({"a":10})");
    EXPECT_EQ(json_at(values, 1), R"({"b":20})");
    EXPECT_EQ(json_at(values, 2), R"({"a":30})");
    EXPECT_EQ(json_at(values, 3), R"({"b":40})");
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
